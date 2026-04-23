<template>
  <div class="limit-up-list">
    <!-- 筛选区 -->
    <div class="filter-bar card">
      <el-form inline>
        <el-form-item label="连板天数">
          <el-select v-model="filters.minContinuousDays" placeholder="全部" clearable style="width: 120px">
            <el-option label="2板以上" :value="2" />
            <el-option label="3板以上" :value="3" />
            <el-option label="4板以上" :value="4" />
            <el-option label="5板以上" :value="5" />
          </el-select>
        </el-form-item>
        <el-form-item label="涨停原因">
          <el-select v-model="filters.reasonCategory" placeholder="全部" clearable style="width: 140px">
            <el-option v-for="r in reasonCategories" :key="r" :label="r" :value="r" />
          </el-select>
        </el-form-item>
        <el-form-item label="状态">
          <el-select v-model="filters.status" placeholder="全部" clearable style="width: 100px">
            <el-option label="封板" value="sealed" />
            <el-option label="开板" value="opened" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="applyFilters">查询</el-button>
          <el-button @click="resetFilters">重置</el-button>
          <el-button type="warning" @click="refreshData" :loading="refreshing">刷新数据</el-button>
          <el-button @click="exportData">导出</el-button>
        </el-form-item>
        <el-form-item label="排序">
          <el-button-group size="small">
            <el-button :type="sortBy === 'time' ? 'primary' : 'default'" @click="setSortBy('time')">首封</el-button>
            <el-button :type="sortBy === 'reseal_time' ? 'primary' : 'default'" @click="setSortBy('reseal_time')">回封</el-button>
            <el-button :type="sortBy === 'seal_amount' ? 'primary' : 'default'" @click="setSortBy('seal_amount')">封单</el-button>
            <el-button :type="sortBy === 'continuous_days' ? 'primary' : 'default'" @click="setSortBy('continuous_days')">连板</el-button>
          </el-button-group>
        </el-form-item>
      </el-form>
    </div>

    <!-- 数据表格 -->
    <div class="data-table card">
      <el-table 
        :data="tableData" 
        v-loading="loading"
        stripe
        height="calc(100vh - 260px)"
        @row-click="handleRowClick"
        :header-cell-style="{ background: '#fafafa', fontWeight: 500 }"
      >
        <el-table-column prop="stock_code" label="代码" width="95" fixed />
        <el-table-column prop="stock_name" label="名称" width="90" fixed />
        <el-table-column prop="continuous_limit_up_days" label="连板" width="75" align="center">
          <template #default="{ row }">
            <el-tag 
              v-if="row.continuous_limit_up_days > 1" 
              type="info" 
              size="small"
            >{{ row.continuous_limit_up_days }}板</el-tag>
            <span v-else>首板</span>
          </template>
        </el-table-column>
        <el-table-column prop="first_limit_up_time" label="首封时间" width="95" />
        <el-table-column label="状态" width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.is_sealed ? 'info' : 'warning'" size="small">
              {{ row.is_sealed ? '封板' : '炸板' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="open_count" label="开板" width="65" align="center">
          <template #default="{ row }">
            <span v-if="row.open_count > 0" class="open-count">{{ row.open_count }}次</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="回封时间" width="90" align="center">
          <template #default="{ row }">
            <span v-if="row.final_seal_time">{{ row.final_seal_time }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="limit_up_price" label="涨停价" width="85" align="right">
          <template #default="{ row }">
            {{ row.limit_up_price?.toFixed(2) }}
          </template>
        </el-table-column>
        <el-table-column prop="seal_amount" label="封单(万)" width="95" align="right">
          <template #default="{ row }">
            {{ row.seal_amount ? row.seal_amount.toFixed(0) : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="turnover_rate" label="换手率" width="80" align="right">
          <template #default="{ row }">
            {{ formatTurnoverRate(row.turnover_rate) }}
          </template>
        </el-table-column>
        <el-table-column prop="amount" label="成交额(亿)" width="100" align="right">
          <template #default="{ row }">
            {{ formatAmountInYi(row.amount) }}
          </template>
        </el-table-column>
        <el-table-column prop="tradable_market_value" label="实际流通值(亿)" width="120" align="right">
          <template #default="{ row }">
            {{ formatTradableMarketValue(row.tradable_market_value) }}
          </template>
        </el-table-column>
        <el-table-column prop="reason_category" label="题材" width="100" />
        <el-table-column prop="limit_up_reason" label="涨停原因" min-width="180" show-overflow-tooltip />
      </el-table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, reactive, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { getRealtimeLimitUp, refreshLimitUpData } from '@/api/limit-up'
import { useLimitUpStore } from '@/stores/limit-up'
import type { LimitUpRealtime } from '@/types/limit-up'

const router = useRouter()
const limitUpStore = useLimitUpStore()

const loading = ref(false)
const refreshing = ref(false)
const isFetching = ref(false)
const sortBy = ref<'time' | 'reseal_time' | 'seal_amount' | 'continuous_days'>('time')
const RESYNC_INTERVAL = 90000
let refreshTimer: number | null = null

const filters = reactive({
  minContinuousDays: undefined as number | undefined,
  reasonCategory: '',
  status: ''
})

interface FetchOptions {
  showLoading?: boolean
  silent?: boolean
}

const reasonCategories = computed(() => {
  const cats = new Set<string>()
  limitUpStore.realtimeList.forEach(item => {
    if (item.reason_category) {
      cats.add(item.reason_category)
    }
  })
  return [...cats].sort()
})

const tableData = computed(() => {
  let filtered = [...limitUpStore.realtimeList]

  if (filters.minContinuousDays) {
    filtered = filtered.filter(item => item.continuous_limit_up_days >= filters.minContinuousDays!)
  }
  if (filters.reasonCategory) {
    filtered = filtered.filter(item => item.reason_category === filters.reasonCategory)
  }
  if (filters.status === 'sealed') {
    filtered = filtered.filter(item => item.is_sealed)
  } else if (filters.status === 'opened') {
    filtered = filtered.filter(item => !item.is_sealed)
  }

  switch (sortBy.value) {
    case 'time':
      filtered.sort((a, b) => (a.first_limit_up_time || '').localeCompare(b.first_limit_up_time || ''))
      break
    case 'reseal_time':
      filtered.sort((a, b) => {
        const aTime = a.final_seal_time || ''
        const bTime = b.final_seal_time || ''
        if (!aTime && !bTime) return 0
        if (!aTime) return 1
        if (!bTime) return -1
        return aTime.localeCompare(bTime)
      })
      break
    case 'seal_amount':
      filtered.sort((a, b) => (b.seal_amount || 0) - (a.seal_amount || 0))
      break
    case 'continuous_days':
      filtered.sort((a, b) => (b.continuous_limit_up_days || 0) - (a.continuous_limit_up_days || 0))
      break
  }

  return filtered
})

async function fetchData(options: FetchOptions = {}) {
  const { showLoading = limitUpStore.realtimeList.length === 0, silent = false } = options
  if (isFetching.value) return

  isFetching.value = true
  if (showLoading) {
    loading.value = true
  }

  try {
    const response = await getRealtimeLimitUp()
    limitUpStore.setSnapshot(response.trade_date, response.data || [])
  } catch (e) {
    console.error('Fetch error:', e)
    if (!silent) {
      ElMessage.error('获取数据失败')
    }
  } finally {
    isFetching.value = false
    if (showLoading) {
      loading.value = false
    }
  }
}

function applyFilters() {
  // 列表已由计算属性自动响应筛选条件变化
}

function setSortBy(type: 'time' | 'reseal_time' | 'seal_amount' | 'continuous_days') {
  sortBy.value = type
}

function resetFilters() {
  filters.minContinuousDays = undefined
  filters.reasonCategory = ''
  filters.status = ''
}

async function refreshData() {
  try {
    await ElMessageBox.confirm(
      '将从开盘啦/同花顺重新获取涨停原因和状态，是否继续？',
      '刷新数据',
      { type: 'warning' }
    )

    refreshing.value = true
    await refreshLimitUpData()
    ElMessage.success('刷新请求已提交，请稍后刷新页面查看')

    setTimeout(() => {
      fetchData({ showLoading: false, silent: true })
    }, 3000)
  } catch (e: any) {
    if (e !== 'cancel') {
      console.error('Refresh error:', e)
      ElMessage.error('刷新失败')
    }
  } finally {
    refreshing.value = false
  }
}

function handleRowClick(row: LimitUpRealtime) {
  router.push(`/stock/${row.stock_code}`)
}

function formatTurnoverRate(rate: number | undefined | null): string {
  if (rate == null || rate === 0) return '-'
  return rate.toFixed(2) + '%'
}

function formatAmountInYi(value: number | undefined | null): string {
  if (value == null || value === 0) return '-'
  return (value / 10000).toFixed(2)
}

function formatTradableMarketValue(value: number | undefined | null): string {
  if (value == null || value === 0) return '-'
  return (value / 10000).toFixed(2)
}

function exportData() {
  const headers = ['代码', '名称', '连板', '首封时间', '状态', '封单(万)', '成交额(亿)', '实际流通值(亿)', '涨停原因']
  const rows = tableData.value.map(item => [
    item.stock_code,
    item.stock_name,
    item.continuous_limit_up_days,
    item.first_limit_up_time || '',
    item.is_sealed ? '封板' : '开板',
    item.seal_amount?.toFixed(0) || '',
    formatAmountInYi(item.amount),
    formatTradableMarketValue(item.tradable_market_value),
    item.limit_up_reason || ''
  ])

  const csv = [headers, ...rows].map(row => row.join(',')).join('\n')
  const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `涨停列表_${new Date().toLocaleDateString()}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

onMounted(() => {
  fetchData({ showLoading: limitUpStore.realtimeList.length === 0 })
  refreshTimer = window.setInterval(() => {
    if (document.visibilityState !== 'visible') return
    fetchData({ showLoading: false, silent: true })
  }, RESYNC_INTERVAL)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
})
</script>

<style lang="scss" scoped>
.limit-up-list {
  .filter-bar {
    margin-bottom: 16px;
    
    :deep(.el-form-item) {
      margin-bottom: 0;
    }
  }

  .data-table {
    :deep(.el-table) {
      cursor: pointer;
      
      .el-table__row:hover {
        background-color: #fafafa;
      }
    }
  }
}
</style>
