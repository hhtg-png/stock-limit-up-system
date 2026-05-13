<template>
  <div class="limit-up-list">
    <!-- 筛选区 -->
    <div class="filter-bar card">
      <el-form inline>
        <el-form-item label="交易日期">
          <el-date-picker
            v-model="selectedDate"
            type="date"
            value-format="YYYY-MM-DD"
            format="YYYY-MM-DD"
            :clearable="false"
            style="width: 150px"
            @change="handleDateChange"
          />
        </el-form-item>
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
        <el-form-item v-if="displayTradeDate">
          <el-tag :type="isFallbackDate ? 'warning' : 'info'" size="small">
            数据日期 {{ displayTradeDate }}
          </el-tag>
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
        @sort-change="handleSortChange"
        :header-cell-style="{ background: '#fafafa', fontWeight: 500 }"
      >
        <el-table-column prop="stock_code" label="代码" width="95" fixed />
        <el-table-column prop="stock_name" label="名称" width="90" fixed />
        <el-table-column prop="continuous_limit_up_days" label="连板" width="75" sortable="custom" align="center">
          <template #default="{ row }">
            <el-tag 
              v-if="row.continuous_limit_up_days > 1" 
              type="info" 
              size="small"
            >{{ row.continuous_limit_up_days }}板</el-tag>
            <span v-else>首板</span>
          </template>
        </el-table-column>
        <el-table-column prop="first_limit_up_time" label="首封时间" width="95" sortable="custom" />
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
        <el-table-column prop="seal_amount" label="封单(万)" width="95" sortable="custom" align="right">
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
import { ElMessage } from 'element-plus'
import dayjs from 'dayjs'
import { getRealtimeLimitUp } from '@/api/limit-up'
import { useLimitUpStore } from '@/stores/limit-up'
import type { LimitUpRealtime } from '@/types/limit-up'

const router = useRouter()
const limitUpStore = useLimitUpStore()

const loading = ref(false)
const refreshing = ref(false)
const isFetching = ref(false)
const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const tableSort = ref<{ prop: string; order: 'ascending' | 'descending' | null }>({
  prop: '',
  order: null
})
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

const displayTradeDate = computed(() => limitUpStore.tradeDate)
const isSelectedToday = computed(() => selectedDate.value === dayjs().format('YYYY-MM-DD'))
const isFallbackDate = computed(
  () => Boolean(displayTradeDate.value && displayTradeDate.value !== selectedDate.value)
)

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

  const { prop, order } = tableSort.value
  if (!prop || !order) {
    return filtered
  }

  const sortOrder = order === 'ascending' ? 1 : -1
  return [...filtered].sort((a: any, b: any) => {
    const va = a[prop]
    const vb = b[prop]

    if (va == null && vb == null) return 0
    if (va == null) return 1
    if (vb == null) return -1

    if (typeof va === 'string' || typeof vb === 'string') {
      return String(va).localeCompare(String(vb)) * sortOrder
    }

    return (va - vb) * sortOrder
  })
})

async function fetchData(options: FetchOptions = {}) {
  const { showLoading = limitUpStore.realtimeList.length === 0, silent = false } = options
  if (isFetching.value) return

  isFetching.value = true
  if (showLoading) {
    loading.value = true
  }

  try {
    const response = await getRealtimeLimitUp({
      trade_date: selectedDate.value
    })
    limitUpStore.setSnapshot(response.trade_date, response.data || [])
    if (response.is_fallback && !silent) {
      ElMessage.warning(`未找到 ${selectedDate.value} 数据，已显示 ${response.trade_date}`)
    }
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

function updateRealtimeMode() {
  limitUpStore.setAcceptRealtimeUpdates(isSelectedToday.value)
}

function handleDateChange() {
  updateRealtimeMode()
  fetchData({ showLoading: true })
}

function resetFilters() {
  filters.minContinuousDays = undefined
  filters.reasonCategory = ''
  filters.status = ''
}

async function refreshData() {
  try {
    refreshing.value = true
    await fetchData({ showLoading: false, silent: false })
    ElMessage.success('数据已刷新')
  } catch (e: any) {
    console.error('Refresh error:', e)
    ElMessage.error('刷新失败')
  } finally {
    refreshing.value = false
  }
}

function handleSortChange({ prop, order }: { prop?: string; order?: 'ascending' | 'descending' | null }) {
  tableSort.value = {
    prop: prop || '',
    order: order || null
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
  updateRealtimeMode()
  fetchData({ showLoading: limitUpStore.realtimeList.length === 0 })
  refreshTimer = window.setInterval(() => {
    if (document.visibilityState !== 'visible') return
    if (!isSelectedToday.value) return
    fetchData({ showLoading: false, silent: true })
  }, RESYNC_INTERVAL)
})

onUnmounted(() => {
  limitUpStore.setAcceptRealtimeUpdates(true)
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
