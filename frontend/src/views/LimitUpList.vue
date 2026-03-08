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
        <el-table-column prop="amount" label="成交额(万)" width="100" align="right">
          <template #default="{ row }">
            {{ row.amount ? row.amount.toFixed(0) : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="reason_category" label="题材" width="100" />
        <el-table-column prop="limit_up_reason" label="涨停原因" min-width="180" show-overflow-tooltip />
        <el-table-column label="操作" width="90" fixed="right" align="center">
          <template #default="{ row }">
            <el-button link type="primary" @click.stop="addToWatch(row)">
              <el-icon><Star /></el-icon>
            </el-button>
            <el-button link type="primary" @click.stop="goToDetail(row.stock_code)">
              <el-icon><View /></el-icon>
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Star, View } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { getRealtimeLimitUp, refreshLimitUpData } from '@/api/limit-up'
import { useConfigStore } from '@/stores/config'
import type { LimitUpRealtime } from '@/types/limit-up'

const router = useRouter()
const configStore = useConfigStore()

const loading = ref(false)
const refreshing = ref(false)
const tableData = ref<LimitUpRealtime[]>([])
const reasonCategories = ref<string[]>([])

const filters = reactive({
  minContinuousDays: undefined as number | undefined,
  reasonCategory: '',
  status: ''
})

// 所有数据（筛选前）
let allRecords: LimitUpRealtime[] = []

// 获取数据
async function fetchData() {
  loading.value = true
  try {
    const response = await getRealtimeLimitUp()
    
    // 从响应中提取数据数组
    allRecords = response.data || []
    
    // 提取实际存在的分类，用于筛选下拉
    const cats = new Set<string>()
    allRecords.forEach(item => {
      if (item.reason_category) cats.add(item.reason_category)
    })
    reasonCategories.value = [...cats].sort()
    
    // 应用本地筛选
    applyFilters()
  } catch (e) {
    console.error('Fetch error:', e)
    ElMessage.error('获取数据失败')
  } finally {
    loading.value = false
  }
}

// 本地筛选
function applyFilters() {
  let filtered = allRecords
  
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
  
  tableData.value = filtered
}

// 重置筛选
function resetFilters() {
  filters.minContinuousDays = undefined
  filters.reasonCategory = ''
  filters.status = ''
  applyFilters()
}

// 刷新数据（从开盘啦/同花顺重新获取涨停原因和状态）
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
    
    // 3秒后自动刷新数据
    setTimeout(() => {
      fetchData()
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

// 排序
function handleSortChange({ prop, order }: any) {
  if (!order) {
    // 取消排序，恢复筛选后的默认顺序
    applyFilters()
    return
  }
  
  const sortOrder = order === 'ascending' ? 1 : -1
  tableData.value.sort((a: any, b: any) => {
    const va = a[prop]
    const vb = b[prop]
    
    // 空值排到最后
    if (va == null && vb == null) return 0
    if (va == null) return 1
    if (vb == null) return -1
    
    // 字符串比较（时间等）
    if (typeof va === 'string' || typeof vb === 'string') {
      return String(va).localeCompare(String(vb)) * sortOrder
    }
    
    // 数值比较
    return (va - vb) * sortOrder
  })
}

// 行点击
function handleRowClick(row: LimitUpRealtime) {
  router.push(`/stock/${row.stock_code}`)
}

// 跳转详情
function goToDetail(code: string) {
  router.push(`/stock/${code}`)
}

// 添加自选
function addToWatch(row: LimitUpRealtime) {
  configStore.addToWatchList(row.stock_code)
  ElMessage.success(`已添加 ${row.stock_name} 到自选`)
}

// 格式化换手率（数据已是百分比格式，如1.71表示1.71%）
function formatTurnoverRate(rate: number | undefined | null): string {
  if (rate == null || rate === 0) return '-'
  return rate.toFixed(2) + '%'
}

// 获取状态类型
function getStatusType(row: LimitUpRealtime): string {
  const status = row.current_status || (row.is_sealed ? 'sealed' : 'opened')
  switch (status) {
    case 'sealed':
    case 'final_sealed':
      return 'danger'
    case 'opened':
      return 'warning'
    case 'broken':
      return 'info'
    default:
      return row.is_sealed ? 'danger' : 'warning'
  }
}

// 获取状态文案
function getStatusText(row: LimitUpRealtime): string {
  const status = row.current_status || (row.is_sealed ? 'sealed' : 'opened')
  switch (status) {
    case 'sealed':
      return '封板中'
    case 'final_sealed':
      return '封板至收盘'
    case 'opened':
      return row.open_count > 0 ? `开板${row.open_count}次` : '开板'
    case 'broken':
      return '已炸板'
    default:
      return row.is_sealed ? '封板' : '开板'
  }
}

// 导出数据
function exportData() {
  // 简单的CSV导出
  const headers = ['代码', '名称', '连板', '首封时间', '状态', '封单(万)', '涨停原因']
  const rows = tableData.value.map(item => [
    item.stock_code,
    item.stock_name,
    item.continuous_limit_up_days,
    item.first_limit_up_time || '',
    item.is_sealed ? '封板' : '开板',
    item.seal_amount?.toFixed(0) || '',
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
  fetchData()
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
