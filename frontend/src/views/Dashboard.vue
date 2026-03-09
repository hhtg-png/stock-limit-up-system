<template>
  <div class="limit-up-list">
    <!-- 筛选区 -->
    <div class="filter-bar card">
      <el-form inline>
        <el-form-item label="日期">
          <el-date-picker
            v-model="filters.tradeDate"
            type="date"
            placeholder="选择日期"
            format="MM-DD"
            value-format="YYYY-MM-DD"
            :disabled-date="disabledDate"
            style="width: 120px"
            :clearable="false"
          />
        </el-form-item>
        <el-form-item label="连板">
          <el-select v-model="filters.continuousDays" placeholder="全部" clearable style="width: 100px">
            <el-option label="首板" :value="1" />
            <el-option label="2板" :value="2" />
            <el-option label="3板" :value="3" />
            <el-option label="4板" :value="4" />
            <el-option label="5板+" :value="5" />
          </el-select>
        </el-form-item>
        <el-form-item label="状态">
          <el-select v-model="filters.status" placeholder="全部" clearable style="width: 100px">
            <el-option label="封板" value="sealed" />
            <el-option label="开板" value="opened" />
          </el-select>
        </el-form-item>
        <el-form-item label="流通盘">
          <el-select v-model="filters.maxFreeFloat" placeholder="全部" clearable style="width: 100px">
            <el-option label="50亿以下" :value="50" />
            <el-option label="100亿以下" :value="100" />
            <el-option label="200亿以下" :value="200" />
            <el-option label="500亿以下" :value="500" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button @click="resetFilters">重置</el-button>
          <el-button type="warning" @click="refreshData" :loading="refreshing">刷新数据</el-button>
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
        ref="tableRef"
        :data="tableData" 
        v-loading="loading"
        stripe
        height="calc(100vh - 260px)"
        @row-click="handleRowClick"
        :header-cell-style="{ background: '#fafafa', fontWeight: 500, cursor: 'move' }"
      >
        <el-table-column 
          v-for="col in visibleColumns" 
          :key="col.prop" 
          v-bind="col"
        >
          <template v-if="col.slot" #default="{ row }">
            <!-- 连板 -->
            <template v-if="col.prop === 'continuous_limit_up_days'">
              <el-tag v-if="row.continuous_limit_up_days > 1" type="info" size="small">
                {{ row.continuous_limit_up_days }}板
              </el-tag>
              <span v-else>首板</span>
            </template>
            <!-- 状态 -->
            <template v-else-if="col.prop === 'status'">
              <el-tag :type="row.is_sealed ? 'info' : 'warning'" size="small">
                {{ row.is_sealed ? '封板' : '炸板' }}
              </el-tag>
            </template>
            <!-- 开板 -->
            <template v-else-if="col.prop === 'open_count'">
              <span v-if="row.open_count > 0" class="open-count">{{ row.open_count }}次</span>
              <span v-else>-</span>
            </template>
            <!-- 回封时间 -->
            <template v-else-if="col.prop === 'final_seal_time'">
              <span v-if="row.final_seal_time">{{ row.final_seal_time }}</span>
              <span v-else>-</span>
            </template>
            <!-- 涨停价 -->
            <template v-else-if="col.prop === 'limit_up_price'">
              {{ row.limit_up_price?.toFixed(2) }}
            </template>
            <!-- 封单 -->
            <template v-else-if="col.prop === 'seal_amount'">
              {{ row.seal_amount ? (row.seal_amount / 10000).toFixed(2) : '-' }}
            </template>
            <!-- 换手率 -->
            <template v-else-if="col.prop === 'turnover_rate'">
              {{ formatTurnoverRate(row.turnover_rate) }}
            </template>
            <!-- 成交额 -->
            <template v-else-if="col.prop === 'amount'">
              {{ row.amount ? (row.amount / 10000).toFixed(2) : '-' }}
            </template>
            <!-- 自由流通市值 -->
            <template v-else-if="col.prop === 'free_float_value'">
              {{ row.free_float_value ? (row.free_float_value / 10000).toFixed(2) : '-' }}
            </template>

          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted, onUnmounted, computed, nextTick, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Star, View } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { getRealtimeLimitUp, refreshLimitUpData, getTableColumns, saveTableColumns } from '@/api/limit-up'
import { useConfigStore } from '@/stores/config'
import { useSpeech } from '@/composables/useSpeech'
import { useWebSocket } from '@/composables/useWebSocket'
import type { LimitUpRealtime } from '@/types/limit-up'
import Sortable from 'sortablejs'

const router = useRouter()
const configStore = useConfigStore()
const { announceNewStocks } = useSpeech()
const { isConnected, onLimitUpUpdate, offLimitUpUpdate } = useWebSocket()

const loading = ref(false)
const refreshing = ref(false)
const tableData = ref<LimitUpRealtime[]>([])
const reasonCategories = ref<string[]>([])
const sortBy = ref<'time' | 'reseal_time' | 'seal_amount' | 'continuous_days'>('time')
const tableRef = ref<any>(null)
let refreshTimer: number | null = null
let sortableInstance: Sortable | null = null

// 列配置定义
interface ColumnConfig {
  prop: string
  label: string
  width?: number | string
  minWidth?: number | string
  fixed?: string | boolean
  align?: string
  slot?: boolean
  showOverflowTooltip?: boolean
}

// 默认列配置（已删除题材列）
const defaultColumns: ColumnConfig[] = [
  { prop: 'stock_code', label: '代码', width: 95, fixed: true },
  { prop: 'stock_name', label: '名称', width: 90, fixed: true },
  { prop: 'continuous_limit_up_days', label: '连板', width: 75, align: 'center', slot: true },
  { prop: 'first_limit_up_time', label: '首封时间', width: 95 },
  { prop: 'status', label: '状态', width: 80, align: 'center', slot: true },
  { prop: 'open_count', label: '开板', width: 65, align: 'center', slot: true },
  { prop: 'final_seal_time', label: '回封时间', width: 90, align: 'center', slot: true },
  { prop: 'limit_up_price', label: '涨停价', width: 85, align: 'right', slot: true },
  { prop: 'seal_amount', label: '封单(亿)', width: 95, align: 'right', slot: true },
  { prop: 'turnover_rate', label: '换手率', width: 80, align: 'right', slot: true },
    { prop: 'amount', label: '成交额(亿)', width: 100, align: 'right', slot: true },
    { prop: 'free_float_value', label: '流通盘(亿)', width: 100, align: 'right', slot: true },
  { prop: 'limit_up_reason', label: '涨停原因', minWidth: 180, showOverflowTooltip: true }
]

// 当前列顺序
const columnOrder = ref<string[]>([])

// 计算可见列（按顺序排列）
const visibleColumns = computed(() => {
  if (columnOrder.value.length === 0) {
    return defaultColumns
  }
  // 按保存的顺序排列
  const orderedCols: ColumnConfig[] = []
  for (const prop of columnOrder.value) {
    const col = defaultColumns.find(c => c.prop === prop)
    if (col) orderedCols.push(col)
  }
  // 添加新增的列（如果有）
  for (const col of defaultColumns) {
    if (!columnOrder.value.includes(col.prop)) {
      orderedCols.push(col)
    }
  }
  return orderedCols
})

const filters = reactive({
  tradeDate: new Date().toISOString().slice(0, 10), // 默认今天
  continuousDays: undefined as number | undefined,
  reasonCategory: '',
  status: '',
  maxFreeFloat: undefined as number | undefined  // 流通盘上限(亿)
})

// 禁用未来日期
function disabledDate(date: Date) {
  return date > new Date()
}

// 筛选条件变化时自动触发查询
watch(
  () => filters.tradeDate,
  (newDate) => {
    // 切换日期时重新获取数据（不播报）
    fetchData(true)
  }
)

// 本地筛选条件变化时应用筛选
watch(
  () => [filters.continuousDays, filters.status, filters.maxFreeFloat],
  () => {
    applyFilters()
  }
)

// 所有数据（筛选前）
let allRecords: LimitUpRealtime[] = []

// 初始化拖拽排序
async function initSortable() {
  await nextTick()
  if (!tableRef.value) return
  
  const el = tableRef.value.$el?.querySelector('.el-table__header-wrapper tr')
  if (!el) return
  
  if (sortableInstance) {
    sortableInstance.destroy()
  }
  
  sortableInstance = Sortable.create(el, {
    animation: 150,
    delay: 0,
    onEnd: async (evt: any) => {
      const { oldIndex, newIndex } = evt
      if (oldIndex === newIndex) return
      
      // 更新列顺序
      const newOrder = [...visibleColumns.value.map(c => c.prop)]
      const [removed] = newOrder.splice(oldIndex, 1)
      newOrder.splice(newIndex, 0, removed)
      columnOrder.value = newOrder
      
      // 保存到后台
      try {
        await saveTableColumns(newOrder)
        ElMessage.success('列顺序已保存')
      } catch (e) {
        console.error('Save column order failed:', e)
      }
    }
  })
}

// 加载列顺序
async function loadColumnOrder() {
  try {
    const saved = await getTableColumns()
    if (saved && saved.length > 0) {
      columnOrder.value = saved
    }
  } catch (e) {
    console.error('Load column order failed:', e)
  }
}

// 获取数据（HTTP）
// skipAnnounce: 是否跳过播报（切换日期时为 true）
async function fetchData(skipAnnounce = false) {
  loading.value = true
  try {
    const response = await getRealtimeLimitUp({
      trade_date: filters.tradeDate
    })
    
    // 从响应中提取数据数组
    const newRecords = response.data || []
    
    // 只有不跳过播报时才检测新涨停
    if (!skipAnnounce) {
      detectAndAnnounce(newRecords)
    }
    
    allRecords = newRecords
    updateCategories()
    applyFilters()
  } catch (e) {
    console.error('Fetch error:', e)
    ElMessage.error('获取数据失败')
  } finally {
    loading.value = false
  }
}

// WebSocket 推送的数据更新处理
function handleWsUpdate(rawData: any[]) {
  if (!rawData || rawData.length === 0) return
  
  // 转换数据格式
  const newRecords: LimitUpRealtime[] = rawData.map(item => ({
    stock_code: item.stock_code || '',
    stock_name: item.stock_name || '',
    trade_date: item.trade_date || '',
    first_limit_up_time: item.first_limit_up_time,
    final_seal_time: item.final_seal_time,
    limit_up_reason: item.limit_up_reason || '',
    reason_category: item.reason_category || '其他',
    continuous_limit_up_days: item.continuous_limit_up_days || 1,
    open_count: item.open_count || 0,
    is_sealed: item.is_final_sealed ?? true,
    current_status: item.is_final_sealed ? 'sealed' : 'opened',
    seal_amount: item.seal_amount || 0,
    seal_volume: item.seal_volume,
    limit_up_price: item.limit_up_price || 0,
    current_price: item.current_price || item.limit_up_price || 0,
    turnover_rate: item.turnover_rate || 0,
    amount: item.amount || 0,
    market: item.market || 'SZ',
    industry: item.industry
  }))
  
  // 检测新涨停并播报
  detectAndAnnounce(newRecords)
  
  allRecords = newRecords
  updateCategories()
  applyFilters()
  
  console.log(`WebSocket 更新涨停列表: ${newRecords.length} 条`)
}

// 检测新涨停并播报
function detectAndAnnounce(newRecords: LimitUpRealtime[]) {
  if (allRecords.length > 0 && configStore.config.alert_limit_up_enabled) {
    const oldCodes = new Set(allRecords.map(r => r.stock_code))
    const newStocks = newRecords.filter(r => !oldCodes.has(r.stock_code))
    if (newStocks.length > 0) {
      announceNewStocks(newStocks.map(s => ({ stock_name: s.stock_name, limit_up_reason: s.limit_up_reason })))
    }
  }
}

// 更新分类列表
function updateCategories() {
  const cats = new Set<string>()
  allRecords.forEach(item => {
    if (item.reason_category) cats.add(item.reason_category)
  })
  reasonCategories.value = [...cats].sort()
}

// 本地筛选
function applyFilters() {
  let filtered = allRecords
  
  if (filters.continuousDays) {
    if (filters.continuousDays === 5) {
      // 5板+：>=5
      filtered = filtered.filter(item => item.continuous_limit_up_days >= 5)
    } else {
      // 首板/2板/3板/4板：精确匹配
      filtered = filtered.filter(item => item.continuous_limit_up_days === filters.continuousDays)
    }
  }
  if (filters.reasonCategory) {
    filtered = filtered.filter(item => item.reason_category === filters.reasonCategory)
  }
  if (filters.status === 'sealed') {
    filtered = filtered.filter(item => item.is_sealed)
  } else if (filters.status === 'opened') {
    filtered = filtered.filter(item => !item.is_sealed)
  }
  // 流通盘筛选（free_float_value 是万元，转换成亿比较）
  if (filters.maxFreeFloat) {
    const maxInWan = filters.maxFreeFloat * 10000  // 亿转万
    filtered = filtered.filter(item => item.free_float_value && item.free_float_value <= maxInWan)
  }
  
  // 应用排序
  let sorted = [...filtered]
  switch (sortBy.value) {
    case 'time':
      sorted.sort((a, b) => (a.first_limit_up_time || '').localeCompare(b.first_limit_up_time || ''))
      break
    case 'reseal_time':
      sorted.sort((a, b) => {
        const aTime = a.final_seal_time || ''
        const bTime = b.final_seal_time || ''
        if (!aTime && !bTime) return 0
        if (!aTime) return 1
        if (!bTime) return -1
        return aTime.localeCompare(bTime)
      })
      break
    case 'seal_amount':
      sorted.sort((a, b) => (b.seal_amount || 0) - (a.seal_amount || 0))
      break
    case 'continuous_days':
      sorted.sort((a, b) => (b.continuous_limit_up_days || 0) - (a.continuous_limit_up_days || 0))
      break
  }
  
  tableData.value = sorted
}

// 设置排序方式
function setSortBy(type: 'time' | 'reseal_time' | 'seal_amount' | 'continuous_days') {
  sortBy.value = type
  applyFilters()
}

// 重置筛选
function resetFilters() {
  const today = new Date().toISOString().slice(0, 10)
  const dateChanged = filters.tradeDate !== today
  
  filters.tradeDate = today
  filters.continuousDays = undefined
  filters.status = ''
  filters.maxFreeFloat = undefined
  
  if (dateChanged) {
    // 日期变了，重新获取数据
    fetchData(true)
  } else {
    // 日期没变，只应用筛选
    applyFilters()
  }
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

// 启动轮询（作为 WebSocket 断连时的备用）
function startPolling() {
  if (refreshTimer) return
  refreshTimer = window.setInterval(() => {
    const now = new Date()
    const hour = now.getHours()
    const minute = now.getMinutes()
    // 交易时段: 9:30-11:30, 13:00-15:00
    const isTradingTime = 
      (hour === 9 && minute >= 30) || 
      (hour === 10) || 
      (hour === 11 && minute <= 30) ||
      (hour >= 13 && hour < 15)
    if (isTradingTime && now.getDay() >= 1 && now.getDay() <= 5) {
      fetchData()
    }
  }, 30000)  // 30秒
}

// 停止轮询
function stopPolling() {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

onMounted(async () => {
  await loadColumnOrder()
  // 首次加载数据
  fetchData()
  await nextTick()
  initSortable()
  
  // 订阅 WebSocket 涨停列表更新
  onLimitUpUpdate(handleWsUpdate)
  
  // 根据 WebSocket 连接状态决定是否启用轮询
  watch(isConnected, (connected) => {
    if (connected) {
      // WebSocket 已连接，停止轮询
      stopPolling()
    } else {
      // WebSocket 断连，启用轮询作为备用
      startPolling()
    }
  }, { immediate: true })
})

onUnmounted(() => {
  // 取消 WebSocket 订阅
  offLimitUpUpdate(handleWsUpdate)
  stopPolling()
  if (sortableInstance) {
    sortableInstance.destroy()
    sortableInstance = null
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
