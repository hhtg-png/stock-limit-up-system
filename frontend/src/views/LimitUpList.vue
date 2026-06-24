<template>
  <div class="limit-up-list">
    <!-- 筛选区 -->
    <div class="filter-bar card">
      <el-form class="filter-form" inline>
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
          <el-select v-model="filters.boardScope" placeholder="全部" clearable style="width: 120px">
            <el-option label="首板" value="first" />
            <el-option label="2板以上" value="min-2" />
            <el-option label="3板以上" value="min-3" />
            <el-option label="4板以上" value="min-4" />
            <el-option label="5板以上" value="min-5" />
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
        <el-form-item label="价格" class="price-filter">
          <div class="price-filter__content">
            <div class="price-presets">
              <el-button
                v-for="range in priceRanges"
                :key="range.label"
                size="small"
                :type="filters.activePriceRange === range.label ? 'primary' : 'default'"
                @click="applyPriceRange(range)"
              >
                {{ range.label }}
              </el-button>
            </div>
            <div class="price-inputs">
              <el-input
                v-model="filters.minPrice"
                class="price-input"
                type="number"
                placeholder="最低"
                size="small"
                clearable
                @input="handleCustomPriceChange"
              />
              <span class="price-separator">-</span>
              <el-input
                v-model="filters.maxPrice"
                class="price-input"
                type="number"
                placeholder="最高"
                size="small"
                clearable
                @input="handleCustomPriceChange"
              />
            </div>
          </div>
        </el-form-item>
        <span class="filter-break" aria-hidden="true"></span>
        <el-form-item v-if="displayTradeDate" class="filter-date-tag">
          <el-tag :type="isFallbackDate ? 'warning' : 'info'" size="small">
            数据日期 {{ displayTradeDate }}
          </el-tag>
        </el-form-item>
        <el-form-item class="filter-actions">
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
        ref="tableRef"
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
            <el-tag v-if="row.is_one_word" class="one-word-tag" type="danger" size="small">
              一字
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

    <MobileLimitUpCards
      :items="tableData"
      :loading="loading"
      @select="handleRowClick"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, reactive, onMounted, onUnmounted, nextTick, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import dayjs from 'dayjs'
import { getRealtimeLimitUp } from '@/api/limit-up'
import { useLimitUpStore } from '@/stores/limit-up'
import type { LimitUpRealtime } from '@/types/limit-up'
import MobileLimitUpCards from '@/components/stock/MobileLimitUpCards.vue'

const router = useRouter()
const limitUpStore = useLimitUpStore()

const loading = ref(false)
const refreshing = ref(false)
const isFetching = ref(false)
const tableRef = ref<any>()
const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const tableSort = ref<{ prop: string; order: 'ascending' | 'descending' | null }>({
  prop: '',
  order: null
})
const RESYNC_INTERVAL = 90000
let refreshTimer: number | null = null

type BoardScope = '' | 'first' | 'min-2' | 'min-3' | 'min-4' | 'min-5'

interface PriceRange {
  label: string
  min: number
  max: number
}

const priceRanges: PriceRange[] = [
  { label: '1-20', min: 1, max: 20 },
  { label: '20-50', min: 20, max: 50 },
  { label: '50-100', min: 50, max: 100 },
  { label: '100-9999', min: 100, max: 9999 }
]

const filters = reactive({
  boardScope: '' as BoardScope,
  reasonCategory: '',
  status: '',
  minPrice: '',
  maxPrice: '',
  activePriceRange: ''
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

const tableData = computed(() => limitUpStore.realtimeList)

const boardFilter = computed(() => {
  const scope = filters.boardScope || ''
  if (scope === 'first') {
    return { continuousDays: undefined, continuousDaysExact: 1 }
  }
  if (scope.startsWith('min-')) {
    const days = Number(scope.replace('min-', ''))
    return {
      continuousDays: Number.isFinite(days) ? days : undefined,
      continuousDaysExact: undefined
    }
  }
  return { continuousDays: undefined, continuousDaysExact: undefined }
})

const normalizedPriceRange = computed(() => ({
  min: parsePriceInput(filters.minPrice),
  max: parsePriceInput(filters.maxPrice)
}))

const hasInvalidPriceRange = computed(() => {
  const { min, max } = normalizedPriceRange.value
  return min !== undefined && max !== undefined && min > max
})

const apiSort = computed(() => {
  if (tableSort.value.prop === 'seal_amount') {
    return { sortBy: 'seal_amount', sortOrder: tableSort.value.order === 'ascending' ? 'asc' : 'desc' }
  }
  if (tableSort.value.prop === 'continuous_limit_up_days') {
    return { sortBy: 'continuous_days', sortOrder: tableSort.value.order === 'ascending' ? 'asc' : 'desc' }
  }
  return { sortBy: 'time', sortOrder: tableSort.value.order === 'descending' ? 'desc' : 'asc' }
})

function parsePriceInput(value: string): number | undefined {
  const text = String(value ?? '').trim()
  if (!text) return undefined
  const parsed = Number(text)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined
}

function validatePriceRange(silent: boolean): boolean {
  if (!hasInvalidPriceRange.value) return true
  if (!silent) {
    ElMessage.warning('价格区间最低价不能大于最高价')
  }
  return false
}

async function fetchData(options: FetchOptions = {}) {
  const { showLoading = limitUpStore.realtimeList.length === 0, silent = false } = options
  if (!validatePriceRange(silent)) return
  if (isFetching.value) return

  isFetching.value = true
  if (showLoading) {
    loading.value = true
  }

  try {
    const response = await getRealtimeLimitUp({
      trade_date: selectedDate.value,
      continuous_days: boardFilter.value.continuousDays,
      continuous_days_exact: boardFilter.value.continuousDaysExact,
      reason_category: filters.reasonCategory || undefined,
      status: filters.status || undefined,
      min_price: normalizedPriceRange.value.min,
      max_price: normalizedPriceRange.value.max,
      sort_by: apiSort.value.sortBy,
      sort_order: apiSort.value.sortOrder
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
  resetTableScroll()
  fetchData({ showLoading: true })
}

function applyPriceRange(range: PriceRange) {
  filters.minPrice = String(range.min)
  filters.maxPrice = String(range.max)
  filters.activePriceRange = range.label
  applyFilters()
}

function handleCustomPriceChange() {
  filters.activePriceRange = ''
}

function updateRealtimeMode() {
  limitUpStore.setAcceptRealtimeUpdates(isSelectedToday.value)
}

function handleDateChange() {
  updateRealtimeMode()
  resetTableScroll()
  fetchData({ showLoading: true })
}

function resetFilters() {
  filters.boardScope = ''
  filters.reasonCategory = ''
  filters.status = ''
  filters.minPrice = ''
  filters.maxPrice = ''
  filters.activePriceRange = ''
  resetTableScroll()
  fetchData({ showLoading: true })
}

async function refreshData() {
  try {
    refreshing.value = true
    await fetchData({ showLoading: false, silent: false })
    resetTableScroll()
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
  resetTableScroll()
  fetchData({ showLoading: true })
}

function resetTableScroll() {
  nextTick(() => {
    tableRef.value?.setScrollTop?.(0)
  })
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

watch(
  () => [filters.boardScope, filters.reasonCategory, filters.status, filters.minPrice, filters.maxPrice] as const,
  () => resetTableScroll()
)

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
    
    .filter-form {
      display: flex;
      flex-wrap: wrap;
      align-items: flex-start;
      gap: 12px 14px;
    }

    :deep(.el-form-item) {
      margin-bottom: 0;
      margin-right: 0;
    }

    :deep(.el-form-item__label) {
      padding-right: 8px;
      color: #606266;
    }

    .price-filter {
      flex: 1 1 480px;
      min-width: 420px;

      :deep(.el-form-item__content) {
        align-items: flex-start;
        width: 100%;
      }
    }

    .price-filter__content {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      width: 100%;
    }

    .price-presets {
      display: grid;
      grid-template-columns: repeat(4, minmax(62px, 1fr));
      gap: 6px;
      flex: 1 1 280px;

      :deep(.el-button) {
        margin-left: 0;
        padding: 5px 0;
      }
    }

    .price-inputs {
      display: flex;
      align-items: center;
      gap: 4px;
      flex: 0 0 160px;
    }

    .price-input {
      width: 68px;

      :deep(.el-input__inner) {
        text-align: right;
      }
    }

    .price-separator {
      color: #909399;
      line-height: 24px;
    }

    .filter-break {
      flex: 1 0 100%;
      height: 0;
    }

    .filter-actions {
      margin-left: auto;

      :deep(.el-form-item__content) {
        display: flex;
        gap: 8px;
        flex-wrap: nowrap;
      }

      :deep(.el-button) {
        margin-left: 0;
      }
    }

    .filter-date-tag {
      align-self: center;
    }
  }

  .data-table {
    :deep(.el-table) {
      cursor: pointer;
      
      .el-table__row:hover {
        background-color: #fafafa;
      }
    }

    :deep(.one-word-tag) {
      margin-left: 4px;
    }
  }
}

@media (max-width: 767px) {
  .limit-up-list {
    .filter-bar {
      margin-bottom: 10px;
      padding: 12px;
      overflow: hidden;

      .filter-form {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 8px;
        min-width: 0;
        width: 100%;
      }

      :deep(.el-form-item) {
        margin: 0;
      }

      :deep(.el-form-item__label) {
        display: none;
      }

      :deep(.el-form-item__content),
      :deep(.el-date-editor),
      :deep(.el-select) {
        margin-left: 0 !important;
        max-width: 100%;
        min-width: 0 !important;
        width: 100% !important;
      }

      :deep(.el-button) {
        min-width: 0;
        padding: 8px 10px;
        width: 100%;
      }

      .filter-actions,
      .filter-date-tag,
      .price-filter {
        grid-column: 1 / -1;
        min-width: 0;
      }

      .filter-break {
        display: none;
      }

      :deep(.filter-actions .el-form-item__content) {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 6px;
      }

      .price-filter__content {
        display: grid;
        grid-template-columns: minmax(0, 1fr);
        width: 100%;
        align-items: stretch;
        gap: 8px;
      }

      .price-presets {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        flex: none;
        min-width: 0;
        width: 100%;
        gap: 6px;

        :deep(.el-button) {
          padding: 6px 0;
        }
      }

      .price-inputs {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
        width: 100%;
        flex: none;
      }

      .price-input {
        flex: 1;
        width: auto;
      }
    }

    .data-table {
      display: none;
    }
  }
}
</style>
