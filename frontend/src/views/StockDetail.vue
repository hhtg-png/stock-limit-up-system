<template>
  <div class="stock-detail" v-loading="loading">
    <section class="stock-hero">
      <div class="stock-title-block">
        <div class="stock-name-row">
          <h2>{{ stockInfo.stock_name || stockCode }}</h2>
          <span class="stock-code">{{ stockCode }}</span>
          <el-tag v-if="stockInfo.market" size="small">{{ stockInfo.market }}</el-tag>
        </div>
        <div class="status-tags">
          <el-tag v-if="stockInfo.continuous_limit_up_days && stockInfo.continuous_limit_up_days > 1" type="danger" size="small">
            {{ stockInfo.continuous_limit_up_days }}连板
          </el-tag>
          <el-tag :type="stockInfo.is_final_sealed ? 'danger' : 'warning'" size="small">
            {{ stockInfo.is_final_sealed ? '涨停封板' : '开板' }}
          </el-tag>
          <el-tag v-if="stockInfo.reason_category" type="success" size="small">{{ stockInfo.reason_category }}</el-tag>
          <el-tag v-if="stockInfo.first_limit_up_time" size="small">首封 {{ formatTime(stockInfo.first_limit_up_time) }}</el-tag>
          <el-tag size="small">开板 {{ stockInfo.open_count ?? 0 }} 次</el-tag>
        </div>
      </div>

      <div class="price-summary">
        <div class="price-main">{{ formatPrice(stockInfo.current_price || stockInfo.limit_up_price) }}</div>
        <div class="summary-item"><span>涨停价</span><strong>{{ formatPrice(stockInfo.limit_up_price) }}</strong></div>
        <div class="summary-item"><span>封单</span><strong>{{ formatWanAmount(stockInfo.seal_amount) }}</strong></div>
        <div class="summary-item"><span>换手</span><strong>{{ formatTurnoverRate(stockInfo.turnover_rate) }}</strong></div>
      </div>

      <el-button :icon="Star" @click="toggleWatch">
        {{ isWatched ? '取消关注' : '加入自选' }}
      </el-button>
    </section>

    <nav class="mobile-detail-anchors" aria-label="个股详情移动端分区">
      <a href="#stock-chart">图表</a>
      <a href="#stock-trading">盘口</a>
      <a href="#stock-timeline">时间线</a>
      <a href="#stock-core">数据</a>
    </nav>

    <section class="detail-workbench">
      <div id="stock-chart" class="chart-panel">
        <div class="panel-header">
          <h3>K线与叠加走势</h3>
          <div class="chart-actions">
            <el-button-group>
              <el-button :type="activePeriod === 'timeline' ? 'primary' : 'default'" size="small" @click="setPeriod('timeline')">分时</el-button>
              <el-button :type="activePeriod === 'day' ? 'primary' : 'default'" size="small" @click="setPeriod('day')">日K</el-button>
              <el-button :type="activePeriod === 'week' ? 'primary' : 'default'" size="small" @click="setPeriod('week')">周K</el-button>
              <el-button :type="activePeriod === 'month' ? 'primary' : 'default'" size="small" @click="setPeriod('month')">月K</el-button>
            </el-button-group>
            <el-button size="small" :type="showLimitUpHighlight ? 'danger' : 'default'" @click="toggleLimitUpHighlight">涨停变色</el-button>
            <el-button size="small" :type="showOverlay ? 'primary' : 'default'" @click="toggleOverlay">叠加标的</el-button>
            <el-button size="small" :icon="Plus" @click="zoomChart(8)" />
            <el-button size="small" :icon="Minus" @click="zoomChart(-8)" />
            <el-button size="small" :icon="Refresh" @click="fetchChartData" />
          </div>
        </div>
        <div class="chart-meta">
          <span class="legend stock"></span>{{ stockInfo.stock_name || stockCode }}
          <button
            v-for="ma in MA_CONFIGS"
            :key="ma.name"
            type="button"
            class="ma-toggle"
            :class="{ active: isMaActive(ma.name) }"
            :style="{ '--ma-color': ma.color }"
            @click="toggleMa(ma.name)"
          >
              <span class="legend ma" :style="{ background: ma.color }"></span><span>{{ ma.name }}</span>
          </button>
        </div>
        <div class="overlay-manager">
          <div class="overlay-input">
            <el-autocomplete
              v-model="overlayInput"
              size="small"
              clearable
              value-key="display"
              :fetch-suggestions="searchOverlaySuggestions"
              :trigger-on-focus="false"
              :debounce="220"
              :loading="overlaySearchLoading"
              placeholder="代码 / 拼音 / 名称"
              @select="selectOverlaySuggestion"
              @keyup.enter="addOverlaySymbol"
            />
            <el-button size="small" @click="addOverlaySymbol">添加</el-button>
          </div>
          <div class="overlay-tags">
            <el-tag
              v-for="(symbol, index) in overlaySymbols"
              :key="symbol"
              size="small"
              closable
              :disable-transitions="true"
              :style="{
                '--overlay-color': getOverlayBaseColor(index),
                '--overlay-limit-color': getOverlayLimitColor(index)
              }"
              @close="removeOverlaySymbol(symbol)"
            >
              {{ formatOverlayTag(symbol) }}
            </el-tag>
          </div>
        </div>
        <div ref="chartRef" v-loading="chartLoading" class="chart-container" :style="{ height: chartHeight }"></div>
      </div>

      <aside id="stock-trading" class="side-panels">
        <div class="side-card">
          <div class="panel-header compact"><h3>盘口</h3></div>
          <div class="orderbook">
            <div v-for="i in 3" :key="'ask' + i" class="book-row">
              <span>卖{{ 4 - i }}</span>
              <strong class="down">{{ formatPrice(orderBook.ask_prices?.[3 - i]) }}</strong>
              <span>{{ orderBook.ask_volumes?.[3 - i] || '-' }}</span>
            </div>
            <div class="current-row">
              <span>当前涨停价</span>
              <strong>{{ formatPrice(orderBook.current_price || stockInfo.limit_up_price) }}</strong>
            </div>
            <div v-for="i in 3" :key="'bid' + i" class="book-row">
              <span>买{{ i }}</span>
              <strong class="up">{{ formatPrice(orderBook.bid_prices?.[i - 1]) }}</strong>
              <span>{{ orderBook.bid_volumes?.[i - 1] || '-' }}</span>
            </div>
          </div>
        </div>

        <div class="side-card">
          <div class="panel-header compact">
            <h3>大单成交</h3>
            <span class="threshold-hint">≥{{ bigOrderThreshold }}手</span>
          </div>
          <div class="bigorder-list">
            <div v-if="filteredBigOrders.length === 0" class="empty-hint">暂无大单</div>
            <div v-for="order in filteredBigOrders" :key="order.id" class="bigorder-item" :class="order.direction">
              <span>{{ formatTime(order.trade_time) }}</span>
              <strong>{{ order.direction === 'buy' ? '买' : '卖' }}</strong>
              <span>{{ formatPrice(order.trade_price) }}</span>
              <span>{{ formatYuanAmount(order.trade_amount) }}</span>
            </div>
          </div>
        </div>
      </aside>

      <div id="stock-timeline" class="timeline-panel">
        <div class="panel-header compact"><h3>涨停时间线</h3></div>
        <div class="timeline-grid">
          <div v-for="item in timelineData" :key="item.change_time" class="timeline-event" :class="item.status">
            <span>{{ formatTime(item.change_time) }}</span>
            <strong>{{ getStatusText(item.status) }}</strong>
            <small>{{ item.price ? formatPrice(item.price) : '' }} {{ item.seal_amount ? '封单 ' + formatWanAmount(item.seal_amount) : '' }}</small>
          </div>
          <div v-if="timelineData.length === 0" class="empty-hint">暂无封板变化记录</div>
        </div>
      </div>

      <div id="stock-core" class="info-panel">
        <div class="panel-header compact"><h3>核心数据</h3></div>
        <div class="info-grid">
          <div class="info-item"><span>题材</span><strong>{{ stockInfo.reason_category || '-' }}</strong></div>
          <div class="info-item"><span>行业</span><strong>{{ stockInfo.industry || '-' }}</strong></div>
          <div class="info-item"><span>成交额</span><strong>{{ formatWanAmount(stockInfo.amount) }}</strong></div>
          <div class="info-item"><span>涨停原因</span><strong>{{ stockInfo.limit_up_reason || '-' }}</strong></div>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import { Minus, Plus, Refresh, Star } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import { getLimitUpDetail } from '@/api/limit-up'
import { getOrderBook, getBigOrders, getTimeline, getKline, getCompareSeries, searchStocks } from '@/api/market'
import { useConfigStore } from '@/stores/config'
import type { LimitUpDetail, LimitUpStatusChange } from '@/types/limit-up'
import type { OrderBook, BigOrder, KlinePeriod, KlinePoint, CompareSeries, ComparePoint, StockSearchItem } from '@/types/market'

const route = useRoute()
const configStore = useConfigStore()

const stockCode = computed(() => route.params.code as string)

const loading = ref(false)
const stockInfo = ref<Partial<LimitUpDetail>>({})
const timelineData = ref<LimitUpStatusChange[]>([])
const orderBook = ref<Partial<OrderBook>>({})
const bigOrders = ref<BigOrder[]>([])

const chartRef = ref<HTMLElement>()
let chart: echarts.ECharts | null = null
let refreshTimer: ReturnType<typeof setInterval> | null = null

const activePeriod = ref<KlinePeriod>('day')
const chartLoading = ref(false)
const klineData = ref<KlinePoint[]>([])
const intradayData = ref<any[]>([])
const overlayTimelineSeries = ref<TimelineOverlaySeries[]>([])
const compareSeries = ref<CompareSeries[]>([])
const overlaySymbols = ref<string[]>(['000001.SH'])
const overlayLabels = ref<Record<string, string>>({ '000001.SH': '上证指数' })
const overlayInput = ref('')
const overlaySearchLoading = ref(false)
const showLimitUpHighlight = ref(true)
const showOverlay = ref(true)
const MAX_PRICE_CHARTS = 6
const MAX_OVERLAY_SYMBOLS = MAX_PRICE_CHARTS - 1
const KLINE_UP_COLOR = '#ef232a'
const KLINE_DOWN_COLOR = '#14a058'
const KLINE_LIMIT_UP_COLOR = '#b00020'
const OVERLAY_PALETTE = [
  { base: '#2563eb', limit: '#1d4ed8' },
  { base: '#f59e0b', limit: '#b45309' },
  { base: '#059669', limit: '#047857' },
  { base: '#7c3aed', limit: '#5b21b6' },
  { base: '#0891b2', limit: '#0e7490' }
]
const MA_CONFIGS = [
  { name: 'MA3', window: 3, color: '#f97316', width: 1.2 },
  { name: 'MA5', window: 5, color: '#7c3aed', width: 1.2 },
  { name: 'MA10', window: 10, color: '#0f766e', width: 1.2 },
  { name: 'MA480', window: 480, color: '#111827', width: 1.8 }
]
const activeMaNames = ref(MA_CONFIGS.map(ma => ma.name))
const MAIN_GRID_HEIGHT = 320
const VOLUME_GRID_HEIGHT = 88
const GRID_TOP = 36
const GRID_GAP = 24
const GRID_BOTTOM_SPACE = 54

interface OverlaySuggestion extends StockSearchItem {
  value: string
  display: string
}

interface TimelineOverlaySeries {
  symbol: string
  name: string
  data: any[]
}

// 根据股票板块判断大单手数阈值（科创/创业板用20cm阈值）
const bigOrderThreshold = computed(() => {
  const code = stockCode.value
  if (code.startsWith('3') || code.startsWith('68')) {
    return configStore.config.big_order_volume_20cm || 200
  }
  return configStore.config.big_order_volume || 300
})

// 过滤大单：仅显示手数 >= 设置阈值的
const filteredBigOrders = computed(() => {
  const threshold = bigOrderThreshold.value
  return bigOrders.value.filter(order => order.trade_volume >= threshold)
})

const visibleOverlayChartCount = computed(() => {
  if (!showOverlay.value) return 0
  if (activePeriod.value === 'timeline') return Math.max(overlayTimelineSeries.value.length, overlaySymbols.value.length)
  if (isKlinePeriod(activePeriod.value)) return Math.max(compareSeries.value.length, overlaySymbols.value.length)
  return 0
})

const chartHeight = computed(() => {
  return `${Math.max(620, getKlineChartHeightPx(visibleOverlayChartCount.value))}px`
})

// 是否已关注
const isWatched = computed(() => 
  configStore.config.watch_list.includes(stockCode.value)
)

function isKlinePeriod(period: KlinePeriod): period is 'day' | 'week' | 'month' {
  return period === 'day' || period === 'week' || period === 'month'
}

async function fetchChartData() {
  if (!chart) return
  chartLoading.value = true
  try {
    if (activePeriod.value === 'timeline') {
      const [data, overlays] = await Promise.all([
        getTimeline(stockCode.value),
        showOverlay.value && overlaySymbols.value.length > 0
          ? Promise.all(overlaySymbols.value.map(symbol =>
              getTimeline(symbol)
                .then(result => ({
                  symbol,
                  name: overlayLabels.value[symbol] || symbol,
                  data: result?.data || []
                }))
                .catch(() => ({
                  symbol,
                  name: overlayLabels.value[symbol] || symbol,
                  data: []
                }))
            ))
          : Promise.resolve([])
      ])
      intradayData.value = data?.data || []
      klineData.value = []
      compareSeries.value = []
      overlayTimelineSeries.value = overlays
    } else if (isKlinePeriod(activePeriod.value)) {
      const [kline, compares] = await Promise.all([
        getKline(stockCode.value, { period: activePeriod.value, limit: 600 }),
        showOverlay.value && overlaySymbols.value.length > 0
          ? getCompareSeries({
              symbols: overlaySymbols.value,
              period: activePeriod.value,
              limit: 600
            }).catch(() => [])
          : Promise.resolve([])
      ])
      klineData.value = kline.data || []
      compareSeries.value = compares
      intradayData.value = []
      overlayTimelineSeries.value = []
    }
    updateChart()
  } catch (e) {
    console.error('Fetch chart data error:', e)
    ElMessage.warning('图表数据暂不可用')
  } finally {
    chartLoading.value = false
  }
}

function setPeriod(period: KlinePeriod) {
  if (activePeriod.value === period) return
  activePeriod.value = period
  fetchChartData()
}

function toggleOverlay() {
  showOverlay.value = !showOverlay.value
  fetchChartData()
}

function toggleLimitUpHighlight() {
  showLimitUpHighlight.value = !showLimitUpHighlight.value
  updateChart()
}

function isMaActive(name: string): boolean {
  return activeMaNames.value.includes(name)
}

function toggleMa(name: string) {
  activeMaNames.value = isMaActive(name)
    ? activeMaNames.value.filter(item => item !== name)
    : [...activeMaNames.value, name]
  updateChart()
}

function getVisibleMaConfigs() {
  return MA_CONFIGS.filter(ma => activeMaNames.value.includes(ma.name))
}

function inferMarketFromCode(code: string): string {
  if (code.startsWith('6')) return 'SH'
  if (code.startsWith('4') || code.startsWith('8') || code.startsWith('920')) return 'BJ'
  return 'SZ'
}

function normalizeDirectOverlaySymbol(value: string): string {
  const raw = value.trim().toUpperCase().replace(/\s+/g, '')
  const match = raw.match(/^(\d{6})(?:\.(SH|SZ|BJ|BSE))?$/)
  if (!match) return ''

  const market = match[2] ? match[2].replace('BSE', 'BJ') : inferMarketFromCode(match[1])
  return `${match[1]}.${market}`
}

function rememberOverlayLabel(symbol: string, name?: string) {
  if (!name || name === symbol) return
  overlayLabels.value = { ...overlayLabels.value, [symbol]: name }
}

function formatOverlayTag(symbol: string): string {
  const name = overlayLabels.value[symbol]
  return name && name !== symbol ? `${name} ${symbol}` : symbol
}

function toOverlaySuggestion(item: StockSearchItem): OverlaySuggestion {
  const pinyin = item.pinyin ? ` / ${item.pinyin}` : ''
  return {
    ...item,
    value: item.symbol,
    display: `${item.stock_name} ${item.symbol}${pinyin}`
  }
}

async function searchOverlaySuggestions(queryString: string, cb: (items: OverlaySuggestion[]) => void) {
  const query = queryString.trim()
  if (!query) {
    cb([])
    return
  }

  overlaySearchLoading.value = true
  try {
    const results = await searchStocks(query, 8)
    cb(results.map(toOverlaySuggestion))
  } catch (e) {
    cb([])
  } finally {
    overlaySearchLoading.value = false
  }
}

function addResolvedOverlaySymbol(symbol: string, name?: string) {
  if (!symbol) {
    ElMessage.warning('请输入叠加代码、拼音或名称')
    return
  }
  if (overlaySymbols.value.includes(symbol)) {
    ElMessage.warning('该标的已在叠加列表')
    return
  }
  if (overlaySymbols.value.length >= MAX_OVERLAY_SYMBOLS) {
    ElMessage.warning(`最多显示${MAX_PRICE_CHARTS}个图（主图+${MAX_OVERLAY_SYMBOLS}个叠加）`)
    return
  }

  rememberOverlayLabel(symbol, name)
  overlaySymbols.value = [...overlaySymbols.value, symbol]
  overlayInput.value = ''
  showOverlay.value = true
  refreshOverlayData()
}

function selectOverlaySuggestion(item: OverlaySuggestion) {
  addResolvedOverlaySymbol(item.symbol, item.stock_name)
}

function getOverlayBaseColor(index: number): string {
  return OVERLAY_PALETTE[index % OVERLAY_PALETTE.length].base
}

function getOverlayLimitColor(_index: number): string {
  return KLINE_LIMIT_UP_COLOR
}

function refreshOverlayData() {
  if (showOverlay.value && (isKlinePeriod(activePeriod.value) || activePeriod.value === 'timeline')) {
    fetchChartData()
    return
  }
  updateChart()
}

async function addOverlaySymbol() {
  const query = overlayInput.value.trim()
  if (!query) {
    ElMessage.warning('请输入叠加代码、拼音或名称')
    return
  }

  const directSymbol = normalizeDirectOverlaySymbol(query)
  try {
    const results = await searchStocks(query, 5)
    const selected = directSymbol
      ? results.find(item => item.symbol === directSymbol) ?? results[0]
      : results[0]

    if (selected) {
      addResolvedOverlaySymbol(selected.symbol, selected.stock_name)
      return
    }
  } catch (e) {
    // Direct code input still works when the remote suggest endpoint is unavailable.
  }

  if (!directSymbol) {
    ElMessage.warning('未找到匹配标的')
    return
  }

  addResolvedOverlaySymbol(directSymbol, directSymbol)
}

function removeOverlaySymbol(symbol: string) {
  overlaySymbols.value = overlaySymbols.value.filter(item => item !== symbol)
  refreshOverlayData()
}

defineExpose({
  setPeriod,
  toggleOverlay,
  toggleLimitUpHighlight,
  zoomChart
})

// 获取数据
async function fetchData() {
  loading.value = true
  try {
    const [detail, ob, orders] = await Promise.all([
      getLimitUpDetail(stockCode.value),
      getOrderBook(stockCode.value).catch(() => ({})),
      getBigOrders(stockCode.value, { page_size: 20 }).catch(() => [])
    ])
    
    stockInfo.value = detail
    timelineData.value = detail.status_changes || []
    orderBook.value = ob
    bigOrders.value = orders

    await fetchChartData()
  } catch (e) {
    console.error('Fetch error:', e)
    ElMessage.error('获取数据失败')
  } finally {
    loading.value = false
  }
}

// 格式化换手率
function formatTurnoverRate(rate: number | undefined | null): string {
  if (rate == null || rate === 0) return '-'
  return rate.toFixed(2) + '%'
}

function formatPrice(value: number | undefined | null): string {
  if (value == null || Number.isNaN(value)) return '-'
  return value.toFixed(2)
}

function formatYuanAmount(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return '-'
  if (Math.abs(value) >= 100000000) return (value / 100000000).toFixed(2) + '亿'
  if (Math.abs(value) >= 10000) return (value / 10000).toFixed(0) + '万'
  return value.toFixed(0)
}

function formatWanAmount(valueWan?: number | null): string {
  if (valueWan == null || Number.isNaN(valueWan)) return '-'
  return formatYuanAmount(valueWan * 10000)
}

function isHighlightedLimitUpPoint(point: KlinePoint): boolean {
  return Boolean(
    point.is_limit_up ||
    (stockInfo.value.is_final_sealed && stockInfo.value.trade_date && point.date === stockInfo.value.trade_date)
  )
}

function getLimitUpColor(point: KlinePoint): string {
  if (showLimitUpHighlight.value && isHighlightedLimitUpPoint(point)) return KLINE_LIMIT_UP_COLOR
  return point.close >= point.open ? KLINE_UP_COLOR : KLINE_DOWN_COLOR
}

function buildMaData<T extends { close: number }>(points: T[], windowSize: number): (number | null)[] {
  return points.map((_point, index) => {
    if (index < windowSize - 1) return null
    const slice = points.slice(index - windowSize + 1, index + 1)
    const total = slice.reduce((sum, item) => sum + item.close, 0)
    return Number((total / windowSize).toFixed(2))
  })
}

function buildOverlayMaData(points: (ComparePoint | null)[], windowSize: number): (number | null)[] {
  return points.map((_point, index) => {
    if (index < windowSize - 1) return null
    const slice = points.slice(index - windowSize + 1, index + 1)
    if (slice.some(point => !point)) return null
    const total = slice.reduce((sum, item) => sum + (item?.close ?? 0), 0)
    return Number((total / windowSize).toFixed(2))
  })
}

function getOverlayPointColor(point: ComparePoint | null, index: number): string {
  if (showLimitUpHighlight.value && point?.is_limit_up) return getOverlayLimitColor(index)
  if (!point) return getOverlayBaseColor(index)
  return point.close >= point.open ? KLINE_UP_COLOR : KLINE_DOWN_COLOR
}

function withAlpha(hexColor: string, alpha: number): string {
  const normalized = hexColor.replace('#', '')
  const value = normalized.length === 3
    ? normalized.split('').map(char => char + char).join('')
    : normalized
  const r = Number.parseInt(value.slice(0, 2), 16)
  const g = Number.parseInt(value.slice(2, 4), 16)
  const b = Number.parseInt(value.slice(4, 6), 16)
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

function getKlineChartHeightPx(overlayCount: number): number {
  const priceChartCount = overlayCount + 1
  const columnCount = getChartGridColumnCount(priceChartCount)
  const priceRowCount = Math.ceil(priceChartCount / columnCount)
  return GRID_TOP
    + (MAIN_GRID_HEIGHT * priceRowCount)
    + (GRID_GAP * Math.max(priceRowCount - 1, 0))
    + GRID_GAP
    + VOLUME_GRID_HEIGHT
    + GRID_BOTTOM_SPACE
}

function getChartGridColumnCount(priceChartCount: number): number {
  return priceChartCount > 2 ? 2 : 1
}

function buildGridLayout(categories: string[], overlayCount: number) {
  const totalHeight = getKlineChartHeightPx(overlayCount)
  const priceChartCount = overlayCount + 1
  const columnCount = getChartGridColumnCount(priceChartCount)
  const priceRowCount = Math.ceil(priceChartCount / columnCount)
  const grids: any[] = []
  const xAxis: any[] = []
  const yAxis: any[] = []

  const addGrid = (
    topPx: number,
    heightPx: number,
    axisLabel = false,
    yOptions: Record<string, unknown> = {},
    gridOptions: Record<string, unknown> = {}
  ) => {
    const gridIndex = grids.length
    grids.push({
      left: 58,
      right: 72,
      top: topPx,
      height: heightPx,
      ...gridOptions
    })
    xAxis.push({
      type: 'category',
      data: categories,
      gridIndex,
      scale: true,
      boundaryGap: true,
      axisPointer: {
        show: true,
        snap: true,
        triggerTooltip: true,
        lineStyle: { color: '#64748b', width: 1, type: 'dashed' },
        label: { show: false }
      },
      axisLabel: { show: axisLabel }
    })
    yAxis.push({
      scale: true,
      gridIndex,
      ...yOptions
    })
    return gridIndex
  }

  const addPriceGrid = (slotIndex: number, yOptions: Record<string, unknown> = {}) => {
    const row = Math.floor(slotIndex / columnCount)
    const column = slotIndex % columnCount
    const top = GRID_TOP + row * (MAIN_GRID_HEIGHT + GRID_GAP)
    const gridOptions = columnCount === 1
      ? {}
      : column === 0
        ? { right: '54%' }
        : { left: '54%', right: 72 }

    return addGrid(top, MAIN_GRID_HEIGHT, false, {
      splitArea: { show: true },
      position: columnCount === 2 && column === 1 ? 'right' : 'left',
      axisLabel: { formatter: '{value}' },
      splitLine: { show: true, lineStyle: { color: '#edf1f7' } },
      ...yOptions
    }, gridOptions)
  }

  const mainGridIndex = addPriceGrid(0)

  const overlayGridIndexes: number[] = []
  for (let i = 0; i < overlayCount; i += 1) {
    overlayGridIndexes.push(addPriceGrid(i + 1))
  }

  const nextTop = GRID_TOP + priceRowCount * MAIN_GRID_HEIGHT + Math.max(priceRowCount - 1, 0) * GRID_GAP + GRID_GAP
  const volumeGridIndex = addGrid(nextTop, VOLUME_GRID_HEIGHT, true, {
    splitNumber: 2
  })

  return {
    totalHeight,
    grids,
    xAxis,
    yAxis,
    mainGridIndex,
    overlayGridIndexes,
    volumeGridIndex,
    xAxisIndexes: xAxis.map((_axis, index) => index)
  }
}

function buildOverlayCandleData(points: (ComparePoint | null)[], index: number) {
  return points.map(point => {
    if (!point) {
      return {
        value: ['-', '-', '-', '-'],
        itemStyle: { opacity: 0 }
      }
    }
    const color = getOverlayPointColor(point, index)
    const isLimit = showLimitUpHighlight.value && point.is_limit_up
    return {
      value: [
        point.open,
        point.close,
        point.low,
        point.high
      ],
      itemStyle: {
        color,
        color0: KLINE_DOWN_COLOR,
        borderColor: color,
        borderColor0: KLINE_DOWN_COLOR,
        borderWidth: isLimit ? 2 : 1,
        opacity: 1
      }
    }
  })
}

function tooltipLine(label: string, value: string, color: string): string {
  return `<div style="display:flex;gap:8px;align-items:center;white-space:nowrap;">
    <span style="width:8px;height:8px;border-radius:999px;background:${color};display:inline-block;"></span>
    <span style="min-width:70px;color:#64748b;">${label}</span>
    <strong style="color:#111827;">${value}</strong>
  </div>`
}

function formatNumber(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return '-'
  return value.toFixed(2)
}

function resolveTooltipDataIndex(params: any, categories: string[]): number {
  const rows = Array.isArray(params) ? params : [params]
  const rowWithIndex = rows.find(row => Number.isInteger(row?.dataIndex))
  if (rowWithIndex) return rowWithIndex.dataIndex

  const axisValue = rows.find(row => row?.axisValue != null)?.axisValue
  const axisIndex = categories.findIndex(item => String(item) === String(axisValue))
  return axisIndex >= 0 ? axisIndex : 0
}

function buildLinkedAxisPointerOption(xAxisIndexes: number[]) {
  return {
    snap: true,
    triggerTooltip: true,
    link: [{ xAxisIndex: xAxisIndexes }]
  }
}

function buildKlineOption() {
  const dates = klineData.value.map(item => item.date)
  const visibleMaConfigs = getVisibleMaConfigs()
  const overlayCharts = showOverlay.value
    ? compareSeries.value.map((overlay, index) => {
        const pointByDate = new Map(overlay.data.map(point => [point.date, point]))
        return {
          overlay,
          index,
          name: overlayLabels.value[overlay.symbol] || overlay.name || overlay.symbol,
          points: dates.map(date => pointByDate.get(date) ?? null)
        }
      })
    : []
  const layout = buildGridLayout(dates, overlayCharts.length)
  const candleData = klineData.value.map(item => ({
    value: [item.open, item.close, item.low, item.high],
    itemStyle: {
      color: getLimitUpColor(item),
      color0: KLINE_DOWN_COLOR,
      borderColor: getLimitUpColor(item),
      borderColor0: KLINE_DOWN_COLOR
    }
  }))

  const series: any[] = [
    {
      name: stockInfo.value.stock_name || stockCode.value,
      type: 'candlestick',
      data: candleData,
      xAxisIndex: layout.mainGridIndex,
      yAxisIndex: layout.mainGridIndex,
      markPoint: {
        symbol: 'pin',
        symbolSize: 42,
        itemStyle: { color: KLINE_LIMIT_UP_COLOR },
        label: { formatter: '涨停', color: '#fff', fontSize: 10 },
        data: klineData.value
          .map((item, index) => ({ item, index }))
          .filter(({ item }) => showLimitUpHighlight.value && isHighlightedLimitUpPoint(item))
          .map(({ item, index }) => ({
            name: '涨停',
            coord: [index, item.high],
            value: item.close
          }))
      }
    },
    {
      name: '成交量',
      type: 'bar',
      data: klineData.value.map(item => ({
        value: item.volume,
        itemStyle: { color: getLimitUpColor(item) }
      })),
      xAxisIndex: layout.volumeGridIndex,
      yAxisIndex: layout.volumeGridIndex
    }
  ]

  if (visibleMaConfigs.length > 0) {
    visibleMaConfigs.forEach(ma => {
      series.push({
        name: ma.name,
        type: 'line',
        data: buildMaData(klineData.value, ma.window),
        smooth: true,
        symbol: 'none',
        xAxisIndex: layout.mainGridIndex,
        yAxisIndex: layout.mainGridIndex,
        z: 20,
        lineStyle: { width: ma.width, color: ma.color }
      })
    })
  }

  overlayCharts.forEach(({ index, name: overlayName, points }) => {
    const baseColor = getOverlayBaseColor(index)
    const overlayGridIndex = layout.overlayGridIndexes[index]
    layout.yAxis[overlayGridIndex].name = overlayName
    layout.yAxis[overlayGridIndex].nameTextStyle = { color: baseColor, fontWeight: 600 }

    series.push({
      name: `${overlayName}K`,
      type: 'candlestick',
      data: buildOverlayCandleData(points, index),
      xAxisIndex: overlayGridIndex,
      yAxisIndex: overlayGridIndex,
      barMaxWidth: 16,
      z: 14 + index,
      itemStyle: {
        color: KLINE_UP_COLOR,
        color0: KLINE_DOWN_COLOR,
        borderColor: KLINE_UP_COLOR,
        borderColor0: KLINE_DOWN_COLOR
      },
      markPoint: {
        symbol: 'pin',
        symbolSize: 42,
        itemStyle: { color: KLINE_LIMIT_UP_COLOR },
        label: { formatter: '涨停', color: '#fff', fontSize: 10 },
        data: points
          .map((point, dataIndex) => ({ point, dataIndex }))
          .filter(({ point }) => showLimitUpHighlight.value && Boolean(point?.is_limit_up))
          .map(({ point, dataIndex }) => ({
            name: '涨停',
            coord: [dataIndex, point!.high],
            value: point!.close
          }))
      }
    })

    if (visibleMaConfigs.length > 0) {
      visibleMaConfigs.forEach(ma => {
        series.push({
          name: `${overlayName}${ma.name}`,
          type: 'line',
          data: buildOverlayMaData(points, ma.window),
          smooth: true,
          symbol: 'none',
          connectNulls: false,
          xAxisIndex: overlayGridIndex,
          yAxisIndex: overlayGridIndex,
          z: 20 + index,
          lineStyle: { width: ma.width, color: ma.color, opacity: 0.9 }
        })
      })
    }
  })

  return {
    animation: false,
    tooltip: {
      trigger: 'axis',
      triggerOn: 'mousemove|click',
      axisPointer: { type: 'cross', snap: true },
      confine: true,
      enterable: false,
      transitionDuration: 0,
      position: [8, 8],
      formatter: (params: any) => {
        const dataIndex = resolveTooltipDataIndex(params, dates)
        const date = dates[dataIndex] || ''
        const main = klineData.value[dataIndex]
        const html = [`<div style="font-size:12px;line-height:1.7;"><strong style="color:#111827;">${date}</strong>`]
        if (main) {
          html.push(tooltipLine(stockInfo.value.stock_name || stockCode.value, `开 ${formatNumber(main.open)} 高 ${formatNumber(main.high)} 低 ${formatNumber(main.low)} 收 ${formatNumber(main.close)}`, getLimitUpColor(main)))
        }
        overlayCharts.forEach(({ name, points, index }) => {
          const point = points[dataIndex]
          if (!point) return
          const suffix = showLimitUpHighlight.value && point.is_limit_up ? ' 涨停' : ''
          html.push(tooltipLine(name, `开 ${formatNumber(point.open)} 高 ${formatNumber(point.high)} 低 ${formatNumber(point.low)} 收 ${formatNumber(point.close)}${suffix}`, getOverlayPointColor(point, index)))
        })
        html.push('</div>')
        return html.join('')
      }
    },
    axisPointer: buildLinkedAxisPointerOption(layout.xAxisIndexes),
    legend: { show: false },
    grid: layout.grids,
    xAxis: layout.xAxis,
    yAxis: layout.yAxis,
    dataZoom: [
      { type: 'inside', xAxisIndex: layout.xAxisIndexes, start: 64, end: 100 },
      { type: 'slider', xAxisIndex: layout.xAxisIndexes, bottom: 8, height: 20, start: 64, end: 100 }
    ],
    series
  }
}

function buildTimelineOption() {
  const times = intradayData.value.map((item: any) => item.time)
  const overlayCharts = showOverlay.value
    ? overlayTimelineSeries.value.map((overlay, index) => {
        const pointByTime = new Map(overlay.data.map((point: any) => [point.time, point]))
        return {
          ...overlay,
          index,
          points: times.map(time => pointByTime.get(time) ?? null)
        }
      })
    : []
  const layout = buildGridLayout(times, overlayCharts.length)
  overlayCharts.forEach(overlay => {
    const gridIndex = layout.overlayGridIndexes[overlay.index]
    layout.yAxis[gridIndex].name = overlay.name
  })

  return {
    animation: false,
    tooltip: {
      trigger: 'axis',
      triggerOn: 'mousemove|click',
      axisPointer: { type: 'cross', snap: true },
      confine: true,
      enterable: false,
      transitionDuration: 0,
      position: [8, 8],
      formatter: (params: any) => {
        const dataIndex = resolveTooltipDataIndex(params, times)
        const time = times[dataIndex] || ''
        const main = intradayData.value[dataIndex]
        const html = [`<div style="font-size:12px;line-height:1.7;"><strong style="color:#111827;">${time}</strong>`]
        if (main) {
          html.push(tooltipLine(stockInfo.value.stock_name || stockCode.value, formatNumber(main.price), KLINE_UP_COLOR))
        }
        overlayCharts.forEach(overlay => {
          const point = overlay.points[dataIndex]
          if (!point) return
          html.push(tooltipLine(overlay.name, formatNumber(point.price), getOverlayBaseColor(overlay.index)))
        })
        html.push('</div>')
        return html.join('')
      }
    },
    axisPointer: buildLinkedAxisPointerOption(layout.xAxisIndexes),
    grid: layout.grids,
    xAxis: layout.xAxis,
    yAxis: layout.yAxis,
    dataZoom: [
      { type: 'inside', xAxisIndex: layout.xAxisIndexes, start: 0, end: 100 },
      { type: 'slider', xAxisIndex: layout.xAxisIndexes, bottom: 8, height: 20, start: 0, end: 100 }
    ],
    series: [
      {
        name: '现价',
        type: 'line',
        data: intradayData.value.map((item: any) => item.price),
        smooth: true,
        symbol: 'none',
        xAxisIndex: layout.mainGridIndex,
        yAxisIndex: layout.mainGridIndex,
        lineStyle: { color: KLINE_UP_COLOR },
        areaStyle: { color: 'rgba(239, 35, 42, 0.08)' }
      },
      ...overlayCharts.map(overlay => ({
        name: overlay.name,
        type: 'line',
        data: overlay.points.map((point: any) => point?.price ?? null),
        smooth: true,
        symbol: 'none',
        xAxisIndex: layout.overlayGridIndexes[overlay.index],
        yAxisIndex: layout.overlayGridIndexes[overlay.index],
        lineStyle: { width: 1.6, color: getOverlayBaseColor(overlay.index) },
        areaStyle: { color: withAlpha(getOverlayBaseColor(overlay.index), 0.08) }
      })),
      {
        name: '成交量',
        type: 'bar',
        data: intradayData.value.map((item: any) => item.volume),
        xAxisIndex: layout.volumeGridIndex,
        yAxisIndex: layout.volumeGridIndex,
        itemStyle: { color: '#64748b' }
      }
    ]
  }
}

// 初始化图表
function initChart() {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  updateChart()
}

function updateChart() {
  if (!chart) return
  const hasData = activePeriod.value === 'timeline'
    ? intradayData.value.length > 0
    : klineData.value.length > 0

  if (!hasData) {
    chart.clear()
    chart.setOption({
      title: {
        text: '暂无图表数据',
        left: 'center',
        top: 'middle',
        textStyle: { color: '#94a3b8', fontSize: 14, fontWeight: 500 }
      }
    })
    return
  }

  chart.setOption(activePeriod.value === 'timeline' ? buildTimelineOption() : buildKlineOption(), true)
}

function resizeChart() {
  chart?.resize()
}

function zoomChart(delta: number) {
  if (!chart) return
  const option: any = chart.getOption()
  const zoom = option.dataZoom?.[0]
  if (!zoom) return
  const start = Math.max(0, Math.min(95, Number(zoom.start ?? 55) + delta))
  const end = Math.max(start + 5, Math.min(100, Number(zoom.end ?? 100) - delta))
  chart.dispatchAction({ type: 'dataZoom', start, end })
}

// 切换关注
function toggleWatch() {
  if (isWatched.value) {
    configStore.removeFromWatchList(stockCode.value)
    ElMessage.success('已取消关注')
  } else {
    configStore.addToWatchList(stockCode.value)
    ElMessage.success('已加入自选')
  }
}

// 状态文本
function getStatusText(status: string) {
  switch (status) {
    case 'sealed': return '封板'
    case 'opened': return '开板'
    case 'resealed': return '回封'
    default: return status
  }
}

// 格式化时间
function formatTime(time: string) {
  return time?.split('T')[1]?.substring(0, 8) || time
}

onMounted(() => {
  nextTick(() => {
    initChart()
    fetchData()
    window.addEventListener('resize', resizeChart)
  })
  
  // 定时刷新
  refreshTimer = setInterval(() => {
    getOrderBook(stockCode.value).then(ob => orderBook.value = ob).catch(() => {})
    getBigOrders(stockCode.value, { page_size: 20 }).then(orders => bigOrders.value = orders).catch(() => {})
  }, 5000)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  window.removeEventListener('resize', resizeChart)
  chart?.dispose()
  chart = null
})

watch(stockCode, async () => {
  await nextTick()
  fetchData()
})
</script>

<style lang="scss" scoped>
.stock-detail {
  --chart-container-height: 620px;

  display: flex;
  flex-direction: column;
  gap: 12px;
}

.stock-hero,
.chart-panel,
.side-card,
.timeline-panel,
.info-panel {
  background: #fff;
  border: 1px solid #e5eaf3;
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}

.mobile-detail-anchors {
  display: none;
}

.stock-hero {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) auto auto;
  gap: 16px;
  align-items: center;
  padding: 16px;
}

.stock-name-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;

  h2 {
    margin: 0;
    font-size: 24px;
    color: #111827;
  }

  .stock-code {
    color: #64748b;
    font-weight: 600;
  }
}

.status-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
}

.price-summary {
  display: flex;
  align-items: flex-end;
  gap: 16px;

  .price-main {
    font-size: 30px;
    line-height: 1;
    font-weight: 800;
    color: #ef232a;
  }

  .summary-item {
    font-size: 12px;
    color: #64748b;

    strong {
      display: block;
      margin-top: 4px;
      color: #111827;
      font-size: 14px;
    }
  }
}

.detail-workbench {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 320px;
  gap: 12px;
}

.chart-panel {
  min-width: 0;
}

.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 12px 14px;
  border-bottom: 1px solid #edf1f7;

  &.compact {
    padding: 10px 12px;
  }

  h3 {
    margin: 0;
    font-size: 15px;
    color: #111827;
  }
}

.chart-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  flex-wrap: wrap;
}

.chart-meta {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  row-gap: 6px;
  padding: 10px 14px 0;
  color: #64748b;
  font-size: 12px;

  .legend {
    width: 18px;
    height: 3px;
    border-radius: 999px;
    display: inline-block;
  }

  .stock { background: #ef232a; }
  .ma { background: #7c3aed; }
}

.ma-toggle {
  --ma-color: #7c3aed;

  display: inline-flex;
  align-items: center;
  gap: 4px;
  border: 1px solid transparent;
  border-radius: 4px;
  background: transparent;
  color: #94a3b8;
  font: inherit;
  line-height: 1.4;
  padding: 2px 4px;
  cursor: pointer;

  .legend {
    opacity: 0.35;
  }

  &.active {
    border-color: color-mix(in srgb, var(--ma-color) 30%, transparent);
    background: color-mix(in srgb, var(--ma-color) 8%, #fff);
    color: #334155;

    .legend {
      opacity: 1;
    }
  }
}

.overlay-manager {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 8px 14px 0;
}

.overlay-input {
  display: flex;
  align-items: center;
  gap: 6px;

  .el-autocomplete {
    width: 220px;
  }
}

.overlay-tags {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;

  :deep(.el-tag) {
    border-color: color-mix(in srgb, var(--overlay-color) 36%, #fff);
    background: color-mix(in srgb, var(--overlay-color) 10%, #fff);
    color: var(--overlay-color);
  }

  :deep(.el-tag::before) {
    content: '';
    width: 14px;
    height: 3px;
    margin-right: 5px;
    border-radius: 999px;
    background: linear-gradient(90deg, var(--overlay-color), var(--overlay-limit-color));
  }
}

.chart-container {
  height: var(--chart-container-height);
}

.side-panels {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.orderbook {
  padding: 10px 12px 12px;
}

.book-row {
  display: grid;
  grid-template-columns: 44px 1fr 1fr;
  gap: 10px;
  padding: 6px 0;
  font-size: 13px;
  color: #64748b;

  strong {
    text-align: right;
  }

  .up { color: #ef232a; }
  .down { color: #14a058; }
}

.current-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 8px 0;
  padding: 10px;
  border-radius: 6px;
  background: #fff1f0;
  color: #ef232a;
  font-weight: 700;
}

.threshold-hint {
  color: #94a3b8;
  font-size: 12px;
}

.bigorder-list {
  max-height: 260px;
  overflow-y: auto;
  padding: 0 12px 10px;
}

.bigorder-item {
  display: grid;
  grid-template-columns: 58px 34px 1fr 64px;
  gap: 8px;
  padding: 8px 0;
  border-bottom: 1px solid #f1f5f9;
  color: #64748b;
  font-size: 13px;

  &.buy strong {
    color: #ef232a;
  }

  &.sell strong {
    color: #14a058;
  }
}

.timeline-panel {
  grid-column: 1 / 2;
}

.timeline-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  padding: 12px;

  > .empty-hint {
    grid-column: 1 / -1;
  }
}

.timeline-event {
  border: 1px solid #e5eaf3;
  border-radius: 8px;
  padding: 10px;
  min-height: 76px;

  span,
  small {
    color: #64748b;
    font-size: 12px;
  }

  strong {
    display: block;
    margin: 6px 0;
    color: #111827;
  }

  &.sealed,
  &.resealed {
    border-color: #ffc9cf;
    background: #fff7f7;

    strong {
      color: #ef232a;
    }
  }

  &.opened {
    border-color: #ffe4ba;
    background: #fffaf0;
  }
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  padding: 12px;
}

.info-item {
  border: 1px solid #e5eaf3;
  border-radius: 8px;
  padding: 10px;
  background: #fbfdff;

  span {
    color: #64748b;
    font-size: 12px;
  }

  strong {
    display: block;
    margin-top: 6px;
    color: #111827;
    font-size: 14px;
  }
}

.empty-hint {
  padding: 18px;
  color: #94a3b8;
  text-align: center;
  font-size: 13px;
}

@media (max-width: 1180px) {
  .stock-hero {
    grid-template-columns: 1fr;
  }

  .price-summary {
    flex-wrap: wrap;
  }

  .detail-workbench {
    grid-template-columns: 1fr;
  }

  .timeline-panel {
    grid-column: auto;
  }

  .timeline-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .stock-detail {
    --chart-container-height: 480px;
  }

  .stock-hero {
    gap: 12px;
    padding: 14px;
  }

  .stock-name-row h2 {
    font-size: 20px;
  }

  .price-summary {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
    width: 100%;

    .price-main {
      grid-column: 1 / -1;
      font-size: 30px;
    }

    .summary-item {
      border-radius: 6px;
      background: #f8fafc;
      padding: 8px;
    }
  }

  .mobile-detail-anchors {
    position: sticky;
    top: 52px;
    z-index: 12;
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 6px;
    margin: -2px 0 10px;
    padding: 8px 0;
    background: #f0f2f5;

    a {
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      background: #fff;
      padding: 8px 0;
      color: #475569;
      font-size: 12px;
      font-weight: 600;
      text-align: center;
      text-decoration: none;
    }
  }

  .chart-actions {
    width: 100%;
    justify-content: flex-start;
    overflow-x: auto;
    padding-bottom: 2px;

    :deep(.el-button) {
      flex-shrink: 0;
    }
  }

  .chart-meta,
  .overlay-manager {
    padding: 10px;
  }

  .overlay-input {
    width: 100%;

    :deep(.el-autocomplete) {
      flex: 1;
    }
  }

  .timeline-grid,
  .info-grid {
    grid-template-columns: 1fr;
  }

  .panel-header {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
