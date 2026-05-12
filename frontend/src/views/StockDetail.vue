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

    <section class="detail-workbench">
      <div class="chart-panel">
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
            <el-button size="small" :type="showOverlay ? 'primary' : 'default'" @click="toggleOverlay">叠加指数</el-button>
            <el-button size="small" :icon="Plus" @click="zoomChart(8)" />
            <el-button size="small" :icon="Minus" @click="zoomChart(-8)" />
            <el-button size="small" :icon="Refresh" @click="fetchChartData" />
          </div>
        </div>
        <div class="chart-meta">
          <span class="legend stock"></span>{{ stockInfo.stock_name || stockCode }}
          <span v-if="showOverlay" class="legend index"></span><span v-if="showOverlay">叠加走势</span>
          <span v-if="showMa" class="legend ma"></span><span v-if="showMa">MA5</span>
        </div>
        <div ref="chartRef" v-loading="chartLoading" class="chart-container"></div>
      </div>

      <aside class="side-panels">
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

      <div class="timeline-panel">
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

      <div class="info-panel">
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
import { getOrderBook, getBigOrders, getTimeline, getKline, getCompareSeries } from '@/api/market'
import { useConfigStore } from '@/stores/config'
import type { LimitUpDetail, LimitUpStatusChange } from '@/types/limit-up'
import type { OrderBook, BigOrder, KlinePeriod, KlinePoint, CompareSeries } from '@/types/market'

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
const compareSeries = ref<CompareSeries[]>([])
const overlaySymbols = ref<string[]>(['000001.SH'])
const showLimitUpHighlight = ref(true)
const showMa = ref(true)
const showOverlay = ref(true)

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
      const data = await getTimeline(stockCode.value)
      intradayData.value = data?.data || []
      klineData.value = []
      compareSeries.value = []
    } else if (isKlinePeriod(activePeriod.value)) {
      const [kline, compares] = await Promise.all([
        getKline(stockCode.value, { period: activePeriod.value, limit: 250 }),
        showOverlay.value
          ? getCompareSeries({
              symbols: overlaySymbols.value,
              period: activePeriod.value,
              limit: 250
            }).catch(() => [])
          : Promise.resolve([])
      ])
      klineData.value = kline.data || []
      compareSeries.value = compares
      intradayData.value = []
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
  return Boolean(point.is_limit_up || (stockInfo.value.trade_date && point.date === stockInfo.value.trade_date))
}

function getLimitUpColor(point: KlinePoint): string {
  if (showLimitUpHighlight.value && isHighlightedLimitUpPoint(point)) return '#8b000f'
  return point.close >= point.open ? '#d82135' : '#1677ff'
}

function buildMaData(points: KlinePoint[], windowSize: number): (number | null)[] {
  return points.map((_point, index) => {
    if (index < windowSize - 1) return null
    const slice = points.slice(index - windowSize + 1, index + 1)
    const total = slice.reduce((sum, item) => sum + item.close, 0)
    return Number((total / windowSize).toFixed(2))
  })
}

function buildKlineOption() {
  const dates = klineData.value.map(item => item.date)
  const candleData = klineData.value.map(item => ({
    value: [item.open, item.close, item.low, item.high],
    itemStyle: {
      color: getLimitUpColor(item),
      color0: '#1677ff',
      borderColor: getLimitUpColor(item),
      borderColor0: '#1677ff'
    }
  }))

  const series: any[] = [
    {
      name: stockInfo.value.stock_name || stockCode.value,
      type: 'candlestick',
      data: candleData,
      xAxisIndex: 0,
      yAxisIndex: 0,
      markPoint: {
        symbol: 'pin',
        symbolSize: 42,
        itemStyle: { color: '#8b000f' },
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
      xAxisIndex: 1,
      yAxisIndex: 2
    }
  ]

  if (showMa.value) {
    series.push({
      name: 'MA5',
      type: 'line',
      data: buildMaData(klineData.value, 5),
      smooth: true,
      symbol: 'none',
      xAxisIndex: 0,
      yAxisIndex: 0,
      lineStyle: { width: 1.5, color: '#7c3aed' }
    })
  }

  if (showOverlay.value) {
    compareSeries.value.forEach((overlay, index) => {
      const pointByDate = new Map(overlay.data.map(point => [point.date, point.change_pct_from_start]))
      series.push({
        name: overlay.name || overlay.symbol,
        type: 'line',
        data: dates.map(date => pointByDate.get(date) ?? null),
        smooth: true,
        symbol: 'none',
        xAxisIndex: 0,
        yAxisIndex: 1,
        lineStyle: {
          width: 1.5,
          color: ['#2563eb', '#f59e0b', '#059669'][index % 3]
        }
      })
    })
  }

  return {
    animation: false,
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { show: false },
    grid: [
      { left: 56, right: 58, top: 42, height: '58%' },
      { left: 56, right: 58, top: '76%', height: '14%' }
    ],
    xAxis: [
      { type: 'category', data: dates, scale: true, boundaryGap: true, axisLabel: { show: false } },
      { type: 'category', data: dates, gridIndex: 1, scale: true, boundaryGap: true }
    ],
    yAxis: [
      { scale: true, splitArea: { show: true } },
      { scale: true, position: 'right', axisLabel: { formatter: '{value}%' }, splitLine: { show: false } },
      { scale: true, gridIndex: 1, splitNumber: 2 }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 55, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], bottom: 8, height: 18, start: 55, end: 100 }
    ],
    series
  }
}

function buildTimelineOption() {
  const times = intradayData.value.map((item: any) => item.time)
  return {
    animation: false,
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    grid: [
      { left: 56, right: 24, top: 32, height: '58%' },
      { left: 56, right: 24, top: '76%', height: '14%' }
    ],
    xAxis: [
      { type: 'category', data: times, axisLabel: { show: false } },
      { type: 'category', data: times, gridIndex: 1 }
    ],
    yAxis: [
      { scale: true },
      { scale: true, gridIndex: 1, splitNumber: 2 }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], bottom: 8, height: 18, start: 0, end: 100 }
    ],
    series: [
      {
        name: '现价',
        type: 'line',
        data: intradayData.value.map((item: any) => item.price),
        smooth: true,
        symbol: 'none',
        xAxisIndex: 0,
        yAxisIndex: 0,
        lineStyle: { color: '#d82135' },
        areaStyle: { color: 'rgba(216, 33, 53, 0.08)' }
      },
      {
        name: '成交量',
        type: 'bar',
        data: intradayData.value.map((item: any) => item.volume),
        xAxisIndex: 1,
        yAxisIndex: 1,
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
  --chart-container-height: 460px;

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
    color: #d82135;
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

  .stock { background: #d82135; }
  .index { background: #2563eb; }
  .ma { background: #7c3aed; }
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

  .up { color: #d82135; }
  .down { color: #1677ff; }
}

.current-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 8px 0;
  padding: 10px;
  border-radius: 6px;
  background: #fff1f0;
  color: #d82135;
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
    color: #d82135;
  }

  &.sell strong {
    color: #1677ff;
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
      color: #d82135;
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
    --chart-container-height: 360px;
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
