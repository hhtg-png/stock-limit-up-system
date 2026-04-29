<template>
  <div class="statistics" v-loading="loading">
    <div class="card summary-card">
      <div class="summary-main">
        <div class="summary-copy">
          <h3>市场复盘统计</h3>
          <p>用复盘指标跟踪连板高度、晋级率、情绪与量能变化。</p>
        </div>

        <el-radio-group v-model="timeRange" size="small">
          <el-radio-button label="7">近7天</el-radio-button>
          <el-radio-button label="30">近30天</el-radio-button>
          <el-radio-button label="90">近3月</el-radio-button>
        </el-radio-group>
      </div>

      <div class="summary-meta">
        <span>区间 {{ activeStartDate }} 至 {{ activeEndDate }}</span>
        <span>明细日期 {{ resolvedTradeDate }}</span>
        <el-tag v-if="hasFallback" type="warning" size="small">
          已回退到最近可用数据
        </el-tag>
      </div>
    </div>

    <el-row :gutter="16">
      <el-col :xs="24" :lg="12">
        <div class="card chart-card">
          <div class="card-header">
            <div>
              <h3>连板高度</h3>
              <p>龙头高度、二板家数与创业板涨停梯度。</p>
            </div>
          </div>
          <div ref="boardHeightChartRef" class="chart-container"></div>
        </div>
      </el-col>
      <el-col :xs="24" :lg="12">
        <div class="card chart-card">
          <div class="card-header">
            <div>
              <h3>晋级率</h3>
              <p>首板进二板、连板晋级与封板率联动观察。</p>
            </div>
          </div>
          <div ref="promotionRateChartRef" class="chart-container"></div>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :lg="12">
        <div class="card chart-card">
          <div class="card-header">
            <div>
              <h3>昨日涨停平均涨幅</h3>
              <p>昨日涨停与昨日连板次日反馈对比。</p>
            </div>
          </div>
          <div ref="yesterdayChangeChartRef" class="chart-container"></div>
        </div>
      </el-col>
      <el-col :xs="24" :lg="12">
        <div class="card chart-card">
          <div class="card-header">
            <div>
              <h3>涨跌停趋势</h3>
              <p>连板家数、涨停与跌停数量的情绪脉冲。</p>
            </div>
          </div>
          <div ref="limitTrendChartRef" class="chart-container"></div>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :lg="12">
        <div class="card chart-card">
          <div class="card-header">
            <div>
              <h3>沪深量能与涨跌家数</h3>
              <p>量能与非ST涨跌家数的市场广度对照。</p>
            </div>
          </div>
          <div ref="breadthChartRef" class="chart-container"></div>
        </div>
      </el-col>
      <el-col :xs="24" :lg="12">
        <div class="card chart-card">
          <div class="card-header">
            <div>
              <h3>涨停/炸板成交额</h3>
              <p>封板成交额与炸板成交额的资金去向。</p>
            </div>
          </div>
          <div ref="amountChartRef" class="chart-container"></div>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :lg="9">
        <div class="card detail-card">
          <div class="card-header">
            <div>
              <h3>连板梯队</h3>
              <p>当前复盘日的高标结构与封板状态。</p>
            </div>
          </div>

          <div v-if="ladderLevels.length" class="ladder-list">
            <div v-for="ladder in ladderLevels" :key="ladder.continuous_days" class="ladder-group">
              <div class="ladder-header">
                <span class="days-badge" :class="'days-' + Math.min(ladder.continuous_days, 10)">
                  {{ ladder.continuous_days }}连板
                </span>
                <span class="count">{{ ladder.count }}只</span>
                <span class="ladder-stat">封板 {{ getSealedCount(ladder) }}</span>
                <span class="ladder-stat ladder-stat-warning">炸板 {{ getOpenedCount(ladder) }}</span>
              </div>

              <div class="stock-chip-list">
                <button
                  v-for="stock in ladder.stocks"
                  :key="stock.stock_code"
                  type="button"
                  class="stock-chip"
                  @click="goToDetail(stock.stock_code)"
                >
                  <span class="code">{{ stock.stock_code }}</span>
                  <span class="name">{{ stock.stock_name }}</span>
                  <el-tag :type="getStockStatusType(stock)" size="small">
                    {{ getStockStatusLabel(stock) }}
                  </el-tag>
                </button>
              </div>
            </div>
          </div>

          <el-empty v-else description="暂无连板梯队数据" />
        </div>
      </el-col>

      <el-col :xs="24" :lg="15">
        <div class="card detail-card">
          <div class="card-header">
            <div>
              <h3>复盘明细</h3>
              <p>按连板高度与成交额排序的个股复盘列表。</p>
            </div>
          </div>

          <div class="detail-summary">
            <div class="summary-item">
              <span class="label">复盘个股</span>
              <strong>{{ detailStocks.length }}</strong>
            </div>
            <div class="summary-item">
              <span class="label">封板收盘</span>
              <strong>{{ sealedCloseCount }}</strong>
            </div>
            <div class="summary-item">
              <span class="label">炸板收盘</span>
              <strong>{{ openedCloseCount }}</strong>
            </div>
            <div class="summary-item">
              <span class="label">主导题材</span>
              <strong>{{ strongestReason }}</strong>
            </div>
          </div>

          <el-table
            :data="detailStocks"
            size="small"
            stripe
            max-height="420"
            empty-text="暂无复盘明细"
            @row-click="handleRowClick"
          >
            <el-table-column prop="stock_name" label="个股" min-width="180">
              <template #default="{ row }">
                <div class="stock-cell">
                  <span class="name">{{ row.stock_name }}</span>
                  <span class="code">{{ row.stock_code }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column prop="today_continuous_days" label="连板" width="80" align="center">
              <template #default="{ row }">
                <span>{{ row.today_continuous_days }}板</span>
              </template>
            </el-table-column>
            <el-table-column label="收盘状态" width="100" align="center">
              <template #default="{ row }">
                <el-tag :type="getStockStatusType(row)" size="small">
                  {{ getStockStatusLabel(row) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="change_pct" label="涨跌幅" width="100" align="right">
              <template #default="{ row }">
                <span :class="getChangeClass(row.change_pct)">
                  {{ formatPercent(row.change_pct) }}
                </span>
              </template>
            </el-table-column>
            <el-table-column prop="amount" label="成交额" width="120" align="right">
              <template #default="{ row }">
                {{ formatAmount(row.amount) }}
              </template>
            </el-table-column>
            <el-table-column prop="limit_up_reason" label="涨停原因" min-width="160" show-overflow-tooltip>
              <template #default="{ row }">
                {{ row.limit_up_reason || '-' }}
              </template>
            </el-table-column>
          </el-table>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import dayjs from 'dayjs'
import { getMarketReviewDaily, getMarketReviewDetail, getMarketReviewLadder } from '@/api'
import type {
  MarketReviewDailyRow,
  MarketReviewDetailResponse,
  MarketReviewDetailStock,
  MarketReviewLadderLevel,
  MarketReviewLadderResponse
} from '@/types/market'

const router = useRouter()

const timeRange = ref('30')
const loading = ref(false)
const activeStartDate = ref(dayjs().subtract(29, 'day').format('YYYY-MM-DD'))
const activeEndDate = ref(dayjs().format('YYYY-MM-DD'))

const dailySeries = ref<string[]>([])
const dailyRows = ref<MarketReviewDailyRow[]>([])
const detailResponse = ref<MarketReviewDetailResponse | null>(null)
const ladderResponse = ref<MarketReviewLadderResponse | null>(null)

const boardHeightChartRef = ref<HTMLElement>()
const promotionRateChartRef = ref<HTMLElement>()
const yesterdayChangeChartRef = ref<HTMLElement>()
const limitTrendChartRef = ref<HTMLElement>()
const breadthChartRef = ref<HTMLElement>()
const amountChartRef = ref<HTMLElement>()

let boardHeightChart: echarts.ECharts | null = null
let promotionRateChart: echarts.ECharts | null = null
let yesterdayChangeChart: echarts.ECharts | null = null
let limitTrendChart: echarts.ECharts | null = null
let breadthChart: echarts.ECharts | null = null
let amountChart: echarts.ECharts | null = null

const detailStocks = computed(() => detailResponse.value?.stocks ?? [])
const ladderLevels = computed(() => ladderResponse.value?.ladders ?? [])
const resolvedTradeDate = computed(
  () => detailResponse.value?.trade_date || ladderResponse.value?.trade_date || activeEndDate.value
)
const hasFallback = computed(
  () => Boolean(detailResponse.value?.is_fallback || ladderResponse.value?.is_fallback)
)
const sealedCloseCount = computed(
  () => detailStocks.value.filter(stock => stock.today_sealed_close).length
)
const openedCloseCount = computed(
  () => detailStocks.value.filter(stock => stock.today_opened_close).length
)
const strongestReason = computed(() => {
  const counter = new Map<string, number>()

  detailStocks.value.forEach(stock => {
    if (!stock.limit_up_reason) {
      return
    }
    counter.set(stock.limit_up_reason, (counter.get(stock.limit_up_reason) ?? 0) + 1)
  })

  let winner = '暂无'
  let maxCount = 0
  counter.forEach((count, reason) => {
    if (count > maxCount) {
      winner = `${reason} (${count})`
      maxCount = count
    }
  })

  return winner
})

function getDateRange() {
  const days = Number.parseInt(timeRange.value, 10)
  const endDate = dayjs().format('YYYY-MM-DD')
  const startDate = dayjs()
    .subtract(Math.max(days - 1, 0), 'day')
    .format('YYYY-MM-DD')

  return { startDate, endDate }
}

function formatDateLabel(value: string) {
  return dayjs(value).format('MM-DD')
}

function formatPercent(value: number | null | undefined) {
  if (value == null) {
    return '-'
  }
  const sign = value > 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

function formatAmount(value: number) {
  if (Math.abs(value) >= 100000000) {
    return `${(value / 100000000).toFixed(2)}亿`
  }
  if (Math.abs(value) >= 10000) {
    return `${(value / 10000).toFixed(2)}万`
  }
  return value.toFixed(0)
}

function getChangeClass(value: number | null | undefined) {
  if (value == null || value === 0) {
    return 'neutral'
  }
  return value > 0 ? 'positive' : 'negative'
}

function getStockStatusType(stock: MarketReviewDetailStock) {
  if (stock.today_sealed_close) {
    return 'danger'
  }
  if (stock.today_opened_close) {
    return 'warning'
  }
  return 'info'
}

function getStockStatusLabel(stock: MarketReviewDetailStock) {
  if (stock.today_sealed_close) {
    return '封板'
  }
  if (stock.today_opened_close) {
    return '炸板'
  }
  return '观察'
}

function getSealedCount(ladder: MarketReviewLadderLevel) {
  return ladder.stocks.filter(stock => stock.today_sealed_close).length
}

function getOpenedCount(ladder: MarketReviewLadderLevel) {
  return ladder.stocks.filter(stock => stock.today_opened_close).length
}

function createEmptyOption(): echarts.EChartsOption {
  return {
    graphic: {
      type: 'text',
      left: 'center',
      top: 'middle',
      style: {
        text: '暂无复盘数据',
        fill: '#999',
        fontSize: 14
      }
    },
    xAxis: {
      type: 'category',
      data: [],
      show: false
    },
    yAxis: {
      type: 'value',
      show: false
    },
    series: []
  }
}

function getBaseGridOption(boundaryGap = false): Pick<echarts.EChartsOption, 'grid' | 'tooltip' | 'legend' | 'xAxis'> {
  return {
    tooltip: {
      trigger: 'axis'
    },
    legend: {
      top: 0
    },
    grid: {
      left: 56,
      right: 24,
      top: 48,
      bottom: 40
    },
    xAxis: {
      type: 'category',
      boundaryGap,
      data: dailySeries.value,
      axisLabel: {
        color: '#666',
        formatter: (value: string) => formatDateLabel(value)
      },
      axisLine: {
        lineStyle: {
          color: '#d9d9d9'
        }
      }
    }
  }
}

function updateCharts() {
  const charts = [
    boardHeightChart,
    promotionRateChart,
    yesterdayChangeChart,
    limitTrendChart,
    breadthChart,
    amountChart
  ]

  if (!dailyRows.value.length) {
    charts.forEach(chart => chart?.setOption(createEmptyOption(), true))
    return
  }

  boardHeightChart?.setOption(
    {
      ...getBaseGridOption(),
      yAxis: {
        type: 'value',
        minInterval: 1
      },
      series: [
        {
          name: '最高板高度',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          data: dailyRows.value.map(row => row.max_board_height),
          itemStyle: { color: '#f5222d' }
        },
        {
          name: '二板家数',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          data: dailyRows.value.map(row => row.second_board_height),
          itemStyle: { color: '#fa8c16' }
        },
        {
          name: '创业板涨停',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          data: dailyRows.value.map(row => row.gem_board_height),
          itemStyle: { color: '#1677ff' }
        }
      ]
    },
    true
  )

  promotionRateChart?.setOption(
    {
      ...getBaseGridOption(),
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: '{value}%'
        }
      },
      series: [
        {
          name: '首板晋级率',
          type: 'line',
          smooth: true,
          data: dailyRows.value.map(row => row.first_to_second_rate),
          itemStyle: { color: '#f5222d' },
          areaStyle: {
            color: 'rgba(245, 34, 45, 0.08)'
          }
        },
        {
          name: '连板晋级率',
          type: 'line',
          smooth: true,
          data: dailyRows.value.map(row => row.continuous_promotion_rate),
          itemStyle: { color: '#722ed1' },
          areaStyle: {
            color: 'rgba(114, 46, 209, 0.08)'
          }
        },
        {
          name: '封板率',
          type: 'line',
          smooth: true,
          data: dailyRows.value.map(row => row.seal_rate),
          itemStyle: { color: '#13c2c2' }
        }
      ]
    },
    true
  )

  yesterdayChangeChart?.setOption(
    {
      ...getBaseGridOption(),
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: '{value}%'
        }
      },
      series: [
        {
          name: '昨日涨停平均涨幅',
          type: 'bar',
          data: dailyRows.value.map(row => row.yesterday_limit_up_avg_change),
          itemStyle: { color: '#ff7875' },
          barMaxWidth: 28
        },
        {
          name: '昨日连板平均涨幅',
          type: 'line',
          smooth: true,
          data: dailyRows.value.map(row => row.yesterday_continuous_avg_change),
          itemStyle: { color: '#52c41a' }
        }
      ]
    },
    true
  )

  limitTrendChart?.setOption(
    {
      ...getBaseGridOption(true),
      yAxis: {
        type: 'value',
        minInterval: 1
      },
      series: [
        {
          name: '连板家数',
          type: 'line',
          smooth: true,
          data: dailyRows.value.map(row => row.continuous_count),
          itemStyle: { color: '#722ed1' }
        },
        {
          name: '涨停数',
          type: 'bar',
          data: dailyRows.value.map(row => row.limit_up_count),
          itemStyle: { color: '#f5222d' },
          barMaxWidth: 22
        },
        {
          name: '跌停数',
          type: 'bar',
          data: dailyRows.value.map(row => row.limit_down_count),
          itemStyle: { color: '#52c41a' },
          barMaxWidth: 22
        }
      ]
    },
    true
  )

  breadthChart?.setOption(
    {
      ...getBaseGridOption(true),
      yAxis: [
        {
          type: 'value',
          name: '成交额',
          axisLabel: {
            formatter: (value: number) => formatAmount(value)
          }
        },
        {
          type: 'value',
          name: '家数',
          minInterval: 1
        }
      ],
      series: [
        {
          name: '沪深成交额',
          type: 'line',
          smooth: true,
          data: dailyRows.value.map(row => row.market_turnover),
          yAxisIndex: 0,
          itemStyle: { color: '#1677ff' }
        },
        {
          name: '非ST上涨家数',
          type: 'bar',
          data: dailyRows.value.map(row => row.up_count_ex_st),
          yAxisIndex: 1,
          itemStyle: { color: '#f5222d' },
          barMaxWidth: 18
        },
        {
          name: '非ST下跌家数',
          type: 'bar',
          data: dailyRows.value.map(row => row.down_count_ex_st),
          yAxisIndex: 1,
          itemStyle: { color: '#52c41a' },
          barMaxWidth: 18
        }
      ]
    },
    true
  )

  amountChart?.setOption(
    {
      ...getBaseGridOption(true),
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: (value: number) => formatAmount(value)
        }
      },
      series: [
        {
          name: '涨停成交额',
          type: 'bar',
          data: dailyRows.value.map(row => row.limit_up_amount),
          itemStyle: { color: '#f5222d' },
          barMaxWidth: 26
        },
        {
          name: '炸板成交额',
          type: 'bar',
          data: dailyRows.value.map(row => row.broken_amount),
          itemStyle: { color: '#faad14' },
          barMaxWidth: 26
        }
      ]
    },
    true
  )
}

function initCharts() {
  if (boardHeightChartRef.value) {
    boardHeightChart = echarts.init(boardHeightChartRef.value)
  }
  if (promotionRateChartRef.value) {
    promotionRateChart = echarts.init(promotionRateChartRef.value)
  }
  if (yesterdayChangeChartRef.value) {
    yesterdayChangeChart = echarts.init(yesterdayChangeChartRef.value)
  }
  if (limitTrendChartRef.value) {
    limitTrendChart = echarts.init(limitTrendChartRef.value)
  }
  if (breadthChartRef.value) {
    breadthChart = echarts.init(breadthChartRef.value)
  }
  if (amountChartRef.value) {
    amountChart = echarts.init(amountChartRef.value)
  }
}

function resizeCharts() {
  boardHeightChart?.resize()
  promotionRateChart?.resize()
  yesterdayChangeChart?.resize()
  limitTrendChart?.resize()
  breadthChart?.resize()
  amountChart?.resize()
}

function disposeCharts() {
  boardHeightChart?.dispose()
  promotionRateChart?.dispose()
  yesterdayChangeChart?.dispose()
  limitTrendChart?.dispose()
  breadthChart?.dispose()
  amountChart?.dispose()

  boardHeightChart = null
  promotionRateChart = null
  yesterdayChangeChart = null
  limitTrendChart = null
  breadthChart = null
  amountChart = null
}

async function fetchData() {
  const { startDate, endDate } = getDateRange()
  activeStartDate.value = startDate
  activeEndDate.value = endDate
  loading.value = true

  try {
    const [dailyResponse, detail, ladder] = await Promise.all([
      getMarketReviewDaily({
        start_date: startDate,
        end_date: endDate
      }),
      getMarketReviewDetail(endDate),
      getMarketReviewLadder(endDate)
    ])

    dailySeries.value = dailyResponse.data.series
    dailyRows.value = dailyResponse.data.rows
    detailResponse.value = detail
    ladderResponse.value = ladder
    updateCharts()
  } catch (error) {
    console.error('Fetch market review error:', error)
    ElMessage.error('获取市场复盘数据失败')
  } finally {
    loading.value = false
  }
}

function goToDetail(stockCode: string) {
  router.push(`/stock/${stockCode}`)
}

function handleRowClick(row: MarketReviewDetailStock) {
  goToDetail(row.stock_code)
}

watch(timeRange, () => {
  fetchData()
})

onMounted(() => {
  initCharts()
  updateCharts()
  fetchData()
  window.addEventListener('resize', resizeCharts)
})

onUnmounted(() => {
  window.removeEventListener('resize', resizeCharts)
  disposeCharts()
})
</script>

<style lang="scss" scoped>
.statistics {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.card {
  background: #fff;
  border-radius: 8px;
  padding: 16px;
}

.summary-card {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.summary-main {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}

.summary-copy {
  h3 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
    color: #262626;
  }

  p {
    margin: 6px 0 0;
    color: #666;
    font-size: 13px;
  }
}

.summary-meta {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  color: #666;
  font-size: 13px;
}

.chart-card,
.detail-card {
  margin-bottom: 16px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 16px;

  h3 {
    margin: 0;
    font-size: 16px;
    font-weight: 600;
    color: #262626;
  }

  p {
    margin: 6px 0 0;
    color: #666;
    font-size: 13px;
  }
}

.chart-container {
  height: 320px;
}

.detail-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}

.summary-item {
  padding: 12px;
  border-radius: 8px;
  background: #fafafa;

  .label {
    display: block;
    margin-bottom: 6px;
    color: #8c8c8c;
    font-size: 12px;
  }

  strong {
    color: #262626;
    font-size: 16px;
    font-weight: 600;
  }
}

.ladder-list {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.ladder-group {
  padding: 12px;
  border-radius: 8px;
  background: #fafafa;
}

.ladder-header {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 10px;
}

.days-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 70px;
  padding: 4px 10px;
  border-radius: 999px;
  color: #fff;
  font-size: 13px;
  font-weight: 600;

  &.days-2 { background: #1677ff; }
  &.days-3 { background: #52c41a; }
  &.days-4 { background: #fa8c16; }
  &.days-5 { background: #f5222d; }
  &.days-6 { background: #eb2f96; }
  &.days-7 { background: #722ed1; }
  &.days-8 { background: #13c2c2; }
  &.days-9 { background: #2f54eb; }
  &.days-10 { background: #a61d24; }
}

.count {
  color: #595959;
  font-size: 13px;
  font-weight: 500;
}

.ladder-stat {
  color: #f5222d;
  font-size: 12px;
}

.ladder-stat-warning {
  color: #fa8c16;
}

.stock-chip-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.stock-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: none;
  border-radius: 6px;
  background: #fff;
  padding: 6px 10px;
  cursor: pointer;
  transition: background 0.2s ease;

  &:hover {
    background: #f0f7ff;
  }

  .code {
    color: #1677ff;
    font-size: 12px;
    font-family: monospace;
  }

  .name {
    color: #262626;
    font-size: 13px;
    font-weight: 500;
  }
}

.stock-cell {
  display: flex;
  flex-direction: column;

  .name {
    color: #262626;
    font-weight: 500;
  }

  .code {
    color: #1677ff;
    font-size: 12px;
    font-family: monospace;
  }
}

.positive {
  color: #f5222d;
  font-weight: 500;
}

.negative {
  color: #52c41a;
  font-weight: 500;
}

.neutral {
  color: #8c8c8c;
}

@media (max-width: 991px) {
  .detail-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 767px) {
  .card {
    padding: 14px;
  }

  .chart-container {
    height: 280px;
  }

  .detail-summary {
    grid-template-columns: 1fr;
  }
}
</style>
