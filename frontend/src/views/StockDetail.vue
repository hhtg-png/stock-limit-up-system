<template>
  <div class="stock-detail" v-loading="loading">
    <!-- 股票头部信息 -->
    <div class="stock-header card">
      <div class="basic-info">
        <h2>{{ stockInfo.stock_name }} <span class="code">{{ stockCode }}</span></h2>
        <div class="tags">
          <el-tag v-if="(stockInfo.continuous_limit_up_days || 0) > 1" type="info" size="small">
            {{ stockInfo.continuous_limit_up_days }}连板
          </el-tag>
          <el-tag :type="stockInfo.is_final_sealed ? 'info' : 'warning'" size="small">
            {{ stockInfo.is_final_sealed ? '封板' : '开板' }}
          </el-tag>
          <el-tag v-if="stockInfo.reason_category" size="small">{{ stockInfo.reason_category }}</el-tag>
          <el-tag v-if="stockInfo.market" type="info" size="small">{{ stockInfo.market }}</el-tag>
        </div>
      </div>
      <div class="price-info">
        <div class="current-price">{{ stockInfo.limit_up_price?.toFixed(2) }}</div>
      </div>
      <div class="action-btns">
        <el-button :icon="Star" @click="toggleWatch">
          {{ isWatched ? '取消关注' : '加入自选' }}
        </el-button>
      </div>
    </div>

    <!-- 核心数据 -->
    <div class="card detail-card">
      <el-descriptions :column="3" border size="default">
        <el-descriptions-item label="首封时间">
          <span class="highlight-value">{{ stockInfo.first_limit_up_time || '-' }}</span>
        </el-descriptions-item>
        <el-descriptions-item label="最终回封">
          <span class="highlight-value">{{ stockInfo.final_seal_time || '-' }}</span>
        </el-descriptions-item>
        <el-descriptions-item label="连板天数">
          <el-tag v-if="(stockInfo.continuous_limit_up_days || 0) > 1" type="danger" size="small">
            {{ stockInfo.continuous_limit_up_days }}板
          </el-tag>
          <span v-else>首板</span>
        </el-descriptions-item>
        <el-descriptions-item label="涨停价">{{ stockInfo.limit_up_price?.toFixed(2) || '-' }}</el-descriptions-item>
        <el-descriptions-item label="封单(万)">{{ stockInfo.seal_amount?.toFixed(0) || '-' }}</el-descriptions-item>
        <el-descriptions-item label="开板次数">{{ stockInfo.open_count ?? '-' }}</el-descriptions-item>
        <el-descriptions-item label="换手率">{{ formatTurnoverRate(stockInfo.turnover_rate) }}</el-descriptions-item>
        <el-descriptions-item label="成交额(万)">{{ stockInfo.amount?.toFixed(0) || '-' }}</el-descriptions-item>
        <el-descriptions-item label="行业">{{ stockInfo.industry || '-' }}</el-descriptions-item>
        <el-descriptions-item label="涨停原因" :span="3">{{ stockInfo.limit_up_reason || '-' }}</el-descriptions-item>
      </el-descriptions>
    </div>

    <el-row :gutter="16">
      <!-- 左侧：K线图和时间轴 -->
      <el-col :span="16">
        <!-- K线图 -->
        <div class="card chart-card">
          <div class="card-header">
            <h3>分时走势</h3>
          </div>
          <div ref="chartRef" class="chart-container"></div>
        </div>

        <!-- 涨停时间轴 -->
        <div class="card timeline-card">
          <div class="card-header">
            <h3>涨停时间轴</h3>
          </div>
          <el-timeline>
            <el-timeline-item
              v-for="item in timelineData"
              :key="item.change_time"
              :type="getTimelineType(item.status)"
              :timestamp="item.change_time"
              placement="top"
            >
              <div class="timeline-content">
                <span class="status">{{ getStatusText(item.status) }}</span>
                <span v-if="item.price" class="price">{{ item.price.toFixed(2) }}</span>
                <span v-if="item.seal_amount" class="seal">封单 {{ item.seal_amount.toFixed(0) }}万</span>
              </div>
            </el-timeline-item>
          </el-timeline>
        </div>
      </el-col>

      <!-- 右侧：盘口和大单 -->
      <el-col :span="8">
        <!-- 五档盘口 -->
        <div class="card orderbook-card">
          <div class="card-header">
            <h3>五档盘口</h3>
          </div>
          <div class="orderbook">
            <div class="asks">
              <div v-for="i in 5" :key="'ask' + i" class="order-row ask">
                <span class="label">卖{{ 6 - i }}</span>
                <span class="price">{{ orderBook.ask_prices?.[5 - i]?.toFixed(2) || '-' }}</span>
                <span class="volume">{{ orderBook.ask_volumes?.[5 - i] || '-' }}</span>
              </div>
            </div>
            <div class="current">
              <span class="label">当前</span>
              <span class="price text-up">{{ orderBook.current_price?.toFixed(2) || '-' }}</span>
            </div>
            <div class="bids">
              <div v-for="i in 5" :key="'bid' + i" class="order-row bid">
                <span class="label">买{{ i }}</span>
                <span class="price">{{ orderBook.bid_prices?.[i - 1]?.toFixed(2) || '-' }}</span>
                <span class="volume">{{ orderBook.bid_volumes?.[i - 1] || '-' }}</span>
              </div>
            </div>
          </div>
        </div>

        <!-- 大单列表 -->
        <div class="card bigorder-card">
          <div class="card-header">
            <h3>大单成交</h3>
            <span class="threshold-hint">≥{{ bigOrderThreshold }}手</span>
          </div>
          <div class="bigorder-list">
            <div v-if="filteredBigOrders.length === 0" class="empty-hint">
              暂无≥{{ bigOrderThreshold }}手的大单
            </div>
            <div 
              v-for="order in filteredBigOrders" 
              :key="order.id" 
              class="bigorder-item"
              :class="order.direction"
            >
              <span class="time">{{ formatTime(order.trade_time) }}</span>
              <span class="direction">{{ order.direction === 'buy' ? '买' : '卖' }}</span>
              <span class="price">{{ order.trade_price.toFixed(2) }}</span>
              <span class="volume">{{ order.trade_volume }}手</span>
              <span class="amount">{{ (order.trade_amount / 10000).toFixed(0) }}万</span>
            </div>
          </div>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import { Star } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import { getLimitUpDetail } from '@/api/limit-up'
import { getOrderBook, getBigOrders, getTimeline } from '@/api/market'
import { useConfigStore } from '@/stores/config'
import type { LimitUpDetail, LimitUpStatusChange } from '@/types/limit-up'
import type { OrderBook, BigOrder } from '@/types/market'

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

    // 获取分时数据并绑定图表
    fetchTimeline()
  } catch (e) {
    console.error('Fetch error:', e)
    ElMessage.error('获取数据失败')
  } finally {
    loading.value = false
  }
}

// 获取分时数据
async function fetchTimeline() {
  try {
    const data = await getTimeline(stockCode.value)
    if (data?.data?.length && chart) {
      const times = data.data.map((d: any) => d.time)
      const prices = data.data.map((d: any) => d.price)
      const volumes = data.data.map((d: any) => d.volume)
      
      chart.setOption({
        xAxis: [{ data: times }, { data: times }],
        series: [
          { data: prices },
          { data: volumes }
        ]
      })
    }
  } catch (e) {
    console.error('Fetch timeline error:', e)
  }
}

// 格式化换手率
function formatTurnoverRate(rate: number | undefined | null): string {
  if (rate == null || rate === 0) return '-'
  return rate.toFixed(2) + '%'
}

// 初始化图表
function initChart() {
  if (!chartRef.value) return
  
  chart = echarts.init(chartRef.value)
  chart.setOption({
    grid: [
      { left: 50, right: 20, top: 20, height: '60%' },
      { left: 50, right: 20, top: '75%', height: '15%' }
    ],
    xAxis: [
      { type: 'category', data: [], gridIndex: 0, axisLabel: { show: false } },
      { type: 'category', data: [], gridIndex: 1 }
    ],
    yAxis: [
      { type: 'value', gridIndex: 0, scale: true },
      { type: 'value', gridIndex: 1, scale: true }
    ],
    series: [
      {
        type: 'line',
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: [],
        smooth: true,
        lineStyle: { color: '#f5222d' },
        areaStyle: { color: 'rgba(245, 34, 45, 0.1)' }
      },
      {
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: [],
        itemStyle: { color: '#1890ff' }
      }
    ],
    tooltip: { trigger: 'axis' }
  })
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

// 时间轴类型
function getTimelineType(status: string) {
  switch (status) {
    case 'sealed': return 'danger'
    case 'opened': return 'warning'
    case 'resealed': return 'success'
    default: return 'info'
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
  initChart()
  fetchData()
  
  // 定时刷新
  const timer = setInterval(() => {
    getOrderBook(stockCode.value).then(ob => orderBook.value = ob).catch(() => {})
    getBigOrders(stockCode.value, { page_size: 20 }).then(orders => bigOrders.value = orders).catch(() => {})
  }, 5000)
  
  onUnmounted(() => {
    clearInterval(timer)
    chart?.dispose()
  })
})
</script>

<style lang="scss" scoped>
.stock-detail {
  .stock-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
    padding: 20px;

    .basic-info {
      h2 {
        margin: 0 0 8px 0;
        font-size: 20px;
        
        .code {
          font-size: 14px;
          color: #8c8c8c;
          font-weight: normal;
        }
      }
      
      .tags {
        display: flex;
        gap: 8px;
      }
    }

    .price-info {
      text-align: right;
      
      .current-price {
        font-size: 32px;
        font-weight: bold;
      }
      
      .change {
        color: #f5222d;
      }
    }
  }

  .card {
    background: #fff;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 16px;

    .card-header {
      margin-bottom: 16px;
      
      h3 {
        margin: 0;
        font-size: 16px;
      }
    }
  }
  
  .detail-card {
    .highlight-value {
      font-weight: 500;
    }
  }

  .chart-card {
    .chart-container {
      height: 300px;
    }
  }

  .timeline-card {
    .timeline-content {
      .status {
        font-weight: 500;
        margin-right: 8px;
      }
      .price {
        color: #666;
        margin-right: 8px;
      }
      .seal {
        color: #8c8c8c;
      }
    }
  }

  .orderbook-card {
    .orderbook {
      font-size: 14px;

      .order-row {
        display: flex;
        padding: 8px 12px;
        border-bottom: 1px solid #f5f5f5;
        
        .label {
          width: 50px;
          color: #8c8c8c;
        }
        .price {
          flex: 1;
          text-align: center;
          font-weight: 500;
          color: #333;
        }
        .volume {
          width: 100px;
          text-align: right;
        }
      }
      
      .current {
        display: flex;
        padding: 14px 12px;
        background: #f5f5f5;
        margin: 8px 0;
        border-radius: 4px;
        
        .label {
          width: 50px;
          color: #8c8c8c;
        }
        .price {
          flex: 1;
          text-align: center;
          font-size: 20px;
          font-weight: bold;
          color: #333;
        }
      }
    }
  }

  .bigorder-card {
    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;

      .threshold-hint {
        font-size: 12px;
        color: #909399;
      }
    }

    .bigorder-list {
      max-height: 320px;
      overflow-y: auto;

      .empty-hint {
        padding: 20px;
        text-align: center;
        color: #909399;
        font-size: 13px;
      }

      .bigorder-item {
        display: flex;
        padding: 10px 8px;
        border-bottom: 1px solid #f5f5f5;
        font-size: 14px;
        
        &:hover {
          background: #fafafa;
        }
        
        &.buy {
          .direction { color: #1890ff; }
        }
        &.sell {
          .direction { color: #8c8c8c; }
        }

        .time {
          width: 80px;
          color: #8c8c8c;
        }
        .direction {
          width: 35px;
          font-weight: 600;
        }
        .price {
          flex: 1;
        }
        .volume {
          width: 75px;
          text-align: right;
          color: #595959;
        }
        .amount {
          width: 70px;
          text-align: right;
          font-weight: 500;
        }
      }
    }
  }
}
</style>
