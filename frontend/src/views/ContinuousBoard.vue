<template>
  <div class="continuous-board">
    <!-- 今日连板梯队 -->
    <div class="section card">
      <div class="section-header">
        <h3>今日连板梯队</h3>
        <div class="header-right">
          <el-button type="primary" size="small" @click="fetchTodayData" :loading="loadingToday">
            刷新
          </el-button>
        </div>
      </div>
      
      <div class="ladder-container" v-loading="loadingToday">
        <div v-for="ladder in todayLadder" :key="ladder.continuous_days" class="ladder-group">
          <div class="ladder-title">
            <span class="days-badge" :class="'days-' + Math.min(ladder.continuous_days, 10)">
              {{ ladder.continuous_days }}连板
            </span>
            <span class="count">({{ ladder.count }}只)</span>
          </div>
          <div class="stock-list">
            <div 
              v-for="stock in ladder.stocks" 
              :key="stock.stock_code"
              class="stock-item"
              @click="goToDetail(stock.stock_code)"
            >
              <span class="code">{{ stock.stock_code }}</span>
              <span class="name">{{ stock.stock_name }}</span>
              <el-tag v-if="stock.is_sealed !== undefined" :type="stock.is_sealed ? 'danger' : 'warning'" size="small">
                {{ stock.is_sealed ? '封板' : '开板' }}
              </el-tag>
              <span class="seal-volume" v-if="stock.is_sealed && stock.bid1_volume">
                {{ formatVolume(stock.bid1_volume) }}
              </span>
              <span class="turnover" v-if="stock.real_turnover_rate">
                {{ stock.real_turnover_rate.toFixed(1) }}%
              </span>
              <span
                v-if="stock.change_pct != null"
                class="change"
                :class="{ positive: (stock.change_pct || 0) > 0, negative: (stock.change_pct || 0) < 0 }"
              >
                {{ formatChange(stock.change_pct) }}
              </span>
            </div>
          </div>
        </div>
        <el-empty v-if="!loadingToday && todayLadder.length === 0" description="暂无数据" />
      </div>
    </div>

    <!-- 昨日连板今日表现 -->
    <div class="section card">
      <div class="section-header">
        <h3>昨日连板今日表现</h3>
        <div class="header-right">
          <span class="date-label" v-if="yesterdayResponse">
            {{ yesterdayResponse.yesterday_date }} → {{ yesterdayResponse.trade_date }}
          </span>
          <el-button type="primary" size="small" @click="fetchYesterdayData" :loading="loadingYesterday">
            刷新
          </el-button>
        </div>
      </div>

      <div class="ladder-container" v-loading="loadingYesterday">
        <div v-for="ladder in yesterdayLadder" :key="ladder.continuous_days" class="ladder-group">
          <div class="ladder-title">
            <span class="days-badge" :class="'days-' + Math.min(ladder.continuous_days, 10)">
              {{ ladder.continuous_days }}连板
            </span>
            <span class="count">({{ ladder.count }}只)</span>
            <span class="stat-info">
              <span class="sealed">晋级 {{ ladder.sealed_count }}</span>
              <span class="opened">炸板 {{ ladder.opened_count }}</span>
              <span class="broken">断板 {{ ladder.broken_count }}</span>
            </span>
          </div>
          <div class="stock-list">
            <div 
              v-for="stock in ladder.stocks" 
              :key="stock.stock_code"
              class="stock-item"
              :class="'status-' + stock.today_status"
              @click="goToDetail(stock.stock_code)"
            >
              <span class="code">{{ stock.stock_code }}</span>
              <span class="name">{{ stock.stock_name }}</span>
              <el-tag :type="getStatusType(stock.today_status)" size="small">
                {{ getStatusText(stock.today_status) }}
              </el-tag>
              <span class="change" :class="{ positive: (stock.today_change_pct || 0) > 0, negative: (stock.today_change_pct || 0) < 0 }">
                {{ formatChange(stock.today_change_pct) }}
              </span>
            </div>
          </div>
        </div>
        <el-empty v-if="!loadingYesterday && yesterdayLadder.length === 0" description="暂无数据" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { getYesterdayContinuous } from '@/api/statistics'
import type { ContinuousLadder, YesterdayContinuousLadder, YesterdayContinuousResponse } from '@/types/market'

const router = useRouter()

const loadingToday = ref(false)
const loadingYesterday = ref(false)
const todayLadder = ref<ContinuousLadder[]>([])
const yesterdayLadder = ref<YesterdayContinuousLadder[]>([])
const yesterdayResponse = ref<YesterdayContinuousResponse | null>(null)

let ws: WebSocket | null = null
let reconnectTimer: number | null = null

// 初始化WebSocket连接
function initWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const host = window.location.host
  const wsUrl = `${protocol}//${host}/ws/continuous`
  
  ws = new WebSocket(wsUrl)
  
  ws.onopen = () => {
    console.log('Continuous WebSocket connected')
    loadingToday.value = false
  }
  
  ws.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data)
      if (message.type === 'continuous_ladder') {
        todayLadder.value = message.data
      }
    } catch (e) {
      console.error('Parse message error:', e)
    }
  }
  
  ws.onclose = () => {
    console.log('Continuous WebSocket disconnected')
    // 重连
    reconnectTimer = window.setTimeout(() => {
      initWebSocket()
    }, 3000)
  }
  
  ws.onerror = (error) => {
    console.error('WebSocket error:', error)
  }
}

// 关闭WebSocket
function closeWebSocket() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer)
    reconnectTimer = null
  }
  if (ws) {
    ws.close()
    ws = null
  }
}

// 手动刷新今日数据
function fetchTodayData() {
  // WebSocket自动推送，这里只是重连WebSocket
  closeWebSocket()
  loadingToday.value = true
  initWebSocket()
}

// 获取昨日连板数据
async function fetchYesterdayData() {
  loadingYesterday.value = true
  try {
    const response = await getYesterdayContinuous()
    yesterdayResponse.value = response
    yesterdayLadder.value = response.data
  } catch (e) {
    console.error('获取昨日连板数据失败:', e)
    ElMessage.error('获取昨日连板数据失败')
  } finally {
    loadingYesterday.value = false
  }
}

// 跳转详情
function goToDetail(code: string) {
  router.push(`/stock/${code}`)
}

// 获取状态标签类型
function getStatusType(status: string): string {
  switch (status) {
    case 'sealed': return 'danger'
    case 'opened': return 'warning'
    case 'broken': return 'info'
    default: return 'info'
  }
}

// 获取状态文本
function getStatusText(status: string): string {
  switch (status) {
    case 'sealed': return '晋级'
    case 'opened': return '炸板'
    case 'broken': return '断板'
    default: return status
  }
}

// 格式化涨跌幅
function formatChange(change: number | null): string {
  if (change == null) return '-'
  const sign = change > 0 ? '+' : ''
  return `${sign}${change.toFixed(2)}%`
}

// 格式化封单量（手）
function formatVolume(volume: number): string {
  if (volume >= 10000) {
    return (volume / 10000).toFixed(1) + '万手'
  } else if (volume >= 1000) {
    return (volume / 1000).toFixed(1) + '千手'
  }
  return volume + '手'
}

onMounted(() => {
  loadingToday.value = true
  initWebSocket()
  fetchYesterdayData()
})

onUnmounted(() => {
  closeWebSocket()
})
</script>

<style lang="scss" scoped>
.continuous-board {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.card {
  background: #fff;
  border-radius: 8px;
  padding: 16px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  
  h3 {
    margin: 0;
    font-size: 16px;
    font-weight: 600;
  }
  
  .header-right {
    display: flex;
    align-items: center;
    gap: 12px;
    
    .date-label {
      font-size: 13px;
      color: #666;
    }
  }
}

.ladder-container {
  min-height: 200px;
}

.ladder-group {
  margin-bottom: 20px;
  
  &:last-child {
    margin-bottom: 0;
  }
}

.ladder-title {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
  padding-bottom: 8px;
  border-bottom: 1px solid #f0f0f0;
  
  .days-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 4px;
    font-size: 14px;
    font-weight: 600;
    color: #fff;
    
    &.days-2 { background: #1890ff; }
    &.days-3 { background: #52c41a; }
    &.days-4 { background: #faad14; }
    &.days-5 { background: #fa541c; }
    &.days-6 { background: #eb2f96; }
    &.days-7 { background: #722ed1; }
    &.days-8 { background: #13c2c2; }
    &.days-9 { background: #2f54eb; }
    &.days-10 { background: #f5222d; }
  }
  
  .count {
    color: #999;
    font-size: 13px;
  }
  
  .stat-info {
    margin-left: auto;
    display: flex;
    gap: 12px;
    font-size: 13px;
    
    .sealed { color: #f5222d; }
    .opened { color: #faad14; }
    .broken { color: #8c8c8c; }
  }
}

.stock-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.stock-item {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 10px;
  background: #fafafa;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: #e6f7ff;
  }
  
  &.status-sealed {
    background: #fff1f0;
    &:hover { background: #ffccc7; }
  }
  
  &.status-opened {
    background: #fffbe6;
    &:hover { background: #fff1b8; }
  }
  
  &.status-broken {
    background: #f5f5f5;
    &:hover { background: #e8e8e8; }
  }
  
  .code {
    color: #1890ff;
    font-size: 13px;
    font-family: monospace;
  }
  
  .name {
    font-size: 13px;
    font-weight: 500;
  }
  
  .time {
    color: #999;
    font-size: 12px;
  }
  
  .change {
    font-size: 12px;
    font-weight: 500;
    
    &.positive { color: #f5222d; }
    &.negative { color: #52c41a; }
  }
  
  .seal-volume {
    font-size: 11px;
    color: #f5222d;
    background: #fff1f0;
    padding: 1px 4px;
    border-radius: 2px;
  }
  
  .turnover {
    font-size: 11px;
    color: #1890ff;
    background: #e6f7ff;
    padding: 1px 4px;
    border-radius: 2px;
  }
}
</style>
