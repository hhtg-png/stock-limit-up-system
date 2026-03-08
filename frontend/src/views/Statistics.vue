<template>
  <div class="statistics">
    <el-row :gutter="16">
      <!-- 趋势图 -->
      <el-col :span="16">
        <div class="card">
          <div class="card-header">
            <h3>涨停数量趋势</h3>
            <el-radio-group v-model="timeRange" size="small">
              <el-radio-button label="7">近7天</el-radio-button>
              <el-radio-button label="30">近30天</el-radio-button>
              <el-radio-button label="90">近3月</el-radio-button>
            </el-radio-group>
          </div>
          <div ref="trendChartRef" class="chart-container"></div>
        </div>
      </el-col>

      <!-- 连板分布 -->
      <el-col :span="8">
        <div class="card">
          <div class="card-header">
            <h3>连板分布</h3>
          </div>
          <div ref="pieChartRef" class="chart-container"></div>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <!-- 炸板率趋势 -->
      <el-col :span="12">
        <div class="card">
          <div class="card-header">
            <h3>炸板率趋势</h3>
          </div>
          <div ref="breakChartRef" class="chart-container"></div>
        </div>
      </el-col>

      <!-- 板块统计 -->
      <el-col :span="12">
        <div class="card">
          <div class="card-header">
            <h3>板块涨停排名</h3>
          </div>
          <div ref="sectorChartRef" class="chart-container"></div>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { getDailyStats, getSectorStats } from '@/api/statistics'
import dayjs from 'dayjs'

const timeRange = ref('30')

const trendChartRef = ref<HTMLElement>()
const pieChartRef = ref<HTMLElement>()
const breakChartRef = ref<HTMLElement>()
const sectorChartRef = ref<HTMLElement>()

let trendChart: echarts.ECharts | null = null
let pieChart: echarts.ECharts | null = null
let breakChart: echarts.ECharts | null = null
let sectorChart: echarts.ECharts | null = null

// 获取数据
async function fetchData() {
  const endDate = dayjs().format('YYYY-MM-DD')
  const startDate = dayjs().subtract(parseInt(timeRange.value), 'day').format('YYYY-MM-DD')
  
  try {
    const [dailyStats, sectorStats] = await Promise.all([
      getDailyStats({ start_date: startDate, end_date: endDate }),
      getSectorStats()
    ])
    
    updateTrendChart(dailyStats)
    updatePieChart(dailyStats[0])
    updateBreakChart(dailyStats)
    updateSectorChart(sectorStats)
  } catch (e) {
    console.error('Fetch stats error:', e)
  }
}

// 更新趋势图
function updateTrendChart(data: any[]) {
  if (!trendChart) return
  
  const dates = data.map(d => d.trade_date).reverse()
  const totals = data.map(d => d.total_limit_up).reverse()
  const news = data.map(d => d.new_limit_up).reverse()
  
  trendChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['涨停总数', '首板数量'] },
    xAxis: { type: 'category', data: dates },
    yAxis: { type: 'value' },
    series: [
      { name: '涨停总数', type: 'line', data: totals, smooth: true },
      { name: '首板数量', type: 'line', data: news, smooth: true }
    ]
  })
}

// 更新饼图
function updatePieChart(data: any) {
  if (!pieChart || !data) return
  
  pieChart.setOption({
    tooltip: { trigger: 'item' },
    series: [{
      type: 'pie',
      radius: ['40%', '70%'],
      data: [
        { value: data.new_limit_up || 0, name: '首板' },
        { value: data.continuous_2 || 0, name: '2连板' },
        { value: data.continuous_3 || 0, name: '3连板' },
        { value: data.continuous_4_plus || 0, name: '4板+' }
      ]
    }]
  })
}

// 更新炸板率图
function updateBreakChart(data: any[]) {
  if (!breakChart) return
  
  const dates = data.map(d => d.trade_date).reverse()
  const rates = data.map(d => d.break_rate).reverse()
  
  breakChart.setOption({
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: dates },
    yAxis: { type: 'value', axisLabel: { formatter: '{value}%' } },
    series: [{
      type: 'bar',
      data: rates,
      itemStyle: { color: '#faad14' }
    }]
  })
}

// 更新板块图
function updateSectorChart(data: any[]) {
  if (!sectorChart) return
  
  const sectors = data.slice(0, 10)
  
  sectorChart.setOption({
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'value' },
    yAxis: { 
      type: 'category', 
      data: sectors.map(s => s.sector_name).reverse() 
    },
    series: [{
      type: 'bar',
      data: sectors.map(s => s.limit_up_count).reverse(),
      itemStyle: { color: '#f5222d' }
    }]
  })
}

// 初始化图表
function initCharts() {
  if (trendChartRef.value) {
    trendChart = echarts.init(trendChartRef.value)
  }
  if (pieChartRef.value) {
    pieChart = echarts.init(pieChartRef.value)
  }
  if (breakChartRef.value) {
    breakChart = echarts.init(breakChartRef.value)
  }
  if (sectorChartRef.value) {
    sectorChart = echarts.init(sectorChartRef.value)
  }
}

watch(timeRange, () => fetchData())

onMounted(() => {
  initCharts()
  fetchData()
  
  const handleResize = () => {
    trendChart?.resize()
    pieChart?.resize()
    breakChart?.resize()
    sectorChart?.resize()
  }
  window.addEventListener('resize', handleResize)
  
  onUnmounted(() => {
    window.removeEventListener('resize', handleResize)
    trendChart?.dispose()
    pieChart?.dispose()
    breakChart?.dispose()
    sectorChart?.dispose()
  })
})
</script>

<style lang="scss" scoped>
.statistics {
  .card {
    background: #fff;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 16px;

    .card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 16px;

      h3 {
        margin: 0;
        font-size: 16px;
      }
    }

    .chart-container {
      height: 300px;
    }
  }
}
</style>
