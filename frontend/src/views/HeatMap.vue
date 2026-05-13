<template>
  <div class="heatmap">
    <div class="card">
      <div class="card-header">
        <h3>涨停热力图</h3>
        <el-radio-group v-model="viewType" size="small">
          <el-radio-button label="sector">板块视图</el-radio-button>
          <el-radio-button label="continuous">连板视图</el-radio-button>
        </el-radio-group>
      </div>
      <div ref="heatmapRef" class="heatmap-container"></div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { getSectorStats, getContinuousLadder } from '@/api/statistics'
import type { ContinuousLadder, SectorStats } from '@/types/market'

const viewType = ref('sector')
const heatmapRef = ref<HTMLElement>()

let chart: echarts.ECharts | null = null

async function fetchData() {
  if (viewType.value === 'sector') {
    const response = await getSectorStats()
    updateSectorHeatmap(response.data)
  } else {
    const response = await getContinuousLadder()
    updateContinuousHeatmap(response.data)
  }
}

function updateSectorHeatmap(data: SectorStats[]) {
  if (!chart) return
  
  // 转换为treemap数据格式
  const treeData = data.map(sector => ({
    name: sector.sector_name,
    value: sector.limit_up_count,
    children: sector.stocks?.slice(0, 10).map((code: string) => ({
      name: code,
      value: 1
    })) || []
  }))
  
  chart.setOption({
    tooltip: {
      formatter: (info: any) => {
        return `${info.name}: ${info.value}家涨停`
      }
    },
    series: [{
      type: 'treemap',
      data: treeData,
      label: {
        show: true,
        formatter: '{b}'
      },
      itemStyle: {
        borderColor: '#fff',
        borderWidth: 2
      },
      levels: [
        {
          itemStyle: {
            borderColor: '#555',
            borderWidth: 4,
            gapWidth: 4
          }
        },
        {
          colorSaturation: [0.3, 0.6],
          itemStyle: {
            borderColorSaturation: 0.7,
            gapWidth: 2,
            borderWidth: 2
          }
        }
      ]
    }]
  })
}

function updateContinuousHeatmap(data: ContinuousLadder[]) {
  if (!chart) return
  
  // 转换为treemap数据格式
  const treeData = data.map(ladder => ({
    name: `${ladder.continuous_days}连板`,
    value: ladder.count,
    itemStyle: {
      color: getColorByDays(ladder.continuous_days)
    },
    children: ladder.stocks?.map(stock => ({
      name: stock.stock_name,
      value: 1
    })) || []
  }))
  
  chart.setOption({
    tooltip: {
      formatter: (info: any) => {
        return `${info.name}: ${info.value}只`
      }
    },
    series: [{
      type: 'treemap',
      data: treeData,
      label: {
        show: true,
        formatter: '{b}'
      },
      itemStyle: {
        borderColor: '#fff',
        borderWidth: 2
      }
    }]
  })
}

function getColorByDays(days: number): string {
  const colors: Record<number, string> = {
    1: '#ffccc7',
    2: '#ffa39e',
    3: '#ff7875',
    4: '#ff4d4f',
    5: '#f5222d',
    6: '#cf1322',
    7: '#a8071a'
  }
  return colors[days] || colors[7]
}

function initChart() {
  if (!heatmapRef.value) return
  chart = echarts.init(heatmapRef.value)
}

watch(viewType, () => fetchData())

onMounted(() => {
  initChart()
  fetchData()
  
  const handleResize = () => chart?.resize()
  window.addEventListener('resize', handleResize)
  
  onUnmounted(() => {
    window.removeEventListener('resize', handleResize)
    chart?.dispose()
  })
})
</script>

<style lang="scss" scoped>
.heatmap {
  .card {
    background: #fff;
    border-radius: 8px;
    padding: 16px;

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

    .heatmap-container {
      height: calc(100vh - 200px);
      min-height: 500px;
    }
  }
}
</style>
