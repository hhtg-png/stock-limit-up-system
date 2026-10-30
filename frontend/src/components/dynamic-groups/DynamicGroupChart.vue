<template>
  <div class="chart-shell">
    <div ref="element" class="chart" :style="{ height: `${height}px` }" role="img" :aria-label="label" />
    <div v-if="!points.length" class="chart-empty">{{ emptyText || '暂无已保存的走势，后台更新后自动显示' }}</div>
  </div>
</template>
<script setup lang="ts">
import { ref, watch, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import type { DynamicPoint } from '@/types/dynamic-groups'
import { historyWindowStart } from '@/utils/dynamicGroups'
const props = withDefaults(defineProps<{ points: DynamicPoint[]; label: string; days?: number; color?: string; height?: number; intraday?: boolean; emptyText?: string }>(), { days: 0, color: '#409eff', height: 340, intraday: false })
const element = ref<HTMLElement>()
let chart: echarts.ECharts | undefined
let observer: ResizeObserver | undefined
let stopped = false
function render(resetZoom = false) {
  if (!chart) return
  chart.setOption({
    animation: false,
    grid: { left: 18, right: 20, top: 22, bottom: props.intraday ? 30 : 64, containLabel: true },
    tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => value == null ? '数据待补' : Number(value).toFixed(2) },
    xAxis: { type: 'category', boundaryGap: false, data: props.points.map(point => point.date), axisLine: { lineStyle: { color: '#d7deea' } }, axisLabel: { color: '#8190a5', hideOverlap: true, formatter: (value: string) => props.intraday ? value.slice(11, 16) || value : value.slice(2) } },
    yAxis: { type: 'value', scale: true, axisLabel: { color: '#8190a5' }, splitLine: { lineStyle: { color: '#edf1f7', type: 'dashed' } } },
    ...(resetZoom ? { dataZoom: props.intraday ? [] : [{ type: 'inside', startValue: historyWindowStart(props.points, props.days), endValue: props.points[props.points.length - 1]?.date }, { type: 'slider', height: 20, bottom: 8, borderColor: 'transparent', startValue: historyWindowStart(props.points, props.days), endValue: props.points[props.points.length - 1]?.date }] } : {}),
    series: [{ name: props.label, type: 'line', data: props.points.map(point => point.value), connectNulls: false, showSymbol: false, lineStyle: { width: 2, color: props.color }, itemStyle: { color: props.color }, areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: `${props.color}25` }, { offset: 1, color: `${props.color}00` }]) } }]
  })
}
watch(() => [props.points, props.color], () => render(), { deep: true })
watch(() => props.days, () => render(true))
function resizeChart() {
  if (stopped || !element.value?.clientWidth || !element.value.clientHeight) return
  if (!chart) { chart = echarts.init(element.value); render(true) }
  else chart.resize()
}
onMounted(() => { if (!element.value) return; observer = new ResizeObserver(resizeChart); observer.observe(element.value); resizeChart() })
onBeforeUnmount(() => { stopped = true; observer?.disconnect(); chart?.dispose(); chart = undefined })
</script>
<style scoped>
.chart-shell{position:relative;min-width:0;width:100%}.chart{width:100%;min-width:0}.chart-empty{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#8b98aa;font-size:13px;pointer-events:none;text-align:center;padding:24px}
</style>
