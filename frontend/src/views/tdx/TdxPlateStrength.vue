<template>
  <main class="target-strong">
    <header class="strong-top">
      <strong>板块轮动</strong>
      <div class="live-meta">
        <button
          type="button"
          class="trend-toggle"
          role="switch"
          :aria-checked="showTrend"
          @click="toggleTrend"
        >
          <span class="switch-track" :class="{ active: showTrend }"><i></i></span>
          {{ showTrend ? '隐藏趋势' : '显示趋势' }}
        </button>
        <span class="live-dot" :class="{ refreshing: loading }">{{ loading ? '刷新中' : '实时' }}</span>
        <time>{{ updatedAt }}</time>
      </div>
    </header>

    <div id="ztlast" class="type-tabs">
      <button
        type="button"
        class="datatype"
        :class="{ active: selectedSource === 'kpl' }"
        :aria-pressed="selectedSource === 'kpl'"
        @click="selectSource('kpl')"
      >
        开盘啦板块
      </button>
      <button
        type="button"
        class="datatype"
        :class="{ active: selectedSource === 'ths' }"
        :aria-pressed="selectedSource === 'ths'"
        @click="selectSource('ths')"
      >
        同花顺板块
      </button>
    </div>

    <div v-show="showTrend" id="dates">
      <div class="scroll-container">
        <div class="dates-container">
          <button
            v-for="days in quickWindows"
            :key="days"
            type="button"
            :class="{ active: !customMode && selectedWindow === days }"
            @click="selectWindow(days)"
          >
            近{{ days }}日
          </button>
          <button type="button" :class="{ active: customMode }" @click="enableCustomWindow">
            自定义
          </button>
          <label v-if="customMode" class="custom-window">
            <input
              v-model.number="customWindow"
              type="number"
              min="5"
              max="120"
              aria-label="自定义交易日窗口"
              @change="applyCustomWindow"
              @keydown.enter="applyCustomWindow"
            >
            <span>日</span>
          </label>
        </div>
      </div>
    </div>

    <div v-if="errorText" class="state-line error-line">{{ errorText }}</div>

    <nav class="compact-chart-tabs" aria-label="窄屏图表切换">
      <button
        type="button"
        :class="{ active: compactChartMode === 'strength' }"
        :aria-pressed="compactChartMode === 'strength'"
        @click="selectCompactChart('strength')"
      >当前强度</button>
      <button
        type="button"
        :class="{ active: compactChartMode === 'breadth' }"
        :aria-pressed="compactChartMode === 'breadth'"
        @click="selectCompactChart('breadth')"
      >涨停广度</button>
      <button
        v-if="showTrend"
        type="button"
        :class="{ active: compactChartMode === 'rotation' }"
        :aria-pressed="compactChartMode === 'rotation'"
        @click="selectCompactChart('rotation')"
      >题材轮动</button>
    </nav>

    <section class="chart-shell" aria-label="板块强度图表">
      <article class="chart-card strength-card" :class="{ 'compact-active': compactChartMode === 'strength' }">
        <h2>当前强度</h2>
        <div id="main2" ref="strengthChartEl" class="chart"></div>
      </article>
      <article class="chart-card breadth-card" :class="{ 'compact-active': compactChartMode === 'breadth' }">
        <h2>涨停广度</h2>
        <div id="main3" ref="breadthChartEl" class="chart"></div>
      </article>
      <article
        v-show="showTrend"
        class="chart-card rotation-card"
        :class="{ 'compact-active': compactChartMode === 'rotation' }"
      >
        <h2>题材轮动 · 近{{ selectedWindow }}个交易日</h2>
        <div id="main1" ref="rotationChartEl" class="chart"></div>
      </article>
    </section>

    <section class="rank-panel">
      <div class="rank-summary">
        <span>{{ sourceLabel }}</span>
        <span v-if="currentPayload?.summary">
          {{ currentPayload.summary.plate_count }}个题材 · {{ currentPayload.summary.limit_up_count }}只涨停 ·
          {{ currentPayload.summary.sealed_count }}只封板
        </span>
      </div>
      <table class="strong-table">
        <thead>
          <tr>
            <th style="width: 38px;">序</th>
            <th style="width: 94px;">板块</th>
            <th style="width: 76px;">强度</th>
            <th style="width: 54px;">涨停</th>
            <th style="width: 66px;">封板率</th>
            <th style="width: 54px;">高度</th>
            <th>核心股</th>
          </tr>
        </thead>
        <tbody>
          <template v-for="item in items" :key="item.plate_name">
            <tr :class="{ expanded: expandedPlate === item.plate_name }">
              <td>
                {{ item.rank }}
                <small v-if="item.rank_change" class="rank-change" :class="item.rank_change > 0 ? 'up' : 'down'">
                  {{ item.rank_change > 0 ? `↑${item.rank_change}` : `↓${Math.abs(item.rank_change)}` }}
                </small>
              </td>
              <td class="plate-name">
                <button type="button" class="plate-link" @click="togglePlate(item.plate_name)">
                  <span class="expand-arrow">{{ expandedPlate === item.plate_name ? '▾' : '▸' }}</span>
                  {{ item.plate_name }}
                </button>
              </td>
              <td class="score">
                {{ item.strength_score }}
                <small v-if="item.score_change" :class="item.score_change > 0 ? 'up' : 'down'">
                  {{ item.score_change > 0 ? '+' : '' }}{{ item.score_change }}
                </small>
              </td>
              <td class="positive">{{ item.limit_up_count }}</td>
              <td>{{ item.seal_rate }}%</td>
              <td>{{ item.max_board }}板</td>
              <td class="core-stocks">
                <button
                  v-for="stock in item.core_stocks"
                  :key="stock.stock_code"
                  type="button"
                  :class="{ opened: !stock.is_sealed }"
                  :title="`${stock.stock_code} · ${stock.is_sealed ? '封板' : '已开板'}`"
                  @click="openStock(stock.stock_code)"
                >
                  {{ stock.stock_name }}{{ stock.board }}
                </button>
              </td>
            </tr>
            <tr v-if="expandedPlate === item.plate_name" class="constituent-row">
              <td colspan="7" class="constituent-cell">
                <div class="constituent-panel">
                  <div class="constituent-summary">
                    <strong>{{ item.plate_name }} · 成分股实时涨幅</strong>
                    <span v-if="currentConstituentPayload?.summary">
                      {{ currentConstituentPayload.summary.stock_count }}只 ·
                      上涨{{ currentConstituentPayload.summary.up_count }} ·
                      下跌{{ currentConstituentPayload.summary.down_count }} ·
                      涨停{{ currentConstituentPayload.summary.limit_up_count }}
                    </span>
                    <span v-if="constituentLoading" class="loading-note">刷新中...</span>
                    <span v-if="constituentItems.length" class="constituent-scroll-actions">
                      <button type="button" @click.stop="scrollConstituents($event, 'up')">上翻</button>
                      <button type="button" @click.stop="scrollConstituents($event, 'down')">下翻</button>
                    </span>
                  </div>
                  <div v-if="currentConstituentPayload?.source_note" class="constituent-source-note">
                    成分口径：{{ currentConstituentPayload.source_note }}
                  </div>
                  <div v-if="constituentError" class="constituent-state error-line">{{ constituentError }}</div>
                  <div v-else-if="constituentLoading && !currentConstituentPayload" class="constituent-state">
                    正在加载板块成分股实时行情...
                  </div>
                  <div v-else-if="!constituentItems.length" class="constituent-state">
                    {{ currentConstituentPayload?.warnings?.[0] || '暂无匹配的板块成分股' }}
                  </div>
                  <div
                    v-else
                    class="constituent-scroll"
                    tabindex="0"
                    aria-label="板块成分股滚动列表"
                    @wheel.stop
                    @touchmove.stop
                  >
                    <table class="constituent-table">
                      <thead>
                        <tr>
                          <th>序</th>
                          <th>个股</th>
                          <th>标记</th>
                          <th>现价</th>
                          <th>涨跌幅</th>
                          <th>成交额</th>
                          <th>换手率</th>
                          <th>状态</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr v-for="(stock, index) in constituentItems" :key="stock.stock_code">
                          <td>{{ index + 1 }}</td>
                          <td class="constituent-name">
                            <button type="button" @click="openStock(stock.stock_code)">{{ stock.stock_name }}</button>
                            <small>{{ stock.stock_code }}</small>
                          </td>
                          <td class="intraday-tags-cell">
                            <span
                              v-for="tag in displayIntradayTags(stock)"
                              :key="`${stock.stock_code}-${tag.label}`"
                              class="intraday-tag"
                              :class="intradayTagClass(tag)"
                              :title="tag.reason"
                            >{{ tag.label }}</span>
                          </td>
                          <td data-label="现价">{{ formatPrice(stock.price) }}</td>
                          <td data-label="涨幅" :class="changeClass(stock.change_pct)">{{ formatPct(stock.change_pct) }}</td>
                          <td data-label="成交">{{ formatAmount(stock.amount) }}</td>
                          <td data-label="换手">{{ stock.turnover_rate ? `${stock.turnover_rate.toFixed(2)}%` : '--' }}</td>
                          <td data-label="状态">
                            <span v-if="stock.board" :class="stock.is_sealed ? 'limit-state' : 'opened-state'">
                              {{ stock.is_sealed ? `${stock.board}板封板` : `${stock.board}板开板` }}
                            </span>
                            <span v-else-if="stock.is_limit_up" class="limit-state">涨停封板</span>
                            <span v-else>--</span>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                  <div v-if="currentConstituentPayload?.warnings?.length" class="constituent-warning">
                    {{ currentConstituentPayload.warnings.join('；') }}
                  </div>
                </div>
              </td>
            </tr>
          </template>
        </tbody>
      </table>
    </section>

    <div v-if="loading && !currentPayload" class="state-line">加载实时板块题材强度...</div>
    <div v-else-if="!items.length && !errorText" class="state-line">{{ emptyText }}</div>
    <footer v-if="currentPayload?.warnings?.length" class="warning-line">{{ currentPayload.warnings.join('；') }}</footer>
  </main>
</template>

<script setup lang="ts">
import * as echarts from 'echarts'
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { getTdxPlateConstituents, getTdxPlateStrength } from '@/api/tdx-plugins'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import {
  buildPlateStrengthChartData,
  buildPlateConstituentRequest,
  buildPlateStrengthRequest,
  hasRenderableChartSize,
  intradayTagClass,
  matchesPlateStrengthSelection,
  nextConstituentScrollTop,
  nextExpandedPlate,
  normalizeCompactChartMode,
  normalizePlateStrengthWindow,
  PlateStrengthRequestGate
} from '@/utils/tdxPlateStrength'
import type { CompactChartMode, ConstituentScrollDirection } from '@/utils/tdxPlateStrength'
import type {
  TdxIntradayTag,
  TdxPlateConstituent,
  TdxPlateConstituentPayload,
  TdxPlateStrengthPayload
} from '@/types/tdx-plugins'

const quickWindows = [10, 20, 30, 50]
const payload = ref<TdxPlateStrengthPayload | null>(null)
const loading = ref(false)
const errorText = ref('')
const selectedSource = ref<'kpl' | 'ths'>('kpl')
const selectedWindow = ref(20)
const customMode = ref(false)
const customWindow = ref(20)
const showTrend = ref(true)
const compactChartMode = ref<CompactChartMode>('strength')
const expandedPlate = ref<string | null>(null)
const constituentPayload = ref<TdxPlateConstituentPayload | null>(null)
const constituentLoading = ref(false)
const constituentError = ref('')
const strengthChartEl = ref<HTMLElement | null>(null)
const breadthChartEl = ref<HTMLElement | null>(null)
const rotationChartEl = ref<HTMLElement | null>(null)
const { openStock } = useTdxStockLink()

let refreshTimer = 0
const requestGate = new PlateStrengthRequestGate()
const constituentRequestGate = new PlateStrengthRequestGate()
let strengthChart: echarts.ECharts | null = null
let breadthChart: echarts.ECharts | null = null
let rotationChart: echarts.ECharts | null = null

const currentPayload = computed(() => (
  matchesPlateStrengthSelection(payload.value, selectedSource.value, selectedWindow.value)
    ? payload.value
    : null
))
const items = computed(() => currentPayload.value?.items || [])
const currentConstituentPayload = computed(() => (
  constituentPayload.value?.plate_name === expandedPlate.value &&
  constituentPayload.value?.source === selectedSource.value
    ? constituentPayload.value
    : null
))
const constituentItems = computed(() => currentConstituentPayload.value?.items || [])
const updatedAt = computed(() => (currentPayload.value?.updated_at || '').replace('T', ' ').slice(5, 19))
const emptyText = computed(() => currentPayload.value?.warnings?.[0] || '暂无板块题材强度数据')
const sourceLabel = computed(() => (
  (currentPayload.value?.source_status?.plate_source || selectedSource.value) === 'ths'
    ? '同花顺细分题材口径'
    : '开盘啦兼容口径'
))

async function loadData(silent = false) {
  const decision = requestGate.begin(silent)
  if (decision !== 'start') {
    if (decision === 'queue') loading.value = true
    return
  }
  const requestSource = selectedSource.value
  const requestWindow = selectedWindow.value
  if (!silent) loading.value = true
  errorText.value = ''
  try {
    const nextPayload = await getTdxPlateStrength(
      buildPlateStrengthRequest(requestSource, requestWindow)
    )
    if (requestSource !== selectedSource.value || requestWindow !== selectedWindow.value) return
    payload.value = nextPayload
    if (expandedPlate.value && !nextPayload.items.some(item => item.plate_name === expandedPlate.value)) {
      closePlate()
    }
    await nextTick()
    renderCharts()
  } catch (error: any) {
    if (requestSource !== selectedSource.value || requestWindow !== selectedWindow.value) return
    errorText.value = error?.response?.data?.detail || error?.message || '实时板块强度加载失败'
  } finally {
    if (requestGate.finish()) {
      void loadData(false)
    } else {
      loading.value = false
    }
  }
}

function selectSource(source: 'kpl' | 'ths') {
  if (selectedSource.value === source) return
  closePlate()
  selectedSource.value = source
  clearCharts()
  void loadData()
}

function selectWindow(days: number) {
  customMode.value = false
  if (selectedWindow.value === days) return
  selectedWindow.value = days
  customWindow.value = days
  clearCharts()
  void loadData()
}

function enableCustomWindow() {
  customMode.value = true
  customWindow.value = selectedWindow.value
}

function applyCustomWindow() {
  const normalized = normalizePlateStrengthWindow(customWindow.value)
  customWindow.value = normalized
  const changed = selectedWindow.value !== normalized
  selectedWindow.value = normalized
  if (changed) clearCharts()
  void loadData()
}

async function toggleTrend() {
  showTrend.value = !showTrend.value
  compactChartMode.value = normalizeCompactChartMode(compactChartMode.value, showTrend.value)
  await nextTick()
  renderCharts()
  resizeCharts()
}

async function selectCompactChart(mode: CompactChartMode) {
  compactChartMode.value = normalizeCompactChartMode(mode, showTrend.value)
  await nextTick()
  renderCharts()
  resizeCharts()
}

function togglePlate(plateName: string) {
  const nextPlate = nextExpandedPlate(expandedPlate.value, plateName)
  if (!nextPlate) {
    closePlate()
    return
  }
  expandedPlate.value = nextPlate
  constituentPayload.value = null
  constituentError.value = ''
  void loadConstituents(false)
}

function closePlate() {
  expandedPlate.value = null
  constituentPayload.value = null
  constituentError.value = ''
  constituentLoading.value = false
}

function scrollConstituents(event: MouseEvent, direction: ConstituentScrollDirection) {
  const trigger = event.currentTarget as HTMLElement | null
  const scrollElement = trigger
    ?.closest('.constituent-panel')
    ?.querySelector<HTMLDivElement>('.constituent-scroll')
  if (!scrollElement) return
  scrollElement.scrollTop = nextConstituentScrollTop(
    scrollElement.scrollTop,
    scrollElement.clientHeight,
    scrollElement.scrollHeight,
    direction
  )
  scrollElement.focus()
}

async function loadConstituents(silent = false) {
  const plateName = expandedPlate.value
  if (!plateName) return
  const decision = constituentRequestGate.begin(silent)
  if (decision !== 'start') {
    if (decision === 'queue') constituentLoading.value = true
    return
  }

  const requestSource = selectedSource.value
  if (!silent) constituentLoading.value = true
  constituentError.value = ''
  try {
    const nextPayload = await getTdxPlateConstituents(
      buildPlateConstituentRequest(plateName, requestSource)
    )
    if (expandedPlate.value !== plateName || selectedSource.value !== requestSource) return
    constituentPayload.value = nextPayload
  } catch (error: any) {
    if (expandedPlate.value !== plateName || selectedSource.value !== requestSource) return
    constituentError.value = error?.response?.data?.detail || error?.message || '板块成分股加载失败'
  } finally {
    if (constituentRequestGate.finish() && expandedPlate.value) {
      void loadConstituents(false)
    } else {
      constituentLoading.value = false
    }
  }
}

function formatPrice(value: number | null): string {
  return value == null ? '--' : value.toFixed(2)
}

function displayIntradayTags(stock: TdxPlateConstituent): TdxIntradayTag[] {
  if (stock.tags?.length) return stock.tags
  if (!stock.dragon_tag) return []
  return [{ label: stock.dragon_tag, type: 'dragon', reason: stock.dragon_reason }]
}

function formatPct(value: number | null): string {
  if (value == null) return '--'
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

function changeClass(value: number | null): string {
  if (value == null || value === 0) return ''
  return value > 0 ? 'up' : 'down'
}

function formatAmount(value: number): string {
  if (!value) return '--'
  return value >= 10000 ? `${(value / 10000).toFixed(2)}亿` : `${value.toFixed(0)}万`
}

function renderCharts() {
  if (!currentPayload.value || !strengthChartEl.value || !breadthChartEl.value || !rotationChartEl.value) return
  const chartData = buildPlateStrengthChartData(currentPayload.value)

  if (hasRenderableChartSize(strengthChartEl.value.clientWidth, strengthChartEl.value.clientHeight)) {
    strengthChart ||= echarts.init(strengthChartEl.value, undefined, { renderer: 'canvas' })
    strengthChart.setOption({
      animationDuration: 300,
      grid: { top: 6, right: 38, bottom: 22, left: 86 },
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      xAxis: {
        type: 'value',
        splitLine: { lineStyle: { color: '#252936' } },
        axisLabel: { color: '#8f98aa' }
      },
      yAxis: {
        type: 'category',
        data: chartData.strength.names,
        axisLabel: { color: '#d8dee9', width: 78, overflow: 'truncate' },
        axisLine: { lineStyle: { color: '#343a48' } }
      },
      series: [{
        name: '强度',
        type: 'bar',
        data: chartData.strength.scores,
        barMaxWidth: 16,
        label: { show: true, position: 'right', color: '#ff7b72' },
        itemStyle: { color: '#b23b37', borderRadius: [0, 3, 3, 0] }
      }]
    }, true)
  }

  if (hasRenderableChartSize(breadthChartEl.value.clientWidth, breadthChartEl.value.clientHeight)) {
    breadthChart ||= echarts.init(breadthChartEl.value, undefined, { renderer: 'canvas' })
    breadthChart.setOption({
      animationDuration: 300,
      color: ['#d84a4a', '#687080'],
      grid: { top: 20, right: 12, bottom: 56, left: 38 },
      legend: { top: 0, right: 8, textStyle: { color: '#aab2c0' }, data: ['封板', '开板'] },
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      xAxis: {
        type: 'category',
        data: chartData.breadth.names,
        axisLabel: { color: '#9aa4b2', rotate: 35, interval: 0 },
        axisLine: { lineStyle: { color: '#343a48' } }
      },
      yAxis: {
        type: 'value',
        minInterval: 1,
        axisLabel: { color: '#8f98aa' },
        splitLine: { lineStyle: { color: '#252936' } }
      },
      series: [
        { name: '封板', type: 'bar', stack: 'total', data: chartData.breadth.sealed, barMaxWidth: 22 },
        { name: '开板', type: 'bar', stack: 'total', data: chartData.breadth.opened, barMaxWidth: 22 }
      ]
    }, true)
  }

  if (hasRenderableChartSize(rotationChartEl.value.clientWidth, rotationChartEl.value.clientHeight)) {
    rotationChart ||= echarts.init(rotationChartEl.value, undefined, { renderer: 'canvas' })
    rotationChart.setOption({
      animationDuration: 300,
      color: ['#f0be83', '#ff6b6b', '#9d7cff', '#4ecdc4', '#6ea8fe', '#c0ca55'],
      grid: { top: 34, right: 20, bottom: 28, left: 44 },
      legend: { top: 0, textStyle: { color: '#aab2c0' } },
      tooltip: { trigger: 'axis' },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: chartData.rotation.dates,
        axisLabel: { color: '#8f98aa' },
        axisLine: { lineStyle: { color: '#343a48' } }
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#8f98aa' },
        splitLine: { lineStyle: { color: '#252936' } }
      },
      series: chartData.rotation.series.map(series => ({
        name: series.name,
        type: 'line',
        connectNulls: false,
        showSymbol: chartData.rotation.dates.length <= 20,
        symbolSize: 5,
        data: series.values,
        lineStyle: { width: 2 }
      }))
    }, true)
  }
}

function clearCharts() {
  strengthChart?.clear()
  breadthChart?.clear()
  rotationChart?.clear()
}

function resizeCharts() {
  const hasNewlyVisibleChart = (
    (!strengthChart && strengthChartEl.value && hasRenderableChartSize(strengthChartEl.value.clientWidth, strengthChartEl.value.clientHeight)) ||
    (!breadthChart && breadthChartEl.value && hasRenderableChartSize(breadthChartEl.value.clientWidth, breadthChartEl.value.clientHeight)) ||
    (!rotationChart && rotationChartEl.value && hasRenderableChartSize(rotationChartEl.value.clientWidth, rotationChartEl.value.clientHeight))
  )
  if (hasNewlyVisibleChart) renderCharts()

  if (strengthChartEl.value && hasRenderableChartSize(strengthChartEl.value.clientWidth, strengthChartEl.value.clientHeight)) {
    strengthChart?.resize()
  }
  if (breadthChartEl.value && hasRenderableChartSize(breadthChartEl.value.clientWidth, breadthChartEl.value.clientHeight)) {
    breadthChart?.resize()
  }
  if (rotationChartEl.value && hasRenderableChartSize(rotationChartEl.value.clientWidth, rotationChartEl.value.clientHeight)) {
    rotationChart?.resize()
  }
}

onMounted(() => {
  void loadData()
  refreshTimer = window.setInterval(() => {
    void loadData(true)
    if (expandedPlate.value) void loadConstituents(true)
  }, 5000)
  window.addEventListener('resize', resizeCharts)
})

onUnmounted(() => {
  window.clearInterval(refreshTimer)
  window.removeEventListener('resize', resizeCharts)
  strengthChart?.dispose()
  breadthChart?.dispose()
  rotationChart?.dispose()
})
</script>

<style scoped>
.target-strong {
  min-height: 100vh;
  overflow: auto;
  background: #111219;
  color: #e2e8f0;
  font-size: 12px;
}

.strong-top,
.live-meta,
.rank-summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.strong-top {
  min-height: 30px;
  padding: 3px 8px;
  border-bottom: 1px solid #222;
  background: #151515;
}

.strong-top strong {
  color: #f0be83;
  font-size: 13px;
}

.live-meta {
  gap: 10px;
  color: #9099a8;
}

.trend-toggle {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 0;
  border: 0;
  background: transparent;
  color: #aab2c0;
  font-size: 11px;
  cursor: pointer;
}

.switch-track {
  position: relative;
  width: 25px;
  height: 13px;
  border-radius: 8px;
  background: #454b58;
  transition: background 0.15s ease;
}

.switch-track i {
  position: absolute;
  top: 2px;
  left: 2px;
  width: 9px;
  height: 9px;
  border-radius: 50%;
  background: #d8dee9;
  transition: transform 0.15s ease;
}

.switch-track.active {
  background: #b23b37;
}

.switch-track.active i {
  transform: translateX(12px);
}

.live-dot::before {
  display: inline-block;
  width: 6px;
  height: 6px;
  margin-right: 5px;
  border-radius: 50%;
  background: #46c37b;
  content: '';
}

.live-dot.refreshing::before {
  background: #f0ad4e;
}

.type-tabs {
  display: flex;
  width: 100%;
}

.datatype {
  width: 50%;
  height: 25px;
  border: 1px solid #4b4f59;
  background: #16171d;
  color: #c9ced8;
  font-size: 12px;
  cursor: pointer;
}

.datatype.active {
  border-color: #b23b37;
  background: #b23b37;
  color: #fff;
}

#dates {
  overflow-x: auto;
  border-bottom: 1px solid #252936;
}

.scroll-container {
  overflow: auto;
  white-space: nowrap;
  scrollbar-width: none;
}

.dates-container {
  display: flex;
  align-items: center;
  gap: 5px;
  min-width: max-content;
  padding: 5px 7px;
}

.dates-container button {
  height: 23px;
  padding: 0 9px;
  border: 1px solid #555b68;
  border-radius: 3px;
  background: transparent;
  color: #c9ced8;
  font-size: 12px;
  cursor: pointer;
}

.dates-container button.active {
  border-color: #f0ad4e;
  background: #f0ad4e;
  color: #111;
}

.custom-window {
  display: flex;
  align-items: center;
  gap: 3px;
  color: #aab2c0;
}

.custom-window input {
  width: 56px;
  height: 23px;
  border: 1px solid #555b68;
  border-radius: 3px;
  background: #0f1118;
  color: #f0be83;
  text-align: center;
}

.compact-chart-tabs {
  display: none;
}

.chart-shell {
  display: grid;
  grid-template-columns: minmax(280px, 0.9fr) minmax(320px, 1.1fr);
  gap: 6px;
  min-width: 760px;
  padding: 6px;
}

.chart-card {
  border: 1px solid #292d38;
  background: #14161e;
}

.chart-card h2 {
  height: 26px;
  margin: 0;
  padding: 6px 8px 0;
  color: #d5dae3;
  font-size: 12px;
  font-weight: 500;
}

.chart {
  width: 100%;
  height: 220px;
}

.rotation-card {
  grid-column: 1 / -1;
}

.rotation-card .chart {
  height: 245px;
}

.rank-panel {
  min-width: 760px;
  padding: 0 6px 6px;
}

.rank-summary {
  min-height: 28px;
  padding: 0 7px;
  border: 1px solid #292d38;
  border-bottom: 0;
  background: #191c25;
  color: #8f98aa;
}

.rank-summary span:first-child {
  color: #f0be83;
}

.strong-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
  background: #111219;
  text-align: center;
}

.strong-table th,
.strong-table td {
  padding: 7px 5px;
  border: 1px solid #292d38;
  overflow: hidden;
  color: #e2e8f0;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.strong-table th {
  background: #202432;
  color: #aab2c0;
  font-weight: 600;
}

.strong-table tr:hover td {
  background: #1b2130;
}

.plate-name,
.core-stocks button {
  color: #f0be83 !important;
}

.plate-link {
  display: inline-flex;
  align-items: center;
  max-width: 100%;
  padding: 0;
  border: 0;
  background: transparent;
  color: #f0be83;
  font-size: 12px;
  cursor: pointer;
}

.expand-arrow {
  width: 13px;
  color: #7f8a9c;
}

.strong-table tr.expanded > td {
  background: #1b2130;
}

.score,
.positive,
.up {
  color: #ff6b6b !important;
}

.down {
  color: #46c37b !important;
}

.score small,
.rank-change {
  margin-left: 3px;
  font-size: 10px;
}

.core-stocks {
  text-align: left;
}

.core-stocks button {
  margin-right: 8px;
  border: 0;
  background: transparent;
  cursor: pointer;
}

.core-stocks button.opened {
  color: #8f98aa !important;
  text-decoration: line-through;
}

.strong-table td.constituent-cell {
  padding: 0;
  overflow: visible;
  white-space: normal;
}

.constituent-panel {
  background: #0d1017;
  text-align: left;
}

.constituent-summary {
  display: flex;
  align-items: center;
  gap: 14px;
  min-height: 30px;
  padding: 0 9px;
  border-bottom: 1px solid #2d3340;
  color: #8f98aa;
}

.constituent-summary strong {
  color: #f0be83;
  font-size: 12px;
}

.loading-note {
  margin-left: auto;
  color: #f0ad4e;
}

.constituent-scroll-actions {
  display: inline-flex;
  gap: 4px;
  margin-left: auto;
}

.constituent-scroll-actions button {
  height: 22px;
  padding: 0 7px;
  border: 1px solid #3a4354;
  border-radius: 2px;
  background: #1b2130;
  color: #bcc6d8;
  font-size: 11px;
  cursor: pointer;
}

.constituent-scroll-actions button:hover,
.constituent-scroll-actions button:focus-visible {
  border-color: #70809a;
  color: #fff;
}

.constituent-source-note {
  padding: 5px 9px;
  border-bottom: 1px solid #242a36;
  color: #697386;
}

.constituent-scroll {
  height: 360px;
  min-height: 180px;
  max-height: calc(100vh - 220px);
  overflow-x: auto;
  overflow-y: scroll;
  overscroll-behavior: contain;
  scrollbar-color: #4b5568 #11151e;
  scrollbar-width: thin;
}

.constituent-scroll:focus {
  outline: 1px solid #44516a;
  outline-offset: -1px;
}

.constituent-scroll::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.constituent-scroll::-webkit-scrollbar-track {
  background: #11151e;
}

.constituent-scroll::-webkit-scrollbar-thumb {
  border-radius: 4px;
  background: #4b5568;
}

.constituent-table {
  width: 100%;
  min-width: 720px;
  border-collapse: collapse;
  table-layout: fixed;
  text-align: center;
}

.strong-table .constituent-table th,
.strong-table .constituent-table td {
  padding: 6px 7px;
  border-width: 0 0 1px;
  border-color: #242a36;
  background: transparent;
}

.strong-table .constituent-table th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: #181c26;
  color: #8f98aa;
}

.constituent-table th:nth-child(1) { width: 38px; }
.constituent-table th:nth-child(2) { width: 145px; }
.constituent-table th:nth-child(3) { width: 185px; }
.constituent-table th:nth-child(4) { width: 70px; }
.constituent-table th:nth-child(5) { width: 75px; }
.constituent-table th:nth-child(6) { width: 85px; }
.constituent-table th:nth-child(7) { width: 70px; }
.constituent-table th:nth-child(8) { width: 80px; }

.constituent-name {
  text-align: left;
}

.constituent-name button {
  padding: 0;
  border: 0;
  background: transparent;
  color: #e5e9f0;
  cursor: pointer;
}

.constituent-name button:hover {
  color: #f0be83;
}

.constituent-name small {
  margin-left: 6px;
  color: #697386;
}

.intraday-tags-cell {
  white-space: normal !important;
}

.intraday-tag {
  display: inline-block;
  min-width: 32px;
  margin: 1px 2px;
  padding: 1px 5px;
  border-radius: 3px;
  color: #121318;
  font-weight: 700;
}

.dragon-one {
  background: #ff6b6b;
}

.dragon-two {
  background: #f0ad4e;
}

.dragon-other {
  background: #c7a86c;
}

.role-high {
  background: #d84a4a;
  color: #fff;
}

.role-pioneer {
  background: #9d7cff;
  color: #fff;
}

.role-core {
  background: #4f86c6;
  color: #fff;
}

.role-catchup {
  background: #4ecdc4;
}

.role-opened {
  border: 1px solid #f0ad4e;
  background: transparent;
  color: #f0ad4e;
}

.role-default {
  background: #687080;
  color: #fff;
}

.limit-state {
  color: #ff6b6b;
}

.opened-state {
  color: #f0ad4e;
}

.constituent-state,
.constituent-warning {
  padding: 12px;
  color: #8f98aa;
  text-align: center;
}

.constituent-warning {
  padding: 6px 9px;
  border-top: 1px solid #3c3528;
  background: #211e18;
  color: #c5ae7d;
  text-align: left;
}

.state-line,
.warning-line {
  padding: 9px;
  color: #9aa4b2;
}

.error-line {
  border-bottom: 1px solid #5b2a2a;
  background: #321d22;
  color: #ff8f8f;
}

.warning-line {
  border-top: 1px solid #3c3528;
  background: #211e18;
  color: #c5ae7d;
}

@media (max-width: 760px) {
  .strong-top,
  .rank-summary {
    align-items: flex-start;
    flex-direction: column;
    gap: 2px;
    padding-top: 4px;
    padding-bottom: 4px;
  }
}

@media (max-width: 520px) {
  .target-strong {
    min-height: 100dvh;
    overflow-x: hidden;
  }

  .strong-top {
    align-items: center;
    flex-direction: row;
    flex-wrap: wrap;
    gap: 3px 8px;
    padding: 4px 6px;
  }

  .live-meta {
    gap: 6px;
    margin-left: auto;
    font-size: 11px;
  }

  .trend-toggle {
    gap: 3px;
  }

  .dates-container {
    padding: 4px;
  }

  .compact-chart-tabs {
    display: flex;
    gap: 3px;
    padding: 4px 4px 0;
  }

  .compact-chart-tabs button {
    flex: 1;
    min-width: 0;
    height: 25px;
    padding: 0 4px;
    border: 1px solid #343a48;
    border-radius: 3px;
    background: #191c25;
    color: #8f98aa;
    font-size: 11px;
    cursor: pointer;
  }

  .compact-chart-tabs button.active {
    border-color: #b23b37;
    background: #2a1b20;
    color: #ff8f8f;
  }

  .chart-shell {
    display: block;
    min-width: 0;
    padding: 4px;
  }

  .chart-card:not(.compact-active) {
    display: none;
  }

  .chart-card h2 {
    height: 23px;
    padding: 5px 7px 0;
  }

  .chart,
  .rotation-card .chart {
    height: 168px;
  }

  .rank-panel {
    min-width: 0;
    padding: 0 4px 4px;
  }

  .rank-summary {
    min-height: 0;
    gap: 2px;
    padding: 5px 7px;
    line-height: 16px;
  }

  .strong-table,
  .strong-table > tbody {
    display: block;
    width: 100%;
  }

  .strong-table > thead {
    display: none;
  }

  .strong-table > tbody > tr:not(.constituent-row) {
    display: grid;
    grid-template-areas:
      'rank plate score limit'
      'seal seal height height'
      'core core core core';
    grid-template-columns: 32px minmax(0, 1fr) 78px 44px;
    align-items: center;
    padding: 6px 7px;
    border: 1px solid #292d38;
    border-top: 0;
    background: #111219;
  }

  .strong-table > tbody > tr:not(.constituent-row):hover,
  .strong-table > tbody > tr.expanded {
    background: #1b2130;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td {
    padding: 2px 3px;
    border: 0;
    overflow: visible;
    background: transparent !important;
    text-overflow: clip;
    white-space: normal;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(1) {
    grid-area: rank;
    padding-left: 0;
    color: #8f98aa;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(2) {
    grid-area: plate;
    min-width: 0;
    text-align: left;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(3) {
    grid-area: score;
    text-align: right;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(4) {
    grid-area: limit;
    text-align: right;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(4)::before {
    color: #697386;
    content: '板 ';
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(5) {
    grid-area: seal;
    color: #aab2c0;
    text-align: left;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(5)::before {
    color: #697386;
    content: '封板率 ';
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(6) {
    grid-area: height;
    color: #aab2c0;
    text-align: right;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(6)::before {
    color: #697386;
    content: '高度 ';
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(7) {
    grid-area: core;
    padding-top: 4px;
    text-align: left;
  }

  .strong-table > tbody > tr:not(.constituent-row) > td:nth-child(7)::before {
    margin-right: 5px;
    color: #697386;
    content: '核心';
  }

  .plate-link {
    width: 100%;
    font-size: 13px;
    font-weight: 600;
  }

  .core-stocks button {
    margin: 1px 6px 1px 0;
    padding: 1px 0;
    font-size: 11px;
  }

  .constituent-row,
  .strong-table td.constituent-cell {
    display: block;
    width: 100%;
  }

  .constituent-summary {
    flex-wrap: wrap;
    gap: 2px 8px;
    padding: 6px 8px;
    line-height: 16px;
  }

  .constituent-summary strong {
    flex: 1 1 190px;
  }

  .loading-note {
    margin-left: 0;
  }

  .constituent-scroll-actions {
    margin-left: 0;
  }

  .constituent-source-note {
    padding: 5px 8px;
    line-height: 16px;
  }

  .constituent-scroll {
    height: 320px;
    min-height: 170px;
    max-height: calc(100vh - 260px);
    overflow-x: hidden;
    overflow-y: scroll;
    -webkit-overflow-scrolling: touch;
  }

  .constituent-table,
  .constituent-table > tbody {
    display: block;
    width: 100%;
    min-width: 0;
  }

  .constituent-table > thead {
    display: none;
  }

  .constituent-table > tbody > tr {
    display: grid;
    grid-template-areas: 'index name tags change state';
    grid-template-columns: 22px minmax(90px, 1fr) minmax(0, auto) 64px 58px;
    align-items: center;
    gap: 0 4px;
    min-height: 34px;
    padding: 3px 6px;
    border-bottom: 1px solid #242a36;
  }

  .strong-table .constituent-table td {
    padding: 1px 0;
    border: 0;
    overflow: visible;
    text-overflow: clip;
    white-space: normal;
  }

  .constituent-table td:nth-child(1) {
    grid-area: index;
    color: #697386;
  }

  .constituent-table td:nth-child(2) {
    grid-area: name;
    min-width: 0;
  }

  .constituent-table td:nth-child(3) {
    grid-area: tags;
    min-width: 0;
    overflow: hidden;
    padding: 0;
    white-space: nowrap !important;
  }

  .constituent-table td:nth-child(4) {
    display: none;
  }

  .constituent-table td:nth-child(5) {
    grid-area: change;
    font-size: 13px;
    font-weight: 700;
    text-align: right;
  }

  .constituent-table td:nth-child(6) {
    display: none;
  }

  .constituent-table td:nth-child(7) {
    display: none;
  }

  .constituent-table td:nth-child(8) {
    grid-area: state;
    overflow: hidden;
    font-size: 11px;
    text-align: right;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .constituent-name button {
    color: #f0be83;
    font-size: 12px;
    font-weight: 600;
  }

  .constituent-name small {
    margin-left: 3px;
    font-size: 10px;
  }

  .intraday-tag {
    min-width: 24px;
    margin: 0 1px;
    padding: 0 3px;
    border-radius: 2px;
    font-size: 10px;
    line-height: 17px;
  }

  .intraday-tags-cell .intraday-tag:first-child {
    margin-left: 0;
  }
}
</style>
