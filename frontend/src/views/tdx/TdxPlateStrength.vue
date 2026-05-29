<template>
  <main class="target-strong">
    <header class="strong-top">
      <strong>板块轮动</strong>
      <time>{{ updatedAt }}</time>
    </header>

    <div id="ztlast" class="type-tabs">
      <button type="button" class="datatype active">开盘啦板块</button>
      <button type="button" class="datatype">同花顺板块</button>
    </div>

    <div id="dates">
      <div class="scroll-container">
        <div class="dates-container">
          <button v-for="days in [10, 20, 30, 50]" :key="days" type="button" :class="{ active: days === 20 }">
            近{{ days }}日
          </button>
          <button type="button" disabled>自定义</button>
        </div>
      </div>
    </div>

    <section class="chart-shell">
      <div id="main2" class="mini-chart">
        <div v-for="item in topItems" :key="`score-${item.plate_name}`" class="bar-row">
          <span>{{ item.plate_name }}</span>
          <i :style="{ width: barWidth(item.strength_score) }"></i>
          <em>{{ item.strength_score }}</em>
        </div>
      </div>
      <div id="main3" class="volume-chart">
        <span v-for="item in topItems" :key="`volume-${item.plate_name}`" :style="{ height: volumeHeight(item.limit_up_count) }"></span>
      </div>
      <div id="main1" class="rotation-chart">
        <article v-for="item in topItems" :key="`rotation-${item.plate_name}`" class="rotation-node">
          <strong>{{ item.plate_name }}</strong>
          <small>{{ item.limit_up_count }}只 / {{ item.max_board }}板</small>
        </article>
      </div>
    </section>

    <section class="rank-panel">
      <table class="strong-table">
        <thead>
          <tr>
            <th style="width: 38px;">序</th>
            <th style="width: 90px;">板块</th>
            <th style="width: 62px;">强度</th>
            <th style="width: 54px;">涨停</th>
            <th style="width: 66px;">封板率</th>
            <th style="width: 54px;">高度</th>
            <th>核心股</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in items" :key="item.plate_name">
            <td>{{ index + 1 }}</td>
            <td class="plate-name">{{ item.plate_name }}</td>
            <td class="score">{{ item.strength_score }}</td>
            <td class="positive">{{ item.limit_up_count }}</td>
            <td>{{ item.seal_rate }}%</td>
            <td>{{ item.max_board }}板</td>
            <td class="core-stocks">
              <button
                v-for="stock in item.core_stocks"
                :key="stock.stock_code"
                type="button"
                @click="openStock(stock.stock_code)"
              >
                {{ stock.stock_name }}{{ stock.board }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="loading" class="state-line">加载中...</div>
    <div v-else-if="!items.length" class="state-line">{{ emptyText }}</div>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { getTdxPlateStrength } from '@/api/tdx-plugins'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import type { TdxPlateStrength, TdxPluginPayload } from '@/types/tdx-plugins'

const payload = ref<TdxPluginPayload<TdxPlateStrength> | null>(null)
const loading = ref(false)
const { openStock } = useTdxStockLink()
let refreshTimer = 0

const items = computed(() => payload.value?.items || [])
const topItems = computed(() => items.value.slice(0, 12))
const maxScore = computed(() => Math.max(1, ...topItems.value.map(item => item.strength_score || 0)))
const maxLimitUp = computed(() => Math.max(1, ...topItems.value.map(item => item.limit_up_count || 0)))
const updatedAt = computed(() => (payload.value?.updated_at || '').replace('T', ' ').slice(5, 19))
const emptyText = computed(() => payload.value?.warnings?.[0] || '暂无板块强度数据')

async function loadData() {
  loading.value = true
  try {
    payload.value = await getTdxPlateStrength()
  } finally {
    loading.value = false
  }
}

function barWidth(value: number) {
  return `${Math.max(4, Math.round((value / maxScore.value) * 100))}%`
}

function volumeHeight(value: number) {
  return `${Math.max(10, Math.round((value / maxLimitUp.value) * 100))}%`
}

onMounted(() => {
  loadData()
  refreshTimer = window.setInterval(loadData, 15000)
})

onUnmounted(() => {
  window.clearInterval(refreshTimer)
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

.strong-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 28px;
  padding: 3px 6px;
  border-bottom: 1px solid #222;
  background: #151515;
}

.strong-top strong {
  color: #f0be83;
  font-size: 13px;
}

.strong-top time {
  color: #b0b0b0;
}

.type-tabs {
  display: flex;
  width: 100%;
}

.datatype {
  width: 50%;
  height: 23px;
  border: 1px solid #666;
  background: transparent;
  color: #ddd;
  font-size: 12px;
}

.datatype.active {
  background: #b23b37;
  color: #fff;
}

#dates {
  overflow-x: auto;
  border-bottom: 1px solid #222;
}

.scroll-container {
  overflow: auto;
  white-space: nowrap;
}

.dates-container {
  display: flex;
  gap: 4px;
  min-width: max-content;
  padding: 4px 6px;
}

.dates-container button {
  height: 22px;
  padding: 0 8px;
  border: 1px solid #666;
  border-radius: 3px;
  background: transparent;
  color: #ddd;
  font-size: 12px;
}

.dates-container button.active {
  background: #f0ad4e;
  color: #111;
}

.chart-shell {
  min-width: 760px;
  padding: 5px 6px 0;
}

.mini-chart {
  height: 100px;
  display: grid;
  gap: 3px;
  padding: 4px 0;
  border-bottom: 1px solid #2a2a2a;
}

.bar-row {
  display: grid;
  grid-template-columns: 78px minmax(80px, 1fr) 58px;
  align-items: center;
  gap: 6px;
  color: #ddd;
}

.bar-row span,
.bar-row em {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bar-row em {
  color: #ff6b6b;
  font-style: normal;
  text-align: right;
}

.bar-row i {
  display: block;
  height: 8px;
  border-radius: 2px;
  background: linear-gradient(90deg, #6739b6, #ff6b6b);
}

.volume-chart {
  display: flex;
  align-items: end;
  gap: 10px;
  height: 70px;
  padding: 8px 4px 0;
  border-bottom: 1px solid #2a2a2a;
}

.volume-chart span {
  width: 18px;
  min-height: 8px;
  background: #6739b6;
}

.rotation-chart {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 160px;
  padding: 10px 0;
  border-bottom: 1px solid #2a2a2a;
}

.rotation-node {
  display: grid;
  align-content: center;
  gap: 4px;
  width: 92px;
  min-height: 62px;
  border: 1px solid #333;
  background: #161922;
  text-align: center;
}

.rotation-node strong {
  color: #f0be83;
  font-weight: 400;
}

.rotation-node small {
  color: #b0b0b0;
}

.rank-panel {
  min-width: 760px;
  padding: 5px;
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
  border-bottom: 1px solid #2d3748;
  overflow: hidden;
  color: #e2e8f0;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.strong-table th {
  background: #202432;
  color: #b0b0b0;
  font-weight: 600;
}

.strong-table tr:hover td {
  background: #1f2937;
}

.plate-name {
  color: #f0be83 !important;
}

.score,
.positive {
  color: #ff6b6b !important;
}

.core-stocks {
  text-align: left;
}

.core-stocks button {
  margin-right: 8px;
  border: 0;
  background: transparent;
  color: #f0be83;
  cursor: pointer;
}

.state-line {
  padding: 10px;
  color: #b0b0b0;
}

@media (max-width: 640px) {
  .strong-top {
    align-items: flex-start;
    flex-direction: column;
    gap: 2px;
  }
}
</style>
