<template>
  <main class="target-ztlive" id="dark">
    <div id="plates">
      <div class="scroll-container">
        <div class="dates-container">
          <button
            v-for="plate in plateFilters"
            :key="plate.name"
            type="button"
            class="plate-pill"
            :class="{ active: activePlate === plate.name }"
            @click="togglePlate(plate.name)"
          >
            {{ plate.name }} <span>({{ plate.count }})</span>
          </button>
        </div>
      </div>
      <label class="hide-check">
        <input v-model="hideOpened" type="checkbox" />
        <span>隐藏</span>
      </label>
    </div>

    <section class="zt-panel">
      <table class="target-table zt-head">
        <thead>
          <tr>
            <th style="width: 60px;">名称</th>
            <th style="width: 50px;">代码</th>
            <th style="width: 50px;">涨幅</th>
            <th style="width: 50px;">状态</th>
            <th style="width: 50px;">首封</th>
            <th style="width: 50px;">封单</th>
            <th class="plate-col">
              板块
              <span class="audio-control">
                语音
                <label class="switch">
                  <input type="checkbox" :checked="speechUnlocked" @change="unlockSpeech" />
                  <span class="slider round"></span>
                </label>
              </span>
            </th>
          </tr>
        </thead>
      </table>

      <div class="zt-body">
        <table class="target-table">
          <tbody>
            <tr
              v-for="item in visibleEvents"
              :key="item.event_id"
              class="kline"
              :code="item.stock_code"
              @click="openStock(item.stock_code)"
            >
              <td class="stockname" style="width: 60px;">{{ item.stock_name }}</td>
              <td style="width: 50px;">{{ item.stock_code }}</td>
              <td style="width: 50px;" :class="item.change_pct >= 0 ? 'positive' : 'negative'">
                {{ formatPct(item.change_pct) }}
              </td>
              <td style="width: 50px;" :class="item.is_sealed ? 'positive' : 'negative'">
                {{ displayStatus(item) }}
              </td>
              <td style="width: 50px;">{{ item.event_time }}</td>
              <td style="width: 50px;" class="positive">{{ item.target_seal_amount || formatAmount(item.seal_amount) }}</td>
              <td class="gn" :title="item.target_reason_summary || item.reason">
                {{ item.target_reason_summary || item.target_plate || item.reason_category }}
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="loading && !events.length" class="state-line">加载中...</div>
        <div v-else-if="errorText || !visibleEvents.length" class="state-line">{{ emptyText }}</div>
      </div>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { getTdxLimitUpLive } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import type { TdxLimitUpEvent, TdxPluginPayload } from '@/types/tdx-plugins'

const payload = ref<TdxPluginPayload<TdxLimitUpEvent> | null>(null)
const activePlate = ref('')
const hideOpened = ref(false)
const loading = ref(false)
const errorText = ref('')
const { enqueuePluginSpeech, unlockSpeech, speechUnlocked } = useSpeech()
const { openStock } = useTdxStockLink()
let refreshTimer = 0

const plateFilters = computed(() => payload.value?.plate_filters || [])
const events = computed(() => payload.value?.items || [])
const emptyText = computed(() => {
  if (errorText.value) return errorText.value
  if (payload.value?.warnings?.length) return payload.value.warnings[0]
  if (activePlate.value) return '当前板块暂无涨停播报数据'
  return '暂无涨停播报数据'
})
const visibleEvents = computed(() => events.value.filter(item => {
  if (hideOpened.value && !item.is_sealed) return false
  if (!activePlate.value) return true
  return (item.target_plate || item.reason || '').includes(activePlate.value)
}))

async function loadData() {
  loading.value = true
  errorText.value = ''
  try {
    const next = await getTdxLimitUpLive()
    payload.value = next
    for (const item of next.items.slice(0, 3)) {
      enqueuePluginSpeech(`${item.stock_name}${item.target_status_label || item.event_label}`, item.event_id)
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : '接口请求失败'
    errorText.value = `涨停播报加载失败：${message}`
  } finally {
    loading.value = false
  }
}

function togglePlate(name: string) {
  activePlate.value = activePlate.value === name ? '' : name
}

function formatPct(value: number) {
  if (!Number.isFinite(value) || value === 0) return '-'
  return `${value.toFixed(2)}%`
}

function formatAmount(value: number) {
  if (!value) return '--'
  if (value >= 100000000) return `${(value / 100000000).toFixed(2)}亿`
  return `${(value / 10000).toFixed(0)}万`
}

function displayStatus(item: TdxLimitUpEvent) {
  return item.target_status_label || item.event_label || (item.is_sealed ? '封死涨停' : '涨停打开')
}

onMounted(() => {
  loadData()
  refreshTimer = window.setInterval(loadData, 5000)
})

onUnmounted(() => {
  window.clearInterval(refreshTimer)
})
</script>

<style scoped>
.target-ztlive {
  --bg-primary: #111219;
  --bg-secondary: #1a202c;
  --bg-tertiary: #2d3748;
  --text-primary: #e2e8f0;
  --text-secondary: #b0b0b0;
  --border-color: #2d3748;
  --positive-color: #ff6b6b;
  --negative-color: #51cf66;
  --stock-name: #f0be83;
  display: flex;
  flex-direction: column;
  height: 100vh;
  overflow: hidden;
  background: var(--bg-primary);
  color: var(--text-primary);
  font-size: 12px;
}

#plates {
  display: flex;
  flex-direction: row;
  gap: 4px;
  min-height: 31px;
  padding: 3px 4px;
  background: #202432;
  border-bottom: 1px solid var(--border-color);
}

.scroll-container {
  flex: 1;
  overflow-x: auto;
  white-space: nowrap;
}

.dates-container {
  display: flex;
  gap: 4px;
  min-width: max-content;
  white-space: nowrap;
}

.plate-pill {
  height: 24px;
  padding: 1px 8px;
  border: 1px solid #4a5568;
  border-radius: 6px;
  background: #2d3748;
  color: #d9e6f6;
  font-size: 12px;
  cursor: pointer;
}

.plate-pill span {
  color: #ff6b6b;
  font-weight: 700;
}

.plate-pill.active {
  background: #111;
  color: #f0be83;
}

.hide-check {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  color: #ddd;
  font-size: 12px;
}

.zt-panel {
  display: flex;
  flex: 1;
  flex-direction: column;
  overflow: hidden;
}

.zt-body {
  position: relative;
  flex: 1;
  min-height: 275px;
  overflow: auto;
}

.target-table {
  width: 100%;
  margin-bottom: 0;
  border-collapse: collapse;
  table-layout: fixed;
  background: var(--bg-primary);
  text-align: center;
  font-size: 12px;
}

.target-table th,
.target-table td {
  padding: 8px 5px;
  border-top: 0;
  border-bottom: 1px solid var(--border-color);
  color: var(--text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.target-table th {
  color: var(--text-secondary);
  background: #202432;
  font-weight: 600;
}

.target-table tr:hover td {
  background: #1f2937;
}

.state-line {
  padding: 12px 8px;
  border-bottom: 1px solid var(--border-color);
  color: #8da3bd;
  line-height: 1.5;
}

.stockname {
  color: var(--stock-name);
  cursor: pointer;
}

.positive {
  color: var(--positive-color) !important;
}

.negative {
  color: var(--negative-color) !important;
}

.gn {
  width: 100px;
  max-width: 160px;
  text-align: left;
}

.plate-col {
  width: 100px;
  max-width: 160px;
  text-align: left;
}

.audio-control {
  float: right;
  margin-right: 10px;
  color: #ddd;
  font-weight: 400;
}

.switch {
  position: relative;
  display: inline-block;
  width: 30px;
  height: 16px;
  margin: 0 0 0 3px;
  vertical-align: middle;
}

.switch input {
  display: none;
}

.slider {
  position: absolute;
  inset: 0;
  cursor: pointer;
  background: #555;
  transition: .2s;
}

.slider:before {
  position: absolute;
  content: "";
  width: 12px;
  height: 12px;
  left: 2px;
  bottom: 2px;
  background: #ccc;
  transition: .2s;
}

input:checked + .slider {
  background: #96cdfa;
}

input:checked + .slider:before {
  transform: translateX(14px);
}

.slider.round {
  border-radius: 16px;
}

.slider.round:before {
  border-radius: 50%;
}
</style>
