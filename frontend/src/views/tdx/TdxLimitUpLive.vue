<template>
  <main class="target-ztlive" id="dark">
    <div id="plates">
      <div
        ref="plateScroller"
        class="scroll-container"
        :class="{ dragging: isPlateDragging }"
        @mousedown="startPlateDrag"
        @mousemove="dragPlateScroller"
        @mouseup="stopPlateDrag"
        @mouseleave="stopPlateDrag"
      >
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
                  <input type="checkbox" :checked="speechUnlocked" @change="handleSpeechToggle" />
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
              @click="handleStockClick(item)"
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
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { getTdxLimitUpLive, getTdxLimitUpLiveStatus } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import { useTdxPluginRealtime } from '@/composables/useWebSocket'
import { useLimitUpStore } from '@/stores/limit-up'
import { formatTdxSealAmount, pickDisplayChangePct } from '@/utils/tdxLimitUpDisplay'
import type { LimitUpRealtime } from '@/types/limit-up'
import type { TdxLimitUpEvent, TdxPluginPayload } from '@/types/tdx-plugins'

const QUOTE_REFRESH_MS = 3000
const SNAPSHOT_REFRESH_MS = 30000

const payload = ref<TdxPluginPayload<TdxLimitUpEvent> | null>(null)
const statusPayload = ref<TdxPluginPayload<TdxLimitUpEvent> | null>(null)
const activePlate = ref('')
const hideOpened = ref(false)
const loading = ref(false)
const errorText = ref('')
const seenSpeechKeys = new Set<string>()
const plateScroller = ref<HTMLElement | null>(null)
const isPlateDragging = ref(false)
const { enqueuePluginSpeech, unlockSpeech, lockSpeech, speechUnlocked } = useSpeech()
const { openStock } = useTdxStockLink()
const { realtimeLimitUpEvents } = useTdxPluginRealtime()
const limitUpStore = useLimitUpStore()
const { realtimeList } = storeToRefs(limitUpStore)
let snapshotTimer = 0
let quoteTimer = 0
let snapshotInFlight = false
let statusInFlight = false
let hasPrimedLimitUpSpeech = false
let plateDragStartX = 0
let plateDragStartLeft = 0

const events = computed(() => buildMergedEvents())
const plateFilters = computed(() => buildPlateFilters(events.value))
const emptyText = computed(() => {
  if (errorText.value) return errorText.value
  if (payload.value?.warnings?.length) return payload.value.warnings[0]
  if (statusPayload.value?.warnings?.length) return statusPayload.value.warnings[0]
  if (activePlate.value) return '当前板块暂无涨停播报数据'
  return '暂无涨停播报数据'
})
const visibleEvents = computed(() => events.value.filter(item => {
  if (hideOpened.value && !item.is_sealed) return false
  if (!activePlate.value) return true
  return (item.target_plate || item.target_reason_summary || item.reason || '').includes(activePlate.value)
}))

async function loadData(options: { silent?: boolean } = {}) {
  if (snapshotInFlight) return
  snapshotInFlight = true
  if (!options.silent && !payload.value) loading.value = true
  errorText.value = ''
  try {
    const next = await getTdxLimitUpLive()
    payload.value = next
    rememberExistingEvents(next.items)
  } catch (error) {
    const message = error instanceof Error ? error.message : '接口请求失败'
    errorText.value = `涨停播报加载失败：${message}`
  } finally {
    snapshotInFlight = false
    loading.value = false
  }
}

async function loadQuoteStatus() {
  if (statusInFlight) return
  statusInFlight = true
  try {
    const next = await getTdxLimitUpLiveStatus()
    statusPayload.value = next
    handleStatusEvents(next.items)
  } finally {
    statusInFlight = false
  }
}

async function refreshSnapshotWhenStructureChanged() {
  if (!payload.value || snapshotInFlight) return
  const statusItems = statusPayload.value?.items || []
  if (!hasSnapshotStructureChanged(statusItems, payload.value.items || [])) return
  await loadData({ silent: true })
}

function hasSnapshotStructureChanged(statusItems: readonly TdxLimitUpEvent[], snapshotItems: readonly TdxLimitUpEvent[]) {
  if (!statusItems.length || !snapshotItems.length) return false
  if (statusItems.length !== snapshotItems.length) return true

  const snapshotCodes = new Set(snapshotItems.map(item => item.stock_code).filter(Boolean))
  if (snapshotCodes.size !== statusItems.length) return true
  return statusItems.some(item => !snapshotCodes.has(item.stock_code))
}

function rememberExistingEvents(items: TdxLimitUpEvent[]) {
  for (const item of items) {
    seenSpeechKeys.add(limitUpEventSpeechKey(item))
  }
}

function announceNewStatusEvents(items: TdxLimitUpEvent[]) {
  for (const item of items) {
    const key = limitUpEventSpeechKey(item)
    if (!key || seenSpeechKeys.has(key)) continue
    seenSpeechKeys.add(key)
    enqueuePluginSpeech(limitUpSpeechText(item), key, { force: true, urgent: true })
  }
}

function handleStatusEvents(items: TdxLimitUpEvent[]) {
  if (!hasPrimedLimitUpSpeech) {
    rememberExistingEvents(items)
    hasPrimedLimitUpSpeech = true
    return
  }
  announceNewStatusEvents(items)
}

function handleSpeechToggle(event: Event) {
  const input = event.target as HTMLInputElement | null
  if (input && !input.checked) {
    lockSpeech()
    return
  }
  unlockSpeech({ silent: true })
}

function handleStockClick(item: TdxLimitUpEvent) {
  openStock(item.stock_code)
}

function startPlateDrag(event: MouseEvent) {
  const element = plateScroller.value
  if (!element) return
  isPlateDragging.value = true
  plateDragStartX = event.clientX
  plateDragStartLeft = element.scrollLeft
  event.preventDefault()
}

function dragPlateScroller(event: MouseEvent) {
  const element = plateScroller.value
  if (!element || !isPlateDragging.value) return
  element.scrollLeft = plateDragStartLeft - (event.clientX - plateDragStartX)
  event.preventDefault()
}

function stopPlateDrag() {
  isPlateDragging.value = false
}

function buildMergedEvents() {
  const byCode = new Map<string, TdxLimitUpEvent>()
  for (const item of payload.value?.items || []) {
    upsertEvent(byCode, item)
  }
  for (const item of statusPayload.value?.items || []) {
    upsertEvent(byCode, item)
  }
  for (const item of realtimeList.value || []) {
    upsertEvent(byCode, realtimeToTdxEvent(item))
  }
  for (const item of realtimeLimitUpEvents.value || []) {
    upsertEvent(byCode, item)
  }
  return Array.from(byCode.values()).sort((a, b) => {
    const timeOrder = (b.event_time || '').localeCompare(a.event_time || '')
    return timeOrder || (b.board || 0) - (a.board || 0)
  })
}

function upsertEvent(map: Map<string, TdxLimitUpEvent>, next: TdxLimitUpEvent) {
  const code = next.stock_code
  if (!code) return
  const previous = map.get(code)
  map.set(code, previous ? mergeLimitUpEvent(previous, next) : normalizeTdxEvent(next))
}

function mergeLimitUpEvent(previous: TdxLimitUpEvent, next: TdxLimitUpEvent) {
  const merged = { ...previous, ...next }
  const sealAmount = Number(merged.seal_amount || 0)
  return {
    ...merged,
    change_pct: pickDisplayChangePct(previous.change_pct, next.change_pct),
    reason: next.reason || previous.reason,
    reason_category: next.reason_category && next.reason_category !== '其他' ? next.reason_category : previous.reason_category,
    sources: next.sources?.length ? next.sources : previous.sources,
    target_plate: next.target_plate || previous.target_plate,
    target_reason_summary: next.target_reason_summary || previous.target_reason_summary,
    target_status_label: next.target_status_label || previous.target_status_label,
    target_seal_amount: formatTdxSealAmount(sealAmount),
    event_id: next.event_id || previous.event_id,
    event_time: next.event_time || previous.event_time
  }
}

function normalizeTdxEvent(item: TdxLimitUpEvent): TdxLimitUpEvent {
  const sealAmount = Number(item.seal_amount || 0)
  return {
    event_id: item.event_id || `tdx-${item.stock_code}-${item.event_time}`,
    event_type: item.event_type || (item.is_sealed ? 'limit_up_sealed' : 'limit_up_opened'),
    event_label: item.event_label || (item.is_sealed ? '封死涨停' : '涨停打开'),
    event_time: item.event_time || '',
    stock_code: item.stock_code,
    stock_name: item.stock_name || item.stock_code,
    board: Number(item.board || 1),
    reason: item.reason || '',
    reason_category: item.reason_category || '其他',
    change_pct: Number(item.change_pct || 0),
    seal_amount: sealAmount,
    amount: Number(item.amount || 0),
    turnover_rate: Number(item.turnover_rate || 0),
    is_sealed: Boolean(item.is_sealed),
    open_count: Number(item.open_count || 0),
    sources: item.sources || [],
    target_status_label: item.target_status_label || '',
    target_plate: item.target_plate || '',
    target_reason_summary: item.target_reason_summary || '',
    target_seal_amount: item.target_seal_amount || formatTdxSealAmount(sealAmount)
  }
}

function realtimeToTdxEvent(item: LimitUpRealtime): TdxLimitUpEvent {
  const isSealed = Boolean(item.is_sealed ?? item.is_final_sealed)
  const status = String(item.current_status || (isSealed ? 'sealed' : 'opened'))
  const openCount = Number(item.open_count || 0)
  const sealAmount = Number(item.seal_amount || 0)
  const eventLabel = !isSealed || status === 'opened'
    ? '涨停打开'
    : status === 'resealed' || openCount > 0
      ? '涨停回封'
      : '封死涨停'
  const eventType = !isSealed || status === 'opened'
    ? 'limit_up_opened'
    : status === 'resealed' || openCount > 0
      ? 'limit_up_resealed'
      : 'limit_up_sealed'
  const eventTime = formatEventTime(item.first_limit_up_time || item.final_seal_time)
  return {
    event_id: `realtime-${item.trade_date || ''}-${item.stock_code}-${eventType}-${eventTime}`,
    event_type: eventType,
    event_label: eventLabel,
    event_time: eventTime,
    stock_code: item.stock_code,
    stock_name: item.stock_name,
    board: Number(item.continuous_limit_up_days || 1),
    reason: item.limit_up_reason || item.reason_category || '',
    reason_category: item.reason_category || '其他',
    change_pct: Number((item as LimitUpRealtime & { change_pct?: number }).change_pct || 0),
    seal_amount: sealAmount,
    amount: Number(item.amount || 0),
    turnover_rate: Number(item.turnover_rate || 0),
    is_sealed: isSealed,
    open_count: openCount,
    sources: ['实时涨停池'],
    target_status_label: targetStatusLabel(isSealed, Number(item.continuous_limit_up_days || 1)),
    target_plate: item.reason_category || item.industry || '',
    target_reason_summary: item.limit_up_reason || item.reason_category || '',
    target_seal_amount: formatTdxSealAmount(sealAmount)
  }
}

function buildPlateFilters(items: TdxLimitUpEvent[]) {
  const counts = new Map<string, number>()
  for (const item of items) {
    if (!item.is_sealed) continue
    const plate = item.target_plate || item.reason_category || ''
    if (!plate) continue
    for (const name of plate.split(/[+、,，]/).map(value => value.trim()).filter(Boolean)) {
      counts.set(name, (counts.get(name) || 0) + 1)
    }
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 24)
    .map(([name, count]) => ({ name, count }))
}

function targetStatusLabel(isSealed: boolean, board: number) {
  if (!isSealed) return '炸板'
  return board > 1 ? `${board}板` : '首板'
}

function limitUpStatusSpeechLabel(item: TdxLimitUpEvent) {
  const rawLabel = item.target_status_label || item.event_label || ''
  if (rawLabel && !rawLabel.includes('封死涨停')) return rawLabel
  if (item.event_type === 'limit_up_opened' || !item.is_sealed) return '炸板'
  if (item.event_type === 'limit_up_resealed') return '回封'
  return targetStatusLabel(true, Number(item.board || 1))
}

function limitUpSpeechText(item: TdxLimitUpEvent) {
  return `${item.stock_name}${limitUpStatusSpeechLabel(item)}`
}

function limitUpEventSpeechKey(item: TdxLimitUpEvent) {
  return item.event_id || `${item.stock_code}-${item.event_type}-${item.event_time}`
}

function formatEventTime(value?: string | Date | null) {
  if (!value) return ''
  if (value instanceof Date) return value.toTimeString().slice(0, 8)
  const text = String(value)
  const match = text.match(/(\d{2}:\d{2}:\d{2})/)
  return match ? match[1] : text
}

function togglePlate(name: string) {
  activePlate.value = activePlate.value === name ? '' : name
}

function formatPct(value: number) {
  if (!Number.isFinite(value) || value === 0) return '-'
  return `${value.toFixed(2)}%`
}

function formatAmount(value: number) {
  return formatTdxSealAmount(value)
}

function displayStatus(item: TdxLimitUpEvent) {
  return item.target_status_label || item.event_label || (item.is_sealed ? '封死涨停' : '涨停打开')
}

onMounted(() => {
  loadData()
  loadQuoteStatus()
  snapshotTimer = window.setInterval(refreshSnapshotWhenStructureChanged, SNAPSHOT_REFRESH_MS)
  quoteTimer = window.setInterval(loadQuoteStatus, QUOTE_REFRESH_MS)
})

watch(realtimeLimitUpEvents, (nextItems, previousItems) => {
  const previousKeys = new Set(previousItems.map(item => item.event_id))
  const newRealtimeItems = nextItems.filter(item => !previousKeys.has(item.event_id))
  handleStatusEvents(newRealtimeItems)
})

onUnmounted(() => {
  window.clearInterval(snapshotTimer)
  window.clearInterval(quoteTimer)
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
  cursor: grab;
  -ms-overflow-style: none;
  scrollbar-width: none;
  user-select: none;
}

.scroll-container::-webkit-scrollbar {
  display: none;
}

.scroll-container.dragging {
  cursor: grabbing;
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
  min-height: 0;
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
