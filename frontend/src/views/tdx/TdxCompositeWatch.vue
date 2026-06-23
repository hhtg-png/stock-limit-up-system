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

    <section
      ref="ztPanel"
      class="zt-panel"
      :class="{ resizing: isMovePanelResizing }"
      :style="panelLayoutStyle"
    >
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

      <div
        class="move-panel-resizer"
        title="拖拽调整高度"
        @pointerdown="startMovePanelResize"
      ></div>

      <section class="embedded-move-panel">
        <header class="embedded-move-head" @click="selectedMoveItem && openStock(selectedMoveItem.stock_code)">
          <strong v-if="selectedMoveItem">{{ selectedMoveItem.stock_name }}（{{ selectedMoveItem.stock_code }}）</strong>
          <small v-if="selectedMoveItem">最近涨停：{{ selectedMoveItem.trade_date || '-' }}</small>
          <small v-else>{{ stockMoveEmptyText }}</small>
        </header>
        <div class="embedded-move-body">
          <template v-if="selectedMoveItem">
            <p class="embedded-reason-title">{{ stockMoveReasonTitle(selectedMoveItem) }}</p>
            <p v-for="line in stockMoveReasonLines(selectedMoveItem)" :key="line" class="embedded-reason-line">
              {{ line }}
            </p>
            <div v-if="stockMoveTags(selectedMoveItem).length" class="embedded-move-tags">
              <span v-for="tag in stockMoveTags(selectedMoveItem)" :key="tag">{{ tag }}</span>
            </div>
          </template>
          <div v-else class="state-line">{{ stockMoveEmptyText }}</div>
        </div>
      </section>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { getTdxLimitUpLive, getTdxLimitUpLiveStatus, getTdxNews, getTdxStockMove } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import { installTdxStockSelectionBridge } from '@/composables/useTdxStockSelection'
import { clearTdxPluginRealtime, useTdxPluginRealtime } from '@/composables/useWebSocket'
import { useLimitUpStore } from '@/stores/limit-up'
import { formatTdxSealAmount, pickDisplayChangePct, resolveTdxMergedDisplayState } from '@/utils/tdxLimitUpDisplay'
import type { LimitUpRealtime } from '@/types/limit-up'
import type { TdxLimitUpEvent, TdxNewsItem, TdxPluginPayload, TdxStockMove } from '@/types/tdx-plugins'

const QUOTE_REFRESH_MS = 3000
const SNAPSHOT_REFRESH_MS = 30000
const NEWS_SNAPSHOT_LIMIT = 20
const MOVE_PANEL_DEFAULT_PERCENT = 33
const MOVE_PANEL_MIN_PERCENT = 18
const MOVE_PANEL_MAX_PERCENT = 62
const MOVE_PANEL_STORAGE_KEY = 'tdx-limit-up-move-panel-percent'
const STOCK_MOVE_CACHE_TTL_MS = 5 * 60 * 1000
const STOCK_MOVE_CACHE_MAX = 160
const LIMIT_UP_SPEECH_DEDUPE_WINDOW_MS = 60 * 1000

type StockMoveCacheEntry = {
  payload: TdxPluginPayload<TdxStockMove>
  cachedAt: number
}

const stockMoveCache = new Map<string, StockMoveCacheEntry>()

const payload = ref<TdxPluginPayload<TdxLimitUpEvent> | null>(null)
const statusPayload = ref<TdxPluginPayload<TdxLimitUpEvent> | null>(null)
const activePlate = ref('')
const hideOpened = ref(false)
const loading = ref(false)
const errorText = ref('')
const activeTradeDate = ref('')
const seenSpeechKeys = new Set<string>()
const seenTouchedStockCodes = new Set<string>()
const spokenLimitUpSpeechAt = new Map<string, number>()
const knownNewsKeys = new Set<string>()
const spokenNewsKeys = new Set<string>()
const stockMovePayload = ref<TdxPluginPayload<TdxStockMove> | null>(null)
const stockMoveLoading = ref(false)
const stockMoveErrorText = ref('')
const selectedMoveCode = ref('')
const plateScroller = ref<HTMLElement | null>(null)
const isPlateDragging = ref(false)
const ztPanel = ref<HTMLElement | null>(null)
const movePanelPercent = ref(readStoredMovePanelPercent())
const isMovePanelResizing = ref(false)
const { enqueuePluginSpeech, unlockSpeech, lockSpeech, speechUnlocked } = useSpeech()
const { openStock } = useTdxStockLink()
const { realtimeLimitUpEvents, realtimeNewsItems } = useTdxPluginRealtime()
const limitUpStore = useLimitUpStore()
const { realtimeList } = storeToRefs(limitUpStore)
let snapshotTimer = 0
let quoteTimer = 0
let snapshotHydrationTimer = 0
let snapshotInFlight = false
let statusInFlight = false
let hasPrimedLimitUpSpeech = false
let stockMoveRequestId = 0
let plateDragStartX = 0
let plateDragStartLeft = 0
let tdxSelectionCleanup: (() => void) | null = null

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
const stockMoveItems = computed(() => stockMovePayload.value?.items || [])
const selectedMoveItem = computed(() => stockMoveItems.value[0] || null)
const stockMoveEmptyText = computed(() => stockMoveErrorText.value || stockMovePayload.value?.warnings?.[0] || '点击股票查看详情')
const panelLayoutStyle = computed(() => ({
  '--zt-table-flex': String(100 - movePanelPercent.value),
  '--move-panel-flex': String(movePanelPercent.value)
}))

async function loadData(options: { silent?: boolean } = {}) {
  if (snapshotInFlight) return
  snapshotInFlight = true
  if (!options.silent && !payload.value) loading.value = true
  errorText.value = ''
  try {
    const next = await getTdxLimitUpLive()
    applyPayloadTradeDate(next)
    payload.value = next
    rememberExistingEvents(next.items)
    primeStockMove(next.items)
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
    applyPayloadTradeDate(next)
    statusPayload.value = next
    handleStatusEvents(next.items, { primeOnly: true })
    primeStockMove(next.items)
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

function hydrateSnapshotAfterStatus() {
  window.clearTimeout(snapshotHydrationTimer)
  snapshotHydrationTimer = window.setTimeout(() => {
    loadData({ silent: true })
  }, 250)
}

function hasSnapshotStructureChanged(statusItems: readonly TdxLimitUpEvent[], snapshotItems: readonly TdxLimitUpEvent[]) {
  if (!statusItems.length || !snapshotItems.length) return false
  if (statusItems.length !== snapshotItems.length) return true

  const snapshotCodes = new Set(snapshotItems.map(item => item.stock_code).filter(Boolean))
  if (snapshotCodes.size !== statusItems.length) return true
  return statusItems.some(item => !snapshotCodes.has(item.stock_code))
}

function applyPayloadTradeDate(next: TdxPluginPayload<TdxLimitUpEvent>) {
  const tradeDate = payloadTradeDate(next)
  if (!tradeDate || activeTradeDate.value === tradeDate) return
  resetDailyState(tradeDate)
}

function payloadTradeDate(next: TdxPluginPayload<TdxLimitUpEvent> | null) {
  return String(next?.updated_at || '').slice(0, 10)
}

function resetDailyState(tradeDate: string) {
  activeTradeDate.value = tradeDate
  payload.value = null
  statusPayload.value = null
  activePlate.value = ''
  seenSpeechKeys.clear()
  seenTouchedStockCodes.clear()
  spokenLimitUpSpeechAt.clear()
  hasPrimedLimitUpSpeech = false
  stockMoveRequestId++
  selectedMoveCode.value = ''
  stockMovePayload.value = null
  stockMoveErrorText.value = ''
  stockMoveLoading.value = false
  stockMoveCache.clear()
  limitUpStore.setRealtimeSnapshot(tradeDate, [])
  clearTdxPluginRealtime()
}

function isCurrentTradeDate(value?: string | null) {
  return !activeTradeDate.value || !value || value === activeTradeDate.value
}

function rememberExistingEvents(items: TdxLimitUpEvent[]) {
  for (const item of items) {
    seenSpeechKeys.add(limitUpEventSpeechKey(item))
    rememberTouchedStock(item)
  }
}

function announceNewStatusEvents(items: TdxLimitUpEvent[]) {
  for (const item of items) {
    const key = limitUpEventSpeechKey(item)
    if (!key || seenSpeechKeys.has(key)) continue
    const isFirstTouch = isFirstTouchedStock(item)
    if (!isFirstTouch && isPlainSealedStatusEvent(item)) {
      seenSpeechKeys.add(key)
      continue
    }
    const speechText = limitUpSpeechText(item, isFirstTouch)
    if (!rememberLimitUpSpeech(item, speechText)) {
      seenSpeechKeys.add(key)
      continue
    }
    if (!enqueuePluginSpeech(speechText, key, { force: true, urgent: true })) {
      forgetLimitUpSpeech(item, speechText)
      continue
    }
    seenSpeechKeys.add(key)
    rememberTouchedStock(item)
  }
}

function isFirstTouchedStock(item: TdxLimitUpEvent) {
  const code = item.stock_code
  return Boolean(code && !seenTouchedStockCodes.has(code))
}

function rememberTouchedStock(item: TdxLimitUpEvent) {
  const code = item.stock_code
  if (!code) return false
  const isFirstTouch = !seenTouchedStockCodes.has(code)
  seenTouchedStockCodes.add(code)
  return isFirstTouch
}

function handleStatusEvents(items: TdxLimitUpEvent[], options: { primeOnly?: boolean } = {}) {
  if (options.primeOnly && !hasPrimedLimitUpSpeech) {
    rememberExistingEvents(items)
    hasPrimedLimitUpSpeech = true
    return
  }
  announceNewStatusEvents(items)
}

function newsKey(item: TdxNewsItem) {
  return `news-${item.news_id || `${item.time}-${item.title}`}`
}

function normalizeSpeechPart(value?: string) {
  return (value || '').replace(/\s+/g, ' ').trim()
}

function newsSpeechText(item: TdxNewsItem) {
  const source = normalizeSpeechPart(item.source)
  const title = normalizeSpeechPart(item.title)
  if (item.source === '韭研公社') {
    return `${source ? `${source}新帖，` : '新帖，'}${title}`.slice(0, 120)
  }
  return title.slice(0, 120)
}

function rememberKnownNews(items: readonly TdxNewsItem[]) {
  for (const item of items) {
    if (item.news_id && item.title) knownNewsKeys.add(newsKey(item))
  }
}

function speakNews(item: TdxNewsItem) {
  if (!item.news_id || !item.title || !speechUnlocked.value) return false
  const key = newsKey(item)
  if (spokenNewsKeys.has(key)) return false
  const queued = enqueuePluginSpeech(newsSpeechText(item), key, { force: true })
  if (!queued) return false
  spokenNewsKeys.add(key)
  knownNewsKeys.add(key)
  return true
}

async function loadInitialNewsSnapshot() {
  try {
    const next = await getTdxNews({ limit: NEWS_SNAPSHOT_LIMIT })
    rememberKnownNews(next.items || [])
  } catch {
    // 快讯语音不再显示状态；失败时保持静默，等待后续实时消息。
  }
}

function handleSpeechToggle(event: Event) {
  const input = event.target as HTMLInputElement | null
  if (input && !input.checked) {
    lockSpeech()
    return
  }
  unlockSpeech({ silent: true })
}

function normalizeStockCode(code: string) {
  const digits = code.replace(/\D/g, '').slice(-6)
  return digits ? digits.padStart(6, '0') : ''
}

function primeStockMove(items: readonly TdxLimitUpEvent[]) {
  if (selectedMoveCode.value) return
  const first = items.find(item => item.stock_code)
  if (first?.stock_code) selectStockMove(first.stock_code)
}

function handleStockClick(item: TdxLimitUpEvent) {
  selectStockMove(item.stock_code)
  openStock(item.stock_code)
}

function handleExternalStockSelection(code: string) {
  selectStockMove(code)
}

function selectStockMove(code: string) {
  const stockCode = normalizeStockCode(code)
  if (!stockCode) return
  if (selectedMoveCode.value === stockCode && stockMovePayload.value) return
  selectedMoveCode.value = stockCode
  const cached = readCachedStockMove(stockCode)
  if (cached) {
    stockMovePayload.value = cached
    stockMoveErrorText.value = ''
  } else {
    stockMovePayload.value = null
  }
  loadStockMove(stockCode)
}

async function loadStockMove(stockCode = selectedMoveCode.value) {
  if (!stockCode) return
  const requestId = ++stockMoveRequestId
  const cached = readCachedStockMove(stockCode)
  if (cached) {
    stockMovePayload.value = cached
    stockMoveErrorText.value = ''
  }
  stockMoveLoading.value = true
  stockMoveErrorText.value = ''
  try {
    const next = await getTdxStockMove(stockCode, activeTradeDate.value ? { trade_date: activeTradeDate.value } : undefined)
    if (requestId !== stockMoveRequestId) return
    rememberCachedStockMove(stockCode, next)
    stockMovePayload.value = next
  } catch (error) {
    if (requestId !== stockMoveRequestId) return
    const message = error instanceof Error ? error.message : '接口请求失败'
    stockMoveErrorText.value = `加载失败：${message}`
  } finally {
    if (requestId === stockMoveRequestId) stockMoveLoading.value = false
  }
}

function readCachedStockMove(stockCode: string) {
  const entry = stockMoveCache.get(stockMoveCacheKey(stockCode))
  if (!entry) return null
  if (Date.now() - entry.cachedAt > STOCK_MOVE_CACHE_TTL_MS) {
    stockMoveCache.delete(stockMoveCacheKey(stockCode))
    return null
  }
  return entry.payload
}

function rememberCachedStockMove(stockCode: string, next: TdxPluginPayload<TdxStockMove>) {
  stockMoveCache.set(stockMoveCacheKey(stockCode), { payload: next, cachedAt: Date.now() })
  if (stockMoveCache.size <= STOCK_MOVE_CACHE_MAX) return
  const oldestKey = stockMoveCache.keys().next().value
  if (oldestKey) stockMoveCache.delete(oldestKey)
}

function stockMoveCacheKey(stockCode: string) {
  return `${activeTradeDate.value || 'latest'}:${normalizeStockCode(stockCode)}`
}

function stockMoveReasonTitle(item: TdxStockMove) {
  return item.reasons?.[0]?.title || item.related_plates?.join('+') || item.industry || '暂无异动原因'
}

function stockMoveReasonLines(item: TdxStockMove) {
  const lines: string[] = []
  for (const reason of item.reasons || []) {
    const numbered = numberedParagraphs(reason.content)
    const chunks = reason.content.split(/[。；;]/).map(part => part.trim()).filter(Boolean)
    if (numbered.length) lines.push(...numbered)
    else if (chunks.length) lines.push(...chunks)
    else if (reason.content) lines.push(reason.content)
  }
  if (item.announcements?.length) lines.push(...item.announcements)
  return lines.length ? lines : ['暂无解析数据']
}

function stockMoveTags(item: TdxStockMove) {
  return Array.from(new Set([...(item.related_plates || []), ...(item.concepts || []), item.industry].filter(Boolean))).slice(0, 10)
}

function numberedParagraphs(content: string) {
  const paragraphs = content.split(/\n+/).map(part => part.trim()).filter(Boolean)
  if (paragraphs.length > 1 && paragraphs.every(part => /^\d+、/.test(part))) {
    return paragraphs
  }
  return []
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

function readStoredMovePanelPercent() {
  if (typeof window === 'undefined') return MOVE_PANEL_DEFAULT_PERCENT
  const saved = Number(window.localStorage.getItem(MOVE_PANEL_STORAGE_KEY))
  return clampMovePanelPercent(Number.isFinite(saved) ? saved : MOVE_PANEL_DEFAULT_PERCENT)
}

function clampMovePanelPercent(value: number) {
  return Math.min(MOVE_PANEL_MAX_PERCENT, Math.max(MOVE_PANEL_MIN_PERCENT, value))
}

function startMovePanelResize(event: PointerEvent) {
  isMovePanelResizing.value = true
  window.addEventListener('pointermove', resizeMovePanel)
  window.addEventListener('pointerup', stopMovePanelResize)
  window.addEventListener('pointercancel', stopMovePanelResize)
  event.preventDefault()
}

function resizeMovePanel(event: PointerEvent) {
  const element = ztPanel.value
  if (!element || !isMovePanelResizing.value) return
  const rect = element.getBoundingClientRect()
  const distanceFromBottom = rect.bottom - event.clientY
  const nextPercent = clampMovePanelPercent((distanceFromBottom / rect.height) * 100)
  movePanelPercent.value = Number(nextPercent.toFixed(1))
  window.localStorage.setItem(MOVE_PANEL_STORAGE_KEY, String(movePanelPercent.value))
}

function stopMovePanelResize() {
  isMovePanelResizing.value = false
  window.removeEventListener('pointermove', resizeMovePanel)
  window.removeEventListener('pointerup', stopMovePanelResize)
  window.removeEventListener('pointercancel', stopMovePanelResize)
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
    if (!isCurrentTradeDate(item.trade_date)) continue
    upsertEvent(byCode, realtimeToTdxEvent(item))
  }
  for (const item of realtimeLimitUpEvents.value || []) {
    if (!isCurrentTradeDate(item.trade_date)) continue
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
  const displayState = resolveTdxMergedDisplayState(previous, next)
  const merged = { ...previous, ...next, ...displayState }
  const sealAmount = Number(displayState.seal_amount || 0)
  return {
    ...merged,
    change_pct: pickDisplayChangePct(previous.change_pct, next.change_pct),
    reason: next.reason || previous.reason,
    reason_category: next.reason_category && next.reason_category !== '其他' ? next.reason_category : previous.reason_category,
    sources: next.sources?.length ? next.sources : previous.sources,
    target_plate: next.target_plate || previous.target_plate,
    target_reason_summary: next.target_reason_summary || previous.target_reason_summary,
    target_status_label: resolvedTargetStatusLabel(merged),
    target_seal_amount: displayState.target_seal_amount || formatTdxSealAmount(sealAmount),
    event_id: next.event_id || previous.event_id,
    event_time: next.event_time || previous.event_time
  }
}

function normalizeTdxEvent(item: TdxLimitUpEvent): TdxLimitUpEvent {
  const sealAmount = Number(item.seal_amount || 0)
  return {
    event_id: item.event_id || `tdx-${item.stock_code}-${item.event_time}`,
    trade_date: item.trade_date || activeTradeDate.value,
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
    trade_date: item.trade_date || activeTradeDate.value,
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
  if (item.event_type === 'limit_up_opened' || !item.is_sealed) return '炸板'
  const rawLabel = item.target_status_label || item.event_label || ''
  if (rawLabel && !rawLabel.includes('封死涨停')) return rawLabel
  if (item.event_type === 'limit_up_resealed') return '回封'
  return targetStatusLabel(true, Number(item.board || 1))
}

function limitUpTouchSpeechLabel(item: TdxLimitUpEvent) {
  const rawLabel = item.target_status_label || ''
  if (rawLabel && !/炸板|涨停打开|回封/.test(rawLabel)) return rawLabel
  return targetStatusLabel(true, Number(item.board || 1))
}

function isPlainSealedStatusEvent(item: TdxLimitUpEvent) {
  return item.event_type === 'limit_up_sealed' || (
    item.is_sealed &&
    item.event_type !== 'limit_up_touched' &&
    item.event_type !== 'limit_up_resealed'
  )
}

function limitUpSpeechReason(item: TdxLimitUpEvent) {
  if (item.event_type === 'limit_up_opened' || !item.is_sealed) return ''
  const rawReason = [
    item.target_reason_summary,
    item.reason,
    item.target_plate,
    item.reason_category
  ].find(value => normalizeSpeechReason(value))
  const reason = normalizeSpeechReason(rawReason)
  return reason ? `，${reason}` : ''
}

function normalizeSpeechReason(value?: string | null) {
  return String(value || '')
    .replace(/暂无[^+、/，,;；]*/g, '')
    .replace(/其他/g, '')
    .replace(/[+、/，,;；]+/g, '加')
    .replace(/\s+/g, '')
    .replace(/加+/g, '加')
    .replace(/^加|加$/g, '')
    .slice(0, 24)
}

function limitUpSpeechText(item: TdxLimitUpEvent, isFirstTouch = false) {
  return `${item.stock_name}${isFirstTouch ? limitUpTouchSpeechLabel(item) : limitUpStatusSpeechLabel(item)}${limitUpSpeechReason(item)}`
}

function limitUpEventSpeechKey(item: TdxLimitUpEvent) {
  return item.event_id || `${item.stock_code}-${item.event_type}-${item.event_time}`
}

function limitUpSpeechDedupeKey(item: TdxLimitUpEvent, speechText: string) {
  return `${item.stock_code}-${speechText.replace(/\s+/g, '')}`
}

function pruneLimitUpSpeechDedupe(now = Date.now()) {
  for (const [key, spokenAt] of spokenLimitUpSpeechAt) {
    if (now - spokenAt > LIMIT_UP_SPEECH_DEDUPE_WINDOW_MS) {
      spokenLimitUpSpeechAt.delete(key)
    }
  }
}

function rememberLimitUpSpeech(item: TdxLimitUpEvent, speechText: string, now = Date.now()) {
  pruneLimitUpSpeechDedupe(now)
  const key = limitUpSpeechDedupeKey(item, speechText)
  const spokenAt = spokenLimitUpSpeechAt.get(key)
  if (spokenAt && now - spokenAt < LIMIT_UP_SPEECH_DEDUPE_WINDOW_MS) return false
  spokenLimitUpSpeechAt.set(key, now)
  return true
}

function forgetLimitUpSpeech(item: TdxLimitUpEvent, speechText: string) {
  spokenLimitUpSpeechAt.delete(limitUpSpeechDedupeKey(item, speechText))
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

function resolvedTargetStatusLabel(item: TdxLimitUpEvent) {
  if (item.event_type === 'limit_up_opened' || !item.is_sealed) return '炸板'
  return item.target_status_label || item.event_label || targetStatusLabel(true, Number(item.board || 1))
}

function displayStatus(item: TdxLimitUpEvent) {
  return resolvedTargetStatusLabel(item)
}

onMounted(() => {
  tdxSelectionCleanup = installTdxStockSelectionBridge(handleExternalStockSelection)
  loadQuoteStatus()
  hydrateSnapshotAfterStatus()
  loadInitialNewsSnapshot()
  snapshotTimer = window.setInterval(refreshSnapshotWhenStructureChanged, SNAPSHOT_REFRESH_MS)
  quoteTimer = window.setInterval(loadQuoteStatus, QUOTE_REFRESH_MS)
})

watch(realtimeNewsItems, (nextItems, previousItems) => {
  const previousKeys = new Set(previousItems.map(newsKey))
  for (const item of nextItems) {
    const key = newsKey(item)
    const wasKnown = knownNewsKeys.has(key) || previousKeys.has(key)
    knownNewsKeys.add(key)
    if (!wasKnown) speakNews(item)
  }
})

watch(realtimeLimitUpEvents, (nextItems, previousItems) => {
  const previousKeys = new Set(previousItems.map(item => item.event_id))
  const newRealtimeItems = nextItems.filter(item => !previousKeys.has(item.event_id))
  handleStatusEvents(newRealtimeItems, { primeOnly: false })
})

onUnmounted(() => {
  window.clearTimeout(snapshotHydrationTimer)
  window.clearInterval(snapshotTimer)
  window.clearInterval(quoteTimer)
  stopMovePanelResize()
  tdxSelectionCleanup?.()
  tdxSelectionCleanup = null
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
  --zt-table-flex: 67;
  --move-panel-flex: 33;
}

.zt-body {
  position: relative;
  flex: var(--zt-table-flex) 1 0;
  min-height: 0;
  overflow: auto;
}

.move-panel-resizer {
  flex: 0 0 6px;
  border-top: 1px solid #30394a;
  border-bottom: 1px solid #30394a;
  background: linear-gradient(#171d29, #121722);
  cursor: row-resize;
}

.move-panel-resizer::before {
  display: block;
  width: 34px;
  height: 2px;
  margin: 2px auto;
  border-radius: 2px;
  background: #65748a;
  content: "";
}

.zt-panel.resizing,
.zt-panel.resizing * {
  cursor: row-resize !important;
  user-select: none;
}

.embedded-move-panel {
  display: flex;
  flex: var(--move-panel-flex) 1 0;
  min-height: 0;
  flex-direction: column;
  background: #111219;
}

.embedded-move-head {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 30px;
  padding: 5px 8px;
  border-bottom: 1px solid #2d3748;
  background: #212433;
  color: #8da3bd;
  cursor: pointer;
}

.embedded-move-head span {
  color: #f0be83;
}

.embedded-move-head strong {
  color: #ff6b6b;
  font-size: 13px;
  font-weight: 400;
}

.embedded-move-head small {
  color: #b0b0b0;
}

.embedded-move-body {
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 7px 8px 10px;
  color: #ddd;
  line-height: 1.55;
}

.embedded-reason-title {
  margin: 0 0 8px;
  color: #f0be83;
  font-size: 13px;
}

.embedded-reason-line {
  margin: 0 0 6px;
  white-space: normal;
  word-break: break-word;
}

.embedded-move-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 6px;
}

.embedded-move-tags span {
  padding: 1px 5px;
  border: 1px solid #4a5568;
  color: #f0be83;
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
