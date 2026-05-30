<template>
  <main class="target-yidong">
    <article v-for="item in items" :key="item.stock_code" class="move-text">
      <header class="stock-head" @click="openStock(item.stock_code)">
        {{ item.stock_name }}（{{ item.stock_code }}）&nbsp;&nbsp;&nbsp;
        最近涨停：{{ item.trade_date || '-' }}
      </header>

      <section class="reason-block">
        <p class="reason-title">{{ reasonTitle(item) }}</p>
        <p v-for="line in reasonLines(item)" :key="line" class="reason-line">{{ line }}</p>
      </section>

    </article>

    <div v-if="!loading && !items.length" class="state-line">{{ emptyText }}</div>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { getTdxStockMove } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import { installTdxStockSelectionBridge } from '@/composables/useTdxStockSelection'
import type { TdxPluginPayload, TdxStockMove } from '@/types/tdx-plugins'

const STOCK_MOVE_CACHE_TTL_MS = 5 * 60 * 1000
const STOCK_MOVE_CACHE_MAX = 160

type StockMoveCacheEntry = {
  payload: TdxPluginPayload<TdxStockMove>
  cachedAt: number
}

const stockMoveCache = new Map<string, StockMoveCacheEntry>()

const route = useRoute()
const payload = ref<TdxPluginPayload<TdxStockMove> | null>(null)
const loading = ref(false)
const stockCode = ref(routeStockCode() || '600589')
const { enqueuePluginSpeech } = useSpeech()
const { openStock } = useTdxStockLink()
let stockMoveRequestId = 0
let tdxSelectionCleanup: (() => void) | null = null

const items = computed(() => payload.value?.items || [])
const emptyText = computed(() => payload.value?.warnings?.[0] || '暂无异动解析数据')

function routeCode() {
  const value = route.params.code
  return Array.isArray(value) ? value[0] : value
}

function routeStockCode() {
  return normalizeStockCode(routeCode() || '')
}

async function loadData() {
  if (!stockCode.value) return
  const requestId = ++stockMoveRequestId
  const cached = readCachedStockMove(stockCode.value)
  if (cached) {
    payload.value = cached
  } else {
    payload.value = null
  }
  loading.value = true
  try {
    const next = await getTdxStockMove(stockCode.value)
    if (requestId !== stockMoveRequestId) return
    rememberCachedStockMove(stockCode.value, next)
    payload.value = next
    const item = payload.value.items[0]
    if (item?.reasons?.[0]) {
      enqueuePluginSpeech(`${item.stock_name}异动，${item.reasons[0].title}`, `stock-move-${item.stock_code}-${item.reasons[0].title}`, { force: true })
    }
  } finally {
    if (requestId === stockMoveRequestId) loading.value = false
  }
}

function handleExternalStockSelection(code: string) {
  const nextCode = normalizeStockCode(code)
  if (!nextCode || (nextCode === stockCode.value && payload.value)) return
  stockCode.value = nextCode
  loadData()
}

function readCachedStockMove(stockCode: string) {
  const key = normalizeStockCode(stockCode)
  const entry = stockMoveCache.get(key)
  if (!entry) return null
  if (Date.now() - entry.cachedAt > STOCK_MOVE_CACHE_TTL_MS) {
    stockMoveCache.delete(key)
    return null
  }
  return entry.payload
}

function rememberCachedStockMove(stockCode: string, next: TdxPluginPayload<TdxStockMove>) {
  const key = normalizeStockCode(stockCode)
  if (!key) return
  stockMoveCache.set(key, { payload: next, cachedAt: Date.now() })
  if (stockMoveCache.size <= STOCK_MOVE_CACHE_MAX) return
  const oldestKey = stockMoveCache.keys().next().value
  if (oldestKey) stockMoveCache.delete(oldestKey)
}

function normalizeStockCode(code: string) {
  const digits = String(code || '').replace(/\D/g, '').slice(-6)
  return digits ? digits.padStart(6, '0') : ''
}

function reasonTitle(item: TdxStockMove) {
  return item.reasons?.[0]?.title || item.related_plates?.join('+') || item.industry || '暂无异动原因'
}

function reasonLines(item: TdxStockMove) {
  const lines: string[] = []
  for (const reason of item.reasons || []) {
    const numbered = numberedParagraphs(reason.content)
    const chunks = reason.content.split(/[。；;]/).map(part => part.trim()).filter(Boolean)
    if (numbered.length) lines.push(...numbered)
    else if (chunks.length) lines.push(...chunks)
    else if (reason.content) lines.push(reason.content)
  }
  if (item.announcements?.length) lines.push(...item.announcements)
  return lines.length ? lines : ['暂无异动解析数据']
}

function numberedParagraphs(content: string) {
  const paragraphs = content.split(/\n+/).map(part => part.trim()).filter(Boolean)
  if (paragraphs.length > 1 && paragraphs.every(part => /^\d+、/.test(part))) {
    return paragraphs
  }
  return []
}

watch(() => route.params.code, () => {
  const nextCode = routeStockCode()
  if (nextCode) handleExternalStockSelection(nextCode)
})

onMounted(() => {
  tdxSelectionCleanup = installTdxStockSelectionBridge(handleExternalStockSelection)
  if (!stockMoveRequestId) loadData()
})

onUnmounted(() => {
  tdxSelectionCleanup?.()
  tdxSelectionCleanup = null
})
</script>

<style scoped>
.target-yidong {
  min-height: 100vh;
  overflow: auto;
  background: #111219;
  color: #ddd;
  font-family: "Microsoft YaHei", Arial, sans-serif;
  font-size: 12px;
  line-height: 1.5;
}

.move-text {
  min-height: 100vh;
  background: #111219;
}

.stock-head {
  height: 30px;
  line-height: 30px;
  padding: 5px 8px;
  border-bottom: 1px solid #555;
  background: #212433;
  color: #ff4a4a;
  font-size: 14px;
  font-weight: 400;
  cursor: pointer;
}

.reason-block {
  width: 96%;
  margin-left: 5px;
  font-size: 12px;
}

.reason-block p {
  margin: 0 0 10px;
  white-space: normal;
  word-break: break-word;
}

.reason-title {
  margin-top: 10px !important;
  color: #F0BE83;
  font-size: 14px;
  font-weight: 400;
}

.state-line {
  padding: 10px;
  color: #999;
}
</style>
