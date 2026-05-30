<template>
  <main id="black" class="target-news">
    <header class="topset">
      <div class="top-title">语音资讯</div>
      <button type="button" class="setting-btn">设置</button>
      <label class="audiotext">
        语音
        <span class="switch">
          <input type="checkbox" :checked="speechUnlocked" @change="handleSpeechToggle" />
          <span class="slider round"></span>
        </span>
      </label>
    </header>

    <section class="panel">
      <div class="panel-heading">
        <span>聚合快讯</span>
        <div class="btn-group">
          <button type="button" class="active">全部</button>
          <button type="button">加红</button>
        </div>
      </div>

      <ul class="layui-timeline">
        <li v-for="item in aggregateItems" :key="item.news_id" class="layui-timeline-item">
          <i class="layui-icon layui-timeline-axis"></i>
          <article class="layui-timeline-content" :class="{ importnews: item.importance >= 80 }">
            <div class="news-meta">
              <time>{{ item.time }}</time>
              <strong class="newstitle">{{ item.title }}</strong>
              <span class="source">{{ item.source }}</span>
            </div>
            <p class="digest" :class="{ collapsed: !isExpanded(item.news_id) }">{{ item.content }}</p>
            <footer>
              <span class="news-tag">{{ item.source }}</span>
              <span v-for="plate in item.related_plates" :key="plate" class="news-tag">{{ plate }}</span>
              <button v-if="item.jump_url" type="button" @click="openUrl(item.jump_url)">原文</button>
              <button type="button" class="expand-btn" @click="toggleExpanded(item.news_id)">
                {{ isExpanded(item.news_id) ? '收起' : '展开' }}
              </button>
            </footer>
          </article>
        </li>
      </ul>
    </section>

    <section class="panel identify-panel">
      <div class="panel-heading">
        <span>韭研社 | 识别区</span>
        <div class="btn-group">
          <button type="button" class="active">全部</button>
          <button type="button">暂停</button>
        </div>
      </div>

      <div class="identify-list">
        <article v-for="item in identifyItems" :key="`identify-${item.news_id}`" class="identify-row">
          <div>
            <strong>{{ item.title }}</strong>
            <p>{{ item.content }}</p>
          </div>
          <time>{{ item.time }}</time>
        </article>
      </div>
    </section>

    <section class="panel topic-panel">
      <div class="panel-heading">
        <span>题材库</span>
        <div class="btn-group">
          <button type="button" class="active">题材</button>
          <button type="button">个股</button>
          <button type="button">暂停</button>
        </div>
      </div>

      <div class="topic-list">
        <article v-for="item in topicItems" :key="`topic-${item.news_id}`" class="topic-row">
          <strong>{{ topicTitle(item) }}</strong>
          <p>{{ item.content }}</p>
        </article>
      </div>
    </section>

    <div v-if="loading" class="state-line">加载中...</div>
    <div v-else-if="!aggregateItems.length" class="state-line">{{ emptyText }}</div>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { getTdxNews } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxPluginRealtime } from '@/composables/useWebSocket'
import type { TdxNewsItem, TdxPluginPayload } from '@/types/tdx-plugins'

const payload = ref<TdxPluginPayload<TdxNewsItem> | null>(null)
const loading = ref(false)
const expandedNewsIds = ref<Set<string>>(new Set())
const { enqueuePluginSpeech, unlockSpeech, speechUnlocked } = useSpeech()
const { realtimeNewsItems } = useTdxPluginRealtime()
const spokenNewsKeys = new Set<string>()
const knownNewsKeys = new Set<string>()
const NEW_SPEECH_LIMIT = 3
const NEWS_REFRESH_MS = 10000
let refreshTimer = 0
let hasLoadedInitialSnapshot = false

const items = computed(() => mergeRealtimeNews(realtimeNewsItems.value, payload.value?.items || []))
const aggregateItems = computed(() => items.value.filter(isAggregateNewsItem))
const identifyItems = computed(() => {
  const jygsItems = items.value.filter(item => item.source === '韭研公社')
  return (jygsItems.length ? jygsItems : items.value).slice(0, 8)
})
const topicItems = computed(() => {
  const withPlates = items.value.filter(item => item.related_plates?.length)
  return (withPlates.length ? withPlates : items.value).slice(0, 8)
})
const emptyText = computed(() => payload.value?.warnings?.[0] || '暂无聚合快讯数据')

function mergeRealtimeNews(realtimeItems: readonly TdxNewsItem[], snapshotItems: TdxNewsItem[]) {
  const byId = new Map<string, TdxNewsItem>()
  for (const item of snapshotItems) {
    byId.set(item.news_id, item)
  }
  for (const item of realtimeItems) {
    byId.delete(item.news_id)
    byId.set(item.news_id, item)
  }
  return Array.from(byId.values()).sort((a, b) => {
    const timeOrder = (b.time || '').localeCompare(a.time || '')
    return timeOrder || (b.importance || 0) - (a.importance || 0)
  })
}

async function loadData() {
  loading.value = true
  try {
    const nextPayload = await getTdxNews({ limit: 80 })
    payload.value = nextPayload
    if (!hasLoadedInitialSnapshot) {
      markKnownNews(nextPayload.items)
      hasLoadedInitialSnapshot = true
    } else {
      speakNewNews(nextPayload.items)
    }
  } finally {
    loading.value = false
  }
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

function shouldSpeakNews(item: TdxNewsItem) {
  return Boolean(item.news_id && item.title)
}

function isAggregateNewsItem(item: TdxNewsItem) {
  return item.source !== '韭研公社'
}

function markKnownNews(newsItems: readonly TdxNewsItem[]) {
  for (const item of newsItems) {
    knownNewsKeys.add(newsKey(item))
  }
}

function speakNews(item: TdxNewsItem) {
  if (!shouldSpeakNews(item)) return false
  const key = newsKey(item)
  if (spokenNewsKeys.has(key)) return false
  const queued = enqueuePluginSpeech(newsSpeechText(item), key, { force: true })
  if (queued) {
    spokenNewsKeys.add(key)
    knownNewsKeys.add(key)
  }
  return queued
}

function speakNewNews(newsItems: readonly TdxNewsItem[], limit = NEW_SPEECH_LIMIT) {
  let spokenCount = 0
  for (const item of newsItems) {
    const key = newsKey(item)
    const wasKnown = knownNewsKeys.has(key)
    knownNewsKeys.add(key)
    if (!wasKnown && speakNews(item)) spokenCount += 1
    if (spokenCount >= limit) break
  }
}

function handleSpeechToggle(event: Event) {
  const input = event.target as HTMLInputElement | null
  if (input && !input.checked) return
  unlockSpeech({ silent: true })
}

function openUrl(url?: string) {
  if (!url) return
  window.open(url, '_blank')
}

function isExpanded(newsId: string) {
  return expandedNewsIds.value.has(newsId)
}

function toggleExpanded(newsId: string) {
  const next = new Set(expandedNewsIds.value)
  if (next.has(newsId)) {
    next.delete(newsId)
  } else {
    next.add(newsId)
  }
  expandedNewsIds.value = next
}

function topicTitle(item: TdxNewsItem) {
  return item.related_plates?.length ? item.related_plates.join('、') : item.title
}

onMounted(() => {
  loadData()
  refreshTimer = window.setInterval(loadData, NEWS_REFRESH_MS)
})

watch(realtimeNewsItems, (nextItems, previousItems) => {
  const previousKeys = new Set(previousItems.map(newsKey))
  speakNewNews(nextItems.filter(item => !previousKeys.has(newsKey(item))))
})

onUnmounted(() => {
  window.clearInterval(refreshTimer)
})
</script>

<style scoped>
.target-news {
  --bg-primary: #111219;
  --bg-secondary: #1a202c;
  --bg-tertiary: #2d3748;
  --text-primary: #e2e8f0;
  --text-secondary: #b0b0b0;
  --positive-color: #ff6b6b;
  --stock-name: #f0be83;
  height: 100dvh;
  min-height: 0;
  overflow-x: hidden;
  overflow-y: auto;
  overscroll-behavior: contain;
  -webkit-overflow-scrolling: touch;
  background: var(--bg-primary);
  color: var(--text-primary);
  font-size: 12px;
}

.topset {
  position: sticky;
  top: 0;
  z-index: 3;
  display: grid;
  grid-template-columns: max-content max-content 1fr;
  align-items: center;
  gap: 8px;
  height: 34px;
  padding: 3px 8px;
  border-bottom: 1px solid #222;
  background: #151515;
}

.top-title {
  color: #fff;
  font-size: 14px;
  font-weight: 700;
}

.setting-btn,
.btn-group button,
.layui-timeline-content footer button {
  height: 22px;
  border: 1px solid #4a5568;
  border-radius: 4px;
  background: #2d3748;
  color: #d9e6f6;
  font-size: 12px;
}

.audiotext {
  justify-self: end;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  color: #ddd;
}

.panel {
  margin: 6px;
  border: 1px solid #222;
  background: #111219;
}

.panel-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 31px;
  padding: 4px 8px;
  border-bottom: 1px solid #222;
  background: #212433;
  color: #f0be83;
  font-weight: 700;
}

.btn-group {
  display: inline-flex;
  gap: 4px;
}

.btn-group button.active {
  color: #ffba00;
  border-color: #666;
}

.layui-timeline {
  margin: 0;
  padding: 6px 8px 2px 20px;
  list-style: none;
}

.layui-timeline-item {
  position: relative;
  min-height: 48px;
  padding: 0 0 10px 15px;
  border-left: 1px solid #2d3748;
}

.layui-timeline-axis {
  position: absolute;
  top: 5px;
  left: -5px;
  width: 9px;
  height: 9px;
  border: 1px solid #5b6475;
  border-radius: 50%;
  background: #111219;
}

.layui-timeline-content {
  color: var(--text-secondary);
  line-height: 1.45;
}

.layui-timeline-content.importnews .newstitle {
  color: var(--positive-color);
}

.news-meta {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 7px;
}

.news-meta time {
  color: #8da3bd;
}

.newstitle {
  color: #f5f5f5;
  font-size: 13px;
}

.source {
  color: #7f93ad;
}

.digest {
  margin: 3px 0 0;
  color: #c3cad5;
  white-space: normal;
  word-break: break-word;
}

.digest.collapsed {
  display: -webkit-box;
  overflow: hidden;
  -webkit-line-clamp: 4;
  -webkit-box-orient: vertical;
}

.layui-timeline-content footer {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 4px;
}

.expand-btn {
  margin-left: auto;
}

.news-tag {
  padding: 1px 5px;
  border: 1px solid #4a5568;
  color: #f0be83;
}

.identify-panel {
  margin-top: 8px;
}

.identify-list,
.topic-list {
  display: grid;
  gap: 1px;
}

.identify-row,
.topic-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 54px;
  gap: 8px;
  padding: 7px 8px;
  border-bottom: 1px solid #242a36;
}

.topic-row {
  grid-template-columns: 88px minmax(0, 1fr);
}

.identify-row strong,
.topic-row strong {
  color: #f5f5f5;
}

.identify-row p,
.topic-row p {
  margin: 3px 0 0;
  color: #b0b0b0;
  overflow-wrap: anywhere;
}

.identify-row time {
  color: #8da3bd;
  text-align: right;
}

.state-line {
  padding: 10px;
  color: #b0b0b0;
}

.switch {
  position: relative;
  display: inline-block;
  width: 30px;
  height: 16px;
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

@media (max-width: 640px) {
  .topset {
    grid-template-columns: max-content max-content 1fr;
  }

  .panel {
    margin: 4px;
  }
}
</style>
