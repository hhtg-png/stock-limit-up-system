<template>
  <section class="tdx-news-voice">
    <header class="voice-header">
      <div>
        <h1>聚合快讯语音</h1>
        <span :class="['status-dot', statusClass]">{{ statusText }}</span>
      </div>
      <label class="voice-switch">
        <input type="checkbox" :checked="speechUnlocked" @change="handleSpeechToggle" />
        <span>{{ speechUnlocked ? '播报中' : '开启播报' }}</span>
      </label>
    </header>

    <main class="voice-main">
      <div class="metric">
        <span>已播报</span>
        <strong>{{ spokenCount }}</strong>
      </div>
      <div class="metric">
        <span>最新时间</span>
        <strong>{{ latestTime || '--' }}</strong>
      </div>
      <p class="latest-title">{{ recentTitle || '等待聚合快讯' }}</p>
      <p v-if="errorMessage" class="voice-error">{{ errorMessage }}</p>
    </main>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { getTdxNews } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxPluginRealtime } from '@/composables/useWebSocket'
import type { TdxNewsItem } from '@/types/tdx-plugins'

const { enqueuePluginSpeech, unlockSpeech, speechUnlocked } = useSpeech()
const { realtimeNewsItems } = useTdxPluginRealtime()

const knownNewsKeys = new Set<string>()
const spokenNewsKeys = new Set<string>()
const recentTitle = ref('')
const latestTime = ref('')
const spokenCount = ref(0)
const loaded = ref(false)
const errorMessage = ref('')

const statusText = computed(() => {
  if (errorMessage.value) return '异常'
  if (!loaded.value) return '连接中'
  return speechUnlocked.value ? '语音已启用' : '待开启'
})

const statusClass = computed(() => {
  if (errorMessage.value) return 'error'
  if (!loaded.value) return 'pending'
  return speechUnlocked.value ? 'ok' : 'idle'
})

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

function rememberKnown(items: readonly TdxNewsItem[]) {
  for (const item of items) {
    if (item.news_id && item.title) knownNewsKeys.add(newsKey(item))
  }
}

function speakNews(item: TdxNewsItem) {
  if (!item.news_id || !item.title || !speechUnlocked.value) return false
  const key = newsKey(item)
  if (spokenNewsKeys.has(key)) return false

  const text = newsSpeechText(item)
  const queued = enqueuePluginSpeech(text, key, { force: true })
  if (!queued) return false

  spokenNewsKeys.add(key)
  knownNewsKeys.add(key)
  recentTitle.value = normalizeSpeechPart(item.title)
  latestTime.value = item.time || ''
  spokenCount.value += 1
  return true
}

function handleSpeechToggle(event: Event) {
  const input = event.target as HTMLInputElement | null
  if (input && !input.checked) return
  unlockSpeech({ silent: true })
}

async function loadInitialSnapshot() {
  try {
    const payload = await getTdxNews({ limit: 20 })
    rememberKnown(payload.items || [])
    loaded.value = true
    errorMessage.value = ''
  } catch {
    loaded.value = true
    errorMessage.value = '快讯连接异常'
  }
}

onMounted(() => {
  loadInitialSnapshot()
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
</script>

<style scoped>
.tdx-news-voice {
  min-height: 100vh;
  padding: 8px;
  background: #050b12;
  color: #d6e4ff;
  font-size: 12px;
}

.voice-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  border-bottom: 1px solid #26364f;
  padding-bottom: 7px;
}

.voice-header h1 {
  margin: 0 0 3px;
  color: #f8fafc;
  font-size: 14px;
  line-height: 1.2;
}

.status-dot {
  color: #94a3b8;
}

.status-dot.ok {
  color: #22c55e;
}

.status-dot.error {
  color: #ef4444;
}

.status-dot.pending {
  color: #f59e0b;
}

.voice-switch {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  white-space: nowrap;
  color: #e2e8f0;
  cursor: pointer;
}

.voice-main {
  display: grid;
  gap: 8px;
  padding-top: 9px;
}

.metric {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border: 1px solid #1e293b;
  background: #0b1220;
  padding: 6px 8px;
}

.metric span {
  color: #94a3b8;
}

.metric strong {
  color: #facc15;
  font-size: 13px;
}

.latest-title {
  margin: 0;
  color: #f8fafc;
  line-height: 1.55;
  word-break: break-word;
}

.voice-error {
  margin: 0;
  color: #f87171;
}
</style>
