<template>
  <TdxPluginShell
    title="聚合快讯"
    subtitle="知识库和市场快讯聚合，重要消息进入语音队列"
    :updated-at="payload?.updated_at"
    :source-status="payload?.source_status"
    :warnings="payload?.warnings"
    :is-cache="payload?.is_cache"
    :loading="loading"
    @refresh="loadData"
    @unlock-speech="unlockSpeech"
  >
    <div class="news-list">
      <article v-for="item in items" :key="item.news_id" class="news-item">
        <time>{{ item.time }}</time>
        <strong :class="item.importance >= 80 ? 'tdx-red' : 'tdx-yellow'">
          {{ item.importance }}
        </strong>
        <section>
          <h2>{{ item.title }}</h2>
          <p>{{ item.content }}</p>
          <footer>
            <span class="tdx-tag">{{ item.source }}</span>
            <button type="button" @click="speakNews(item)">播报</button>
            <button v-if="item.jump_url" type="button" @click="openUrl(item.jump_url)">原文</button>
          </footer>
        </section>
      </article>
    </div>
  </TdxPluginShell>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import TdxPluginShell from '@/components/tdx/TdxPluginShell.vue'
import { getTdxNews } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import type { TdxNewsItem, TdxPluginPayload } from '@/types/tdx-plugins'

const payload = ref<TdxPluginPayload<TdxNewsItem> | null>(null)
const loading = ref(false)
const { enqueuePluginSpeech, unlockSpeech } = useSpeech()
let refreshTimer = 0

const items = computed(() => payload.value?.items || [])

async function loadData() {
  loading.value = true
  try {
    payload.value = await getTdxNews({ limit: 80 })
    const important = payload.value.items.find(item => item.importance >= 80)
    if (important) speakNews(important)
  } finally {
    loading.value = false
  }
}

function speakNews(item: TdxNewsItem) {
  enqueuePluginSpeech(item.title, `news-${item.news_id}`)
}

function openUrl(url?: string) {
  if (!url) return
  window.open(url, '_blank')
}

onMounted(() => {
  loadData()
  refreshTimer = window.setInterval(loadData, 30000)
})

onUnmounted(() => {
  window.clearInterval(refreshTimer)
})
</script>

<style scoped>
.news-list {
  display: grid;
  gap: 6px;
}

.news-item {
  display: grid;
  grid-template-columns: 112px 44px minmax(0, 1fr);
  gap: 8px;
  padding: 7px;
  border: 1px solid #173858;
  background: #081827;
}

.news-item time {
  color: #7f93ad;
}

.news-item h2 {
  margin: 0 0 4px;
  color: #f4f8ff;
  font-size: 12px;
}

.news-item p {
  margin: 0;
  color: #b7c7dc;
  white-space: normal;
  word-break: break-word;
}

.news-item footer {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 6px;
}

.news-item button {
  height: 22px;
  border: 1px solid #244b75;
  border-radius: 2px;
  background: #102238;
  color: #9fd0ff;
}

@media (max-width: 640px) {
  .news-item {
    grid-template-columns: 86px 36px minmax(0, 1fr);
  }
}
</style>
