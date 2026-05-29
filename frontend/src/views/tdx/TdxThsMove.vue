<template>
  <main class="target-yidong">
    <article v-for="item in items" :key="item.stock_code" class="move-text">
      <header class="stock-head" @click="openStock(item.stock_code)">
        同花顺异动解析 {{ item.stock_name }}（{{ item.stock_code }}）&nbsp;&nbsp;&nbsp;
        最近涨停：{{ item.trade_date || '-' }}
      </header>

      <section class="reason-block">
        <p class="reason-title">{{ reasonTitle(item) }}</p>
        <p v-for="line in reasonLines(item)" :key="line" class="reason-line">{{ line }}</p>
      </section>

      <footer class="tag-line">
        <span v-for="concept in item.concepts" :key="concept">{{ concept }}</span>
      </footer>
    </article>

    <div v-if="loading" class="state-line">加载中...</div>
    <div v-else-if="!items.length" class="state-line">{{ emptyText }}</div>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { getTdxThsMove } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import type { TdxPluginPayload, TdxStockMove } from '@/types/tdx-plugins'

const route = useRoute()
const payload = ref<TdxPluginPayload<TdxStockMove> | null>(null)
const loading = ref(false)
const stockCode = ref(routeCode() || '600589')
const { enqueuePluginSpeech } = useSpeech()
const { openStock } = useTdxStockLink()

const items = computed(() => payload.value?.items || [])
const emptyText = computed(() => payload.value?.warnings?.[0] || '暂无同花顺异动解析数据')

function routeCode() {
  const value = route.params.code
  return Array.isArray(value) ? value[0] : value
}

async function loadData() {
  if (!stockCode.value) return
  loading.value = true
  try {
    payload.value = await getTdxThsMove(stockCode.value)
    const item = payload.value.items[0]
    if (item?.reasons?.[0]) {
      enqueuePluginSpeech(`${item.stock_name}同花顺异动，${item.reasons[0].title}`, `ths-move-${item.stock_code}-${item.reasons[0].title}`, { force: true })
    }
  } finally {
    loading.value = false
  }
}

function reasonTitle(item: TdxStockMove) {
  return item.reasons?.[0]?.title || item.related_plates?.join('+') || '暂无同花顺异动'
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
  return lines.length ? lines : ['暂无同花顺异动解析数据']
}

function numberedParagraphs(content: string) {
  const paragraphs = content.split(/\n+/).map(part => part.trim()).filter(Boolean)
  if (paragraphs.length > 1 && paragraphs.every(part => /^\d+、/.test(part))) {
    return paragraphs
  }
  return []
}

watch(() => route.params.code, () => {
  stockCode.value = routeCode() || stockCode.value
  loadData()
})

onMounted(loadData)
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

.tag-line {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  padding: 6px 8px;
}

.tag-line span {
  color: #f0be83;
}

.state-line {
  padding: 10px;
  color: #999;
}
</style>
