<template>
  <TdxPluginShell
    title="异动解析（同花顺版）"
    subtitle="只展示同花顺口径的概念、异动和解析"
    :updated-at="payload?.updated_at"
    :source-status="payload?.source_status"
    :warnings="payload?.warnings"
    :is-cache="payload?.is_cache"
    :loading="loading"
    @refresh="loadData"
    @unlock-speech="unlockSpeech"
  >
    <div class="query-bar">
      <input v-model="stockCode" maxlength="6" placeholder="输入股票代码" @keyup.enter="loadData" />
      <button type="button" @click="loadData">查询</button>
    </div>

    <article v-for="item in items" :key="item.stock_code" class="move-panel">
      <header>
        <button class="tdx-stock-link" type="button" @click="openStock(item.stock_code)">
          {{ item.stock_name }} {{ item.stock_code }}
        </button>
        <span class="tdx-tag">同花顺版</span>
        <span v-if="item.latest_limit_up" class="tdx-tag">{{ item.latest_limit_up.board }}板</span>
      </header>

      <section class="reason-list">
        <h2>同花顺异动解析</h2>
        <p v-for="reason in item.reasons" :key="`${reason.source}-${reason.title}`">
          <b>{{ reason.title }}</b>
          <span>{{ reason.content }}</span>
        </p>
      </section>

      <section class="tag-line">
        <span v-for="concept in item.concepts" :key="concept" class="tdx-tag">{{ concept }}</span>
      </section>
    </article>
  </TdxPluginShell>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import TdxPluginShell from '@/components/tdx/TdxPluginShell.vue'
import { getTdxThsMove } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import type { TdxPluginPayload, TdxStockMove } from '@/types/tdx-plugins'

const route = useRoute()
const payload = ref<TdxPluginPayload<TdxStockMove> | null>(null)
const loading = ref(false)
const stockCode = ref(routeCode() || '001259')
const { enqueuePluginSpeech, unlockSpeech } = useSpeech()
const { openStock } = useTdxStockLink()

const items = computed(() => payload.value?.items || [])

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
      enqueuePluginSpeech(`${item.stock_name}同花顺异动，${item.reasons[0].title}`, `ths-move-${item.stock_code}-${item.reasons[0].title}`)
    }
  } finally {
    loading.value = false
  }
}

watch(() => route.params.code, () => {
  stockCode.value = routeCode() || stockCode.value
  loadData()
})

onMounted(loadData)
</script>

<style scoped>
.query-bar,
.move-panel header,
.tag-line {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}

.query-bar {
  margin-bottom: 8px;
}

.query-bar input,
.query-bar button {
  height: 26px;
  border: 1px solid #244b75;
  border-radius: 2px;
  background: #081827;
  color: #d7e3f4;
}

.query-bar input {
  width: 128px;
  padding: 0 8px;
}

.query-bar button {
  padding: 0 12px;
  color: #9fd0ff;
}

.move-panel {
  border: 1px solid #173858;
  background: #081827;
}

.move-panel header,
.reason-list,
.tag-line {
  padding: 8px;
  border-bottom: 1px solid #132b44;
}

.reason-list h2 {
  margin: 0 0 6px;
  color: #89a7c9;
  font-size: 12px;
}

.reason-list p {
  display: grid;
  gap: 4px;
  margin: 0 0 8px;
}

.reason-list b {
  color: #ffd36d;
}
</style>
