<template>
  <TdxPluginShell
    title="涨停播报"
    subtitle="封死涨停、涨停打开、涨停回封实时事件"
    :updated-at="payload?.updated_at"
    :source-status="payload?.source_status"
    :warnings="payload?.warnings"
    :is-cache="payload?.is_cache"
    :loading="loading"
    @refresh="loadData"
    @unlock-speech="unlockSpeech"
  >
    <table class="tdx-table">
      <thead>
        <tr>
          <th style="width: 64px;">时间</th>
          <th style="width: 132px;">股票</th>
          <th style="width: 54px;">板</th>
          <th style="width: 78px;">状态</th>
          <th>原因</th>
          <th style="width: 88px;">封单</th>
          <th style="width: 70px;">开板</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in events" :key="item.event_id">
          <td>{{ item.event_time }}</td>
          <td>
            <button class="tdx-stock-link" type="button" @click="openStock(item.stock_code)">
              {{ item.stock_name }} {{ item.stock_code }}
            </button>
          </td>
          <td><span class="tdx-tag">{{ item.board }}板</span></td>
          <td :class="item.is_sealed ? 'tdx-red' : 'tdx-green'">{{ item.event_label }}</td>
          <td :title="item.reason">{{ item.reason || item.reason_category }}</td>
          <td>{{ formatAmount(item.seal_amount) }}</td>
          <td>{{ item.open_count }}次</td>
        </tr>
      </tbody>
    </table>
  </TdxPluginShell>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import TdxPluginShell from '@/components/tdx/TdxPluginShell.vue'
import { getTdxLimitUpLive } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import type { TdxLimitUpEvent, TdxPluginPayload } from '@/types/tdx-plugins'

const payload = ref<TdxPluginPayload<TdxLimitUpEvent> | null>(null)
const loading = ref(false)
const { enqueuePluginSpeech, unlockSpeech } = useSpeech()
const { openStock } = useTdxStockLink()
let refreshTimer = 0

const events = computed(() => payload.value?.items || [])

async function loadData() {
  loading.value = true
  try {
    const next = await getTdxLimitUpLive()
    payload.value = next
    for (const item of next.items.slice(0, 3)) {
      enqueuePluginSpeech(`${item.stock_name}${item.event_label}`, item.event_id)
    }
  } finally {
    loading.value = false
  }
}

function formatAmount(value: number) {
  if (!value) return '-'
  if (value >= 100000000) return `${(value / 100000000).toFixed(1)}亿`
  return `${(value / 10000).toFixed(0)}万`
}

onMounted(() => {
  loadData()
  refreshTimer = window.setInterval(loadData, 5000)
})

onUnmounted(() => {
  window.clearInterval(refreshTimer)
})
</script>
