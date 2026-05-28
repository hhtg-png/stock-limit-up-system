<template>
  <TdxPluginShell
    title="实时板块强度"
    subtitle="融合涨停数、封板率、连板高度和核心股贡献"
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
          <th style="width: 42px;">序</th>
          <th style="width: 130px;">板块</th>
          <th style="width: 72px;">强度</th>
          <th style="width: 70px;">涨停</th>
          <th style="width: 70px;">封板</th>
          <th style="width: 64px;">高度</th>
          <th>核心股</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(item, index) in items" :key="item.plate_name">
          <td>{{ index + 1 }}</td>
          <td class="tdx-yellow">{{ item.plate_name }}</td>
          <td class="tdx-red">{{ item.strength_score }}</td>
          <td>{{ item.limit_up_count }}</td>
          <td>{{ item.sealed_count }} / {{ item.seal_rate }}%</td>
          <td>{{ item.max_board }}板</td>
          <td>
            <button
              v-for="stock in item.core_stocks"
              :key="stock.stock_code"
              type="button"
              class="tdx-stock-link core-stock"
              @click="openStock(stock.stock_code)"
            >
              {{ stock.stock_name }}{{ stock.board }}
            </button>
          </td>
        </tr>
      </tbody>
    </table>
  </TdxPluginShell>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import TdxPluginShell from '@/components/tdx/TdxPluginShell.vue'
import { getTdxPlateStrength } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxStockLink } from '@/composables/useTdxStockLink'
import type { TdxPlateStrength, TdxPluginPayload } from '@/types/tdx-plugins'

const payload = ref<TdxPluginPayload<TdxPlateStrength> | null>(null)
const loading = ref(false)
const { unlockSpeech } = useSpeech()
const { openStock } = useTdxStockLink()
let refreshTimer = 0

const items = computed(() => payload.value?.items || [])

async function loadData() {
  loading.value = true
  try {
    payload.value = await getTdxPlateStrength()
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadData()
  refreshTimer = window.setInterval(loadData, 15000)
})

onUnmounted(() => {
  window.clearInterval(refreshTimer)
})
</script>

<style scoped>
.core-stock {
  margin-right: 8px;
}
</style>
