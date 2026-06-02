<template>
  <div class="ultra-short-signals">
    <div class="toolbar">
      <div class="toolbar-title">
        <h3>超短信号</h3>
      </div>
      <div class="toolbar-actions">
        <el-date-picker
          v-model="selectedDate"
          type="date"
          value-format="YYYY-MM-DD"
          format="YYYY-MM-DD"
          :clearable="false"
          :editable="false"
          @change="fetchData"
        />
        <el-button :icon="Refresh" @click="fetchData" :loading="loading">刷新</el-button>
        <el-button :icon="Files" @click="openObsidianDashboard" :disabled="!obsidianReady">
          Obsidian
        </el-button>
      </div>
    </div>

    <el-alert
      v-if="errorMessage"
      class="state-alert"
      type="error"
      :title="errorMessage"
      show-icon
      :closable="false"
    />

    <div v-loading="loading" class="signal-list">
      <el-empty v-if="!loading && signals.length === 0" description="暂无超短信号" />

      <article v-for="signal in signals" :key="signalKey(signal)" class="signal-row">
        <header>
          <div>
            <strong>{{ signal.label }}</strong>
            <span>{{ signal.setup }} · {{ signal.alert_type }} · {{ signal.source }}</span>
          </div>
          <el-tag size="small" type="warning">
            {{ signal.manual_required ? '人工确认' : '自动' }}
          </el-tag>
        </header>

        <div class="signal-grid">
          <section>
            <h4>模式</h4>
            <div class="tag-list">
              <el-tag v-for="tag in signal.tags" :key="tag" size="small" effect="plain">{{ tag }}</el-tag>
              <span v-if="signal.tags.length === 0" class="empty-text">-</span>
            </div>
          </section>

          <section>
            <h4>依据</h4>
            <p>{{ signal.reason || '-' }}</p>
          </section>

          <section>
            <h4>风险</h4>
            <ul v-if="signal.risk_flags.length">
              <li v-for="item in signal.risk_flags" :key="item">{{ item }}</li>
            </ul>
            <span v-else class="empty-text">人工确认承接和风险后再行动</span>
          </section>
        </div>

        <footer>
          <span>评分 {{ signal.score }}</span>
          <span>模拟 {{ signal.sim_result }}</span>
          <el-button v-if="signal.stock_code" link type="primary" @click="goStock(signal.stock_code)">
            个股详情
          </el-button>
        </footer>
      </article>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { Files, Refresh } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { getObsidianStatus, getUltraShortSignals } from '@/api/intelligence'
import type { ObsidianStatus, UltraShortSignal } from '@/types/intelligence'

const router = useRouter()
const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const signals = ref<UltraShortSignal[]>([])
const obsidianStatus = ref<ObsidianStatus | null>(null)
const loading = ref(false)
const errorMessage = ref('')

const obsidianReady = computed(() => Boolean(obsidianStatus.value?.enabled && obsidianStatus.value?.vault_configured))

onMounted(() => {
  fetchData()
  refreshObsidianStatus()
})

async function fetchData() {
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await getUltraShortSignals(selectedDate.value)
    signals.value = response.items
  } catch (error) {
    console.error('获取超短信号失败:', error)
    errorMessage.value = '获取超短信号失败'
    signals.value = []
  } finally {
    loading.value = false
  }
}

async function refreshObsidianStatus() {
  try {
    obsidianStatus.value = await getObsidianStatus()
  } catch (error) {
    console.warn('获取 Obsidian 状态失败:', error)
    obsidianStatus.value = null
  }
}

function openObsidianDashboard() {
  if (!obsidianReady.value) {
    ElMessage.warning('Obsidian 未启用')
    return
  }
  window.location.href = obsidianUri('Dashboards/超短线驾驶舱.md')
}

function obsidianUri(filePath: string): string {
  const vaultName = (obsidianStatus.value?.vault_path || '').split(/[\\/]/).filter(Boolean).pop() || ''
  return `obsidian://open?vault=${encodeURIComponent(vaultName)}&file=${encodeURIComponent(filePath)}`
}

function goStock(stockCode?: string) {
  if (!stockCode) return
  router.push(`/stock/${stockCode}`)
}

function signalKey(signal: UltraShortSignal): string {
  return `${signal.trade_date}-${signal.setup}-${signal.stock_code || signal.label}-${signal.source}`
}
</script>

<style lang="scss" scoped>
.ultra-short-signals {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.toolbar {
  min-height: 52px;
  padding: 12px 14px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.toolbar-title h3 {
  margin: 0;
  font-size: 18px;
  color: #1f2937;
}

.toolbar-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.state-alert {
  border-radius: 6px;
}

.signal-list {
  min-height: 360px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.signal-row {
  padding: 14px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;

  header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 12px;
  }

  strong {
    display: block;
    color: #111827;
    font-size: 16px;
  }

  header span,
  footer span {
    color: #6b7280;
    font-size: 12px;
  }

  footer {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 12px;
  }
}

.signal-grid {
  display: grid;
  grid-template-columns: minmax(180px, 0.8fr) minmax(260px, 1.3fr) minmax(220px, 1fr);
  gap: 14px;

  h4 {
    margin: 0 0 8px;
    color: #4b5563;
    font-size: 13px;
  }

  p,
  ul {
    margin: 0;
    color: #374151;
    line-height: 1.7;
    overflow-wrap: anywhere;
  }

  ul {
    padding-left: 18px;
  }
}

.tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.empty-text {
  color: #9ca3af;
  font-size: 13px;
}

@media (max-width: 900px) {
  .toolbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .toolbar-actions,
  :deep(.el-date-editor) {
    width: 100%;
  }

  .signal-grid {
    grid-template-columns: 1fr;
  }
}
</style>
