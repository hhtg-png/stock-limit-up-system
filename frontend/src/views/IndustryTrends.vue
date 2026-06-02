<template>
  <div class="industry-trends">
    <div class="toolbar">
      <div class="toolbar-title">
        <h3>产业趋势</h3>
      </div>
      <div class="toolbar-actions">
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

    <div v-loading="loading" class="trend-list">
      <el-empty v-if="!loading && trends.length === 0" description="暂无产业趋势" />

      <article v-for="trend in trends" :key="trend.theme" class="trend-row">
        <header>
          <div>
            <strong>{{ trend.theme }}</strong>
            <span>{{ trend.last_seen }} · {{ trend.confidence }}</span>
          </div>
          <el-tag size="small" type="info">{{ trend.status }}</el-tag>
        </header>

        <div class="trend-body">
          <section>
            <h4>催化</h4>
            <div class="tag-list">
              <el-tag v-for="item in trend.catalysts" :key="item" effect="plain">{{ item }}</el-tag>
              <span v-if="trend.catalysts.length === 0" class="empty-text">-</span>
            </div>
          </section>

          <section>
            <h4>标的</h4>
            <div class="stock-list">
              <button
                v-for="stock in trend.stocks"
                :key="stock.code || stock.name"
                type="button"
                @click="goStock(stock.code)"
              >
                {{ stock.name }}<span v-if="stock.code">({{ stock.code }})</span>
              </button>
              <span v-if="trend.stocks.length === 0" class="empty-text">-</span>
            </div>
          </section>

          <section>
            <h4>证据</h4>
            <p>{{ trend.evidence[0] || '-' }}</p>
          </section>
        </div>

        <footer>
          <a
            v-for="source in trend.sources"
            :key="`${source.title}-${source.url}`"
            :href="source.url || undefined"
            target="_blank"
            rel="noreferrer"
          >
            {{ source.title }}
          </a>
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
import { getIndustryTrends, getObsidianStatus } from '@/api/intelligence'
import type { IndustryTrend, ObsidianStatus } from '@/types/intelligence'

const router = useRouter()
const trends = ref<IndustryTrend[]>([])
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
    const response = await getIndustryTrends()
    trends.value = response.items
  } catch (error) {
    console.error('获取产业趋势失败:', error)
    errorMessage.value = '获取产业趋势失败'
    trends.value = []
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
  window.location.href = obsidianUri('Dashboards/产业趋势.md')
}

function obsidianUri(filePath: string): string {
  const vaultName = (obsidianStatus.value?.vault_path || '').split(/[\\/]/).filter(Boolean).pop() || ''
  return `obsidian://open?vault=${encodeURIComponent(vaultName)}&file=${encodeURIComponent(filePath)}`
}

function goStock(stockCode?: string) {
  if (!stockCode) return
  router.push(`/stock/${stockCode}`)
}
</script>

<style lang="scss" scoped>
.industry-trends {
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
}

.state-alert {
  border-radius: 6px;
}

.trend-list {
  min-height: 360px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.trend-row {
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

  header span {
    color: #6b7280;
    font-size: 12px;
  }

  footer {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-top: 12px;

    a {
      color: #1677ff;
      font-size: 13px;
      text-decoration: none;
      overflow-wrap: anywhere;
    }
  }
}

.trend-body {
  display: grid;
  grid-template-columns: minmax(180px, 0.8fr) minmax(180px, 0.8fr) minmax(260px, 1.4fr);
  gap: 14px;

  h4 {
    margin: 0 0 8px;
    color: #4b5563;
    font-size: 13px;
  }

  p {
    margin: 0;
    color: #374151;
    line-height: 1.7;
    overflow-wrap: anywhere;
  }
}

.tag-list,
.stock-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.stock-list button {
  border: 1px solid #dbeafe;
  border-radius: 4px;
  background: #eff6ff;
  color: #1d4ed8;
  cursor: pointer;
  padding: 4px 7px;

  span {
    color: #64748b;
  }
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

  .trend-body {
    grid-template-columns: 1fr;
  }
}
</style>
