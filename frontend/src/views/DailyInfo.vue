<template>
  <div class="daily-info">
    <div class="toolbar">
      <div class="toolbar-title">
        <h3>每日资讯</h3>
      </div>
      <div class="toolbar-actions">
        <el-input
          v-model="searchKeyword"
          class="search-input"
          clearable
          placeholder="搜索摘要/原文/个股"
          :prefix-icon="Search"
          @keyup.enter="searchData"
          @clear="clearSearch"
        />
        <el-button :icon="Search" @click="searchData" :loading="searching">搜索</el-button>
        <el-date-picker
          v-model="selectedDate"
          type="date"
          value-format="YYYY-MM-DD"
          format="YYYY-MM-DD"
          :clearable="false"
          :editable="false"
          @change="handleDateChange"
        />
        <el-button :icon="Refresh" @click="refreshData" :loading="loading || probing">刷新</el-button>
        <el-button type="primary" :icon="Files" @click="syncData" :loading="syncing || syncRunning">同步知识库</el-button>
        <el-button :icon="Files" @click="exportToObsidian" :loading="exportingObsidian" :disabled="!obsidianReady">
          Obsidian
        </el-button>
        <el-button v-if="lastObsidianFile" link type="primary" @click="openObsidianNote">打开笔记</el-button>
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

    <el-alert
      v-if="syncNotice"
      class="state-alert"
      type="info"
      :title="syncNotice"
      show-icon
      :closable="false"
    />

    <el-alert
      v-if="!errorMessage && !syncNotice && modelStatus"
      class="state-alert"
      :type="modelStatus === 'ready' ? 'success' : 'warning'"
      :title="modelStatusText"
      show-icon
      :closable="false"
    />

    <div v-loading="loading" class="content">
      <el-empty
        v-if="!loading && isEmpty"
        :description="emptyDescription"
      />

      <template v-else-if="dailyInfo">
        <div class="intelligence-layout">
          <aside class="history-panel">
            <div class="section-header">
              <h4>{{ hasActiveSearch ? '搜索结果' : '历史摘要' }}</h4>
              <el-tag size="small" type="info">{{ displayHistoryItems.length }}</el-tag>
            </div>
            <div v-if="displayHistoryItems.length" class="history-list">
              <button
                v-for="item in displayHistoryItems"
                :key="historyKey(item)"
                class="history-item"
                :class="{ active: isActiveHistory(item) }"
                type="button"
                @click="selectHistory(item)"
              >
                <span class="history-date">{{ item.trade_date }}</span>
                <span class="history-overview" v-html="highlightText(item.summary.overview || '暂无摘要')" />
                <span class="history-meta">{{ item.source_count }} 篇 · {{ formatTime(item.generated_at) }}</span>
              </button>
            </div>
            <span v-else class="empty-text">暂无历史摘要</span>
          </aside>

          <div class="detail-content">
            <div class="summary-row">
              <div class="metric">
                <span>日期</span>
                <strong>{{ dailyInfo.trade_date }}</strong>
              </div>
              <div class="metric">
                <span>资料数</span>
                <strong>{{ dailyInfo.source_count }}</strong>
              </div>
              <div class="metric">
                <span>状态</span>
                <strong>{{ statusText }}</strong>
              </div>
              <div class="metric">
                <span>生成时间</span>
                <strong>{{ formatTime(dailyInfo.generated_at) }}</strong>
              </div>
            </div>

            <section class="panel overview-panel">
              <div class="section-header overview-header">
                <h4>每日复盘</h4>
                <el-tag size="small" :type="dailyInfo.cache_hit ? 'info' : 'success'">
                  {{ dailyInfo.cache_hit ? '缓存命中' : '已更新' }}
                </el-tag>
              </div>
              <p v-html="highlightText(dailyInfo.summary.overview || '-')" />
            </section>

            <section class="panel">
              <div class="section-header">
                <h4>个股提及</h4>
                <el-tag size="small" type="warning">{{ mentionedStocks.length }} 个 · 不构成推荐</el-tag>
              </div>
              <el-table v-if="mentionedStocks.length" :data="mentionedStocks" size="small" class="stock-table desktop-stock-table">
                <el-table-column label="方向" min-width="130">
                  <template #default="{ row }">
                    <el-tag v-if="row.sector" size="small" effect="plain">
                      <span v-html="highlightText(row.sector)" />
                    </el-tag>
                    <span v-else class="empty-text">-</span>
                  </template>
                </el-table-column>
                <el-table-column label="个股" min-width="110">
                  <template #default="{ row }">
                    <span class="stock-name" v-html="highlightText(row.name)" />
                  </template>
                </el-table-column>
                <el-table-column label="代码" min-width="90">
                  <template #default="{ row }">
                    <el-tag v-if="row.code" size="small" effect="plain">
                      <span v-html="highlightText(row.code)" />
                    </el-tag>
                    <span v-else class="empty-text">-</span>
                  </template>
                </el-table-column>
                <el-table-column label="个股总结" min-width="280">
                  <template #default="{ row }">
                    <span v-html="highlightText(row.summary || row.reason || '-')" />
                  </template>
                </el-table-column>
                <el-table-column label="催化依据" min-width="220">
                  <template #default="{ row }">
                    <span v-html="highlightText(row.reason || '-')" />
                  </template>
                </el-table-column>
                <el-table-column label="来源" min-width="180">
                  <template #default="{ row }">
                    <span v-html="highlightText(row.source_title || '-')" />
                  </template>
                </el-table-column>
              </el-table>
              <div v-if="mentionedStocks.length" class="stock-card-list">
                <article v-for="stock in mentionedStocks" :key="stockKey(stock)" class="stock-card">
                  <header>
                    <strong v-html="highlightText(stock.name)" />
                    <span v-if="stock.code" v-html="highlightText(stock.code)" />
                    <el-tag v-if="stock.sector" size="small" effect="plain">
                      <span v-html="highlightText(stock.sector)" />
                    </el-tag>
                  </header>
                  <p v-html="highlightText(stock.summary || stock.reason || '-')" />
                  <footer>
                    <span v-html="highlightText(stock.reason || '暂无催化依据')" />
                    <em v-html="highlightText(stock.source_title || '未知来源')" />
                  </footer>
                </article>
              </div>
              <span v-else class="empty-text">暂无个股提及</span>
            </section>

            <div class="two-column">
              <section class="panel">
                <div class="section-header">
                  <h4>盘前/盘后主线</h4>
                </div>
                <div v-if="mainLines.length" class="tag-list">
                  <el-tag v-for="item in mainLines" :key="item" effect="plain">
                    <span v-html="highlightText(item)" />
                  </el-tag>
                </div>
                <span v-else class="empty-text">暂无主线</span>
              </section>

              <section class="panel">
                <div class="section-header">
                  <h4>产业链催化</h4>
                </div>
                <ul v-if="catalysts.length" class="item-list">
                  <li v-for="item in catalysts" :key="item">
                    <span v-html="highlightText(item)" />
                  </li>
                </ul>
                <span v-else class="empty-text">暂无催化</span>
              </section>
            </div>

            <div class="two-column">
              <section class="panel">
                <div class="section-header">
                  <h4>交易预案</h4>
                </div>
                <p v-html="highlightText(dailyInfo.summary.plan || '-')" />
              </section>

              <section class="panel risk-panel">
                <div class="section-header">
                  <h4>风险点</h4>
                </div>
                <ul v-if="risks.length" class="item-list">
                  <li v-for="item in risks" :key="item">
                    <span v-html="highlightText(item)" />
                  </li>
                </ul>
                <span v-else class="empty-text">暂无风险提示</span>
              </section>
            </div>

            <section class="panel">
              <div class="section-header">
                <h4>来源引用</h4>
              </div>
              <div v-if="sourceRefs.length" class="source-list">
                <el-button
                  v-for="source in sourceRefs"
                  :key="`${source.id}-${source.title}`"
                  link
                  type="primary"
                  :icon="Document"
                  @click="openSource(source)"
                >
                  <span v-html="highlightText(source.title)" />
                </el-button>
              </div>
              <span v-else class="empty-text">暂无来源</span>
            </section>
          </div>
        </div>
      </template>
    </div>

    <el-dialog v-model="sourceDialogVisible" width="76%" class="source-dialog" destroy-on-close>
      <template #header>
        <div class="dialog-title">
          <span v-html="highlightText(sourceDetail?.title || '来源原文')" />
          <el-tag v-if="sourceDetail?.media_type_name" size="small" type="info">
            {{ sourceDetail.media_type_name }}
          </el-tag>
        </div>
      </template>
      <div v-loading="sourceLoading" class="source-detail">
        <div v-if="sourceDetail" class="source-meta">
          <span v-html="highlightText(sourceDetail.source_name)" />
          <span>{{ sourceDetail.trade_date || '-' }}</span>
          <a v-if="sourceDetail.jump_url" :href="sourceDetail.jump_url" target="_blank" rel="noreferrer">
            打开知识库链接
          </a>
        </div>
        <pre v-html="highlightText(sourceBody)" />
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Document, Files, Refresh, Search } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import {
  getDailyInfo,
  getDailyInfoHistory,
  getDailyInfoSyncStatus,
  getObsidianStatus,
  getIntelligenceDocument,
  exportObsidianKnowledge,
  probeDailyInfo,
  searchDailyInfo,
  syncDailyInfo
} from '@/api/intelligence'
import type {
  DailyInfoMentionedStock,
  DailyInfoResponse,
  DailyInfoSource,
  DailyInfoSourceDetail,
  IntelligenceSyncResponse,
  IntelligenceSyncResult,
  ObsidianStatus
} from '@/types/intelligence'
import { filterVisibleDailyInfoSearchResults } from '@/utils/dailyInfoSearch'

const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const dailyInfo = ref<DailyInfoResponse | null>(null)
const historyItems = ref<DailyInfoResponse[]>([])
const loading = ref(false)
const syncing = ref(false)
const probing = ref(false)
const searching = ref(false)
const errorMessage = ref('')
const searchKeyword = ref('')
const sourceDialogVisible = ref(false)
const sourceLoading = ref(false)
const sourceDetail = ref<DailyInfoSourceDetail | null>(null)
const syncStatus = ref<IntelligenceSyncResponse | null>(null)
const obsidianStatus = ref<ObsidianStatus | null>(null)
const exportingObsidian = ref(false)
const lastObsidianFile = ref('')
let syncPollTimer: number | null = null
let appliedSyncFinishedAt = ''

const activeSearchKeyword = computed(() => searchKeyword.value.trim())
const hasActiveSearch = computed(() => Boolean(activeSearchKeyword.value))
const emptyDescription = computed(() => hasActiveSearch.value ? '没有匹配的每日资讯' : '暂无资讯摘要，可点击同步知识库后再查看')
const mainLines = computed(() => stringList(dailyInfo.value?.summary.main_lines))
const catalysts = computed(() => stringList(dailyInfo.value?.summary.catalysts))
const risks = computed(() => stringList(dailyInfo.value?.summary.risks))
const sourceTitles = computed(() => stringList(dailyInfo.value?.summary.source_titles))
const stockMentionSource = computed(() => firstNonEmptyStockList(
  dailyInfo.value?.summary.mentioned_stocks,
  dailyInfo.value?.summary.stocks
))
const mentionedStocks = computed<DailyInfoMentionedStock[]>(() => stockList(stockMentionSource.value))
const sourceRefs = computed<DailyInfoSource[]>(() => {
  if (dailyInfo.value?.sources?.length) return dailyInfo.value.sources
  return sourceTitles.value.map((title, index) => ({
    id: 0 - index,
    title,
    source_name: '',
    source_key: 'daily',
    media_type_name: '',
  }))
})
const displayHistoryItems = computed<DailyInfoResponse[]>(() => latestHistoryByDate(historyItems.value))
const modelStatus = computed(() => dailyInfo.value?.summary.model_status || '')
const statusText = computed(() => {
  const statusMap: Record<string, string> = {
    ready: '已生成',
    empty: '无新增资料',
    error: '异常'
  }
  return statusMap[dailyInfo.value?.status || ''] || (dailyInfo.value?.status || '-')
})
const modelStatusText = computed(() => {
  const statusMap: Record<string, string> = {
    ready: `DeepSeek ${dailyInfo.value?.model || ''} 已生成摘要`,
    missing_api_key: '未配置 DeepSeek API Key，当前展示本地兜底摘要',
    refreshing_after_key_configured: 'DeepSeek Key 已配置，旧摘要正在后台刷新',
    fallback: '模型结果不可用，当前展示本地兜底摘要',
    error: dailyInfo.value?.summary.error || '模型调用失败，当前展示本地兜底摘要'
  }
  return statusMap[modelStatus.value] || `模型状态：${modelStatus.value}`
})
const isEmpty = computed(() => {
  if (!dailyInfo.value) return true
  return dailyInfo.value.source_count === 0 && !dailyInfo.value.summary.overview
})
const sourceBody = computed(() => {
  if (!sourceDetail.value) return ''
  return sourceDetail.value.content_text || sourceDetail.value.introduction || sourceDetail.value.abstract || '暂无原文内容'
})
const syncRunning = computed(() => ['queued', 'running'].includes(syncStatus.value?.state || ''))
const obsidianReady = computed(() => Boolean(obsidianStatus.value?.enabled && obsidianStatus.value?.vault_configured))
const syncNotice = computed(() => {
  if (probing.value) return '正在检查共享知识库是否有更新'
  if (!syncStatus.value) return ''
  if (syncRunning.value) return '发现同步任务正在运行，摘要生成完成后会自动刷新'
  if (syncStatus.value.state === 'failed') return `知识库同步失败：${syncStatus.value.error || '请稍后重试'}`
  return ''
})

onMounted(async () => {
  await loadInitialData()
  await refreshSyncStatus()
  await refreshObsidianStatus()
  await probeKnowledgeUpdates()
})

onUnmounted(() => {
  stopSyncPolling()
})

async function loadInitialData() {
  loading.value = true
  errorMessage.value = ''
  try {
    const hasHistory = await loadHistory()
    if (!hasHistory) {
      const response = await getDailyInfo(selectedDate.value)
      dailyInfo.value = response
      upsertHistory(response)
    }
  } catch (error) {
    console.error('获取每日资讯失败:', error)
    errorMessage.value = '获取每日资讯失败'
    ElMessage.error(errorMessage.value)
  } finally {
    loading.value = false
  }
}

async function loadHistory(): Promise<boolean> {
  const history = await getDailyInfoHistory()
  historyItems.value = history.items
  if (history.items.length) {
    dailyInfo.value = history.items[0]
    selectedDate.value = history.items[0].trade_date
    return true
  }
  return false
}

async function fetchData(tradeDate = selectedDate.value) {
  if (!tradeDate) return
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await getDailyInfo(tradeDate)
    dailyInfo.value = response
    selectedDate.value = response.trade_date
    upsertHistory(response)
  } catch (error) {
    console.error('获取每日资讯失败:', error)
    errorMessage.value = '获取每日资讯失败'
    ElMessage.error(errorMessage.value)
  } finally {
    loading.value = false
  }
}

async function refreshData() {
  await fetchData()
  await probeKnowledgeUpdates(true)
}

async function syncData() {
  syncing.value = true
  errorMessage.value = ''
  try {
    const response = await syncDailyInfo()
    applySyncStatus(response)
    ElMessage.success('同步任务已启动，完成后自动刷新')
  } catch (error) {
    console.error('同步知识库失败:', error)
    errorMessage.value = '同步知识库失败'
    ElMessage.error(errorMessage.value)
  } finally {
    syncing.value = false
  }
}

async function probeKnowledgeUpdates(showUnchangedMessage = false) {
  probing.value = true
  try {
    const response = await probeDailyInfo()
    applySyncStatus(response.sync)
    if (response.probe.changed) {
      ElMessage.info('发现共享知识库更新，正在后台生成摘要')
    } else if (showUnchangedMessage) {
      ElMessage.success('知识库暂无新内容')
    }
  } catch (error) {
    console.warn('探测知识库更新失败:', error)
    if (showUnchangedMessage) {
      ElMessage.warning('检查知识库更新失败')
    }
  } finally {
    probing.value = false
  }
}

async function refreshSyncStatus() {
  try {
    const status = await getDailyInfoSyncStatus()
    applySyncStatus(status)
  } catch (error) {
    console.warn('获取知识库同步状态失败:', error)
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

async function exportToObsidian() {
  if (!obsidianReady.value) {
    ElMessage.warning('Obsidian 未启用')
    return
  }
  exportingObsidian.value = true
  try {
    const response = await exportObsidianKnowledge(selectedDate.value)
    lastObsidianFile.value = response.written_files.find(item => item.startsWith('50_Daily/')) || ''
    ElMessage.success(response.skipped ? 'Obsidian 未写入' : 'Obsidian 已同步')
  } catch (error) {
    console.error('同步 Obsidian 失败:', error)
    ElMessage.error('同步 Obsidian 失败')
  } finally {
    exportingObsidian.value = false
  }
}

function openObsidianNote() {
  const filePath = lastObsidianFile.value || `50_Daily/${selectedDate.value.slice(0, 4)}/${selectedDate.value}.md`
  window.location.href = obsidianUri(filePath)
}

function obsidianUri(filePath: string): string {
  const vaultName = (obsidianStatus.value?.vault_path || '').split(/[\\/]/).filter(Boolean).pop() || ''
  return `obsidian://open?vault=${encodeURIComponent(vaultName)}&file=${encodeURIComponent(filePath)}`
}

function applySyncStatus(status: IntelligenceSyncResponse) {
  syncStatus.value = status
  if (['queued', 'running'].includes(status.state || '')) {
    startSyncPolling()
    return
  }
  stopSyncPolling()
  if (status.state === 'completed' && status.result && status.finished_at !== appliedSyncFinishedAt) {
    appliedSyncFinishedAt = status.finished_at || ''
    applySyncResult(status.result)
  }
}

function applySyncResult(result: IntelligenceSyncResult) {
  if (result.daily_info) {
    dailyInfo.value = result.daily_info
    selectedDate.value = result.daily_info.trade_date
    upsertHistory(result.daily_info)
  }
  if (result.sources) {
    ElMessage.success(`同步完成，更新 ${changedCount(result.sources)} 篇资料`)
  }
  void loadHistory()
}

function startSyncPolling() {
  if (syncPollTimer !== null) return
  syncPollTimer = window.setInterval(() => {
    refreshSyncStatus()
  }, 3000)
}

function stopSyncPolling() {
  if (syncPollTimer === null) return
  window.clearInterval(syncPollTimer)
  syncPollTimer = null
}

async function searchData() {
  const keyword = activeSearchKeyword.value
  if (!keyword) {
    await clearSearch()
    return
  }
  searching.value = true
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await searchDailyInfo(keyword)
    const filteredItems = filterVisibleDailyInfoSearchResults(response.items, keyword)
    historyItems.value = filteredItems
    dailyInfo.value = filteredItems[0] || null
    if (dailyInfo.value) {
      selectedDate.value = dailyInfo.value.trade_date
    }
  } catch (error) {
    console.error('搜索每日资讯失败:', error)
    errorMessage.value = '搜索每日资讯失败'
    ElMessage.error(errorMessage.value)
  } finally {
    searching.value = false
    loading.value = false
  }
}

async function clearSearch() {
  searchKeyword.value = ''
  loading.value = true
  errorMessage.value = ''
  try {
    const hasHistory = await loadHistory()
    if (!hasHistory) {
      dailyInfo.value = null
    }
  } catch (error) {
    console.error('恢复历史摘要失败:', error)
    errorMessage.value = '恢复历史摘要失败'
    ElMessage.error(errorMessage.value)
  } finally {
    loading.value = false
  }
}

function handleDateChange(value: string) {
  fetchData(value)
}

function selectHistory(item: DailyInfoResponse) {
  dailyInfo.value = item
  selectedDate.value = item.trade_date
}

async function openSource(source: DailyInfoSource) {
  if (source.id <= 0) {
    ElMessage.warning('该来源暂无缓存原文')
    return
  }
  sourceDialogVisible.value = true
  sourceLoading.value = true
  sourceDetail.value = null
  try {
    sourceDetail.value = await getIntelligenceDocument(source.id)
  } catch (error) {
    console.error('获取来源原文失败:', error)
    ElMessage.error('获取来源原文失败')
  } finally {
    sourceLoading.value = false
  }
}

function upsertHistory(item: DailyInfoResponse) {
  const next = historyItems.value.filter(history => history.trade_date !== item.trade_date)
  next.push(item)
  next.sort(compareHistoryItems)
  historyItems.value = next
}

function isActiveHistory(item: DailyInfoResponse): boolean {
  return Boolean(dailyInfo.value && historyKey(item) === historyKey(dailyInfo.value))
}

function latestHistoryByDate(items: DailyInfoResponse[]): DailyInfoResponse[] {
  const latestHistoryByDate = new Map<string, DailyInfoResponse>()
  for (const item of items.slice().sort(compareHistoryItems)) {
    if (!latestHistoryByDate.has(item.trade_date)) {
      latestHistoryByDate.set(item.trade_date, item)
    }
  }
  return Array.from(latestHistoryByDate.values()).sort(compareHistoryItems)
}

function historyKey(item: DailyInfoResponse): string {
  return `digest-${item.trade_date}`
}

function compareHistoryItems(a: DailyInfoResponse, b: DailyInfoResponse): number {
  const dateCompare = b.trade_date.localeCompare(a.trade_date)
  if (dateCompare !== 0) return dateCompare
  const timeCompare = safeTimestamp(b.generated_at) - safeTimestamp(a.generated_at)
  if (timeCompare !== 0) return timeCompare
  return (b.version_id || b.id || 0) - (a.version_id || a.id || 0)
}

function safeTimestamp(value?: string | null): number {
  const timestamp = value ? dayjs(value).valueOf() : 0
  return Number.isFinite(timestamp) ? timestamp : 0
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.map(item => String(item)).filter(Boolean)
}

function stockList(value: unknown): DailyInfoMentionedStock[] {
  if (!Array.isArray(value)) return []
  return value
    .filter(item => item && typeof item === 'object')
    .map(item => item as DailyInfoMentionedStock)
    .filter(item => Boolean(item.name))
}

function firstNonEmptyStockList(...values: unknown[]): unknown[] {
  for (const value of values) {
    if (Array.isArray(value) && value.length) return value
  }
  return []
}

function stockKey(stock: DailyInfoMentionedStock): string {
  return `${stock.code || stock.name}-${stock.source_title || ''}-${stock.summary || stock.reason || ''}`
}

function changedCount(sources: Record<string, { changed_documents: number }>): number {
  return Object.values(sources).reduce((total, source) => total + source.changed_documents, 0)
}

function highlightText(value: unknown, fallback = '-'): string {
  const text = String(value ?? '')
  const displayText = text || fallback
  const keyword = activeSearchKeyword.value
  if (!keyword) return escapeHtml(displayText)

  const matcher = new RegExp(escapeRegExp(keyword), 'gi')
  let highlighted = ''
  let lastIndex = 0
  for (const match of displayText.matchAll(matcher)) {
    const index = match.index ?? 0
    highlighted += escapeHtml(displayText.slice(lastIndex, index))
    highlighted += `<mark class="search-highlight">${escapeHtml(match[0])}</mark>`
    lastIndex = index + match[0].length
  }
  highlighted += escapeHtml(displayText.slice(lastIndex))
  return highlighted
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, char => {
    const entities: Record<string, string> = {
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    }
    return entities[char]
  })
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function formatTime(value?: string | null): string {
  if (!value) return '-'
  return dayjs(value).format('YYYY-MM-DD HH:mm')
}
</script>

<style lang="scss" scoped>
.daily-info {
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

.toolbar-title {
  min-width: 0;

  h3 {
    margin: 0;
    font-size: 18px;
    color: #1f2937;
  }
}

.toolbar-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.search-input {
  width: 280px;
}

.state-alert {
  border-radius: 6px;
}

:deep(.search-highlight) {
  display: inline;
  padding: 0 2px;
  border-radius: 2px;
  background: #fff2b8;
  color: #8a4b00;
  font-weight: 600;
}

.content {
  min-height: 360px;
}

.intelligence-layout {
  display: grid;
  grid-template-columns: 280px minmax(0, 1fr);
  gap: 14px;
  align-items: start;
}

.history-panel,
.detail-content {
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-width: 0;
}

.history-panel {
  position: sticky;
  top: 12px;
  max-height: calc(100dvh - 120px);
  overflow: hidden;
  padding: 14px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
}

.history-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: calc(100dvh - 190px);
  overflow-y: auto;
  padding-right: 3px;
}

.history-item {
  width: 100%;
  min-width: 0;
  min-height: 78px;
  padding: 10px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  background: #fff;
  cursor: pointer;
  text-align: left;
  display: flex;
  flex-direction: column;
  gap: 5px;

  &.active {
    border-color: #1677ff;
    background: #f0f7ff;
  }
}

.history-date {
  color: #111827;
  font-size: 14px;
  font-weight: 600;
}

.history-overview {
  color: #4b5563;
  font-size: 12px;
  line-height: 1.5;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.history-meta {
  color: #9ca3af;
  font-size: 12px;
}

.summary-row {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.metric {
  padding: 12px 14px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;

  span {
    display: block;
    color: #6b7280;
    font-size: 12px;
    margin-bottom: 6px;
  }

  strong {
    display: block;
    color: #111827;
    font-size: 16px;
    font-weight: 600;
    overflow-wrap: anywhere;
  }
}

.panel {
  min-width: 0;
  overflow: hidden;
  padding: 16px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;

  p {
    margin: 0;
    color: #374151;
    line-height: 1.8;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
}

.overview-panel {
  border-left: 4px solid #1677ff;
}

.overview-header {
  align-items: flex-start;
  flex-wrap: wrap;
}

.risk-panel {
  border-left: 4px solid #f59e0b;
}

.two-column {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;

  > .panel {
    min-width: 0;
  }
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 12px;

  h4 {
    margin: 0;
    color: #1f2937;
    font-size: 15px;
    font-weight: 600;
  }
}

.tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  min-width: 0;

  :deep(.el-tag) {
    max-width: 100%;
    height: auto;
    min-height: 24px;
    padding: 4px 8px;
    white-space: normal;
    line-height: 1.5;
    overflow-wrap: anywhere;
  }

  :deep(.el-tag__content) {
    white-space: normal;
    overflow-wrap: anywhere;
  }
}

.item-list {
  margin: 0;
  padding-left: 18px;
  color: #374151;
  line-height: 1.8;

  li {
    overflow-wrap: anywhere;
  }
}

.stock-table {
  width: 100%;

  :deep(.cell) {
    white-space: normal;
    overflow-wrap: anywhere;
  }
}

.stock-card-list {
  display: none;
}

.stock-card {
  padding: 12px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  background: #fff;

  header {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 8px;
  }

  strong {
    color: #111827;
    font-size: 15px;
  }

  header span {
    color: #6b7280;
    font-size: 12px;
  }

  p {
    margin: 0;
    color: #374151;
    line-height: 1.7;
  }

  footer {
    display: grid;
    gap: 4px;
    margin-top: 8px;
    color: #6b7280;
    font-size: 12px;
    line-height: 1.5;
  }

  em {
    color: #9ca3af;
    font-style: normal;
  }
}

.stock-name {
  color: #111827;
  font-weight: 600;
}

.source-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;

  :deep(.el-button) {
    max-width: 100%;
    height: auto;
    min-height: 28px;
    padding: 4px 0;
    white-space: normal;
    text-align: left;
  }
}

.empty-text {
  color: #9ca3af;
  font-size: 13px;
}

.dialog-title {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;

  span:first-child {
    overflow-wrap: anywhere;
  }
}

.source-detail {
  min-height: 240px;
}

.source-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 12px;
  color: #6b7280;
  font-size: 13px;

  a {
    color: #1677ff;
    text-decoration: none;
  }
}

.source-detail pre {
  max-height: 62vh;
  margin: 0;
  padding: 14px;
  background: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  color: #1f2937;
  line-height: 1.7;
  white-space: pre-wrap;
  overflow: auto;
  font-family: inherit;
}

@media (max-width: 960px) {
  .toolbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .toolbar-actions,
  .search-input {
    width: 100%;
  }

  .intelligence-layout {
    grid-template-columns: 1fr;
  }

  .history-panel {
    position: static;
    max-height: none;
    overflow: visible;
  }

  .history-list {
    flex-direction: row;
    max-height: none;
    overflow-x: auto;
    overflow-y: hidden;
    padding: 0 0 2px;
    scroll-snap-type: x proximity;
    -webkit-overflow-scrolling: touch;
  }

  .history-item {
    flex: 0 0 220px;
    width: 220px;
    min-height: 96px;
    scroll-snap-align: start;
  }

  .summary-row,
  .two-column {
    grid-template-columns: 1fr;
  }

  .desktop-stock-table {
    display: none;
  }

  .stock-card-list {
    display: grid;
    gap: 10px;
  }
}
</style>
