<template>
  <div class="daily-info">
    <div class="toolbar">
      <div class="toolbar-title">
        <h3>每日资讯</h3>
        <span>知识库增量同步后保存每日摘要，最新日期显示在最上方</span>
      </div>
      <div class="toolbar-actions">
        <el-date-picker
          v-model="selectedDate"
          type="date"
          value-format="YYYY-MM-DD"
          format="YYYY-MM-DD"
          :clearable="false"
          :editable="false"
          @change="handleDateChange"
        />
        <el-button :icon="Refresh" @click="fetchData()" :loading="loading">刷新</el-button>
        <el-button type="primary" :icon="Files" @click="syncData" :loading="syncing">同步知识库</el-button>
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
      v-else-if="modelStatus"
      class="state-alert"
      :type="modelStatus === 'ready' ? 'success' : 'warning'"
      :title="modelStatusText"
      show-icon
      :closable="false"
    />

    <div v-loading="loading" class="content">
      <el-empty
        v-if="!loading && isEmpty"
        description="暂无资讯摘要，可点击同步知识库后再查看"
      />

      <template v-else-if="dailyInfo">
        <div class="intelligence-layout">
          <aside class="history-panel">
            <div class="section-header">
              <h4>历史摘要</h4>
              <el-tag size="small" type="info">{{ historyItems.length }}</el-tag>
            </div>
            <div v-if="historyItems.length" class="history-list">
              <button
                v-for="item in historyItems"
                :key="item.trade_date"
                class="history-item"
                :class="{ active: item.trade_date === dailyInfo.trade_date }"
                type="button"
                @click="selectHistory(item)"
              >
                <span class="history-date">{{ item.trade_date }}</span>
                <span class="history-overview">{{ item.summary.overview || '暂无摘要' }}</span>
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
              <div class="section-header">
                <h4>每日复盘</h4>
                <el-tag size="small" :type="dailyInfo.cache_hit ? 'info' : 'success'">
                  {{ dailyInfo.cache_hit ? '缓存命中' : '已更新' }}
                </el-tag>
              </div>
              <p>{{ dailyInfo.summary.overview || '-' }}</p>
            </section>

            <div class="two-column">
              <section class="panel">
                <div class="section-header">
                  <h4>盘前/盘后主线</h4>
                </div>
                <div v-if="mainLines.length" class="tag-list">
                  <el-tag v-for="item in mainLines" :key="item" effect="plain">{{ item }}</el-tag>
                </div>
                <span v-else class="empty-text">暂无主线</span>
              </section>

              <section class="panel">
                <div class="section-header">
                  <h4>产业链催化</h4>
                </div>
                <ul v-if="catalysts.length" class="item-list">
                  <li v-for="item in catalysts" :key="item">{{ item }}</li>
                </ul>
                <span v-else class="empty-text">暂无催化</span>
              </section>
            </div>

            <div class="two-column">
              <section class="panel">
                <div class="section-header">
                  <h4>交易预案</h4>
                </div>
                <p>{{ dailyInfo.summary.plan || '-' }}</p>
              </section>

              <section class="panel risk-panel">
                <div class="section-header">
                  <h4>风险点</h4>
                </div>
                <ul v-if="risks.length" class="item-list">
                  <li v-for="item in risks" :key="item">{{ item }}</li>
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
                  {{ source.title }}
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
          <span>{{ sourceDetail?.title || '来源原文' }}</span>
          <el-tag v-if="sourceDetail?.media_type_name" size="small" type="info">
            {{ sourceDetail.media_type_name }}
          </el-tag>
        </div>
      </template>
      <div v-loading="sourceLoading" class="source-detail">
        <div v-if="sourceDetail" class="source-meta">
          <span>{{ sourceDetail.source_name }}</span>
          <span>{{ sourceDetail.trade_date || '-' }}</span>
          <a v-if="sourceDetail.jump_url" :href="sourceDetail.jump_url" target="_blank" rel="noreferrer">
            打开知识库链接
          </a>
        </div>
        <pre>{{ sourceBody }}</pre>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Document, Files, Refresh } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { getDailyInfo, getDailyInfoHistory, getIntelligenceDocument, syncDailyInfo } from '@/api/intelligence'
import type { DailyInfoResponse, DailyInfoSource, DailyInfoSourceDetail } from '@/types/intelligence'

const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const dailyInfo = ref<DailyInfoResponse | null>(null)
const historyItems = ref<DailyInfoResponse[]>([])
const loading = ref(false)
const syncing = ref(false)
const errorMessage = ref('')
const sourceDialogVisible = ref(false)
const sourceLoading = ref(false)
const sourceDetail = ref<DailyInfoSourceDetail | null>(null)

const mainLines = computed(() => stringList(dailyInfo.value?.summary.main_lines))
const catalysts = computed(() => stringList(dailyInfo.value?.summary.catalysts))
const risks = computed(() => stringList(dailyInfo.value?.summary.risks))
const sourceTitles = computed(() => stringList(dailyInfo.value?.summary.source_titles))
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

onMounted(() => {
  loadInitialData()
})

async function loadInitialData() {
  loading.value = true
  errorMessage.value = ''
  try {
    const history = await getDailyInfoHistory()
    historyItems.value = history.items
    if (history.items.length) {
      dailyInfo.value = history.items[0]
      selectedDate.value = history.items[0].trade_date
    } else {
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

async function syncData() {
  syncing.value = true
  errorMessage.value = ''
  try {
    const response = await syncDailyInfo()
    dailyInfo.value = response.daily_info
    selectedDate.value = response.daily_info.trade_date
    const history = await getDailyInfoHistory()
    historyItems.value = history.items
    ElMessage.success(`同步完成，更新 ${changedCount(response.sources)} 篇资料`)
  } catch (error) {
    console.error('同步知识库失败:', error)
    errorMessage.value = '同步知识库失败'
    ElMessage.error(errorMessage.value)
  } finally {
    syncing.value = false
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
  next.sort((a, b) => b.trade_date.localeCompare(a.trade_date))
  historyItems.value = next
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.map(item => String(item)).filter(Boolean)
}

function changedCount(sources: Record<string, { changed_documents: number }>): number {
  return Object.values(sources).reduce((total, source) => total + source.changed_documents, 0)
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
  min-height: 64px;
  padding: 14px 16px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.toolbar-title {
  h3 {
    margin: 0 0 4px;
    font-size: 18px;
    color: #1f2937;
  }

  span {
    color: #6b7280;
    font-size: 13px;
  }
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
}

.history-panel {
  position: sticky;
  top: 12px;
  padding: 14px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
}

.history-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.history-item {
  width: 100%;
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
  padding: 16px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;

  p {
    margin: 0;
    color: #374151;
    line-height: 1.8;
    white-space: pre-wrap;
  }
}

.overview-panel {
  border-left: 4px solid #1677ff;
}

.risk-panel {
  border-left: 4px solid #f59e0b;
}

.two-column {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
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
}

.item-list {
  margin: 0;
  padding-left: 18px;
  color: #374151;
  line-height: 1.8;
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

  .intelligence-layout {
    grid-template-columns: 1fr;
  }

  .history-panel {
    position: static;
  }

  .summary-row,
  .two-column {
    grid-template-columns: 1fr;
  }
}
</style>
