<template>
  <div class="daily-info">
    <div class="toolbar">
      <div class="toolbar-title">
        <h3>每日资讯</h3>
        <span>知识库增量同步后生成的盘前/盘后资讯和交易预案</span>
      </div>
      <div class="toolbar-actions">
        <el-date-picker
          v-model="selectedDate"
          type="date"
          value-format="YYYY-MM-DD"
          format="YYYY-MM-DD"
          :clearable="false"
          :editable="false"
        />
        <el-button :icon="Refresh" @click="fetchData" :loading="loading">刷新</el-button>
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
          <div v-if="sourceTitles.length" class="source-list">
            <span v-for="title in sourceTitles" :key="title">{{ title }}</span>
          </div>
          <span v-else class="empty-text">暂无来源</span>
        </section>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { Files, Refresh } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { getDailyInfo, syncDailyInfo } from '@/api/intelligence'
import type { DailyInfoResponse } from '@/types/intelligence'

const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const dailyInfo = ref<DailyInfoResponse | null>(null)
const loading = ref(false)
const syncing = ref(false)
const errorMessage = ref('')

const mainLines = computed(() => stringList(dailyInfo.value?.summary.main_lines))
const catalysts = computed(() => stringList(dailyInfo.value?.summary.catalysts))
const risks = computed(() => stringList(dailyInfo.value?.summary.risks))
const sourceTitles = computed(() => stringList(dailyInfo.value?.summary.source_titles))
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
    fallback: '模型结果不可用，当前展示本地兜底摘要',
    error: dailyInfo.value?.summary.error || '模型调用失败，当前展示本地兜底摘要'
  }
  return statusMap[modelStatus.value] || `模型状态：${modelStatus.value}`
})
const isEmpty = computed(() => {
  if (!dailyInfo.value) return true
  return dailyInfo.value.source_count === 0 && !dailyInfo.value.summary.overview
})

watch(selectedDate, () => {
  fetchData()
})

onMounted(() => {
  fetchData()
})

async function fetchData() {
  loading.value = true
  errorMessage.value = ''
  try {
    dailyInfo.value = await getDailyInfo(selectedDate.value)
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
    ElMessage.success(`同步完成，更新 ${changedCount(response.sources)} 篇资料`)
  } catch (error) {
    console.error('同步知识库失败:', error)
    errorMessage.value = '同步知识库失败'
    ElMessage.error(errorMessage.value)
  } finally {
    syncing.value = false
  }
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
  display: flex;
  flex-direction: column;
  gap: 14px;
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

  span {
    max-width: 100%;
    padding: 5px 8px;
    background: #f3f4f6;
    border-radius: 4px;
    color: #374151;
    font-size: 13px;
    overflow-wrap: anywhere;
  }
}

.empty-text {
  color: #9ca3af;
  font-size: 13px;
}

@media (max-width: 960px) {
  .toolbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .summary-row,
  .two-column {
    grid-template-columns: 1fr;
  }
}
</style>
