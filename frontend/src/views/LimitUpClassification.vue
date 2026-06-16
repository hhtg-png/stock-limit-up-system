<template>
  <div class="limit-up-classification">
    <div class="toolbar">
      <div class="toolbar-title">
        <h3>涨停分类</h3>
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
        <el-button :icon="Refresh" @click="refreshData" :loading="loading">刷新</el-button>
        <el-button :icon="MagicStick" @click="regenerateAiClassification" :loading="aiRefreshing">
          重算AI分类
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
    <el-alert
      v-else-if="classification?.is_fallback"
      class="state-alert"
      type="warning"
      :title="`未找到 ${classification.requested_date} 数据，已显示 ${classification.trade_date}`"
      show-icon
      :closable="false"
    />

    <div v-loading="loading" class="content">
      <el-empty
        v-if="!loading && !groups.length"
        description="暂无涨停分类数据"
      />

      <template v-else>
        <div class="summary-row">
          <div class="metric">
            <span>数据日期</span>
            <strong>{{ classification?.trade_date || selectedDate }}</strong>
          </div>
          <div class="metric">
            <span>涨停数</span>
            <strong>{{ classification?.total_count || 0 }}</strong>
          </div>
          <div class="metric">
            <span>板块数</span>
            <strong>{{ groups.length }}</strong>
          </div>
          <div class="metric">
            <span>来源</span>
            <strong>{{ sourceText }}</strong>
          </div>
          <div class="metric">
            <span>分类方式</span>
            <strong>{{ classificationText }}</strong>
          </div>
        </div>

        <section
          v-for="group in groups"
          :key="group.plate_name"
          class="classification-group"
        >
          <header class="group-header">
            <div>
              <h4>{{ group.plate_name }}</h4>
              <span>{{ group.earliest_first_limit_time || '-' }} - {{ group.latest_first_limit_time || '-' }}</span>
            </div>
            <div class="group-tags">
              <el-tag type="danger" effect="plain">{{ group.count }} 只</el-tag>
              <el-tag type="success" effect="plain">{{ group.sealed_count }} 封板</el-tag>
              <el-tag v-if="group.opened_count" type="warning" effect="plain">{{ group.opened_count }} 炸板</el-tag>
            </div>
          </header>

          <el-table
            :data="group.stocks"
            size="small"
            class="classification-table"
            @row-click="openStock"
          >
            <el-table-column prop="stock_code" label="代码" width="95" />
            <el-table-column prop="stock_name" label="名称" width="100" />
            <el-table-column label="连板" width="76" align="center">
              <template #default="{ row }">
                <el-tag v-if="row.continuous_limit_up_days > 1 || row.board_label" size="small" type="info">
                  {{ boardLabel(row) }}
                </el-tag>
                <span v-else>首板</span>
              </template>
            </el-table-column>
            <el-table-column prop="first_limit_up_time" label="首封时间" width="100" />
            <el-table-column label="回封时间" width="100">
              <template #default="{ row }">
                {{ row.final_seal_time || '-' }}
              </template>
            </el-table-column>
            <el-table-column label="状态" width="82" align="center">
              <template #default="{ row }">
                <el-tag :type="row.is_sealed ? 'danger' : 'warning'" size="small" effect="plain">
                  {{ row.is_sealed ? '封板' : '炸板' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="分类依据" width="112" align="center">
              <template #default="{ row }">
                <el-tooltip
                  v-if="classificationTooltip(row)"
                  :content="classificationTooltip(row)"
                  placement="top"
                >
                  <el-tag :type="classificationTagType(row)" size="small" effect="plain">
                    {{ classificationLabel(row) }}
                  </el-tag>
                </el-tooltip>
                <el-tag v-else :type="classificationTagType(row)" size="small" effect="plain">
                  {{ classificationLabel(row) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="细分题材" min-width="170">
              <template #default="{ row }">
                <div class="theme-tags">
                  <el-tag
                    v-for="theme in displayThemes(row)"
                    :key="theme"
                    size="small"
                    effect="plain"
                  >
                    {{ theme }}
                  </el-tag>
                  <span v-if="!displayThemes(row).length">-</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="同花顺异动解读" min-width="190" show-overflow-tooltip>
              <template #default="{ row }">
                {{ row.ths_move_title || '-' }}
              </template>
            </el-table-column>
            <el-table-column label="证据句" min-width="250" show-overflow-tooltip>
              <template #default="{ row }">
                <a
                  v-if="row.ths_article_url"
                  class="analysis-link"
                  :href="row.ths_article_url"
                  target="_blank"
                  rel="noopener"
                  @click.stop
                >
                  {{ row.classification_evidence || row.ths_move_summary || '-' }}
                </a>
                <span v-else>{{ row.classification_evidence || row.ths_move_summary || '-' }}</span>
              </template>
            </el-table-column>
            <el-table-column label="封单(万)" width="100" align="right">
              <template #default="{ row }">
                {{ formatWan(row.seal_amount) }}
              </template>
            </el-table-column>
            <el-table-column label="换手率" width="86" align="right">
              <template #default="{ row }">
                {{ formatRate(row.turnover_rate) }}
              </template>
            </el-table-column>
            <el-table-column label="成交额(亿)" width="100" align="right">
              <template #default="{ row }">
                {{ formatYi(row.amount) }}
              </template>
            </el-table-column>
            <el-table-column prop="limit_up_reason" label="同花顺涨停原因" min-width="220" show-overflow-tooltip />
          </el-table>

          <div class="classification-card-list">
            <article
              v-for="stock in group.stocks"
              :key="stock.stock_code"
              class="classification-card"
              @click="openStock(stock)"
            >
              <header>
                <div>
                  <strong>{{ stock.stock_name }}</strong>
                  <span>{{ stock.stock_code }}</span>
                </div>
                <el-tag :type="stock.is_sealed ? 'danger' : 'warning'" size="small" effect="plain">
                  {{ stock.is_sealed ? '封板' : '炸板' }}
                </el-tag>
              </header>
              <div class="card-metrics">
                <span>首封 {{ stock.first_limit_up_time || '-' }}</span>
                <span>回封 {{ stock.final_seal_time || '-' }}</span>
                <span>{{ boardLabel(stock) }}</span>
                <span>{{ classificationLabel(stock) }}</span>
              </div>
              <div v-if="displayThemes(stock).length" class="theme-tags">
                <el-tag
                  v-for="theme in displayThemes(stock)"
                  :key="theme"
                  size="small"
                  effect="plain"
                >
                  {{ theme }}
                </el-tag>
              </div>
              <p v-if="stock.ai_reason_summary" class="ai-summary">{{ stock.ai_reason_summary }}</p>
              <p v-if="stock.ths_move_title" class="move-summary">
                同花顺异动解读：{{ stock.ths_move_title }}
              </p>
              <p v-if="stock.classification_evidence" class="move-summary">
                证据句：{{ stock.classification_evidence }}
              </p>
              <p v-if="stock.ths_move_summary" class="move-summary">{{ stock.ths_move_summary }}</p>
              <p>{{ stock.limit_up_reason || '暂无同花顺涨停原因' }}</p>
            </article>
          </div>
        </section>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { MagicStick, Refresh } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { getLimitUpClassification } from '@/api/limit-up'
import type {
  LimitUpClassificationGroup,
  LimitUpClassificationResponse,
  LimitUpClassificationStock
} from '@/types/limit-up'

const router = useRouter()
const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const classification = ref<LimitUpClassificationResponse | null>(null)
const loading = ref(false)
const aiRefreshing = ref(false)
const errorMessage = ref('')

const groups = computed<LimitUpClassificationGroup[]>(() => classification.value?.groups || [])
const sourceText = computed(() => {
  const status = classification.value?.source_status || {}
  if (status.ths_article_analysis === 'ok' || status.ths_article_analysis === 'partial') {
    return '同花顺异动分析'
  }
  if (status.ths_move_classification === 'ok' || status.ths_move_classification === 'partial') {
    return '实时池+同花顺异动'
  }
  if (status.limit_up_pool === 'ok') return '实时池+同花顺'
  if (status.limit_up_db === 'ok') return '历史缓存'
  return '同花顺'
})
const classificationText = computed(() => {
  const status = classification.value?.source_status || {}
  if (status.ai_classification === 'cache_hit') return 'DeepSeek缓存'
  if (status.ai_classification === 'ready') return 'DeepSeek生成'
  if (status.ai_classification === 'refresh_scheduled') return 'AI生成中'
  if (status.ai_classification === 'missing_api_key') return '规则分类'
  if (status.ai_classification === 'error') return '规则分类'
  if (status.ths_article_analysis === 'ok') return '异动分析规则'
  if (status.ths_article_analysis === 'partial') return '异动分析+兜底'
  if (status.ths_move_classification === 'ok') return '异动解读规则'
  if (status.ths_move_classification === 'partial') return '异动解读+短原因'
  return classification.value?.classification_method === 'ai' ? 'DeepSeek分类' : '规则分类'
})

async function fetchData(options: { silent?: boolean; forceAi?: boolean } = {}) {
  if (options.forceAi) {
    aiRefreshing.value = true
  } else {
    loading.value = true
  }
  errorMessage.value = ''
  try {
    classification.value = await getLimitUpClassification({
      trade_date: selectedDate.value,
      force_ai: options.forceAi || undefined
    })
  } catch (error) {
    console.error('Fetch limit-up classification error:', error)
    errorMessage.value = '获取涨停分类失败'
    if (!options.silent) {
      ElMessage.error(errorMessage.value)
    }
  } finally {
    if (options.forceAi) {
      aiRefreshing.value = false
    } else {
      loading.value = false
    }
  }
}

function handleDateChange() {
  fetchData()
}

async function refreshData() {
  await fetchData()
  if (!errorMessage.value) {
    ElMessage.success('数据已刷新')
  }
}

async function regenerateAiClassification() {
  await fetchData({ forceAi: true })
  if (errorMessage.value) return
  const status = classification.value?.source_status?.ai_classification
  if (status === 'missing_api_key') {
    ElMessage.warning('未配置 DeepSeek Key，已使用规则分类')
  } else if (status === 'error') {
    ElMessage.warning('AI分类失败，已使用规则分类')
  } else if (status === 'refresh_scheduled') {
    ElMessage.success('AI分类已开始生成，稍后刷新查看')
  } else {
    ElMessage.success('AI分类已更新')
  }
}

function openStock(row: LimitUpClassificationStock) {
  if (!row.stock_code) return
  router.push(`/stock/${row.stock_code}`)
}

function classificationLabel(row: LimitUpClassificationStock) {
  if (row.classification_method !== 'ai') {
    if (row.classification_basis === 'ths_move_analysis') return '异动分析'
    return row.classification_basis === 'ths_move' ? '异动解读' : '短原因'
  }
  return row.ai_confidence ? `AI ${(row.ai_confidence * 100).toFixed(0)}%` : 'AI'
}

function classificationTagType(row: LimitUpClassificationStock) {
  if (row.classification_method !== 'ai' && row.classification_basis === 'ths_move_analysis') return 'danger'
  if (row.classification_method !== 'ai' && row.classification_basis === 'ths_move') return 'primary'
  return row.classification_method === 'ai' ? 'success' : 'info'
}

function classificationTooltip(row: LimitUpClassificationStock) {
  return row.ai_reason_summary || row.classification_evidence || row.ths_move_summary || ''
}

function boardLabel(row: LimitUpClassificationStock) {
  return row.board_label || (row.continuous_limit_up_days > 1 ? `${row.continuous_limit_up_days}板` : '首板')
}

function displayThemes(row: LimitUpClassificationStock) {
  const themes = [
    row.fine_theme,
    ...(row.secondary_themes || []),
    ...(row.fine_themes || [])
  ].filter(Boolean)
  return Array.from(new Set(themes)).slice(0, 5)
}

function formatWan(value?: number | null) {
  if (!value) return '-'
  return value.toFixed(0)
}

function formatRate(value?: number | null) {
  if (!value) return '-'
  return `${value.toFixed(2)}%`
}

function formatYi(value?: number | null) {
  if (!value) return '-'
  return (value / 10000).toFixed(2)
}

onMounted(() => {
  fetchData({ silent: true })
})
</script>

<style lang="scss" scoped>
.limit-up-classification {
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
    color: #1f2937;
    font-size: 18px;
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
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 10px;
}

.metric {
  padding: 12px 14px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;

  span {
    display: block;
    margin-bottom: 6px;
    color: #6b7280;
    font-size: 12px;
  }

  strong {
    display: block;
    color: #111827;
    font-size: 16px;
    font-weight: 600;
    overflow-wrap: anywhere;
  }
}

.classification-group {
  min-width: 0;
  overflow: hidden;
  padding: 14px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
}

.group-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;

  h4 {
    margin: 0 0 5px;
    color: #111827;
    font-size: 16px;
    font-weight: 600;
  }

  span {
    color: #6b7280;
    font-size: 12px;
  }
}

.group-tags {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 6px;
}

.classification-table {
  :deep(.el-table__row) {
    cursor: pointer;
  }
}

.theme-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  align-items: center;
}

.analysis-link {
  color: #2563eb;
  text-decoration: none;
}

.analysis-link:hover {
  text-decoration: underline;
}

.classification-card-list {
  display: none;
}

@media (max-width: 767px) {
  .limit-up-classification {
    gap: 10px;
  }

  .toolbar {
    min-height: auto;
    padding: 12px;
    align-items: stretch;
    flex-direction: column;
  }

  .toolbar-actions {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;

    :deep(.el-date-editor) {
      grid-column: 1 / -1;
      width: 100%;
    }

    .el-button {
      margin-left: 0;
      width: 100%;
    }
  }

  .summary-row {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
  }

  .metric {
    padding: 10px;
  }

  .classification-group {
    padding: 12px;
  }

  .group-header {
    flex-direction: column;
    gap: 8px;
  }

  .group-tags {
    justify-content: flex-start;
  }

  .classification-table {
    display: none;
  }

  .classification-card-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .classification-card {
    padding: 10px;
    border: 1px solid #e5e7eb;
    border-radius: 6px;
    background: #fff;
    cursor: pointer;

    header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 8px;
    }

    strong {
      display: block;
      color: #111827;
      font-size: 15px;
      font-weight: 600;
    }

    header span {
      color: #6b7280;
      font-size: 12px;
    }

    p {
      margin: 0;
      color: #374151;
      font-size: 13px;
      line-height: 1.6;
      overflow-wrap: anywhere;
    }

    .ai-summary {
      margin-bottom: 5px;
      color: #166534;
    }

    .move-summary {
      margin-bottom: 5px;
      color: #1f2937;
    }
  }

  .card-metrics {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 8px;

    span {
      padding: 3px 6px;
      border-radius: 4px;
      background: #f3f4f6;
      color: #4b5563;
      font-size: 12px;
    }
  }

  .theme-tags {
    margin-bottom: 8px;
  }
}
</style>
