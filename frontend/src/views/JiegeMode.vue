<template>
  <div class="jiege-mode">
    <div class="toolbar">
      <div class="toolbar-title">
        <h3>交易模式</h3>
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
        <el-button type="primary" :icon="Files" @click="rebuildData" :loading="rebuilding">重算模式</el-button>
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

    <div v-loading="loading" class="content">
      <el-empty v-if="!loading && !signalData" description="暂无模式数据，可先同步知识库或重算模式" />

      <template v-else-if="signalData">
        <div class="summary-row">
          <div class="metric phase">
            <span>情绪周期</span>
            <strong>{{ signalData.market_phase.label }}</strong>
          </div>
          <div class="metric">
            <span>环境分</span>
            <strong>{{ signalData.market_phase.score }}</strong>
          </div>
          <div class="metric">
            <span>封板/开板</span>
            <strong>{{ signalData.review.sealed_count }} / {{ signalData.review.opened_count }}</strong>
          </div>
          <div class="metric">
            <span>最高连板</span>
            <strong>{{ signalData.review.max_board_height }}</strong>
          </div>
        </div>

        <section class="panel">
          <div class="section-header">
            <h4>L1 市场环境</h4>
            <el-tag size="small" :type="jiegeMode?.cache_hit ? 'info' : 'success'">
              {{ jiegeMode?.cache_hit ? '缓存命中' : '已更新' }}
            </el-tag>
          </div>
          <div v-if="signalData.market_phase.basis.length" class="tag-list">
            <el-tag v-for="item in signalData.market_phase.basis" :key="item" effect="plain">
              {{ item }}
            </el-tag>
          </div>
          <span v-else class="empty-text">暂无市场环境依据</span>
        </section>

        <section class="panel rules-panel" :class="{ 'is-expanded': rulesExpanded }">
          <div class="section-header">
            <h4>规则体系</h4>
            <div class="section-actions">
              <span>{{ signalData.rules.length }} 条规则</span>
              <el-button
                size="small"
                text
                :icon="rulesExpanded ? ArrowUp : ArrowDown"
                @click="rulesExpanded = !rulesExpanded"
              >
                {{ rulesExpanded ? '收起' : '展开' }}
              </el-button>
            </div>
          </div>
          <div v-if="rulesExpanded && signalData.rules.length" class="rule-grid">
            <article v-for="rule in signalData.rules" :key="rule.rule_key" class="rule-item">
              <div class="rule-title">
                <el-tag size="small" type="info">{{ rule.category || '规则' }}</el-tag>
                <strong>{{ rule.title }}</strong>
              </div>
              <p>{{ rule.summary || '-' }}</p>
              <div v-if="payloadLabels(rule.payload).length" class="payload-list">
                <span v-for="label in payloadLabels(rule.payload)" :key="label">{{ label }}</span>
              </div>
            </article>
          </div>
          <el-empty v-else-if="rulesExpanded" description="暂无规则" />
        </section>

        <section class="panel yesterday-panel">
          <div class="section-header">
            <div>
              <h4>昨日预判</h4>
              <p class="section-note">{{ yesterdayPredictionLabel }}</p>
            </div>
            <span>{{ yesterdayCandidates.length }} 个候选</span>
          </div>
          <el-table
            v-if="yesterdayCandidates.length"
            :data="yesterdayCandidates"
            border
            size="small"
            :header-cell-style="{ background: '#fafafa', fontWeight: 600 }"
          >
            <el-table-column prop="label" label="标的" min-width="140">
              <template #default="{ row }">
                <el-button link type="primary" @click="goStock(row.stock_code)">
                  {{ row.label }}
                </el-button>
              </template>
            </el-table-column>
            <el-table-column prop="tags" label="模式" min-width="150">
              <template #default="{ row }">
                <div class="tag-list compact">
                  <el-tag
                    v-for="tag in row.tags"
                    :key="tag"
                    size="small"
                    :type="candidateTagType(tag)"
                  >
                    {{ tag }}
                  </el-tag>
                </div>
              </template>
            </el-table-column>
            <el-table-column prop="score" label="评分" width="74" align="right" />
            <el-table-column prop="reason" label="昨日依据" min-width="180" show-overflow-tooltip />
          </el-table>
          <span v-else class="empty-text">{{ yesterdayPrediction?.notes || '暂无昨日预判候选' }}</span>
          <ul v-if="yesterdayRiskFlags.length" class="item-list compact-risk">
            <li v-for="item in yesterdayRiskFlags" :key="item">{{ item }}</li>
          </ul>
        </section>

        <div class="two-column">
          <section class="panel">
            <div class="section-header">
              <h4>当日模式结果</h4>
              <span>{{ candidates.length }} 个候选</span>
            </div>
            <el-table
              :data="candidates"
              border
              size="small"
              :header-cell-style="{ background: '#fafafa', fontWeight: 600 }"
            >
              <el-table-column prop="label" label="标的" min-width="140">
                <template #default="{ row }">
                  <el-button link type="primary" @click="goStock(row.stock_code)">
                    {{ row.label }}
                  </el-button>
                </template>
              </el-table-column>
              <el-table-column prop="tags" label="模式" min-width="150">
                <template #default="{ row }">
                  <div class="tag-list compact">
                    <el-tag
                      v-for="tag in row.tags"
                      :key="tag"
                      size="small"
                      :type="candidateTagType(tag)"
                    >
                      {{ tag }}
                    </el-tag>
                  </div>
                </template>
              </el-table-column>
              <el-table-column prop="score" label="评分" width="74" align="right" />
              <el-table-column prop="reason" label="原因" min-width="180" show-overflow-tooltip />
            </el-table>
          </section>

          <section class="panel risk-panel">
            <div class="section-header">
              <h4>风险否决原因</h4>
            </div>
            <ul v-if="riskFlags.length" class="item-list">
              <li v-for="item in riskFlags" :key="item">{{ item }}</li>
            </ul>
            <span v-else class="empty-text">暂无风险否决信号</span>
          </section>
        </div>

        <section class="panel">
          <div class="section-header">
            <h4>复盘验证</h4>
            <span>{{ formatTime(jiegeMode?.generated_at) }}</span>
          </div>
          <p>{{ signalData.review.notes || '-' }}</p>
          <div v-if="dailyAnalysisItems.length" class="analysis-list">
            <div v-for="item in dailyAnalysisItems" :key="item.key" class="analysis-item">
              <strong>{{ item.key }}</strong>
              <span>{{ item.text }}</span>
            </div>
          </div>
        </section>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { ArrowDown, ArrowUp, Files, Refresh } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { exportObsidianKnowledge, getJiegeMode, getObsidianStatus, rebuildJiegeMode } from '@/api/intelligence'
import type { JiegeCandidate, JiegeModeResponse, ObsidianStatus } from '@/types/intelligence'

const router = useRouter()
const selectedDate = ref(dayjs().format('YYYY-MM-DD'))
const jiegeMode = ref<JiegeModeResponse | null>(null)
const loading = ref(false)
const rebuilding = ref(false)
const exportingObsidian = ref(false)
const errorMessage = ref('')
const rulesExpanded = ref(false)
const obsidianStatus = ref<ObsidianStatus | null>(null)
const lastObsidianFile = ref('')

const signalData = computed(() => jiegeMode.value?.data || null)
const candidates = computed<JiegeCandidate[]>(() => signalData.value?.prediction.candidates || [])
const riskFlags = computed(() => signalData.value?.prediction.risk_flags || [])
const yesterdayPrediction = computed(() => signalData.value?.yesterday_prediction || null)
const yesterdayCandidates = computed<JiegeCandidate[]>(() => yesterdayPrediction.value?.candidates || [])
const yesterdayRiskFlags = computed(() => yesterdayPrediction.value?.risk_flags || [])
const yesterdayPredictionLabel = computed(() => {
  if (!yesterdayPrediction.value) return '等待后端返回昨日预判数据'
  const { source_date: sourceDate, target_date: targetDate } = yesterdayPrediction.value
  if (!sourceDate) return yesterdayPrediction.value.notes || '暂无昨日复盘数据'
  return `基于 ${sourceDate} 盘后复盘，预判 ${targetDate} 盘前观察方向`
})
const dailyAnalysisItems = computed(() => {
  const source = signalData.value?.prediction.daily_analysis || {}
  return Object.entries(source)
    .map(([key, value]) => ({ key, text: readableCell(value) }))
    .filter(item => item.text)
})
const obsidianReady = computed(() => Boolean(obsidianStatus.value?.enabled && obsidianStatus.value?.vault_configured))

watch(selectedDate, () => {
  fetchData()
})

onMounted(() => {
  fetchData()
  refreshObsidianStatus()
})

async function fetchData() {
  loading.value = true
  errorMessage.value = ''
  try {
    jiegeMode.value = await getJiegeMode(selectedDate.value)
  } catch (error) {
    console.error('获取交易模式失败:', error)
    errorMessage.value = '获取交易模式失败'
    ElMessage.error(errorMessage.value)
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

async function rebuildData() {
  rebuilding.value = true
  errorMessage.value = ''
  try {
    jiegeMode.value = await rebuildJiegeMode(selectedDate.value)
    ElMessage.success('模式重算完成')
  } catch (error) {
    console.error('重算交易模式失败:', error)
    errorMessage.value = '重算交易模式失败'
    ElMessage.error(errorMessage.value)
  } finally {
    rebuilding.value = false
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

function goStock(stockCode: string) {
  if (!stockCode) return
  router.push(`/stock/${stockCode}`)
}

function candidateTagType(tag: string): string {
  if (tag.includes('分歧')) return 'warning'
  if (tag.includes('20cm')) return 'success'
  if (tag.includes('首板')) return 'info'
  return ''
}

function payloadLabels(payload: Record<string, unknown>): string[] {
  const labels: string[] = []
  Object.values(payload || {}).forEach(value => {
    if (Array.isArray(value)) {
      value.slice(0, 5).forEach(item => labels.push(String(item)))
    }
  })
  return labels.slice(0, 8)
}

function readableCell(value: unknown): string {
  if (typeof value === 'string') return value
  if (!value || typeof value !== 'object') return ''
  const record = value as Record<string, unknown>
  if (typeof record.content === 'string' && record.content) return record.content
  if (Array.isArray(record.items)) {
    return record.items
      .map(item => {
        if (!item || typeof item !== 'object') return ''
        return String((item as Record<string, unknown>).label || '')
      })
      .filter(Boolean)
      .join('、')
  }
  return ''
}

function formatTime(value?: string | null): string {
  if (!value) return '-'
  return dayjs(value).format('YYYY-MM-DD HH:mm')
}
</script>

<style lang="scss" scoped>
.jiege-mode {
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
    font-size: 18px;
    font-weight: 600;
    overflow-wrap: anywhere;
  }
}

.phase {
  border-left: 4px solid #1677ff;
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

.risk-panel {
  border-left: 4px solid #f59e0b;
}

.yesterday-panel {
  border-left: 4px solid #1677ff;
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

  span {
    color: #6b7280;
    font-size: 13px;
  }

  .section-note {
    margin: 4px 0 0;
    color: #6b7280;
    font-size: 12px;
    line-height: 1.5;
  }
}

.section-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.rules-panel:not(.is-expanded) {
  .section-header {
    margin-bottom: 0;
  }
}

.two-column {
  display: grid;
  grid-template-columns: minmax(0, 2fr) minmax(280px, 1fr);
  gap: 14px;
}

.rule-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
  gap: 10px;
}

.rule-item {
  padding: 12px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  background: #fafafa;

  p {
    margin: 8px 0 0;
    font-size: 13px;
  }
}

.rule-title {
  display: flex;
  align-items: center;
  gap: 8px;

  strong {
    color: #111827;
    font-size: 14px;
    overflow-wrap: anywhere;
  }
}

.payload-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 10px;

  span {
    padding: 3px 6px;
    background: #eef2ff;
    border-radius: 4px;
    color: #374151;
    font-size: 12px;
  }
}

.tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;

  &.compact {
    gap: 5px;
  }
}

.item-list {
  margin: 0;
  padding-left: 18px;
  color: #374151;
  line-height: 1.8;

  &.compact-risk {
    margin-top: 10px;
  }
}

.analysis-list {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 8px;
  margin-top: 12px;
}

.analysis-item {
  padding: 10px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  background: #fafafa;

  strong {
    display: block;
    margin-bottom: 5px;
    color: #111827;
    font-size: 13px;
  }

  span {
    color: #4b5563;
    font-size: 13px;
    line-height: 1.6;
    overflow-wrap: anywhere;
  }
}

.empty-text {
  color: #9ca3af;
  font-size: 13px;
}

@media (max-width: 1080px) {
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
