<template>
  <div class="daily-analysis">
    <div class="toolbar card">
      <div class="toolbar-title">
        <h3>每日分析</h3>
        <span>近10日涨停/触板池自动识别，支持人工修正</span>
      </div>
      <div class="toolbar-actions">
        <el-date-picker
          v-model="selectedMonth"
          type="month"
          value-format="YYYY-MM"
          format="YYYY-MM"
          :clearable="false"
          :editable="false"
        />
        <el-button :icon="Refresh" @click="fetchData" :loading="loading">刷新</el-button>
        <el-button type="primary" :icon="Files" @click="backfillMonth" :loading="backfilling">
          回填本月
        </el-button>
      </div>
    </div>

    <div class="table-wrap card">
      <el-table
        :data="rows"
        v-loading="loading"
        border
        height="calc(100vh - 178px)"
        :header-cell-style="{ background: '#fafafa', fontWeight: 600 }"
      >
        <el-table-column prop="trade_date" label="时间" width="116" fixed>
          <template #default="{ row }">
            <div class="date-cell">
              <strong>{{ row.trade_date }}</strong>
              <span>v{{ row.calc_version }}</span>
            </div>
          </template>
        </el-table-column>

        <el-table-column
          v-for="column in analysisColumns"
          :key="column"
          :label="column"
          min-width="230"
        >
          <template #default="{ row }">
            <div class="analysis-cell" :class="{ manual: cell(row, column).is_manual }">
              <div class="cell-tools">
                <el-tag v-if="cell(row, column).is_manual" size="small" type="warning">人工</el-tag>
                <el-tooltip content="编辑单元格" placement="top">
                  <el-button :icon="Edit" link size="small" @click.stop="openEdit(row, column)" />
                </el-tooltip>
                <el-tooltip v-if="cell(row, column).is_manual" content="恢复自动结果" placement="top">
                  <el-button :icon="RefreshLeft" link size="small" @click.stop="restoreOverride(row, column)" />
                </el-tooltip>
              </div>

              <div v-if="cell(row, column).is_manual" class="manual-content">
                {{ cell(row, column).content || '-' }}
              </div>

              <template v-else-if="cell(row, column).items.length">
                <div class="item-list">
                  <button
                    v-for="item in cell(row, column).items"
                    :key="`${item.stock_code || item.label}-${item.tags.join('-')}`"
                    class="signal-item"
                    :class="{ clickable: Boolean(item.stock_code) }"
                    type="button"
                    @click.stop="goStock(item.stock_code)"
                  >
                    <span class="item-label">{{ item.label }}</span>
                    <span v-if="item.time" class="item-time">{{ item.time }}</span>
                    <span class="tag-list">
                      <el-tag
                        v-for="tag in item.tags"
                        :key="tag"
                        :type="tagType(tag)"
                        size="small"
                      >
                        {{ tag }}
                      </el-tag>
                    </span>
                  </button>
                </div>
              </template>

              <span v-else class="empty-cell">-</span>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="操作" width="106" fixed="right" align="center">
          <template #default="{ row }">
            <el-button :icon="Refresh" link type="primary" @click.stop="rebuildRow(row)">
              重算
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="mobile-analysis-list" v-loading="loading">
        <article v-for="row in rows" :key="row.trade_date" class="mobile-analysis-card">
          <div class="mobile-analysis-header">
            <div>
              <strong>{{ row.trade_date }}</strong>
              <span>v{{ row.calc_version }}</span>
            </div>
            <el-button :icon="Refresh" link type="primary" @click.stop="rebuildRow(row)">
              重算
            </el-button>
          </div>

          <section
            v-for="column in analysisColumns"
            :key="`${row.trade_date}-${column}`"
            class="mobile-analysis-section"
            :class="{ manual: cell(row, column).is_manual }"
          >
            <div class="mobile-section-title">
              <span>{{ column }}</span>
              <div class="mobile-section-actions">
                <el-tag v-if="cell(row, column).is_manual" size="small" type="warning">人工</el-tag>
                <el-button :icon="Edit" link size="small" @click.stop="openEdit(row, column)" />
                <el-button
                  v-if="cell(row, column).is_manual"
                  :icon="RefreshLeft"
                  link
                  size="small"
                  @click.stop="restoreOverride(row, column)"
                />
              </div>
            </div>

            <p v-if="cell(row, column).is_manual" class="manual-content">
              {{ cell(row, column).content || '-' }}
            </p>

            <div v-else-if="cell(row, column).items.length" class="mobile-signal-list">
              <button
                v-for="item in cell(row, column).items"
                :key="`${item.stock_code || item.label}-${item.tags.join('-')}`"
                class="signal-item"
                :class="{ clickable: Boolean(item.stock_code) }"
                type="button"
                @click.stop="goStock(item.stock_code)"
              >
                <span class="item-label">{{ item.label }}</span>
                <span v-if="item.time" class="item-time">{{ item.time }}</span>
                <span class="tag-list">
                  <el-tag
                    v-for="tag in item.tags"
                    :key="tag"
                    :type="tagType(tag)"
                    size="small"
                  >
                    {{ tag }}
                  </el-tag>
                </span>
              </button>
            </div>

            <span v-else class="empty-cell">-</span>
          </section>
        </article>
      </div>

      <el-empty v-if="!loading && rows.length === 0" description="本月暂无分析数据，可先回填本月" />
    </div>

    <el-dialog v-model="editVisible" :title="editTitle" width="560px">
      <div class="edit-dialog">
        <div class="auto-preview">
          <span>自动结果</span>
          <p>{{ editingAutoContent || '-' }}</p>
        </div>
        <el-input
          v-model="editText"
          type="textarea"
          :rows="5"
          maxlength="500"
          show-word-limit
          placeholder="填写人工修正内容"
        />
      </div>
      <template #footer>
        <el-button @click="editVisible = false">取消</el-button>
        <el-button v-if="canRestoreEditingCell" @click="restoreEditingOverride" :loading="saving">
          恢复自动
        </el-button>
        <el-button type="primary" @click="saveOverride" :loading="saving">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Edit, Files, Refresh, RefreshLeft } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import {
  backfillDailyAnalysis,
  getDailyAnalysisMonth,
  rebuildDailyAnalysis,
  updateDailyAnalysisOverrides
} from '@/api/daily-analysis'
import {
  DAILY_ANALYSIS_COLUMNS,
  type DailyAnalysisCell,
  type DailyAnalysisColumn,
  type DailyAnalysisRow
} from '@/types/daily-analysis'

const router = useRouter()
const analysisColumns = DAILY_ANALYSIS_COLUMNS
const selectedMonth = ref(dayjs().format('YYYY-MM'))
const rows = ref<DailyAnalysisRow[]>([])
const loading = ref(false)
const backfilling = ref(false)
const saving = ref(false)

const editVisible = ref(false)
const editText = ref('')
const editingRow = ref<DailyAnalysisRow | null>(null)
const editingColumn = ref<DailyAnalysisColumn | null>(null)

const emptyCell: DailyAnalysisCell = {
  items: [],
  content: ''
}

const editTitle = computed(() => {
  if (!editingRow.value || !editingColumn.value) return '编辑'
  return `${editingRow.value.trade_date} ${editingColumn.value}`
})

const editingAutoContent = computed(() => {
  if (!editingRow.value || !editingColumn.value) return ''
  return editingRow.value.auto_result[editingColumn.value]?.content || ''
})

const canRestoreEditingCell = computed(() => {
  if (!editingRow.value || !editingColumn.value) return false
  return Boolean(editingRow.value.columns[editingColumn.value]?.is_manual)
})

watch(selectedMonth, () => {
  fetchData()
})

onMounted(() => {
  fetchData()
})

async function fetchData() {
  loading.value = true
  try {
    const response = await getDailyAnalysisMonth(selectedMonth.value)
    rows.value = response.data
  } catch (error) {
    console.error('获取每日分析失败:', error)
    ElMessage.error('获取每日分析失败')
  } finally {
    loading.value = false
  }
}

async function backfillMonth() {
  try {
    await ElMessageBox.confirm(`将按现有历史数据生成 ${selectedMonth.value} 月表，是否继续？`, '回填本月', {
      type: 'warning'
    })
    backfilling.value = true
    const response = await backfillDailyAnalysis(selectedMonth.value)
    ElMessage.success(`已生成 ${response.built_count} 个交易日`)
    await fetchData()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('回填每日分析失败:', error)
      ElMessage.error('回填失败')
    }
  } finally {
    backfilling.value = false
  }
}

async function rebuildRow(row: DailyAnalysisRow) {
  try {
    await ElMessageBox.confirm(`重算 ${row.trade_date} 的自动分析，人工修正会保留。`, '重算单日', {
      type: 'warning'
    })
    const updated = await rebuildDailyAnalysis(row.trade_date)
    replaceRow(updated)
    ElMessage.success('重算完成')
  } catch (error) {
    if (error !== 'cancel') {
      console.error('重算每日分析失败:', error)
      ElMessage.error('重算失败')
    }
  }
}

function openEdit(row: DailyAnalysisRow, column: DailyAnalysisColumn) {
  editingRow.value = row
  editingColumn.value = column
  editText.value = row.manual_overrides[column] ?? row.columns[column]?.content ?? ''
  editVisible.value = true
}

async function saveOverride() {
  if (!editingRow.value || !editingColumn.value) return

  saving.value = true
  try {
    const updated = await updateDailyAnalysisOverrides(editingRow.value.trade_date, {
      [editingColumn.value]: editText.value
    })
    replaceRow(updated)
    editVisible.value = false
    ElMessage.success('已保存人工修正')
  } catch (error) {
    console.error('保存人工修正失败:', error)
    ElMessage.error('保存失败')
  } finally {
    saving.value = false
  }
}

async function restoreEditingOverride() {
  if (!editingRow.value || !editingColumn.value) return
  await restoreOverride(editingRow.value, editingColumn.value)
  editVisible.value = false
}

async function restoreOverride(row: DailyAnalysisRow, column: DailyAnalysisColumn) {
  saving.value = true
  try {
    const updated = await updateDailyAnalysisOverrides(row.trade_date, {
      [column]: null
    })
    replaceRow(updated)
    ElMessage.success('已恢复自动结果')
  } catch (error) {
    console.error('恢复自动结果失败:', error)
    ElMessage.error('恢复失败')
  } finally {
    saving.value = false
  }
}

function replaceRow(updated: DailyAnalysisRow) {
  const index = rows.value.findIndex(row => row.trade_date === updated.trade_date)
  if (index >= 0) {
    rows.value.splice(index, 1, updated)
  } else {
    rows.value.unshift(updated)
  }
}

function cell(row: DailyAnalysisRow, column: DailyAnalysisColumn): DailyAnalysisCell {
  return row.columns[column] || emptyCell
}

function goStock(stockCode?: string) {
  if (!stockCode) return
  router.push(`/stock/${stockCode}`)
}

function tagType(tag: string): 'success' | 'warning' | 'danger' | 'info' {
  if (tag.includes('负') || tag.includes('炸')) return 'danger'
  if (tag.includes('人工') || tag.includes('长上影')) return 'warning'
  if (tag.includes('趋势') || tag.includes('二波')) return 'success'
  return 'info'
}
</script>

<style lang="scss" scoped>
.daily-analysis {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;

  .toolbar-title {
    display: flex;
    flex-direction: column;
    gap: 4px;

    h3 {
      margin: 0;
      font-size: 16px;
      font-weight: 600;
      color: #1f1f1f;
    }

    span {
      font-size: 12px;
      color: #8c8c8c;
    }
  }

  .toolbar-actions {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
}

.table-wrap {
  position: relative;
  padding: 0;
  overflow: hidden;
}

.mobile-analysis-list {
  display: none;
}

.date-cell {
  display: flex;
  flex-direction: column;
  gap: 4px;

  strong {
    font-size: 13px;
    color: #262626;
  }

  span {
    font-size: 12px;
    color: #8c8c8c;
  }
}

.analysis-cell {
  min-height: 76px;
  position: relative;
  padding: 4px 0 0;

  &.manual {
    background: #fffbe6;
    margin: -8px;
    padding: 8px;
    min-height: 92px;
  }
}

.cell-tools {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 2px;
  min-height: 24px;
}

.item-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.signal-item {
  width: 100%;
  border: 1px solid #f0f0f0;
  border-radius: 6px;
  background: #fafafa;
  padding: 6px;
  display: flex;
  align-items: flex-start;
  gap: 6px;
  flex-wrap: wrap;
  text-align: left;
  color: #262626;

  &.clickable {
    cursor: pointer;

    &:hover {
      border-color: #91caff;
      background: #e6f4ff;
    }
  }
}

.item-label {
  font-size: 13px;
  font-weight: 600;
  line-height: 22px;
}

.item-time {
  font-size: 12px;
  color: #8c8c8c;
  line-height: 22px;
}

.tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.manual-content {
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 13px;
  line-height: 1.6;
  color: #594214;
}

.empty-cell {
  display: block;
  color: #bfbfbf;
  font-size: 13px;
  padding-top: 10px;
}

.edit-dialog {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.auto-preview {
  background: #fafafa;
  border: 1px solid #f0f0f0;
  border-radius: 6px;
  padding: 10px;

  span {
    color: #8c8c8c;
    font-size: 12px;
  }

  p {
    margin: 6px 0 0;
    color: #262626;
    font-size: 13px;
    line-height: 1.5;
    word-break: break-word;
  }
}

@media (max-width: 767px) {
  .daily-analysis {
    gap: 10px;
  }

  .toolbar {
    align-items: flex-start;
    flex-direction: column;
    padding: 12px;

    .toolbar-actions {
      width: 100%;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;

      :deep(.el-date-editor),
      :deep(.el-button) {
        width: 100%;
      }
    }
  }

  .table-wrap {
    padding: 0;
    overflow: visible;

    :deep(.el-table) {
      display: none;
    }
  }

  .mobile-analysis-list {
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-height: 160px;
  }

  .mobile-analysis-card {
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    background: #fff;
    overflow: hidden;
  }

  .mobile-analysis-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px;
    border-bottom: 1px solid #f1f5f9;

    strong {
      display: block;
      color: #111827;
      font-size: 15px;
    }

    span {
      color: #94a3b8;
      font-size: 12px;
    }
  }

  .mobile-analysis-section {
    padding: 12px;
    border-bottom: 1px solid #f1f5f9;

    &:last-child {
      border-bottom: none;
    }

    &.manual {
      background: #fffbe6;
    }
  }

  .mobile-section-title {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 8px;

    > span {
      color: #111827;
      font-size: 14px;
      font-weight: 600;
    }
  }

  .mobile-section-actions {
    display: flex;
    align-items: center;
    gap: 4px;
  }

  .mobile-signal-list {
    display: flex;
    flex-direction: column;
    gap: 7px;
  }

  .signal-item {
    padding: 7px;
  }

  .edit-dialog {
    :deep(.el-textarea__inner) {
      min-height: 140px !important;
    }
  }
}
</style>
