<template>
  <div class="dynamic-page">
    <header class="page-heading"><div><p class="eyebrow">自定义观察 · 入选档案</p><h1>动态板块</h1><p class="intro">条件选股，持续跟踪每一次入选。</p></div><div class="heading-actions"><el-button class="mobile-directory-button" @click="directoryOpen = true">板块目录 <span class="count">{{ total }}</span></el-button><el-button type="primary" @click="openEditor()">＋ 新建板块</el-button></div></header>
    <el-alert v-if="fieldsError" type="warning" :closable="false" :title="fieldsError"><el-button link @click="loadFields">重新加载条件</el-button></el-alert>
    <div class="workspace">
      <aside class="desktop-directory"><DynamicGroupDirectory :groups="groups" :total="total" :folders="folders" :fields="fields" :query="query" :selected-id="selectedId" :loading="listLoading" :error="listError" @query="updateQuery" @select="selectGroup" @create="openEditor()" @retry="loadDirectory" /></aside>
      <main class="detail">
        <template v-if="activeGroup">
          <section class="panel group-overview">
            <div class="overview-heading"><div class="group-name"><i :style="{ background: activeGroup.color || '#409eff' }" /><h2>{{ activeGroup.name }}</h2><el-tag size="small" :type="activeGroup.archived || !activeGroup.enabled ? 'info' : 'success'">{{ activeGroup.archived ? '已归档' : activeGroup.enabled ? '自动跟踪' : '已暂停' }}</el-tag></div><el-dropdown trigger="click" :disabled="actionLoading" @command="groupAction"><el-button :loading="actionLoading">管理板块 ▾</el-button><template #dropdown><el-dropdown-menu><el-dropdown-item command="edit" :disabled="activeGroup.archived">编辑条件</el-dropdown-item><el-dropdown-item command="pin" :disabled="activeGroup.archived">{{ activeGroup.pinned ? '取消置顶' : '置顶板块' }}</el-dropdown-item><el-dropdown-item command="pause" :disabled="activeGroup.archived">{{ activeGroup.enabled ? '暂停新入选' : '恢复自动选股' }}</el-dropdown-item><el-dropdown-item command="archive" divided :disabled="activeGroup.archived">归档板块</el-dropdown-item></el-dropdown-menu></template></el-dropdown></div>
            <p v-if="activeGroup.description" class="description">{{ activeGroup.description }}</p>
            <div class="overview-meta"><span>{{ activeGroup.folder || '未分类' }}</span><span>{{ activeGroup.universe === 'all_a' ? '全沪深 A 股' : '今日触板及昨日涨停' }}</span><span>{{ activeGroup.membership_mode === 'retain' ? '持续跟踪' : '动态进出' }}</span><span>规则 v{{ activeGroup.version }}</span></div>
            <p class="rule-summary">{{ ruleSummary(activeGroup.rules, fields, activeGroup.match_mode) }}</p>
          </section>
          <div class="detail-toolbar"><el-radio-group v-model="mode" @change="changeMode"><el-radio-button value="current">当前成分</el-radio-button><el-radio-button value="history">历史入选</el-radio-button></el-radio-group><div class="update-info"><span class="data-state">{{ statusLabel(current?.status || activeGroup.data_status) }}</span><span>{{ current?.source_time ? `行情 ${dateTime(current.source_time)}` : '等待有效行情' }}</span><el-button text type="primary" :loading="refreshing" :disabled="activeGroup.archived" @click="requestRefresh">更新</el-button></div></div>
          <p v-if="detailError" class="error" role="alert">{{ detailError }} <el-button link @click="reloadSelected">重试</el-button></p>
          <section v-if="mode === 'history'" class="panel history-picker">
            <div class="picker-field"><label for="archive-date">交易日</label><el-select id="archive-date" v-model="selectedDate" :loading="datesLoading" placeholder="选择已有档案日期" @change="changeDate"><el-option v-for="date in dates" :key="date" :value="date" :label="date" /></el-select></div>
            <div class="picker-field batch-picker"><label for="archive-batch">入选批次</label><el-select id="archive-batch" v-model="selectedBatchId" :loading="selectionsLoading" placeholder="当日最后一批" @change="changeBatch"><el-option v-for="batch in selections" :key="batch.id" :value="batch.id" :label="`${dateTime(batch.selected_at)} · ${batch.member_count}只 · v${batch.version}${batch.kind === 'close' ? ' · 收盘' : ''}`" /><template v-if="selections.length < selectionTotal" #footer><el-button text type="primary" :loading="selectionsLoading" @click="loadSelections(true)">加载更早批次（{{ selections.length }}/{{ selectionTotal }}）</el-button></template></el-select></div>
            <p v-if="!datesLoading && !dates.length" class="hint">尚无入选档案。首次有效选股后，交易日和批次会保存在这里。</p>
            <p v-else class="hint">名单与条件取自当时存档。盘中退出的股票仍可在原批次查看。</p>
          </section>
          <div class="stats-grid" v-loading="detailLoading">
            <div class="stat"><span>{{ mode === 'history' ? '本批成分' : '当前成分' }}</span><strong>{{ stockCount }}<small>只</small></strong></div>
            <div class="stat"><span>{{ mode === 'history' ? '入选时平均涨幅' : '平均当日涨幅' }}<small v-if="validChangeCount < stockCount">（{{ validChangeCount }}/{{ stockCount }}只）</small></span><strong :class="tone(averageChange)">{{ formatValue(averageChange) }}<small>%</small></strong></div>
            <div class="stat"><span>{{ mode === 'history' ? '入选时涨 / 跌' : '上涨 / 下跌' }}</span><strong><b class="up">{{ rising }}</b><small>/</small><b class="down">{{ falling }}</b></strong></div>
            <div class="stat"><span>{{ mode === 'history' ? '批次形成时间' : '最近选股' }}</span><strong class="time-value">{{ dateTime(mode === 'history' ? selectedBatch?.selected_at : activeGroup.last_evaluated_at) }}</strong></div>
          </div>
          <section v-if="dataWarnings.length" class="panel data-notes"><el-collapse><el-collapse-item :title="`数据说明 · 来源与覆盖（${dataWarnings.length}项）`" name="sources"><ul><li v-for="warning in dataWarnings" :key="warning">{{ warning }}</li></ul></el-collapse-item></el-collapse></section>
          <section class="panel trend-panel" v-loading="batchLoading">
            <div class="section-heading"><div><h3>固定成分历史指数</h3><p>按本次成分回溯 <span v-if="selectedBatch">· {{ dateTime(selectedBatch.selected_at) }} · {{ selectedBatch.member_count }} 只</span></p></div><el-tag :type="trend?.status === 'ready' || trend?.status === 'complete' ? 'success' : 'info'" size="small">{{ statusLabel(trend?.status || (selectedBatchId ? 'pending' : 'empty')) }}</el-tag></div>
            <div class="chart-toolbar"><el-radio-group v-model="windowDays" size="small"><el-radio-button :value="30">近一月</el-radio-button><el-radio-button :value="90">近三月</el-radio-button><el-radio-button :value="180">近六月</el-radio-button><el-radio-button :value="365">近一年</el-radio-button><el-radio-button :value="0">全部</el-radio-button></el-radio-group><span v-if="trend?.expected_count" class="hint">行情覆盖 {{ trend.covered_count }}/{{ trend.expected_count }} 只</span></div>
            <p v-if="trendError" class="error" role="alert">{{ trendError }} <el-button link @click="reloadTrend">重试</el-button></p>
            <DynamicGroupChart :key="`${selectedId}-${selectedBatchId}`" :points="trend?.points || []" :days="windowDays" :color="activeGroup.color || '#409eff'" label="固定成分历史指数" :empty-text="trend?.status === 'building' || trend?.status === 'pending' ? '历史行情正在补充，完成后自动显示' : '本批暂无可验证的历史走势'" />
            <div class="chart-notes"><p>同一批股票，期初等额配置并固定股数持有；共同基准日 {{ trend?.base_date || '待确认' }} = 1000 点。切换时间范围只缩放图表。</p><p>回溯可早于板块创建日；缺失行情显示缺口。历史档案记录系统实际入选结果。</p><p v-if="trend?.updated_at">行情来源：{{ trend.source || '腾讯复权日线' }} · 更新 {{ dateTime(trend.updated_at) }}</p><p v-for="warning in trend?.warnings || []" :key="warning" class="warning-text">{{ warning }}</p></div>
          </section>
          <section v-if="mode === 'current'" class="panel intraday-panel"><el-collapse v-model="openPanels"><el-collapse-item title="当前板块盘中连续走势" name="intraday"><p class="hint">{{ intraday?.trade_date || current?.trade_date || '—' }} · 每日从 1000 点起算，缺口后新段另起；随真实成分调整，与固定成分回溯分别展示。</p><DynamicGroupChart :points="intradayPoints" label="盘中连续指数" :intraday="true" :height="220" :color="activeGroup.color || '#409eff'" /><p class="hint">{{ statusLabel(intraday?.status) }} · 未采集时段保留缺口</p></el-collapse-item></el-collapse></section>
          <section class="panel members-panel" v-loading="batchLoading || detailLoading || memberLoading">
            <div class="section-heading"><div><h3>{{ mode === 'history' ? '本批入选档案' : '当前成分股' }} <span>{{ memberTotal }}{{ memberSearch ? ' 匹配' : '' }}</span></h3><p>{{ mode === 'history' ? `规则 v${selectedBatch?.version || '—'} · 入选时指标` : `${current?.trade_date || '—'} · 最新有效行情` }}</p></div><el-input v-model="memberSearch" clearable placeholder="搜索名称 / 代码" aria-label="搜索成分股" maxlength="100" class="member-search" /></div>
            <p v-if="mode === 'history' && selectedBatch?.definition" class="archive-rules">当时条件：{{ ruleSummary(selectedBatch.definition.rules, fields, selectedBatch.definition.match_mode) }}</p>
            <div class="desktop-members"><el-table :data="pagedMembers" stripe style="width:100%" @row-click="openStock"><el-table-column prop="stock_name" label="股票" min-width="100" fixed><template #default="{ row }"><span class="stock-link">{{ row.stock_name || row.stock_code }}</span><small class="stock-code">{{ row.stock_code }}</small></template></el-table-column><el-table-column label="价格" min-width="82"><template #default="{ row }">{{ formatValue(row.current_price ?? row.price) }}</template></el-table-column><el-table-column label="涨幅 %" min-width="90"><template #default="{ row }"><span :class="tone(row.change_pct)">{{ formatValue(row.change_pct) }}</span></template></el-table-column><el-table-column label="今日竞价 %" min-width="105"><template #default="{ row }">{{ formatValue(row.today_open_change_pct) }}</template></el-table-column><el-table-column label="换手 %" min-width="85"><template #default="{ row }">{{ formatValue(row.turnover_rate) }}</template></el-table-column><el-table-column label="成交额 / 亿" min-width="110"><template #default="{ row }">{{ formatValue(row.amount_yi) }}</template></el-table-column><el-table-column label="连板 / 昨日" min-width="102"><template #default="{ row }">{{ formatValue(row.continuous_limit_up_days, 0) }} / {{ formatValue(row.yesterday_continuous_days, 0) }}</template></el-table-column><el-table-column label="昨日首封 / 终封" min-width="150"><template #default="{ row }">{{ row.yesterday_first_limit_up_time || '—' }} / {{ row.yesterday_final_seal_time || '—' }}</template></el-table-column><el-table-column label="入选时间" min-width="144"><template #default="{ row }">{{ dateTime(row.selected_at || row.first_selected_at) }}</template></el-table-column><el-table-column v-if="mode === 'current' && !activeGroup.archived" label="操作" width="70"><template #default="{ row }"><el-button link type="danger" @click.stop="removeMember(row)">移出</el-button></template></el-table-column><template #empty>暂无符合条件的成分股；数据未就绪时会继续等待更新。</template></el-table></div>
            <div class="mobile-members"><article v-for="stock in pagedMembers" :key="stock.stock_code" class="stock-card"><button type="button" class="stock-card-heading" @click="openStock(stock)"><span><strong>{{ stock.stock_name || stock.stock_code }}</strong><small>{{ stock.stock_code }}</small></span><span :class="tone(stock.change_pct)">{{ formatValue(stock.change_pct) }}%</span></button><dl><div><dt>价格</dt><dd>{{ formatValue(stock.current_price ?? stock.price) }}</dd></div><div><dt>今日竞价</dt><dd>{{ formatValue(stock.today_open_change_pct) }}%</dd></div><div><dt>换手</dt><dd>{{ formatValue(stock.turnover_rate) }}%</dd></div><div><dt>成交额</dt><dd>{{ formatValue(stock.amount_yi) }} 亿</dd></div><div><dt>连板 / 昨日</dt><dd>{{ formatValue(stock.continuous_limit_up_days, 0) }} / {{ formatValue(stock.yesterday_continuous_days, 0) }}</dd></div><div><dt>昨日首封 / 终封</dt><dd>{{ stock.yesterday_first_limit_up_time || '—' }} / {{ stock.yesterday_final_seal_time || '—' }}</dd></div></dl><footer><time>入选 {{ dateTime(stock.selected_at || stock.first_selected_at) }}</time><el-button v-if="mode === 'current' && !activeGroup.archived" link type="danger" @click="removeMember(stock)">移出</el-button></footer></article><el-empty v-if="!pagedMembers.length" :image-size="64" description="暂无成分股" /></div>
            <el-pagination v-if="memberTotal > 50" size="small" background layout="prev, pager, next, total" :pager-count="5" :current-page="memberPage" :page-size="50" :total="memberTotal" @current-change="changeMemberPage" />
          </section>
        </template>
        <section v-else class="panel welcome" v-loading="listLoading"><div class="welcome-icon">◈</div><h2>把选股条件变成持续观察的板块</h2><p>创建板块后，系统会自动选股并保留每次名单。固定成分曲线可立即回溯这些股票的历史走势。</p><el-button type="primary" @click="openEditor()">创建动态板块</el-button><div class="welcome-templates"><button v-for="template in welcomeTemplates" :key="template.name" type="button" @click="openEditor(null, template.name)"><strong>{{ template.name }}</strong><span>{{ template.description || '从条件模板开始创建' }} →</span></button></div></section>
      </main>
    </div>
    <el-drawer v-model="directoryOpen" title="板块目录" direction="ltr" size="min(390px, 94vw)"><DynamicGroupDirectory :groups="groups" :total="total" :folders="folders" :fields="fields" :query="query" :selected-id="selectedId" :loading="listLoading" :error="listError" @query="updateQuery" @select="selectGroup" @create="openEditor()" @retry="loadDirectory" /></el-drawer>
    <DynamicGroupEditor v-model="editorOpen" :group="editingGroup" :fields="fields" :templates="templates" :folders="folders" :initial-template-name="initialTemplateName" @saved="onSaved" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import DynamicGroupDirectory from '@/components/dynamic-groups/DynamicGroupDirectory.vue'
import type { DirectoryQuery } from '@/components/dynamic-groups/DynamicGroupDirectory.vue'
import DynamicGroupEditor from '@/components/dynamic-groups/DynamicGroupEditor.vue'
import DynamicGroupChart from '@/components/dynamic-groups/DynamicGroupChart.vue'
import * as api from '@/api/dynamic-groups'
import type { DynamicCurrent, DynamicField, DynamicGroup, DynamicIntraday, DynamicMember, DynamicSelection, DynamicSelectionDetail, DynamicTemplate, DynamicTrend } from '@/types/dynamic-groups'
import { createRequestGate, formatValue, intradayChartPoints, ruleSummary, statusLabel } from '@/utils/dynamicGroups'

const router = useRouter()
const groups = ref<DynamicGroup[]>([]), total = ref(0), folders = ref<string[]>([]), fields = ref<DynamicField[]>([]), templates = ref<DynamicTemplate[]>([])
const query = ref<DirectoryQuery>({ q: '', folder: '', state: 'all', sort: 'pinned', page: 1 })
const selectedId = ref<number | null>(null), groupSnapshot = ref<DynamicGroup | null>(null), current = ref<DynamicCurrent | null>(null)
const activeGroup = computed(() => current.value?.group?.id === selectedId.value ? current.value.group : groupSnapshot.value)
const mode = ref<'current' | 'history'>('current'), dates = ref<string[]>([]), selectedDate = ref(''), selections = ref<DynamicSelection[]>([]), selectionTotal = ref(0), selectedBatchId = ref<number | null>(null)
const selectedBatch = ref<DynamicSelectionDetail | null>(null), trend = ref<DynamicTrend | null>(null), intraday = ref<DynamicIntraday | null>(null), windowDays = ref(365), openPanels = ref<string[]>([])
const listLoading = ref(false), detailLoading = ref(false), batchLoading = ref(false), datesLoading = ref(false), selectionsLoading = ref(false), actionLoading = ref(false), refreshing = ref(false)
const listError = ref(''), detailError = ref(''), trendError = ref(''), fieldsError = ref('')
const directoryOpen = ref(false), editorOpen = ref(false), editingGroup = ref<DynamicGroup | null>(null)
const currentMemberSearch = ref(''), historyMemberSearch = ref(''), currentMemberPage = ref(1), historyMemberPage = ref(1), initialTemplateName = ref('')
const memberSearch = computed({ get: () => mode.value === 'history' ? historyMemberSearch.value : currentMemberSearch.value, set: value => { if (mode.value === 'history') historyMemberSearch.value = value; else currentMemberSearch.value = value } })
const memberPage = computed({ get: () => mode.value === 'history' ? historyMemberPage.value : currentMemberPage.value, set: value => { if (mode.value === 'history') historyMemberPage.value = value; else currentMemberPage.value = value } })
const memberLoading = ref(false)
let resettingMembers = false
const listGate = createRequestGate(), groupGate = createRequestGate(), currentGate = createRequestGate(), selectionGate = createRequestGate(), batchGate = createRequestGate(), trendGate = createRequestGate(), memberGate = createRequestGate()
let searchTimer: ReturnType<typeof setTimeout> | undefined
let memberSearchTimer: ReturnType<typeof setTimeout> | undefined
let refreshTimer: ReturnType<typeof setInterval> | undefined
let polling = false, disposed = false
const visibleMembers = computed(() => mode.value === 'history' ? selectedBatch.value?.items || [] : current.value?.items || [])
const pagedMembers = visibleMembers
const memberTotal = computed(() => mode.value === 'history' ? selectedBatch.value?.total || 0 : current.value?.total || 0)
const memberSummary = computed(() => mode.value === 'history' ? selectedBatch.value?.summary : current.value?.summary)
const stockCount = computed(() => memberSummary.value?.stock_count ?? (mode.value === 'history' ? selectedBatch.value?.member_count : activeGroup.value?.member_count) ?? 0)
const validChangeCount = computed(() => memberSummary.value?.valid_change_count ?? 0)
const averageChange = computed(() => memberSummary.value?.average_change_pct ?? null)
const rising = computed(() => memberSummary.value?.up_count ?? 0), falling = computed(() => memberSummary.value?.down_count ?? 0)
const dataWarnings = computed(() => [...new Set(mode.value === 'history' ? selectedBatch.value?.warnings || [] : current.value?.warnings || [])])
const intradayPoints = computed(() => intradayChartPoints(intraday.value?.points || []))
const welcomeTemplates = computed(() => templates.value.length ? templates.value.slice(0, 3) : [{ name: '二板竞价', description: '昨日首板，观察今日竞价' }, { name: '昨日涨停', description: '持续跟踪昨日涨停股' }, { name: '高换手活跃', description: '关注量能与交易活跃度' }])
watch(currentMemberSearch, () => scheduleMemberSearch('current'), { flush: 'sync' })
watch(historyMemberSearch, () => scheduleMemberSearch('history'), { flush: 'sync' })
function memberQuery(target: 'current' | 'history') {
  return { offset: ((target === 'current' ? currentMemberPage.value : historyMemberPage.value) - 1) * 50, limit: 50, q: (target === 'current' ? currentMemberSearch.value : historyMemberSearch.value).trim() }
}
function scheduleMemberSearch(target: 'current' | 'history') {
  if (target === 'current') { currentMemberPage.value = 1; currentGate.invalidate() }
  else { historyMemberPage.value = 1; memberGate.invalidate() }
  clearTimeout(memberSearchTimer)
  if (resettingMembers || target !== mode.value || !selectedId.value) return
  memberLoading.value = true
  memberSearchTimer = setTimeout(() => { void loadMemberPage() }, 300)
}
async function changeMemberPage(page: number) {
  clearTimeout(memberSearchTimer); memberPage.value = page
  await loadMemberPage()
}
async function loadMemberPage() {
  const id = selectedId.value
  if (!id || disposed) return
  memberLoading.value = true
  if (mode.value === 'current') { await loadCurrent(id, false); return }
  const selectionId = selectedBatchId.value
  if (!selectionId) { memberLoading.value = false; return }
  const request = memberGate.begin()
  try {
    const data = await api.getDynamicSelection(id, selectionId, memberQuery('history'))
    if (memberGate.isCurrent(request) && id === selectedId.value && mode.value === 'history' && selectionId === selectedBatchId.value) { selectedBatch.value = data; detailError.value = '' }
  } catch (e) { if (memberGate.isCurrent(request)) detailError.value = errorText(e, '成分股加载失败，请重试') }
  finally { if (memberGate.isCurrent(request)) memberLoading.value = false }
}
function dateTime(value?: string | null) { return value ? value.replace('T', ' ').slice(0, 16) : '待更新' }
function tone(value: unknown) { return typeof value === 'number' ? value > 0 ? 'up' : value < 0 ? 'down' : '' : '' }
function errorText(e: unknown, fallback: string) { const detail = (e as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail; return typeof detail === 'string' ? detail : fallback }

async function loadFields() {
  try { const data = await api.getDynamicFields(); if (disposed) return; fields.value = data.fields; templates.value = data.templates || []; fieldsError.value = '' }
  catch { if (!disposed) fieldsError.value = '条件列表暂时无法加载，请重试。' }
}
async function loadDirectory() {
  const request = listGate.begin(); listLoading.value = true; listError.value = ''
  try {
    const data = await api.getDynamicGroups({ ...query.value, offset: (query.value.page - 1) * 20, limit: 20 })
    if (!listGate.isCurrent(request)) return
    groups.value = data.items; total.value = data.total; folders.value = data.folders
    if (selectedId.value === null && data.items.length) await selectGroup(data.items[0])
  } catch (e) { if (listGate.isCurrent(request)) listError.value = errorText(e, '板块目录加载失败') }
  finally { if (listGate.isCurrent(request)) listLoading.value = false }
}
function updateQuery(value: DirectoryQuery) { query.value = value; listGate.invalidate(); clearTimeout(searchTimer); searchTimer = setTimeout(loadDirectory, 300) }
async function selectGroup(group: DynamicGroup) {
  directoryOpen.value = false
  const request = groupGate.begin(); currentGate.invalidate(); selectionGate.invalidate(); batchGate.invalidate(); trendGate.invalidate(); memberGate.invalidate(); clearTimeout(memberSearchTimer); resettingMembers = true; currentMemberSearch.value = ''; historyMemberSearch.value = ''; currentMemberPage.value = 1; historyMemberPage.value = 1; memberLoading.value = false; resettingMembers = false
  selectedId.value = group.id; groupSnapshot.value = group; current.value = null; trend.value = null; intraday.value = null; dates.value = []; selections.value = []; selectedBatch.value = null; selectedBatchId.value = null; selectedDate.value = ''; mode.value = 'current'; memberSearch.value = ''; memberPage.value = 1; detailError.value = ''; trendError.value = ''; batchLoading.value = false; selectionsLoading.value = false; datesLoading.value = true
  const results = await Promise.allSettled([loadCurrent(group.id), api.getDynamicDates(group.id)])
  if (!groupGate.isCurrent(request)) return
  if (results[1].status === 'fulfilled') dates.value = results[1].value.dates.slice().sort().reverse()
  else detailError.value = '入选日期加载失败，请重试。'
  datesLoading.value = false
}
async function loadCurrent(id: number, updateChart = true) {
  const request = currentGate.begin(); detailLoading.value = !current.value
  try {
    const [data, day] = await Promise.all([api.getDynamicCurrent(id, memberQuery('current')), updateChart ? api.getDynamicIntraday(id).catch(() => null) : Promise.resolve(intraday.value)])
    if (!currentGate.isCurrent(request) || id !== selectedId.value) return
    const lastPage = Math.max(1, Math.ceil(data.total / 50))
    if (currentMemberPage.value > lastPage) { currentMemberPage.value = lastPage; await loadCurrent(id, updateChart); return }
    current.value = data; intraday.value = day; groupSnapshot.value = data.group; if (mode.value === 'current') detailError.value = ''
    if (mode.value === 'current') {
      const latestId = data.selection_id ?? data.group.latest_selection_id
      if (latestId !== selectedBatchId.value) { selectedBatchId.value = latestId; await loadBatch(id, latestId) }
      else if (updateChart && latestId) await reloadTrend()
    }
  } catch (e) { if (currentGate.isCurrent(request) && id === selectedId.value) detailError.value = errorText(e, '当前成分加载失败') }
  finally { if (currentGate.isCurrent(request)) { detailLoading.value = false; if (mode.value === 'current') memberLoading.value = false } }
}
async function changeMode() {
  selectionGate.invalidate(); batchGate.invalidate(); trendGate.invalidate(); memberGate.invalidate(); clearTimeout(memberSearchTimer); memberLoading.value = false; selectedBatch.value = null; trend.value = null; trendError.value = ''; detailError.value = ''; batchLoading.value = false; memberPage.value = 1
  if (!selectedId.value) return
  if (mode.value === 'history') { selectedDate.value = selectedDate.value || dates.value[0] || ''; selectedBatchId.value = null; if (selectedDate.value) await loadSelections() }
  else { selectedBatchId.value = current.value?.selection_id ?? activeGroup.value?.latest_selection_id ?? null; await Promise.all([loadCurrent(selectedId.value, false), loadBatch(selectedId.value, selectedBatchId.value)]) }
}
async function changeDate() { memberGate.invalidate(); clearTimeout(memberSearchTimer); memberLoading.value = false; historyMemberPage.value = 1; selectedBatchId.value = null; selectedBatch.value = null; trend.value = null; batchGate.invalidate(); trendGate.invalidate(); await loadSelections() }
async function loadSelections(append = false) {
  const id = selectedId.value, date = selectedDate.value
  if (!id || !date) return
  const request = selectionGate.begin(); selectionsLoading.value = true
  if (!append) { selections.value = []; selectionTotal.value = 0; detailError.value = '' }
  try {
    const data = await api.getDynamicSelections(id, date, append ? selections.value.length : 0)
    if (!selectionGate.isCurrent(request) || mode.value !== 'history' || id !== selectedId.value || date !== selectedDate.value) return
    selections.value = append ? [...selections.value, ...data.items] : data.items; selectionTotal.value = data.total
    if (!append) { selectedBatchId.value = data.items[0]?.id ?? null; await loadBatch(id, selectedBatchId.value) }
  } catch (e) { if (selectionGate.isCurrent(request)) detailError.value = errorText(e, '入选批次加载失败') }
  finally { if (selectionGate.isCurrent(request)) selectionsLoading.value = false }
}
async function changeBatch() { historyMemberPage.value = 1; if (selectedId.value) await loadBatch(selectedId.value, selectedBatchId.value) }
async function loadBatch(id: number, selectionId: number | null) {
  const request = batchGate.begin(); const memberRequest = memberGate.begin(); clearTimeout(memberSearchTimer); trendGate.invalidate(); trend.value = null; selectedBatch.value = null; trendError.value = ''; batchLoading.value = false
  if (!selectionId) return
  batchLoading.value = true
  const results = await Promise.allSettled([api.getDynamicSelection(id, selectionId, mode.value === 'history' ? memberQuery('history') : { offset: 0, limit: 1, q: '' }), api.getDynamicTrend(id, selectionId)])
  if (!batchGate.isCurrent(request) || id !== selectedId.value || selectionId !== selectedBatchId.value) return
  if (results[0].status === 'fulfilled' && memberGate.isCurrent(memberRequest)) selectedBatch.value = results[0].value
  else if (results[0].status === 'rejected' && memberGate.isCurrent(memberRequest)) detailError.value = errorText(results[0].reason, '入选名单加载失败')
  if (results[1].status === 'fulfilled') trend.value = results[1].value
  else trendError.value = errorText(results[1].reason, '历史走势加载失败')
  batchLoading.value = false
}
async function reloadTrend() {
  const id = selectedId.value, batch = selectedBatchId.value
  if (!id || !batch || batchLoading.value) return
  const request = trendGate.begin()
  try { const data = await api.getDynamicTrend(id, batch); if (trendGate.isCurrent(request) && id === selectedId.value && batch === selectedBatchId.value) { trend.value = data; trendError.value = '' } }
  catch (e) { if (trendGate.isCurrent(request)) trendError.value = errorText(e, '历史走势暂时无法更新，已保留上次结果') }
}
async function reloadSelected() { if (mode.value === 'history') { if (selectedBatchId.value && selectedId.value) await loadBatch(selectedId.value, selectedBatchId.value); else if (selectedDate.value) await loadSelections(); else if (selectedId.value) { const data = await api.getDynamicDates(selectedId.value).catch(() => null); if (data) { dates.value = data.dates.slice().sort().reverse(); selectedDate.value = dates.value[0] || ''; if (selectedDate.value) await loadSelections() } } } else if (selectedId.value) await loadCurrent(selectedId.value) }
async function requestRefresh() {
  if (!selectedId.value || refreshing.value) return
  const id = selectedId.value; refreshing.value = true
  try { await api.refreshDynamicGroup(id); ElMessage.success('已安排更新，结果就绪后自动显示'); if (id === selectedId.value) await loadCurrent(id) }
  catch (e) { ElMessage.error(errorText(e, '无法安排更新，请重试')) }
  finally { refreshing.value = false }
}
function openEditor(group: DynamicGroup | null = null, templateName = '') { editingGroup.value = group; initialTemplateName.value = templateName; directoryOpen.value = false; editorOpen.value = true }
async function onSaved(group: DynamicGroup) { await selectGroup(group); await loadDirectory(); ElMessage.success('已保存，正在更新选股结果') }
async function groupAction(command: string) {
  const group = activeGroup.value; if (!group) return
  if (command === 'edit') { openEditor(group); return }
  try {
    if (command === 'archive') await ElMessageBox.confirm('归档后停止新入选，历史名单与走势继续保留。', `归档“${group.name}”？`, { confirmButtonText: '归档', cancelButtonText: '取消', type: 'warning' })
    actionLoading.value = true
    if (command === 'archive') { await api.archiveDynamicGroup(group.id); await selectGroup({ ...group, archived: true, enabled: false }) }
    else { const updated = await api.updateDynamicGroup(group.id, { ...group, ...(command === 'pin' ? { pinned: !group.pinned } : { enabled: !group.enabled }) }); if (selectedId.value === group.id) { groupSnapshot.value = updated; if (current.value) current.value.group = updated } }
    await loadDirectory()
  } catch (e) { if (e !== 'cancel' && e !== 'close') ElMessage.error(errorText(e, '操作失败，请重试')) }
  finally { actionLoading.value = false }
}
function openStock(stock: DynamicMember) { router.push(`/stock/${encodeURIComponent(stock.stock_code)}`) }
async function removeMember(stock: DynamicMember) {
  const id = selectedId.value; if (!id) return
  try { await ElMessageBox.confirm('移出当前名单后，原入选档案仍保留；下次重新满足条件时可能再次入选。', `移出 ${stock.stock_name || stock.stock_code}？`, { confirmButtonText: '移出', cancelButtonText: '取消' }); await api.removeDynamicMember(id, stock.stock_code); if (selectedId.value === id) await loadCurrent(id); await loadDirectory() }
  catch (e) { if (e !== 'cancel' && e !== 'close') ElMessage.error(errorText(e, '移出失败，请重试')) }
}
async function poll() {
  if (polling || disposed || document.hidden || !selectedId.value) return
  polling = true
  const id = selectedId.value
  try {
    await loadCurrent(id)
    if (id !== selectedId.value || disposed) return
    if (mode.value === 'history') await reloadTrend()
    const result = await api.getDynamicDates(id)
    if (id === selectedId.value && !disposed) dates.value = result.dates.slice().sort().reverse()
  } catch { /* Existing successful archives remain visible during a transient failure. */ }
  finally { polling = false }
}
onMounted(() => { void loadFields(); void loadDirectory(); refreshTimer = setInterval(poll, 30000) })
onBeforeUnmount(() => { disposed = true; clearTimeout(searchTimer); clearTimeout(memberSearchTimer); clearInterval(refreshTimer); [listGate, groupGate, currentGate, selectionGate, batchGate, trendGate, memberGate].forEach(gate => gate.invalidate()) })
</script>

<style scoped>
.dynamic-page{max-width:1680px;margin:0 auto;color:#26364b;min-width:0}.page-heading{display:flex;align-items:center;justify-content:space-between;gap:18px;margin:4px 0 22px}.eyebrow{font-size:11px;letter-spacing:1px;color:#8998ad;margin:0 0 6px}h1{font-size:26px;letter-spacing:-.5px;margin:0;font-weight:650}.intro{font-size:13px;color:#8290a4;margin:7px 0 0}.heading-actions{display:flex;gap:8px}.mobile-directory-button{display:none}.workspace{display:grid;grid-template-columns:300px minmax(0,1fr);gap:22px;align-items:start}.desktop-directory{padding:18px;background:#fafbfd;border:1px solid #e5ebf3;border-radius:14px;position:sticky;top:0;max-height:calc(100vh - 160px);overflow-y:auto}.detail{min-width:0;display:flex;flex-direction:column;gap:16px}.panel{background:white;border:1px solid #e7ecf3;border-radius:12px;padding:22px;min-width:0}.group-overview{border-top:3px solid #d8e8ff}.overview-heading,.group-name,.detail-toolbar,.update-info,.section-heading,.chart-toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px}.group-name{justify-content:flex-start;min-width:0;flex-wrap:wrap}.group-name h2{font-size:21px;margin:0;overflow-wrap:anywhere}.group-name i{width:10px;height:10px;border-radius:50%;flex-shrink:0}.description{font-size:13px;color:#68788f;line-height:1.7;margin:14px 0 0;overflow-wrap:anywhere}.overview-meta{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:#8d9bad;margin-top:14px}.rule-summary{font-size:12px;color:#62758f;line-height:1.8;background:#f5f8fc;border-radius:6px;padding:10px 12px;margin:14px 0 0;overflow-wrap:anywhere}.detail-toolbar{padding:1px 0}.update-info{font-size:11px;color:#8b98a9;gap:6px}.history-picker{display:flex;flex-wrap:wrap;gap:16px;padding:18px 22px}.picker-field{display:flex;flex-direction:column;gap:8px;min-width:160px;flex:1}.picker-field label{font-size:12px;color:#7f8da1}.batch-picker{flex:2;min-width:240px}.history-picker>.hint{width:100%;margin:0}.hint{font-size:12px;color:#8a98ab;line-height:1.7;margin:4px 0}.stats-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.stat{background:#fff;border:1px solid #e7ecf3;border-radius:10px;padding:17px 18px;display:flex;flex-direction:column;gap:12px}.stat>span{font-size:12px;color:#8a97a9}.stat>strong{font-size:25px;letter-spacing:-.5px;font-weight:600;min-width:0}.stat strong small{font-size:12px;font-weight:400;color:#9aa6b7;margin:0 7px}.stat strong b{font-weight:600}.stat .time-value{font-size:14px;letter-spacing:0;line-height:30px}.up{color:#e65965}.down{color:#21a27b}.section-heading{margin-bottom:16px}.section-heading h3{font-size:16px;font-weight:600;margin:0}.section-heading h3>span{font-size:12px;color:#94a1b3;font-weight:400;margin-left:5px}.section-heading p{font-size:11px;color:#94a1b3;margin:8px 0 0}.chart-toolbar{flex-wrap:wrap;gap:10px}.chart-notes{border-top:1px solid #edf1f7;padding-top:10px}.chart-notes p{font-size:11px;color:#8e9bb0;line-height:1.8;margin:3px 0}.chart-notes .warning-text{color:#b18a43}.intraday-panel{padding-top:4px;padding-bottom:4px}.intraday-panel :deep(.el-collapse){border:0}.intraday-panel :deep(.el-collapse-item__header){border:0;color:#64758c;font-size:13px}.intraday-panel :deep(.el-collapse-item__wrap){border:0}.data-notes{padding:0 20px;background:#fffdf7;border-color:#f0e6d0}.data-notes :deep(.el-collapse){border:0}.data-notes :deep(.el-collapse-item__header){color:#ac8a46;background:transparent;font-size:12px;border:0}.data-notes :deep(.el-collapse-item__wrap){border:0;background:transparent}.data-notes ul{padding-left:16px;margin:0;color:#9c8c6b;font-size:12px;line-height:1.9}.member-search{max-width:210px}.stock-link{color:#367dc5;cursor:pointer}.stock-code{display:block;font-size:10px;color:#9aa7b7}.archive-rules{font-size:12px;color:#8290a4;line-height:1.7;margin-bottom:16px}.desktop-members{width:100%;overflow-x:auto}.mobile-members{display:none}.members-panel>.el-pagination{margin-top:16px;justify-content:center}.error{font-size:13px;color:#c75d5d;margin:0;line-height:1.7}.welcome{min-height:480px;text-align:center;display:flex;align-items:center;justify-content:center;flex-direction:column;padding:40px 30px}.welcome-icon{font-size:56px;line-height:1;color:#88b7f7;background:#f1f7ff;border-radius:20px;padding:16px 23px;margin-bottom:22px}.welcome h2{font-size:21px;font-weight:600;line-height:1.5}.welcome>p{color:#8b99ac;font-size:13px;line-height:1.9;max-width:430px;margin:0 0 24px}.welcome-templates{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));width:100%;gap:12px;margin-top:36px}.welcome-templates button{background:#fafbfd;border:1px solid #e7edf5;border-radius:8px;padding:16px 10px;cursor:pointer;text-align:left;font:inherit}.welcome-templates strong{display:block;font-size:13px;color:#5e7698}.welcome-templates span{display:block;font-size:11px;color:#96a3b6;margin-top:8px;line-height:1.7}.dynamic-page>.el-alert{margin-bottom:12px}.detail>.el-alert{flex-shrink:0}@media(max-width:1200px){.workspace{grid-template-columns:260px minmax(0,1fr);gap:16px}.desktop-directory{padding:14px}.panel{padding:18px}.stat{padding:15px 12px}.stat>strong{font-size:22px}.stat .time-value{font-size:12px}.overview-heading{align-items:flex-start}.group-name h2{font-size:19px}.chart-toolbar :deep(.el-radio-button__inner){padding:7px 10px}}@media(max-width:1000px){.workspace{grid-template-columns:minmax(0,1fr)}.desktop-directory{display:none}.mobile-directory-button{display:inline-flex}.count{margin-left:6px;background:#edf4fe;padding:1px 6px;border-radius:5px;color:#5e85bd}.page-heading{margin-bottom:16px}.stats-grid{gap:10px}.welcome{min-height:420px}}@media(max-width:600px){.page-heading{align-items:flex-start;gap:10px;margin:4px 0 16px}.eyebrow{font-size:10px;letter-spacing:0}.page-heading h1{font-size:23px}.intro{font-size:11px;line-height:1.6;max-width:160px}.heading-actions{flex-direction:column;gap:6px}.heading-actions .el-button{margin:0;font-size:12px;height:30px}.detail{gap:12px}.panel{border-radius:10px;padding:16px}.group-name h2{font-size:18px}.overview-heading>.el-dropdown{flex-shrink:0}.overview-heading>.el-dropdown :deep(.el-button){font-size:11px;padding:8px 9px}.overview-meta{gap:8px;font-size:11px}.rule-summary{padding:9px;font-size:11px}.detail-toolbar{flex-wrap:wrap;gap:8px}.update-info{margin-left:auto;font-size:10px}.stats-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.stat{padding:14px}.stat>strong{font-size:25px}.stat .time-value{font-size:13px}.picker-field,.batch-picker{min-width:0;flex-basis:100%}.history-picker{gap:12px}.section-heading{gap:10px;align-items:flex-start}.section-heading h3{font-size:15px}.section-heading p{line-height:1.6}.chart-toolbar :deep(.el-radio-button__inner){padding:7px 9px;font-size:11px}.chart-notes p{font-size:10px}.desktop-members{display:none}.mobile-members{display:flex;flex-direction:column;gap:10px}.stock-card{border:1px solid #e7edf5;border-radius:9px;padding:13px}.stock-card-heading{width:100%;background:none;border:0;padding:0;display:flex;align-items:center;justify-content:space-between;text-align:left;font:inherit;cursor:pointer}.stock-card-heading strong{font-size:14px;color:#3c608c}.stock-card-heading small{margin-left:7px;font-size:10px;color:#9aa7b7}.stock-card-heading>span:last-child{font-weight:600;font-size:17px}.stock-card dl{display:grid;grid-template-columns:1fr 1fr;gap:11px 14px;margin:16px 0 10px}.stock-card dl>div{display:flex;justify-content:space-between;gap:4px;align-items:baseline}.stock-card dl>div:last-child{grid-column:1/-1}.stock-card dt{font-size:10px;color:#98a4b6}.stock-card dd{font-size:11px;color:#65758c;margin:0}.stock-card footer{border-top:1px solid #f0f3f8;padding-top:8px;display:flex;justify-content:space-between;align-items:center;min-height:22px}.stock-card time{font-size:10px;color:#9aa6b7}.members-panel>.section-heading{flex-wrap:wrap}.member-search{max-width:none}.welcome{padding:30px 18px;min-height:460px}.welcome h2{font-size:19px}.welcome>p{font-size:12px}.welcome-templates{grid-template-columns:1fr;gap:8px;margin-top:25px}.welcome-templates button{padding:12px 14px;display:flex;align-items:center;justify-content:space-between;gap:10px}.welcome-templates span{margin:0}.welcome-icon{font-size:43px}.members-panel>.el-pagination{max-width:100%;overflow-x:auto}}
</style>
