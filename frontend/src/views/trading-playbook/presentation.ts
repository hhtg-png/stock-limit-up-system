import type {
  TradingAlertEvent,
  TradingExecutionReviewUpdate,
  TradingManualExecution,
  TradingPlanVersion,
  TradingPlaybookSettingsUpdate,
  TradingUnplannedExecution
} from '@/types/trading-playbook'

export type TradingAlertFilter = 'unread' | 'all'
export type CollectionState = 'loading' | 'error' | 'empty' | 'ready'

const FALLBACK_MODE_NAMES: Record<string, string> = {
  alive_theme_snake_arbitrage: '板块未死的蛇形套利',
  big_middle_army_transition: '大中军过渡套利',
  dead_pile_right_confirmation: '死人堆反转的右侧确认',
  external_high_low_switch: '题材外高低切',
  first_mover_leader: '分歧一致先于龙',
  leader_acceleration_to_divergence: '龙头加速转分歧',
  leader_first_bearish_rebound: '龙头首阴或双头预期',
  leader_stronger_confirmation: '龙头强更强确认',
  leader_turn_two: '龙头一转二',
  new_theme_high_position: '新题材高身位套利',
  new_theme_high_volatility: '新题材高波动套利',
  new_theme_same_level_turnover: '新题材同身位换手',
  resilient_core_exhaustion: '连续分歧后的抗跌核心',
  stage_three_high_low_switch: '三阶段高低切',
  stage_transition_supplement: '转点补涨',
  trend_consolidation_rebreak: '趋势横盘再突破',
  trend_core_pullback: '趋势核心回调',
  trend_turn_two: '趋势一转二',
  unique_survivor_trial: '唯一活口试错'
}

const RADAR_STATUS_LABELS: Record<string, string> = {
  matched: '已命中',
  waiting: '等待确认',
  manual_review: '人工复核',
  not_matched: '未命中'
}

const MARKET_STATE_LABELS: Record<string, Record<string, string>> = {
  style: {
    chaos_retreat: '混沌退潮',
    board_flow: '连板接力',
    trend_main_wave: '趋势主升',
    dual_active: '双主线活跃',
    unknown: '数据不足'
  },
  window: {
    decline: '退潮期',
    outbreak: '爆发期',
    divergence_exhaustion: '分歧衰竭',
    divergence_to_consensus: '分歧转一致',
    stronger_confirmation: '强更强确认',
    second_divergence: '二次分歧',
    first_divergence: '首次分歧',
    unknown: '数据不足'
  }
}

function displayText(value: unknown) {
  return typeof value === 'string' && value.trim() ? value.trim() : null
}

function nonNegativeCount(value: unknown) {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : 0
}

function radarSummary(row: Record<string, unknown>) {
  if (row.summary_counts && typeof row.summary_counts === 'object' && !Array.isArray(row.summary_counts)) {
    return row.summary_counts as Record<string, unknown>
  }
  if (!Array.isArray(row.evidence)) return null
  for (const item of row.evidence) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue
    const summary = (item as Record<string, unknown>).radar_summary
    if (summary && typeof summary === 'object' && !Array.isArray(summary)) {
      return summary as Record<string, unknown>
    }
  }
  return null
}

export function tradingModeLabel(
  row: Record<string, unknown>,
  ruleNames: Record<string, string> = {}
) {
  const key = displayText(row.mode_key)
  const explicit = displayText(row.mode_name)
  if (explicit && explicit !== key) return explicit
  return modeKeyLabel(key, ruleNames)
}

export function modeKeyLabel(
  value: unknown,
  ruleNames: Record<string, string> = {}
) {
  const key = displayText(value)
  if (!key) return '未标注模式'
  return ruleNames[key] || FALLBACK_MODE_NAMES[key] || '未命名模式'
}

export function radarStatusLabel(status: unknown) {
  const value = displayText(status)
  return value ? RADAR_STATUS_LABELS[value] || '状态未知' : '状态未知'
}

export function radarStatusType(status: unknown) {
  return ({
    matched: 'success',
    waiting: 'warning',
    manual_review: 'warning',
    not_matched: 'info'
  } as Record<string, 'success' | 'warning' | 'info'>)[displayText(status) || ''] || 'info'
}

export function radarCandidateLabel(row: Record<string, unknown>) {
  if (row.compacted === true || row.role === 'summary') return '全市场汇总'
  const name = displayText(row.stock_name)
  const code = displayText(row.stock_code)
  if (name && code) return `${name}（${code}）`
  return name || code || '暂无候选'
}

export function radarEvidenceSummary(row: Record<string, unknown>) {
  const summary = radarSummary(row)
  if (summary) {
    return [
      `扫描 ${nonNegativeCount(summary.scanned)} 只`,
      `命中 ${nonNegativeCount(summary.matched)}`,
      `等待确认 ${nonNegativeCount(summary.waiting)}`,
      `人工复核 ${nonNegativeCount(summary.manual_review)}`,
      `未命中 ${nonNegativeCount(summary.not_matched)}`
    ].join(' · ')
  }
  return ({
    matched: '触发条件已满足，具体行动条件见上方候选预案。',
    waiting: '尚缺确认条件，继续观察，不提前行动。',
    manual_review: '自动数据不足，需要人工复核后才能决定。',
    not_matched: '当前数据未满足该模式的触发条件。'
  } as Record<string, string>)[displayText(row.status) || ''] || '暂无可读证据。'
}

export function marketStateLabel(key: 'style' | 'window', value: unknown) {
  const state = displayText(value)
  return state ? MARKET_STATE_LABELS[key][state] || '未识别状态' : '-'
}

export interface ManualExecutionDraft {
  executed: boolean
  execution_price?: number
  quantity?: number
  executed_time?: string
  manual_note?: string
}

export interface UnplannedExecutionDraft {
  stock_code: string
  stock_name: string
  execution_price?: number
  quantity?: number
  executed_time?: string
  manual_note?: string
}

type PlanConfirmationState = Pick<TradingPlanVersion, 'status' | 'data_quality_json'>

function actionQualityReady(quality: unknown) {
  if (!quality || typeof quality !== 'object' || Array.isArray(quality)) return false
  const value = quality as Record<string, unknown>
  return (
    value.status === 'ready' &&
    (!Object.prototype.hasOwnProperty.call(value, 'stale') || value.stale === false)
  )
}

export function canEnableActionAlerts(plan: PlanConfirmationState | null | undefined) {
  return plan?.status === 'draft' && actionQualityReady(plan.data_quality_json)
}

export function isObservationOnly(plan: PlanConfirmationState | null | undefined) {
  return !plan || !actionQualityReady(plan.data_quality_json)
}

export function filterTradingAlerts<T extends Pick<TradingAlertEvent, 'acknowledged_at'>>(
  alerts: T[],
  filter: TradingAlertFilter
) {
  return filter === 'unread'
    ? alerts.filter(item => !item.acknowledged_at)
    : alerts
}

export function collectionState(
  loading: boolean,
  error: string | null | undefined,
  items: unknown[]
): CollectionState {
  if (loading) return 'loading'
  if (error) return 'error'
  if (items.length === 0) return 'empty'
  return 'ready'
}

export function chinaToday(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(now)
  const value = Object.fromEntries(parts.map(part => [part.type, part.value]))
  return `${value.year}-${value.month}-${value.day}`
}

export function formatChinaDateTime(value: string | null | undefined) {
  if (!value) return '-'
  const timestamp = Date.parse(value)
  if (Number.isNaN(timestamp)) return value
  return new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).format(timestamp)
}

export function riskPermissionSummary(settings: Record<string, unknown>) {
  const trial = settings.trial
  const confirmed = settings.confirmed
  const hardStop = settings.hard_stop
  const maximum = settings.max_candidates
  if (
    typeof trial !== 'number' || !Number.isFinite(trial) ||
    typeof confirmed !== 'number' || !Number.isFinite(confirmed) ||
    typeof hardStop !== 'number' || !Number.isFinite(hardStop) ||
    typeof maximum !== 'number' || !Number.isInteger(maximum)
  ) return '-'
  return `试错 ${trial}% · 确认上限 ${confirmed}% · 刚性止损 ${hardStop}% · 最多 ${maximum} 只`
}

function recordValue(value: unknown) {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null
}

function finiteNumber(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function formatPrice(value: unknown) {
  const number = finiteNumber(value)
  return number === null ? null : `${number.toFixed(2)} 元`
}

function formatPercent(value: unknown) {
  const number = finiteNumber(value)
  return number === null ? null : `${number}%`
}

export function roleLabel(value: unknown) {
  const role = displayText(value)
  return ({
    high_position: '高身位核心',
    high_volatility: '高波动核心',
    same_level_turnover: '同身位换手',
    leader: '龙头',
    core: '核心',
    middle_army: '中军',
    supplement: '补涨',
    low_position: '低位'
  } as Record<string, string>)[role || ''] || '待确认'
}

export function conditionSummary(value: unknown) {
  const condition = recordValue(value)
  if (!condition) return '暂无明确条件'

  const parts: string[] = []
  const label = displayText(condition.label)
  if (label) parts.push(label)

  const referencePrice = formatPrice(condition.reference_price)
  if (referencePrice) parts.push(`参考价 ${referencePrice}`)

  const priceUpper = formatPrice(condition.price_lte)
  if (priceUpper) parts.push(`价格不高于 ${priceUpper}`)

  const priceLower = formatPrice(condition.price_gte)
  if (priceLower) parts.push(`价格不低于 ${priceLower}`)

  const changeUpper = formatPercent(condition.change_pct_lte)
  if (changeUpper) parts.push(`涨跌幅不高于 ${changeUpper}`)

  const changeLower = formatPercent(condition.change_pct_gte)
  if (changeLower) parts.push(`涨跌幅不低于 ${changeLower}`)

  if (condition.sealed === true) parts.push('封板状态有效')
  if (condition.sealed === false) parts.push('无需封板')

  return parts.length ? parts.join('；') : '按系统条件观察'
}

export function candidateEvidenceSummary(value: unknown) {
  if (!Array.isArray(value) || value.length === 0) return '暂无可用证据'

  const sourceLabels: Record<string, string> = {
    tencent: '实时报价',
    full_market_quote_rank: '全市场排名',
    kline: '日线走势',
    realtime_limit_up_pool: '实时涨停池',
    market_review_stock_daily: '市场复盘',
    computed: '衍生指标'
  }
  const sources = new Set<string>()
  let requirementTotal = 0
  let requirementMatched = 0
  let hardStopPrice: number | null = null

  for (const raw of value) {
    const item = recordValue(raw)
    if (!item) continue
    const source = displayText(item.source)
    if (source && sourceLabels[source]) sources.add(sourceLabels[source])
    if (source === 'mode_requirement') {
      requirementTotal += 1
      if (item.result === 'matched') requirementMatched += 1
    }
    if (source === 'mode_risk') {
      hardStopPrice = finiteNumber(item.hard_stop_price)
    }
  }

  const parts: string[] = []
  if (sources.size) parts.push(`数据：${[...sources].join('、')}`)
  if (requirementTotal) parts.push(`模式条件 ${requirementMatched}/${requirementTotal} 通过`)
  if (hardStopPrice !== null) parts.push(`风控止损 ${hardStopPrice.toFixed(2)} 元`)
  return parts.length ? parts.join('；') : `${value.length} 条系统证据已记录`
}

function changeMatches(value: unknown) {
  return Array.isArray(value)
    ? value.filter(item => recordValue(item))
    : []
}

function uniqueStockCount(matches: unknown[]) {
  const codes = new Set(
    matches
      .map(item => displayText(recordValue(item)?.stock_code))
      .filter((code): code is string => Boolean(code))
  )
  return codes.size
}

export function planChangeSummary(
  value: unknown,
  ruleNames: Record<string, string> = {}
) {
  const change = recordValue(value)
  if (!change) return '未记录版本变化'

  const added = changeMatches(change.added_matches)
  const removed = changeMatches(change.removed_matches)
  const previousId = finiteNumber(change.previous_plan_version_id)
  const parts: string[] = []

  if (added.length) {
    parts.push(`新增 ${added.length} 条命中（${uniqueStockCount(added)} 只股票）`)
  }
  if (removed.length) {
    parts.push(`移除 ${removed.length} 条命中（${uniqueStockCount(removed)} 只股票）`)
  }
  if (!parts.length) parts.push(previousId === null ? '初始版本，无候选变化' : '与上一版本候选一致')

  const modeKeys = [...new Set(
    [...added, ...removed]
      .map(item => displayText(recordValue(item)?.mode_key))
      .filter((key): key is string => Boolean(key))
  )]
  if (modeKeys.length) {
    const labels = modeKeys.slice(0, 2).map(key => modeKeyLabel(key, ruleNames))
    parts.push(`涉及：${labels.join('、')}${modeKeys.length > 2 ? `等 ${modeKeys.length} 个模式` : ''}`)
  }
  return parts.join('；')
}

function positiveNumber(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value) && value > 0
}

function positiveInteger(value: unknown) {
  return typeof value === 'number' && Number.isInteger(value) && value > 0
}

function continuousSessionTime(value: string) {
  return (
    (value >= '09:30:00' && value <= '11:30:00') ||
    (value >= '13:00:00' && value <= '15:00:00')
  )
}

function executedAt(tradeDate: string, time: string | undefined) {
  if (!time) return undefined
  if (!/^\d{2}:\d{2}:\d{2}$/.test(time) || !continuousSessionTime(time)) {
    throw new Error('执行时间必须属于 A 股连续交易时段')
  }
  return `${tradeDate}T${time}+08:00`
}

function manualExecution(
  tradeDate: string,
  draft: ManualExecutionDraft
): TradingManualExecution {
  if (!draft.executed) {
    const result: TradingManualExecution = { executed: false }
    const note = draft.manual_note?.trim()
    if (note) result.manual_note = note
    return result
  }
  const result: TradingManualExecution = { executed: true }
  if (positiveNumber(draft.execution_price)) result.execution_price = draft.execution_price
  if (positiveInteger(draft.quantity)) result.quantity = draft.quantity
  const timestamp = executedAt(tradeDate, draft.executed_time)
  if (timestamp) result.executed_at = timestamp
  const note = draft.manual_note?.trim()
  if (note) result.manual_note = note
  return result
}

export function buildManualExecutionUpdate(
  tradeDate: string,
  planned: Record<string, ManualExecutionDraft>,
  unplanned: UnplannedExecutionDraft[]
): TradingExecutionReviewUpdate {
  const executions = Object.fromEntries(
    Object.entries(planned).map(([candidateId, draft]) => [
      candidateId,
      manualExecution(tradeDate, draft)
    ])
  )
  const unplannedExecutions: TradingUnplannedExecution[] = unplanned.map((draft, index) => {
    const stockCode = draft.stock_code.trim()
    const stockName = draft.stock_name.trim()
    if (!/^\d{6}$/.test(stockCode) || !stockName) {
      throw new Error(`第 ${index + 1} 条计划外执行需要六位股票代码和股票名称`)
    }
    return {
      ...manualExecution(tradeDate, { ...draft, executed: true }),
      executed: true,
      stock_code: stockCode,
      stock_name: stockName
    }
  })
  return {
    executions,
    unplanned_executions: unplannedExecutions
  }
}

interface SettingsDraft {
  enabled?: boolean
  trial_position_pct?: number
  confirmed_position_pct?: number
  hard_stop_pct?: number
  max_action_candidates?: number
  in_app_enabled?: boolean
  wechat_enabled?: boolean
}

export function buildSettingsUpdate(draft: SettingsDraft): TradingPlaybookSettingsUpdate {
  if (
    draft.trial_position_pct !== undefined &&
    draft.confirmed_position_pct !== undefined &&
    draft.trial_position_pct > draft.confirmed_position_pct
  ) {
    throw new Error('试错仓位不能高于确认仓位')
  }
  const result: TradingPlaybookSettingsUpdate = { wechat_enabled: false }
  for (const field of [
    'enabled',
    'trial_position_pct',
    'confirmed_position_pct',
    'hard_stop_pct',
    'max_action_candidates',
    'in_app_enabled'
  ] as const) {
    const value = draft[field]
    if (value !== undefined) Object.assign(result, { [field]: value })
  }
  return result
}
