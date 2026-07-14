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

export function canEnableActionAlerts(plan: PlanConfirmationState | null | undefined) {
  return (
    plan?.status === 'draft' &&
    plan.data_quality_json?.status === 'ready' &&
    plan.data_quality_json.stale !== true
  )
}

export function isObservationOnly(plan: PlanConfirmationState | null | undefined) {
  if (!plan) return true
  const quality = plan.data_quality_json || { status: 'missing' }
  return quality.status === 'missing' || quality.status === 'degraded' || quality.stale === true
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
