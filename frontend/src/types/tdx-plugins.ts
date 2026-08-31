export interface TdxPluginPayload<T> {
  items: T[]
  updated_at: string
  source_status: Record<string, string>
  is_cache: boolean
  warnings: string[]
  plate_filters?: Array<{
    name: string
    count: number
  }>
}

export interface TdxLimitUpEvent {
  event_id: string
  trade_date?: string
  event_type: string
  event_label: string
  event_time: string
  stock_code: string
  stock_name: string
  board: number
  reason: string
  reason_category: string
  change_pct: number
  seal_amount: number
  amount: number
  turnover_rate: number
  is_sealed: boolean
  open_count: number
  sources: string[]
  target_status_label: string
  target_plate: string
  target_reason_summary: string
  target_seal_amount: string
}

export interface TdxStockMove {
  stock_code: string
  stock_name: string
  trade_date: string
  source_scope: 'mixed' | 'ths' | string
  sources: string[]
  latest_limit_up: {
    board: number
    event_label: string
    first_limit_up_time: string
    final_seal_time: string
    open_count: number
    seal_amount: number
  } | null
  reasons: Array<{
    source: string
    title: string
    content: string
  }>
  concepts: string[]
  announcements: string[]
  industry: string
  related_plates: string[]
}

export interface TdxPlateStrength {
  plate_name: string
  strength_score: number
  limit_up_count: number
  sealed_count: number
  opened_count: number
  seal_rate: number
  max_board: number
  total_seal_amount: number
  total_amount: number
  total_open_count: number
  rank: number
  rank_change: number
  score_change: number
  trend: 'new' | 'up' | 'down' | 'flat'
  core_stocks: Array<{
    stock_code: string
    stock_name: string
    board: number
    is_sealed: boolean
    seal_amount: number
  }>
}

export interface TdxPlateStrengthPayload extends TdxPluginPayload<TdxPlateStrength> {
  source: 'kpl' | 'ths'
  window_days: number
  history: Array<{
    trade_date: string
    items: TdxPlateStrength[]
  }>
  summary: {
    plate_count: number
    limit_up_count: number
    sealed_count: number
    total_seal_amount: number
  }
}

export interface TdxIntradayTag {
  label: string
  type: 'dragon' | 'high' | 'pioneer' | 'core' | 'catchup' | 'opened'
  reason: string
}

export interface TdxPlateConstituent {
  stock_code: string
  stock_name: string
  market: string
  price: number | null
  change_pct: number | null
  amount: number
  turnover_rate: number
  is_limit_up: boolean
  is_sealed: boolean
  board: number
  first_limit_up_time: string
  match_reason: string
  dragon_tag: '龙1' | '龙2' | '龙3' | '龙4' | '龙5' | null
  dragon_reason: string
  tags?: TdxIntradayTag[]
}

export interface TdxPlateConstituentPayload extends TdxPluginPayload<TdxPlateConstituent> {
  plate_name: string
  source: 'kpl' | 'ths'
  constituent_source: 'local_topic_knowledge' | 'eastmoney_board' | 'unavailable'
  source_note: string
  summary: {
    stock_count: number
    quoted_count: number
    up_count: number
    down_count: number
    flat_count: number
    limit_up_count: number
  }
}

export interface TdxNewsItem {
  news_id: string
  time: string
  source: string
  title: string
  content: string
  importance: number
  related_stocks: string[]
  related_plates: string[]
  jump_url?: string
}

export interface TdxCalibrationDiff {
  summary: Record<string, number>
  missing_items: Array<Record<string, unknown>>
  extra_items: Array<Record<string, unknown>>
  field_diffs: Array<Record<string, unknown>>
  order_diffs: Array<Record<string, unknown>>
  updated_at: string
}
