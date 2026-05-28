export interface TdxPluginPayload<T> {
  items: T[]
  updated_at: string
  source_status: Record<string, string>
  is_cache: boolean
  warnings: string[]
}

export interface TdxLimitUpEvent {
  event_id: string
  event_type: string
  event_label: string
  event_time: string
  stock_code: string
  stock_name: string
  board: number
  reason: string
  reason_category: string
  seal_amount: number
  amount: number
  turnover_rate: number
  is_sealed: boolean
  open_count: number
  sources: string[]
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
  seal_rate: number
  max_board: number
  trend: string
  core_stocks: Array<{
    stock_code: string
    stock_name: string
    board: number
  }>
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
