// 涨停相关类型
export interface LimitUpRecord {
  id: number
  stock_code: string
  stock_name: string
  trade_date: string
  first_limit_up_time?: string
  final_seal_time?: string
  limit_up_reason?: string
  reason_category?: string
  continuous_limit_up_days: number
  open_count: number
  is_final_sealed: boolean
  current_status?: 'sealed' | 'opened' | 'broken' | 'final_sealed' | 'unknown'
  seal_amount?: number
  seal_volume?: number
  limit_up_price: number
  current_price: number
  turnover_rate?: number
  amount?: number
  tradable_market_value?: number
  market: string
  industry?: string
}

export interface LimitUpRealtime extends LimitUpRecord {
  is_sealed: boolean
}

export interface LimitUpStatusChange {
  change_time: string
  status: 'sealed' | 'opened' | 'resealed'
  price?: number
  seal_amount?: number
}

export interface LimitUpDetail extends LimitUpRecord {
  status_changes: LimitUpStatusChange[]
}

export interface LimitUpReasonStats {
  reason_category: string
  count: number
  percentage: number
  stocks: string[]
}

export interface LimitUpClassificationStock {
  stock_code: string
  stock_name: string
  trade_date: string
  continuous_limit_up_days: number
  current_status: string
  is_sealed: boolean
  open_count: number
  first_limit_up_time: string
  final_seal_time: string
  limit_up_reason: string
  classified_plate: string
  rule_classified_plate: string
  classification_method: 'ai' | 'rule'
  ai_confidence: number
  ai_reason_summary: string
  ai_keywords: string[]
  seal_amount: number
  turnover_rate: number
  amount: number
}

export interface LimitUpClassificationGroup {
  plate_name: string
  count: number
  sealed_count: number
  opened_count: number
  earliest_first_limit_time: string
  latest_first_limit_time: string
  stocks: LimitUpClassificationStock[]
}

export interface LimitUpClassificationResponse {
  requested_date: string
  trade_date: string
  is_fallback: boolean
  updated_at: string
  source_status: Record<string, string>
  classification_method: 'ai' | 'rule'
  total_count: number
  groups: LimitUpClassificationGroup[]
}

// API响应类型（带日期回退信息）
export interface LimitUpRealtimeResponse {
  trade_date: string
  is_fallback: boolean
  data: LimitUpRealtime[]
}
