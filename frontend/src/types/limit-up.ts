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
  free_float_value?: number  // 自由流通市值(万元)
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

// API响应类型（带日期回退信息）
export interface LimitUpRealtimeResponse {
  trade_date: string
  is_fallback: boolean
  data: LimitUpRealtime[]
}
