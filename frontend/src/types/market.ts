// 行情相关类型
export interface OrderBook {
  stock_code: string
  snapshot_time: string
  current_price?: number
  pre_close?: number
  bid_prices: number[]
  bid_volumes: number[]
  ask_prices: number[]
  ask_volumes: number[]
  volume?: number
  amount?: number
}

export interface BigOrder {
  id: number
  stock_code: string
  trade_time: string
  trade_price: number
  trade_volume: number
  trade_amount: number
  direction: 'buy' | 'sell'
  order_type: 'active_buy' | 'passive_buy' | 'active_sell' | 'passive_sell'
  is_limit_up_price: boolean
}

export interface FundFlow {
  stock_code: string
  trade_date: string
  main_in: number
  main_out: number
  main_net: number
  retail_in: number
  retail_out: number
  retail_net: number
}

export interface DailyStats {
  trade_date: string
  total_limit_up: number
  new_limit_up: number
  continuous_2: number
  continuous_3: number
  continuous_4_plus: number
  break_count: number
  break_rate: number
  average_seal_time?: string
  strongest_sector?: string
  limit_down_count: number
}

export interface SectorStats {
  sector_name: string
  limit_up_count: number
  stocks: string[]
  average_gain?: number
}

export interface ContinuousLadder {
  continuous_days: number
  count: number
  stocks: {
    stock_code: string
    stock_name: string
    first_limit_up_time?: string
    final_seal_time?: string
    reason?: string
    is_sealed?: boolean
    open_count?: number
    change_pct?: number
    bid1_volume?: number
    turnover_rate?: number
    real_turnover_rate?: number
  }[]
}

// API响应类型（带日期回退信息）
export interface SectorStatsResponse {
  trade_date: string
  is_fallback: boolean
  data: SectorStats[]
}

export interface ContinuousLadderResponse {
  trade_date: string
  is_fallback: boolean
  data: ContinuousLadder[]
}

// 昨日连板相关类型
export interface YesterdayContinuousStock {
  stock_code: string
  stock_name: string
  yesterday_days: number
  today_status: 'sealed' | 'opened' | 'broken'
  today_change_pct: number | null
}

export interface YesterdayContinuousLadder {
  continuous_days: number
  count: number
  sealed_count: number
  opened_count: number
  broken_count: number
  stocks: YesterdayContinuousStock[]
}

export interface YesterdayContinuousResponse {
  trade_date: string
  yesterday_date: string
  is_fallback: boolean
  data: YesterdayContinuousLadder[]
}
