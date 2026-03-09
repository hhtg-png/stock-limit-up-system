// 基础类型
export interface Stock {
  stock_code: string
  stock_name: string
  market: string
  industry?: string
  is_st?: boolean
  is_kc?: boolean
  is_cy?: boolean
}

export interface StockQuote {
  stock_code: string
  stock_name: string
  current_price: number
  pre_close: number
  open_price: number
  high_price: number
  low_price: number
  volume: number
  amount: number
  change_pct: number
  change_amount: number
  turnover_rate: number
  update_time: string
}
