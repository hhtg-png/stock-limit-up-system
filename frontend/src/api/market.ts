import axios from 'axios'
import type { OrderBook, BigOrder, FundFlow, KlineResponse, CompareSeries, StockSearchItem } from '@/types/market'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

// 获取五档盘口
export async function getOrderBook(stockCode: string): Promise<OrderBook> {
  const { data } = await api.get(`/market/${stockCode}/orderbook`)
  return data
}

// 获取大单记录
export async function getBigOrders(stockCode: string, params?: {
  start_time?: string
  end_time?: string
  min_amount?: number
  direction?: string
  page?: number
  page_size?: number
}): Promise<BigOrder[]> {
  const { data } = await api.get(`/market/${stockCode}/big-orders`, { params })
  return data
}

// 获取资金流向
export async function getFundFlow(stockCode: string, tradeDate?: string): Promise<FundFlow> {
  const { data } = await api.get(`/market/${stockCode}/fund-flow`, {
    params: { trade_date: tradeDate }
  })
  return data
}

// 获取分时数据
export async function getTimeline(stockCode: string, tradeDate?: string) {
  const { data } = await api.get(`/market/${stockCode}/timeline`, {
    params: { trade_date: tradeDate }
  })
  return data
}

export async function getKline(stockCode: string, params?: {
  period?: 'day' | 'week' | 'month'
  limit?: number
}): Promise<KlineResponse> {
  const { data } = await api.get(`/market/${stockCode}/kline`, { params })
  return data
}

export async function getCompareSeries(params: {
  symbols: string[]
  period?: 'day' | 'week' | 'month'
  limit?: number
}): Promise<CompareSeries[]> {
  const { data } = await api.get('/market/compare', {
    params: {
      symbols: params.symbols.join(','),
      period: params.period || 'day',
      limit: params.limit || 250
    }
  })
  return data
}

export async function searchStocks(q: string, limit = 10): Promise<StockSearchItem[]> {
  const { data } = await api.get('/market/search', {
    params: { q, limit }
  })
  return data
}
