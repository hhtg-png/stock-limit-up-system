import axios from 'axios'
import type {
  MarketReviewDailyResponse,
  MarketReviewDetailResponse,
  MarketReviewIntradayResponse,
  MarketReviewLadderResponse
} from '@/types/market'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

export async function getMarketReviewDaily(params?: {
  start_date?: string
  end_date?: string
  days?: number
}): Promise<MarketReviewDailyResponse> {
  const { data } = await api.get('/statistics/review/daily', { params })
  return data
}

export async function getMarketReviewDetail(tradeDate: string): Promise<MarketReviewDetailResponse> {
  const { data } = await api.get('/statistics/review/detail', {
    params: { trade_date: tradeDate }
  })
  return data
}

export async function getMarketReviewLadder(tradeDate: string): Promise<MarketReviewLadderResponse> {
  const { data } = await api.get('/statistics/review/ladder', {
    params: { trade_date: tradeDate }
  })
  return data
}

export async function getMarketReviewIntraday(tradeDate?: string): Promise<MarketReviewIntradayResponse> {
  const { data } = await api.get('/statistics/review/intraday', {
    params: tradeDate ? { trade_date: tradeDate } : undefined
  })
  return data
}

export interface BrokenBoardPoint {
  trade_date: string
  average_change: number | null
  sample_count: number
  priced_count: number
  data_status: string
  stocks: { stock_code: string; stock_name: string; previous_board: number; change_pct: number | null }[]
}

export async function getBrokenBoardPerformance(params: { days: number; end_date: string }): Promise<{ points: BrokenBoardPoint[] }> {
  const { data } = await api.get('/statistics/review/broken-board-performance', { params, timeout: 120000 })
  return data
}
