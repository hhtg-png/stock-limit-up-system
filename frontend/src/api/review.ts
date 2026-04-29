import axios from 'axios'
import type {
  MarketReviewDailyResponse,
  MarketReviewDetailResponse,
  MarketReviewLadderResponse
} from '@/types/market'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

export async function getMarketReviewDaily(params?: {
  start_date?: string
  end_date?: string
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
