import axios from 'axios'
import type { DailyStats, SectorStatsResponse, ContinuousLadderResponse, YesterdayContinuousResponse } from '@/types/market'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

// 获取日统计
export async function getDailyStats(params?: {
  start_date?: string
  end_date?: string
}): Promise<DailyStats[]> {
  const { data } = await api.get('/statistics/daily', { params })
  return data
}

// 获取板块热度（带日期回退信息）
export async function getSectorStats(tradeDate?: string): Promise<SectorStatsResponse> {
  const { data } = await api.get('/statistics/sectors', {
    params: { trade_date: tradeDate }
  })
  return data
}

// 获取连板梯队（带日期回退信息）
export async function getContinuousLadder(tradeDate?: string): Promise<ContinuousLadderResponse> {
  const { data } = await api.get('/statistics/continuous', {
    params: { trade_date: tradeDate }
  })
  return data
}

// 获取实时连板梯队（东方财富）
export async function getContinuousRealtime(tradeDate?: string): Promise<ContinuousLadderResponse> {
  const { data } = await api.get('/statistics/continuous-realtime', {
    params: { trade_date: tradeDate }
  })
  return data
}

// 获取市场概览
export async function getMarketOverview(tradeDate?: string) {
  const { data } = await api.get('/statistics/overview', {
    params: { trade_date: tradeDate }
  })
  return data
}

// 获取昨日连板今日表现
export async function getYesterdayContinuous(tradeDate?: string): Promise<YesterdayContinuousResponse> {
  const { data } = await api.get('/statistics/yesterday-continuous', {
    params: { trade_date: tradeDate }
  })
  return data
}
