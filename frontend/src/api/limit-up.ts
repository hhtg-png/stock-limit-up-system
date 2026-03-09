import axios from 'axios'
import type { LimitUpRealtime, LimitUpDetail, LimitUpReasonStats, LimitUpRealtimeResponse } from '@/types/limit-up'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

// 获取实时涨停列表（带日期回退信息）
export async function getRealtimeLimitUp(params?: {
  trade_date?: string
  continuous_days?: number
  reason_category?: string
  sort_by?: string
}): Promise<LimitUpRealtimeResponse> {
  const { data } = await api.get('/limit-up/realtime', { params })
  return data
}

// 获取涨停详情
export async function getLimitUpDetail(stockCode: string, tradeDate?: string): Promise<LimitUpDetail> {
  const { data } = await api.get(`/limit-up/detail/${stockCode}`, {
    params: { trade_date: tradeDate }
  })
  return data
}

// 获取涨停历史
export async function getLimitUpHistory(params: {
  stock_code?: string
  start_date?: string
  end_date?: string
  reason_category?: string
  min_continuous_days?: number
  page?: number
  page_size?: number
}): Promise<LimitUpRealtime[]> {
  const { data } = await api.get('/limit-up/history', { params })
  return data
}

// 获取涨停原因统计
export async function getLimitUpReasonStats(tradeDate?: string): Promise<LimitUpReasonStats[]> {
  const { data } = await api.get('/limit-up/reasons/statistics', {
    params: { trade_date: tradeDate }
  })
  return data
}

// 刷新涨停数据（从开盘啦/同花顺重新获取涨停原因和状态）
export async function refreshLimitUpData(tradeDate?: string): Promise<any> {
  const { data } = await api.post('/limit-up/refresh', null, {
    params: { trade_date: tradeDate }
  })
  return data
}

// 重新爬取涨停数据
export async function refetchLimitUpData(tradeDate?: string): Promise<any> {
  const { data } = await api.post('/limit-up/refetch', null, {
    params: { trade_date: tradeDate }
  })
  return data
}

// 获取表格列顺序
export async function getTableColumns(): Promise<string[]> {
  const { data } = await api.get('/config/table-columns')
  return data.columns || []
}

// 保存表格列顺序
export async function saveTableColumns(columns: string[]): Promise<void> {
  await api.put('/config/table-columns', columns)
}
