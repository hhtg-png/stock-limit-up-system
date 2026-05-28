import axios from 'axios'
import type {
  TdxCalibrationDiff,
  TdxLimitUpEvent,
  TdxNewsItem,
  TdxPlateStrength,
  TdxPluginPayload,
  TdxStockMove
} from '@/types/tdx-plugins'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

export async function getTdxLimitUpLive(params?: {
  trade_date?: string
}): Promise<TdxPluginPayload<TdxLimitUpEvent>> {
  const { data } = await api.get('/tdx-plugins/limit-up-live', { params })
  return data
}

export async function getTdxStockMove(
  stockCode: string,
  params?: { trade_date?: string }
): Promise<TdxPluginPayload<TdxStockMove>> {
  const { data } = await api.get(`/tdx-plugins/stock-move/${stockCode}`, { params })
  return data
}

export async function getTdxPlateStrength(params?: {
  trade_date?: string
}): Promise<TdxPluginPayload<TdxPlateStrength>> {
  const { data } = await api.get('/tdx-plugins/plate-strength', { params })
  return data
}

export async function getTdxNews(params?: {
  limit?: number
}): Promise<TdxPluginPayload<TdxNewsItem>> {
  const { data } = await api.get('/tdx-plugins/news', { params })
  return data
}

export async function getTdxThsMove(
  stockCode: string,
  params?: { trade_date?: string }
): Promise<TdxPluginPayload<TdxStockMove>> {
  const { data } = await api.get(`/tdx-plugins/ths-move/${stockCode}`, { params })
  return data
}

export async function compareTdxCalibration(payload: {
  key_field?: string
  target_items: Array<Record<string, unknown>>
  ours_items: Array<Record<string, unknown>>
}): Promise<TdxCalibrationDiff> {
  const { data } = await api.post('/tdx-plugins/calibration/compare', payload)
  return data
}
