import axios from 'axios'
import type {
  DailyInfoHistoryResponse,
  DailyInfoResponse,
  DailyInfoSourceDetail,
  IntelligenceSourcesResponse,
  IntelligenceSyncResponse,
  JiegeModeResponse
} from '@/types/intelligence'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 60000
})

export async function getDailyInfo(tradeDate: string): Promise<DailyInfoResponse> {
  const { data } = await api.get('/intelligence/daily-info', {
    params: { trade_date: tradeDate }
  })
  return data
}

export async function getDailyInfoHistory(limit = 30): Promise<DailyInfoHistoryResponse> {
  const { data } = await api.get('/intelligence/daily-info/history', {
    params: { limit }
  })
  return data
}

export async function getIntelligenceDocument(documentId: number): Promise<DailyInfoSourceDetail> {
  const { data } = await api.get(`/intelligence/documents/${documentId}`)
  return data
}

export async function syncDailyInfo(): Promise<IntelligenceSyncResponse> {
  const { data } = await api.post('/intelligence/daily-info/sync')
  return data
}

export async function getJiegeMode(tradeDate: string): Promise<JiegeModeResponse> {
  const { data } = await api.get('/intelligence/jiege-mode', {
    params: { trade_date: tradeDate }
  })
  return data
}

export async function rebuildJiegeMode(tradeDate: string): Promise<JiegeModeResponse> {
  const { data } = await api.post('/intelligence/jiege-mode/rebuild', null, {
    params: { trade_date: tradeDate }
  })
  return data
}

export async function getIntelligenceSources(): Promise<IntelligenceSourcesResponse> {
  const { data } = await api.get('/intelligence/sources')
  return data
}
