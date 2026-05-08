import axios from 'axios'
import type {
  DailyAnalysisBackfillResponse,
  DailyAnalysisColumn,
  DailyAnalysisMonthResponse,
  DailyAnalysisRow
} from '@/types/daily-analysis'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

export async function getDailyAnalysisMonth(month: string): Promise<DailyAnalysisMonthResponse> {
  const { data } = await api.get('/statistics/daily-analysis', {
    params: { month }
  })
  return data
}

export async function rebuildDailyAnalysis(tradeDate: string): Promise<DailyAnalysisRow> {
  const { data } = await api.post(`/statistics/daily-analysis/${tradeDate}/rebuild`)
  return data
}

export async function updateDailyAnalysisOverrides(
  tradeDate: string,
  overrides: Partial<Record<DailyAnalysisColumn, string | null>>
): Promise<DailyAnalysisRow> {
  const { data } = await api.patch(`/statistics/daily-analysis/${tradeDate}/overrides`, {
    overrides
  })
  return data
}

export async function backfillDailyAnalysis(month: string): Promise<DailyAnalysisBackfillResponse> {
  const { data } = await api.post('/statistics/daily-analysis/backfill', {
    month
  })
  return data
}
