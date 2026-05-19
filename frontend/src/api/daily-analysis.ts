import axios from 'axios'
import type {
  DailyAnalysisBackfillResponse,
  DailyAnalysisColumn,
  DailyAnalysisMonthResponse,
  DailyAnalysisRow,
  DailyAnalysisSession
} from '@/types/daily-analysis'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

export async function getDailyAnalysisMonth(
  month: string,
  session: DailyAnalysisSession = 'after_close'
): Promise<DailyAnalysisMonthResponse> {
  const { data } = await api.get('/statistics/daily-analysis', {
    params: { month, session }
  })
  return data
}

export async function rebuildDailyAnalysis(
  tradeDate: string,
  session: DailyAnalysisSession = 'after_close'
): Promise<DailyAnalysisRow> {
  const { data } = await api.post(`/statistics/daily-analysis/${tradeDate}/rebuild`, null, {
    params: { session }
  })
  return data
}

export async function updateDailyAnalysisOverrides(
  tradeDate: string,
  overrides: Partial<Record<DailyAnalysisColumn, string | null>>,
  session: DailyAnalysisSession = 'after_close'
): Promise<DailyAnalysisRow> {
  const { data } = await api.patch(`/statistics/daily-analysis/${tradeDate}/overrides`, {
    overrides
  }, {
    params: { session }
  })
  return data
}

export async function backfillDailyAnalysis(month: string): Promise<DailyAnalysisBackfillResponse> {
  const { data } = await api.post('/statistics/daily-analysis/backfill', {
    month
  })
  return data
}
