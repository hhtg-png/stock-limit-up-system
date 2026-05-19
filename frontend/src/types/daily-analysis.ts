export const DAILY_ANALYSIS_COLUMNS = [
  '连板唯一性',
  '反包+趋势+弹钢琴',
  '炸板反包',
  '辨识度',
  '二波',
  '20cm',
  '一字套利',
  '板块',
  '负反馈'
] as const

export type DailyAnalysisColumn = typeof DAILY_ANALYSIS_COLUMNS[number]
export type DailyAnalysisSession = 'after_close' | 'intraday'

export interface DailyAnalysisItem {
  stock_code?: string
  stock_name?: string
  label: string
  tags: string[]
  reason?: string
  time?: string | null
  score?: number
  content?: string
}

export interface DailyAnalysisCell {
  items: DailyAnalysisItem[]
  content: string
  is_manual?: boolean
}

export interface DailyAnalysisRow {
  session: DailyAnalysisSession
  trade_date: string
  month: string
  status: string
  calc_version: number
  generated_at?: string | null
  updated_at?: string | null
  auto_result: Record<DailyAnalysisColumn, DailyAnalysisCell>
  manual_overrides: Partial<Record<DailyAnalysisColumn, string>>
  columns: Record<DailyAnalysisColumn, DailyAnalysisCell>
}

export interface DailyAnalysisMonthResponse {
  month: string
  session: DailyAnalysisSession
  data: DailyAnalysisRow[]
}

export interface DailyAnalysisBackfillResponse {
  built_count: number
  trade_dates: string[]
}
