export interface DailyInfoSummary {
  overview?: string
  main_lines?: string[]
  catalysts?: string[]
  risks?: string[]
  plan?: string
  source_titles?: string[]
  trade_date?: string
  model?: string
  model_status?: string
  error?: string
}

export interface DailyInfoResponse {
  trade_date: string
  status: string
  source_count: number
  summary: DailyInfoSummary
  model?: string | null
  generated_at?: string | null
  cache_hit: boolean
}

export interface IntelligenceSource {
  key: string
  name: string
  kind: string
  share_id: string
}

export interface IntelligenceSourcesResponse {
  sources: IntelligenceSource[]
}

export interface IntelligenceSyncResponse {
  sources: Record<string, {
    source_key: string
    total_documents: number
    changed_documents: number
    summarized_documents: number
  }>
  daily_info: DailyInfoResponse
  jiege_mode: JiegeModeResponse
}

export interface JiegeRule {
  rule_key: string
  title: string
  category: string
  summary: string
  payload: Record<string, unknown>
}

export interface JiegeMarketPhase {
  label: string
  score: number
  basis: string[]
}

export interface JiegeCandidate {
  stock_code: string
  stock_name: string
  label: string
  tags: string[]
  reason?: string
  score: number
}

export interface JiegeSignalData {
  trade_date: string
  market_phase: JiegeMarketPhase
  rules: JiegeRule[]
  prediction: {
    candidates: JiegeCandidate[]
    daily_analysis: Record<string, unknown>
    risk_flags: string[]
  }
  review: {
    sealed_count: number
    opened_count: number
    max_board_height: number
    notes: string
  }
}

export interface JiegeModeResponse {
  trade_date: string
  status: string
  data: JiegeSignalData
  generated_at?: string | null
  cache_hit: boolean
}
