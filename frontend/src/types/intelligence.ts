export interface DailyInfoSummary {
  overview?: string
  main_lines?: string[]
  catalysts?: string[]
  risks?: string[]
  plan?: string
  source_titles?: string[]
  mentioned_stocks?: DailyInfoMentionedStock[]
  stocks?: DailyInfoMentionedStock[]
  trade_date?: string
  model?: string
  model_status?: string
  error?: string
}

export interface DailyInfoMentionedStock {
  name: string
  code?: string
  reason?: string
  source_title?: string
}

export interface DailyInfoResponse {
  trade_date: string
  status: string
  source_count: number
  summary: DailyInfoSummary
  model?: string | null
  generated_at?: string | null
  cache_hit: boolean
  sources: DailyInfoSource[]
}

export interface DailyInfoSource {
  id: number
  title: string
  source_name: string
  source_key: string
  media_type_name: string
  trade_date?: string | null
  update_time?: string
  jump_url?: string
  source_path?: string
}

export interface DailyInfoSourceDetail extends DailyInfoSource {
  abstract?: string
  introduction?: string
  content_text?: string
  summary?: Record<string, unknown>
}

export interface DailyInfoHistoryResponse {
  items: DailyInfoResponse[]
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
