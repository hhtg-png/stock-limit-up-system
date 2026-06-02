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
  sector?: string
  summary?: string
  reason?: string
  source_title?: string
  sentiment?: string
  risk?: string
  watch_points?: string[] | string
}

export interface DailyInfoResponse {
  id?: number
  version_id?: number | null
  digest_id?: number | null
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
  state?: string
  reason?: string
  queued?: boolean
  started_at?: string | null
  finished_at?: string | null
  error?: string
  result?: IntelligenceSyncResult | null
}

export interface IntelligenceProbeResponse {
  probe: {
    changed: boolean
    reason: string
    media_id?: string
    title?: string
    field?: string
    checked_documents: number
    checked_at: string
  }
  queued: boolean
  sync: IntelligenceSyncResponse
}

export interface IntelligenceSyncResult {
  sources: Record<string, {
    source_key: string
    total_documents: number
    changed_documents: number
    summarized_documents: number
  }>
  daily_info: DailyInfoResponse
  jiege_mode: JiegeModeResponse
  obsidian?: ObsidianExportResponse | null
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

export interface JiegeYesterdayPrediction {
  source_date?: string | null
  target_date: string
  candidates: JiegeCandidate[]
  risk_flags: string[]
  market_phase: JiegeMarketPhase
  notes: string
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
  yesterday_prediction?: JiegeYesterdayPrediction
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

export interface ObsidianStatus {
  enabled: boolean
  vault_configured: boolean
  vault_exists: boolean
  vault_path: string
  auto_git_enabled: boolean
  web_research_enabled: boolean
  web_research_allowlist: string[]
  required_directories?: string[]
}

export interface ObsidianExportResponse {
  trade_date: string
  vault_path: string
  written_files: string[]
  skipped?: boolean
  reason?: string
  git?: {
    enabled: boolean
    committed?: boolean
    reason?: string
    error?: string
  }
}

export interface IndustryTrendSource {
  title: string
  url: string
  source_name: string
  trade_date: string
}

export interface IndustryTrendStock {
  name: string
  code?: string
  sector?: string
  summary?: string
  reason?: string
  source_title?: string
}

export interface IndustryTrend {
  theme: string
  status: string
  confidence: string
  last_seen: string
  catalysts: string[]
  risks: string[]
  stocks: IndustryTrendStock[]
  sources: IndustryTrendSource[]
  evidence: string[]
}

export interface IndustryTrendsResponse {
  items: IndustryTrend[]
}

export interface UltraShortSignal {
  trade_date: string
  setup: string
  source: string
  alert_type: string
  manual_required: boolean
  sim_result: string
  stock_code?: string
  stock_name?: string
  label: string
  tags: string[]
  reason?: string
  score: number
  risk_flags: string[]
}

export interface UltraShortSignalsResponse {
  trade_date: string
  items: UltraShortSignal[]
}
