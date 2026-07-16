export type TradingPlanStage = 'preclose' | 'after_close' | 'overnight' | 'auction'

export type TradingAlertEventType =
  | 'plan_ready'
  | 'confirmation_required'
  | 'watch'
  | 'entry_triggered'
  | 'confirmation_triggered'
  | 'invalidated'
  | 'risk_warning'
  | 'exit_triggered'
  | 'review_ready'

export interface TradingRuleSnapshot {
  mode_key: string
  version: number
  content_hash: string
  [key: string]: unknown
}

export interface TradingPlanCandidate {
  id: number
  stock_code: string
  stock_name: string
  action_trade_date: string
  theme_name: string
  primary_mode_key: string
  supporting_mode_keys_json: string[]
  role: string
  rank: number
  recognition_json: Record<string, unknown>
  entry_trigger_json: Record<string, unknown>
  invalidation_json: Record<string, unknown>
  exit_trigger_json: Record<string, unknown>
  risk_level: 'avoid' | 'watch' | 'trial' | 'confirmed'
  position_reference: number
  evidence_json: Array<Record<string, unknown>>
  manual_overrides_json: Record<string, unknown>
  status: string
}

export interface TradingPlanVersion {
  id: number
  source_trade_date: string
  target_trade_date: string
  stage: TradingPlanStage
  version_no: number
  parent_plan_version_id?: number | null
  status: 'draft' | 'confirmed' | 'active' | 'superseded' | 'expired'
  market_state_json: Record<string, unknown>
  theme_ranking_json: Array<Record<string, unknown>>
  mode_radar_json: Array<Record<string, unknown>>
  rule_snapshot_json: TradingRuleSnapshot[]
  risk_settings_json: Record<string, unknown>
  data_quality_json: { status: string; stale?: boolean; warnings?: string[] }
  change_summary_json: Record<string, unknown>
  input_hash: string
  generated_at: string
  confirmed_at?: string | null
  confirmed_by?: string | null
  candidates: TradingPlanCandidate[]
}

export interface TradingAlertEvent {
  id: number
  plan_version_id: number
  candidate_id?: number | null
  event_type: TradingAlertEventType
  severity: string
  dedup_key: string
  triggered_at: string
  market_snapshot_json: Record<string, unknown>
  message: string
  channel_status_json: Record<string, unknown>
  acknowledged_at?: string | null
}

export interface TradingModeRule {
  id: number
  mode_key: string
  version: number
  name: string
  family: string
  style: string
  window: string
  automation_level: 'automatic' | 'assisted' | 'manual_only'
  description: string
  source_refs_json: Array<{ source_key: string; excerpt: string }>
}

export interface TradingExecutionReview {
  id: number
  trade_date: string
  plan_version_id: number
  signal_review_json: Record<string, unknown>
  manual_execution_json: Record<string, unknown>
  plan_compliance_json: Record<string, unknown>
  outcome_snapshot_json: Record<string, unknown>
  data_quality_json: Record<string, unknown> & {
    status?: string
    warnings?: string[]
  }
  generated_at: string
  finalized_at?: string | null
}

export interface TradingPlaybookSettings {
  enabled: boolean
  trial_position_pct: number
  confirmed_position_pct: number
  hard_stop_pct: number
  max_action_candidates: number
  in_app_enabled: boolean
  wechat_enabled: false
}

export interface TradingPlaybookPersonalWechatStatus {
  provider: 'wxpusher'
  delivery: 'personal_wechat'
  configured: boolean
  enabled: boolean
  recipient_masked?: string | null
  setup_qr_url: string
  docs_url: string
  schedule: string[]
  requires_server_configuration: boolean
}

export interface TradingCandidateOverride {
  candidate_id?: number
  stock_code?: string
  primary_mode_key?: string
  action_trade_date?: string
  entry_trigger?: Record<string, unknown>
  invalidation?: Record<string, unknown>
  exit_trigger?: Record<string, unknown>
  manual_note?: string
}

export interface TradingPlanRevision {
  change_note: string
  candidate_overrides?: TradingCandidateOverride[]
}

export interface TradingManualExecution {
  executed: boolean
  execution_price?: number
  quantity?: number
  executed_at?: string
  manual_note?: string
}

export interface TradingUnplannedExecution extends TradingManualExecution {
  executed: true
  stock_code: string
  stock_name: string
}

export interface TradingExecutionReviewUpdate {
  executions?: Record<string, TradingManualExecution>
  unplanned_executions?: TradingUnplannedExecution[]
}

export type TradingPlaybookSettingsUpdate = Partial<
  Omit<TradingPlaybookSettings, 'wechat_enabled'>
> & { wechat_enabled?: false }

export type TradingPlaybookObsidianPhase =
  | 'catalog'
  | 'preclose'
  | 'initial_review'
  | 'after_close'
  | 'final_review'
  | 'overnight'
  | 'auction'
  | 'reconcile'

export type TradingPlaybookObsidianJsonValue =
  | string
  | number
  | boolean
  | null
  | TradingPlaybookObsidianJsonValue[]
  | { [key: string]: TradingPlaybookObsidianJsonValue }

export type TradingPlaybookObsidianGitStatus = Record<
  string,
  TradingPlaybookObsidianJsonValue
>

export interface TradingPlaybookObsidianStatus {
  enabled: boolean
  configured: boolean
  vault_exists: boolean
  auto_git_enabled: boolean
  last_success_at: string | null
  last_trade_date: string | null
  last_phase: string | null
  pending_count: number
  paused_count: number
  failed_count: number
  last_error: string | null
  recent_files: string[]
  dashboard_path: string
  dashboard_openable: boolean
}

export interface TradingPlaybookObsidianExportRequest {
  trade_date: string
  include_rules?: boolean
  force?: boolean
}

export interface TradingPlaybookObsidianExportResponse {
  trade_date: string
  phase: TradingPlaybookObsidianPhase
  written_files: string[]
  skipped_files: string[]
  pending_files: string[]
  failed_files: string[]
  git_status: TradingPlaybookObsidianGitStatus
  error_summary: string | null
}
