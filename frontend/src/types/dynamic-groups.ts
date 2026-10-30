export interface DynamicRule { field: string; op: string; value: string | number | boolean | string[]; value2?: string | number }
export interface DynamicField {
  key: string; label: string; kind: 'number' | 'boolean' | 'text' | 'time' | 'enum'; operators: string[]; unit?: string
  options?: { label: string; value: string | number | boolean }[]
}
export interface DynamicDefinition {
  name: string; description: string; folder: string; color: string; pinned: boolean; enabled: boolean
  universe: 'all_a' | 'limit_up'; membership_mode: 'retain' | 'dynamic'; match_mode: 'all' | 'any'; rules: DynamicRule[]
}
export interface DynamicGroup extends DynamicDefinition {
  id: number; version: number; archived: boolean; member_count: number; latest_selection_id: number | null
  data_status: string; trade_date: string | null; updated_at: string; last_evaluated_at: string | null; last_error?: string | null
}
export interface DynamicTemplate { name: string; description?: string; rules: DynamicRule[]; universe?: DynamicDefinition['universe']; match_mode?: DynamicDefinition['match_mode']; membership_mode?: DynamicDefinition['membership_mode'] }
export interface DynamicMember {
  stock_code: string; stock_name?: string; current_price?: number | null; change_pct?: number | null
  today_open_change_pct?: number | null; turnover_rate?: number | null; amount_yi?: number | null; market_value_yi?: number | null
  continuous_limit_up_days?: number | null; yesterday_continuous_days?: number | null; yesterday_first_limit_up_time?: string | null
  yesterday_final_seal_time?: string | null; selected_at?: string | null; first_selected_at?: string | null; quote_time?: string | null
  status?: string; [key: string]: unknown
}
export interface DynamicMemberQuery { offset?: number; limit?: number; q?: string }
export interface DynamicMemberSummary { stock_count: number; valid_change_count: number; average_change_pct: number | null; up_count: number; down_count: number }
export interface DynamicCurrent { group: DynamicGroup; items: DynamicMember[]; total: number; summary: DynamicMemberSummary; trade_date: string | null; previous_trade_date?: string | null; source_time: string | null; status: string; warnings: string[]; selection_id: number | null }
export interface DynamicSelection { id: number; trade_date: string; selected_at: string; member_count: number; version: number; kind: string; data_status?: string }
export interface DynamicSelectionDetail extends DynamicSelection { items: DynamicMember[]; total: number; summary: DynamicMemberSummary; rules?: DynamicRule[]; definition?: DynamicDefinition; status?: string; warnings?: string[] }
export interface DynamicPoint { date: string; value: number | null; change_pct?: number | null }
export interface DynamicTrend {
  selection_id: number; status: string; base_date: string | null; points: DynamicPoint[]; expected_count: number; covered_count: number
  warnings: string[]; updated_at: string | null; series_type: 'fixed_basket_history'; source?: string
}
export interface DynamicIntraday { points: { time?: string; timestamp?: string; date?: string; value: number | null; version?: number; segment?: number }[]; status: string; trade_date: string | null; daily_rebased?: boolean }
export interface DynamicDirectory { items: DynamicGroup[]; total: number; folders: string[]; stats?: Record<string, number> }
export interface DynamicPreview { items: DynamicMember[]; total?: number; matched_count?: number; unknown_count?: number; status: string; warnings?: string[]; trade_date?: string; source_time?: string }
