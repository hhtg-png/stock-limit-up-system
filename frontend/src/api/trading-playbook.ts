import axios from 'axios'
import type {
  TradingAlertEvent,
  TradingExecutionReview,
  TradingExecutionReviewUpdate,
  TradingModeRule,
  TradingPlanRevision,
  TradingPlanStage,
  TradingPlanVersion,
  TradingPlaybookObsidianExportRequest,
  TradingPlaybookObsidianExportResponse,
  TradingPlaybookObsidianStatus,
  TradingPlaybookPersonalWechatStatus,
  TradingPlaybookSettings,
  TradingPlaybookSettingsUpdate
} from '@/types/trading-playbook'

export const tradingPlaybookApi = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

export async function getTradingRules() {
  const { data } = await tradingPlaybookApi.get('/trading-playbook/rules')
  return data as { items: TradingModeRule[] }
}

export async function getTradingPlans(tradeDate: string) {
  const { data } = await tradingPlaybookApi.get('/trading-playbook/plans', {
    params: { trade_date: tradeDate }
  })
  return data as { items: TradingPlanVersion[] }
}

export async function getLatestTradingPlanTargetDate() {
  const { data } = await tradingPlaybookApi.get('/trading-playbook/plans/latest-target-date')
  return data as { target_trade_date: string | null }
}

export async function getTradingPlan(planId: number) {
  const { data } = await tradingPlaybookApi.get(`/trading-playbook/plans/${planId}`)
  return data as TradingPlanVersion
}

export async function generateTradingPlan(sourceTradeDate: string, stage: TradingPlanStage) {
  const { data } = await tradingPlaybookApi.post('/trading-playbook/plans/generate', {
    source_trade_date: sourceTradeDate,
    stage
  })
  return data as TradingPlanVersion
}

export async function reviseTradingPlan(planId: number, revision: TradingPlanRevision) {
  const { data } = await tradingPlaybookApi.put(`/trading-playbook/plans/${planId}`, revision)
  return data as TradingPlanVersion
}

export async function confirmTradingPlan(planId: number, confirmedBy: string) {
  const { data } = await tradingPlaybookApi.post(`/trading-playbook/plans/${planId}/confirm`, {
    confirmed_by: confirmedBy
  })
  return data as TradingPlanVersion
}

export async function cancelTradingPlan(planId: number) {
  const { data } = await tradingPlaybookApi.post(`/trading-playbook/plans/${planId}/cancel`)
  return data as TradingPlanVersion
}

export async function getTradingAlerts(unreadOnly = true) {
  const { data } = await tradingPlaybookApi.get('/trading-playbook/alerts', {
    params: { unread_only: unreadOnly }
  })
  return data as { items: TradingAlertEvent[]; limit: number; offset: number }
}

export async function ackTradingAlert(alertId: number) {
  const { data } = await tradingPlaybookApi.post(`/trading-playbook/alerts/${alertId}/ack`)
  return data as TradingAlertEvent
}

export async function getTradingReviews(tradeDate: string, planId?: number) {
  const params: { trade_date: string; plan_id?: number } = { trade_date: tradeDate }
  if (planId !== undefined) params.plan_id = planId
  const { data } = await tradingPlaybookApi.get('/trading-playbook/reviews', { params })
  return data as { items: TradingExecutionReview[] }
}

export async function updateTradingExecutionReview(
  tradeDate: string,
  review: TradingExecutionReviewUpdate,
  planId?: number
) {
  const config = planId === undefined ? undefined : { params: { plan_id: planId } }
  const { data } = await tradingPlaybookApi.put(
    `/trading-playbook/reviews/${tradeDate}`,
    review,
    config
  )
  return data as TradingExecutionReview
}

export async function getTradingPlaybookSettings() {
  const { data } = await tradingPlaybookApi.get('/trading-playbook/settings')
  return data as TradingPlaybookSettings
}

export async function updateTradingPlaybookSettings(settings: TradingPlaybookSettingsUpdate) {
  const { data } = await tradingPlaybookApi.put('/trading-playbook/settings', settings)
  return data as TradingPlaybookSettings
}

export async function getTradingPlaybookPersonalWechatStatus() {
  const { data } = await tradingPlaybookApi.get(
    '/trading-playbook/notifications/personal-wechat/status'
  )
  return data as TradingPlaybookPersonalWechatStatus
}

export async function getObsidianStatus(): Promise<TradingPlaybookObsidianStatus> {
  const { data } = await tradingPlaybookApi.get('/trading-playbook/obsidian/status')
  return data
}

export async function exportToObsidian(
  request: TradingPlaybookObsidianExportRequest
): Promise<TradingPlaybookObsidianExportResponse> {
  const { data } = await tradingPlaybookApi.post('/trading-playbook/obsidian/export', request)
  return data
}
