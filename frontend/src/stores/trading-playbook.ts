import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import {
  ackTradingAlert,
  exportToObsidian as requestObsidianExport,
  getObsidianStatus as getTradingObsidianStatus,
  getTradingAlerts,
  getTradingPlans,
  getTradingPlaybookSettings,
  getTradingReviews
} from '@/api/trading-playbook'
import { getObsidianStatus as getObsidianVaultStatus } from '@/api/intelligence'
import type {
  TradingAlertEvent,
  TradingExecutionReview,
  TradingPlanVersion,
  TradingPlaybookObsidianStatus,
  TradingPlaybookSettings
} from '@/types/trading-playbook'
import type { ObsidianStatus } from '@/types/intelligence'

function requestErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

function latestAcknowledgement(
  current: string | null | undefined,
  incoming: string | null | undefined
) {
  if (!current) return incoming
  if (!incoming) return current
  const currentTime = Date.parse(current)
  const incomingTime = Date.parse(incoming)
  if (Number.isNaN(incomingTime)) return current
  if (Number.isNaN(currentTime)) return incoming
  return incomingTime > currentTime ? incoming : current
}

function alertTime(alert: TradingAlertEvent) {
  const timestamp = Date.parse(alert.triggered_at)
  return Number.isNaN(timestamp) ? 0 : timestamp
}

function mergeAlertInto(
  current: TradingAlertEvent[],
  incoming: TradingAlertEvent
) {
  let merged = { ...incoming }
  const remaining: TradingAlertEvent[] = []
  for (const item of current) {
    if (item.id !== incoming.id && item.dedup_key !== incoming.dedup_key) {
      remaining.push(item)
      continue
    }
    merged = {
      ...item,
      ...merged,
      acknowledged_at: latestAcknowledgement(
        item.acknowledged_at,
        merged.acknowledged_at
      )
    }
  }
  return [merged, ...remaining]
    .sort((left, right) => alertTime(right) - alertTime(left) || right.id - left.id)
    .slice(0, 200)
}

export const useTradingPlaybookStore = defineStore('trading-playbook', () => {
  const plans = ref<TradingPlanVersion[]>([])
  const activePlan = ref<TradingPlanVersion | null>(null)
  const alerts = ref<TradingAlertEvent[]>([])
  const reviews = ref<TradingExecutionReview[]>([])
  const settings = ref<TradingPlaybookSettings | null>(null)

  const plansRequestId = ref(0)
  const plansLoading = ref(false)
  const plansError = ref<string | null>(null)
  const plansRequestedTradeDate = ref<string | null>(null)
  const plansLoadedTradeDate = ref<string | null>(null)

  const reviewsRequestId = ref(0)
  const reviewsLoading = ref(false)
  const reviewsError = ref<string | null>(null)
  const reviewsRequestedTradeDate = ref<string | null>(null)
  const reviewsLoadedTradeDate = ref<string | null>(null)
  const reviewsRequestedPlanId = ref<number | null>(null)
  const reviewsLoadedPlanId = ref<number | null>(null)

  const alertsRequestId = ref(0)
  const alertsLoading = ref(false)
  const alertsError = ref<string | null>(null)
  const alertsRequestedUnreadOnly = ref<boolean | null>(null)
  const alertsLoadedUnreadOnly = ref<boolean | null>(null)

  const settingsRequestId = ref(0)
  const settingsLoading = ref(false)
  const settingsError = ref<string | null>(null)

  const obsidianStatus = ref<TradingPlaybookObsidianStatus | null>(null)
  const obsidianVaultStatus = ref<ObsidianStatus | null>(null)
  const obsidianStatusRequestId = ref(0)
  const obsidianExportRequestId = ref(0)
  const obsidianStatusLoading = ref(false)
  const obsidianExporting = ref(false)
  const obsidianError = ref<string | null>(null)
  let obsidianErrorRequestId = 0
  let obsidianExportsInFlight = 0

  const unreadCount = computed(() => alerts.value.filter(item => !item.acknowledged_at).length)

  function receiveAlert(alert: TradingAlertEvent) {
    alerts.value = mergeAlertInto(alerts.value, alert)
  }

  async function loadPlans(tradeDate: string) {
    const requestId = plansRequestId.value + 1
    plansRequestId.value = requestId
    plansLoading.value = true
    plansError.value = null
    plansRequestedTradeDate.value = tradeDate
    plansLoadedTradeDate.value = null
    plans.value = []
    activePlan.value = null
    try {
      const response = await getTradingPlans(tradeDate)
      if (requestId !== plansRequestId.value) return
      plans.value = response.items
      activePlan.value = response.items.find(item => item.status === 'active') || response.items[0] || null
      plansLoadedTradeDate.value = tradeDate
      plansLoading.value = false
    } catch (error) {
      if (requestId === plansRequestId.value) {
        plans.value = []
        activePlan.value = null
        plansLoadedTradeDate.value = null
        plansError.value = requestErrorMessage(error)
        plansLoading.value = false
      }
      throw error
    }
  }

  async function loadReviews(tradeDate: string, planId?: number) {
    const requestId = reviewsRequestId.value + 1
    reviewsRequestId.value = requestId
    reviewsLoading.value = true
    reviewsError.value = null
    reviewsRequestedTradeDate.value = tradeDate
    reviewsRequestedPlanId.value = planId ?? null
    reviewsLoadedTradeDate.value = null
    reviewsLoadedPlanId.value = null
    reviews.value = []
    try {
      const response = await getTradingReviews(tradeDate, planId)
      if (requestId !== reviewsRequestId.value) return
      reviews.value = response.items
      reviewsLoadedTradeDate.value = tradeDate
      reviewsLoadedPlanId.value = planId ?? null
      reviewsLoading.value = false
    } catch (error) {
      if (requestId === reviewsRequestId.value) {
        reviews.value = []
        reviewsLoadedTradeDate.value = null
        reviewsLoadedPlanId.value = null
        reviewsError.value = requestErrorMessage(error)
        reviewsLoading.value = false
      }
      throw error
    }
  }

  async function loadAlerts(unreadOnly = true) {
    const requestId = alertsRequestId.value + 1
    alertsRequestId.value = requestId
    alertsLoading.value = true
    alertsError.value = null
    alertsRequestedUnreadOnly.value = unreadOnly
    alertsLoadedUnreadOnly.value = null
    try {
      const response = await getTradingAlerts(unreadOnly)
      for (const alert of response.items) receiveAlert(alert)
      if (requestId !== alertsRequestId.value) return
      alertsLoadedUnreadOnly.value = unreadOnly
      alertsLoading.value = false
    } catch (error) {
      if (requestId === alertsRequestId.value) {
        alertsLoadedUnreadOnly.value = null
        alertsError.value = requestErrorMessage(error)
        alertsLoading.value = false
      }
      throw error
    }
  }

  async function acknowledgeAlert(alertId: number) {
    const alert = await ackTradingAlert(alertId)
    receiveAlert(alert)
  }

  async function loadSettings() {
    const requestId = settingsRequestId.value + 1
    settingsRequestId.value = requestId
    settingsLoading.value = true
    settingsError.value = null
    settings.value = null
    try {
      const response = await getTradingPlaybookSettings()
      if (requestId !== settingsRequestId.value) return
      settings.value = response
      settingsLoading.value = false
    } catch (error) {
      if (requestId === settingsRequestId.value) {
        settings.value = null
        settingsError.value = requestErrorMessage(error)
        settingsLoading.value = false
      }
      throw error
    }
  }

  async function loadObsidianStatus(preserveError = false) {
    const requestId = obsidianStatusRequestId.value + 1
    obsidianStatusRequestId.value = requestId
    const errorRequestId = preserveError
      ? obsidianErrorRequestId
      : obsidianErrorRequestId + 1
    if (!preserveError) {
      obsidianErrorRequestId = errorRequestId
      obsidianError.value = null
    }
    obsidianStatusLoading.value = true
    try {
      const [status, vaultStatus] = await Promise.all([
        getTradingObsidianStatus(),
        getObsidianVaultStatus()
      ])
      if (requestId !== obsidianStatusRequestId.value) return
      obsidianStatus.value = status
      obsidianVaultStatus.value = vaultStatus
    } catch (error) {
      if (
        !preserveError &&
        requestId === obsidianStatusRequestId.value &&
        errorRequestId === obsidianErrorRequestId
      ) {
        obsidianError.value = requestErrorMessage(error)
      }
      throw error
    } finally {
      if (requestId === obsidianStatusRequestId.value) {
        obsidianStatusLoading.value = false
      }
    }
  }

  async function exportToObsidian(
    tradeDate: string,
    includeRules = false,
    force = false
  ) {
    const requestId = obsidianExportRequestId.value + 1
    obsidianExportRequestId.value = requestId
    const errorRequestId = obsidianErrorRequestId + 1
    obsidianErrorRequestId = errorRequestId
    obsidianExportsInFlight += 1
    obsidianExporting.value = true
    obsidianError.value = null
    try {
      const result = await requestObsidianExport({
        trade_date: tradeDate,
        include_rules: includeRules,
        force
      })
      await loadObsidianStatus(requestId !== obsidianExportRequestId.value)
      return result
    } catch (error) {
      if (
        requestId === obsidianExportRequestId.value &&
        errorRequestId === obsidianErrorRequestId
      ) {
        obsidianError.value = requestErrorMessage(error)
      }
      throw error
    } finally {
      obsidianExportsInFlight = Math.max(0, obsidianExportsInFlight - 1)
      obsidianExporting.value = obsidianExportsInFlight > 0
    }
  }

  return {
    plans,
    activePlan,
    alerts,
    reviews,
    settings,
    unreadCount,
    plansRequestId,
    plansLoading,
    plansError,
    plansRequestedTradeDate,
    plansLoadedTradeDate,
    reviewsRequestId,
    reviewsLoading,
    reviewsError,
    reviewsRequestedTradeDate,
    reviewsLoadedTradeDate,
    reviewsRequestedPlanId,
    reviewsLoadedPlanId,
    alertsRequestId,
    alertsLoading,
    alertsError,
    alertsRequestedUnreadOnly,
    alertsLoadedUnreadOnly,
    settingsRequestId,
    settingsLoading,
    settingsError,
    obsidianStatus,
    obsidianVaultStatus,
    obsidianStatusRequestId,
    obsidianExportRequestId,
    obsidianStatusLoading,
    obsidianExporting,
    obsidianError,
    receiveAlert,
    loadPlans,
    loadReviews,
    loadAlerts,
    acknowledgeAlert,
    loadSettings,
    loadObsidianStatus,
    exportToObsidian
  }
})
