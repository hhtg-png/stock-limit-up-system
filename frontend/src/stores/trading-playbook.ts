import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { getTradingPlans, getTradingReviews } from '@/api/trading-playbook'
import type {
  TradingAlertEvent,
  TradingExecutionReview,
  TradingPlanVersion
} from '@/types/trading-playbook'

export const useTradingPlaybookStore = defineStore('trading-playbook', () => {
  const plans = ref<TradingPlanVersion[]>([])
  const activePlan = ref<TradingPlanVersion | null>(null)
  const alerts = ref<TradingAlertEvent[]>([])
  const reviews = ref<TradingExecutionReview[]>([])
  const unreadCount = computed(() => alerts.value.filter(item => !item.acknowledged_at).length)

  function receiveAlert(alert: TradingAlertEvent) {
    if (alerts.value.some(item => item.id === alert.id || item.dedup_key === alert.dedup_key)) return
    alerts.value.unshift(alert)
    alerts.value = alerts.value.slice(0, 200)
  }

  async function loadPlans(tradeDate: string) {
    const response = await getTradingPlans(tradeDate)
    plans.value = response.items
    activePlan.value = response.items.find(item => item.status === 'active') || response.items[0] || null
  }

  async function loadReviews(tradeDate: string, planId?: number) {
    const response = await getTradingReviews(tradeDate, planId)
    reviews.value = response.items
  }

  return {
    plans,
    activePlan,
    alerts,
    reviews,
    unreadCount,
    receiveAlert,
    loadPlans,
    loadReviews
  }
})
