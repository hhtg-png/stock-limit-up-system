import type {
  TradingCandidateOverride,
  TradingPlanRevision
} from '@/types/trading-playbook'

export interface ReviewSummaryLike {
  plan_version_id: number
}

export interface ReviewPlanLike {
  id: number
  candidates?: unknown[]
}

export interface ReviewDomainState<P extends ReviewPlanLike> {
  selectedPlanId?: number | null
  reviewPlan?: P | null
  reviewPlanLoading?: boolean
  reviewPlanError?: string | null
}

interface ReviewDomainDependencies<R extends ReviewSummaryLike, P extends ReviewPlanLike> {
  loadReviews: (tradeDate: string) => Promise<R[]>
  loadPlan: (planId: number) => Promise<P>
  update: (patch: ReviewDomainState<P>) => void
  onPlanLoaded?: (plan: P, review: R) => void
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

export function canEditReview(
  review: ReviewSummaryLike | null | undefined,
  plan: ReviewPlanLike | null | undefined,
  loading: boolean
) {
  return Boolean(
    !loading &&
    review &&
    plan &&
    plan.id === review.plan_version_id
  )
}

export async function saveReviewIfEditable(
  review: ReviewSummaryLike | null | undefined,
  plan: ReviewPlanLike | null | undefined,
  loading: boolean,
  save: () => Promise<unknown>
) {
  if (!canEditReview(review, plan, loading)) return false
  await save()
  return true
}

export function createReviewDomainController<
  R extends ReviewSummaryLike,
  P extends ReviewPlanLike
>(dependencies: ReviewDomainDependencies<R, P>) {
  let domainRequestId = 0
  let detailRequestId = 0
  let selectedPlanId: number | null = null
  let reviews: R[] = []

  function clearDetail() {
    detailRequestId += 1
    dependencies.update({
      reviewPlan: null,
      reviewPlanLoading: false,
      reviewPlanError: null
    })
  }

  async function loadDetail(domainId: number, review: R) {
    const planId = review.plan_version_id
    const requestId = ++detailRequestId
    dependencies.update({
      selectedPlanId: planId,
      reviewPlan: null,
      reviewPlanLoading: true,
      reviewPlanError: null
    })
    try {
      const plan = await dependencies.loadPlan(planId)
      if (
        domainId !== domainRequestId ||
        requestId !== detailRequestId ||
        selectedPlanId !== planId
      ) return
      if (plan.id !== planId) {
        throw new Error('复盘详情与所选预案版本不一致')
      }
      dependencies.update({
        reviewPlan: plan,
        reviewPlanLoading: false,
        reviewPlanError: null
      })
      dependencies.onPlanLoaded?.(plan, review)
    } catch (error) {
      if (
        domainId !== domainRequestId ||
        requestId !== detailRequestId ||
        selectedPlanId !== planId
      ) return
      dependencies.update({
        reviewPlan: null,
        reviewPlanLoading: false,
        reviewPlanError: errorMessage(error)
      })
    }
  }

  async function load(tradeDate: string) {
    const requestId = ++domainRequestId
    clearDetail()
    try {
      const loaded = await dependencies.loadReviews(tradeDate)
      if (requestId !== domainRequestId) return
      reviews = loaded
      const selected = (
        reviews.find(item => item.plan_version_id === selectedPlanId) ||
        reviews[0] ||
        null
      )
      selectedPlanId = selected?.plan_version_id || null
      dependencies.update({ selectedPlanId })
      if (!selected) {
        clearDetail()
        return
      }
      await loadDetail(requestId, selected)
    } catch (error) {
      if (requestId !== domainRequestId) return
      reviews = []
      selectedPlanId = null
      clearDetail()
      dependencies.update({
        selectedPlanId: null,
        reviewPlanError: errorMessage(error)
      })
    }
  }

  async function select(planId: number) {
    const review = reviews.find(item => item.plan_version_id === planId)
    if (!review) {
      selectedPlanId = null
      dependencies.update({ selectedPlanId: null })
      clearDetail()
      return
    }
    selectedPlanId = planId
    await loadDetail(domainRequestId, review)
  }

  return { load, select }
}

export type PlanMutationKind = 'confirm' | 'cancel'

interface PlanMutationDependencies {
  confirm: (planId: number) => Promise<unknown>
  cancel: (planId: number) => Promise<unknown>
  reload: () => Promise<unknown>
  success: (kind: PlanMutationKind) => void
  failure: (error: unknown) => void
  updatePending: (kind: PlanMutationKind | null) => void
}

export function createPlanMutationController(dependencies: PlanMutationDependencies) {
  let pending: PlanMutationKind | null = null

  async function run(kind: PlanMutationKind, planId: number) {
    if (pending) return false
    pending = kind
    dependencies.updatePending(kind)
    try {
      await dependencies[kind](planId)
      await dependencies.reload()
      dependencies.success(kind)
      return true
    } catch (error) {
      dependencies.failure(error)
      return false
    } finally {
      pending = null
      dependencies.updatePending(null)
    }
  }

  return { run }
}

export function createConcurrentIdGuard(
  update: (activeIds: ReadonlySet<number>) => void
) {
  const activeIds = new Set<number>()

  async function run(id: number, action: () => Promise<unknown>) {
    if (activeIds.has(id)) return false
    activeIds.add(id)
    update(new Set(activeIds))
    try {
      await action()
      return true
    } finally {
      activeIds.delete(id)
      update(new Set(activeIds))
    }
  }

  return {
    run,
    isActive: (id: number) => activeIds.has(id)
  }
}

export async function runAndClearErrorOnSuccess<T>(
  action: () => Promise<T>,
  clearError: () => void
) {
  const result = await action()
  clearError()
  return result
}

export interface CandidateRevisionDraft {
  candidate_id: number
  action_trade_date?: string
  manual_note?: string
  entry_trigger_text?: string
  invalidation_text?: string
  exit_trigger_text?: string
}

const triggerFields = new Set([
  'label',
  'reference_price',
  'price_gte',
  'price_lte',
  'change_pct_gte',
  'change_pct_lte',
  'sealed',
  'open_count_gte'
])

function parseTrigger(
  text: string | undefined,
  label: string,
  field: 'entry_trigger' | 'invalidation' | 'exit_trigger'
) {
  if (!text?.trim()) return undefined
  let value: unknown
  try {
    value = JSON.parse(text)
  } catch {
    throw new Error(`${label}必须是合法 JSON 对象`)
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label}必须是 JSON 对象`)
  }
  const trigger = value as Record<string, unknown>
  if (field === 'invalidation' && 'price_lte' in trigger) {
    throw new Error('刚性止损不能通过修订覆盖')
  }
  for (const [key, fieldValue] of Object.entries(trigger)) {
    if (!triggerFields.has(key)) throw new Error(`${label}不支持字段 ${key}`)
    if (fieldValue === null) throw new Error(`${label}.${key}不能为 null`)
    if (key === 'label') {
      if (typeof fieldValue !== 'string' || !fieldValue.trim() || fieldValue.length > 500) {
        throw new Error(`${label}.label 必须是非空文本`)
      }
    } else if (key === 'sealed') {
      if (typeof fieldValue !== 'boolean') throw new Error(`${label}.sealed 必须是布尔值`)
    } else if (key === 'open_count_gte') {
      if (!Number.isInteger(fieldValue) || Number(fieldValue) < 0) {
        throw new Error(`${label}.open_count_gte 必须是非负整数`)
      }
    } else if (
      typeof fieldValue !== 'number' ||
      !Number.isFinite(fieldValue) ||
      (['reference_price', 'price_gte', 'price_lte'].includes(key) && fieldValue <= 0) ||
      (key.startsWith('change_pct_') && (fieldValue < -100 || fieldValue > 100))
    ) {
      throw new Error(`${label}.${key}数值无效`)
    }
  }
  return Object.keys(trigger).length ? trigger : undefined
}

function canonicalDate(value: string) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false
  const parsed = new Date(`${value}T00:00:00Z`)
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value
}

export function buildPlanRevision(
  changeNote: string,
  drafts: CandidateRevisionDraft[]
): TradingPlanRevision {
  const note = changeNote.trim()
  if (!note || note.length > 500) throw new Error('修订说明必填且不能超过 500 字')
  if (drafts.length > 3) throw new Error('一次最多修订三只候选')
  const seen = new Set<number>()
  const overrides: TradingCandidateOverride[] = []
  for (const draft of drafts) {
    if (!Number.isInteger(draft.candidate_id) || draft.candidate_id <= 0) {
      throw new Error('候选 id 无效')
    }
    if (seen.has(draft.candidate_id)) throw new Error('候选不能重复修订')
    seen.add(draft.candidate_id)
    const override: TradingCandidateOverride = { candidate_id: draft.candidate_id }
    const actionDate = draft.action_trade_date?.trim()
    if (actionDate) {
      if (!canonicalDate(actionDate)) throw new Error('行动交易日必须使用 YYYY-MM-DD')
      override.action_trade_date = actionDate
    }
    const manualNote = draft.manual_note?.trim()
    if (manualNote) {
      if (manualNote.length > 500) throw new Error('候选备注不能超过 500 字')
      override.manual_note = manualNote
    }
    const entry = parseTrigger(draft.entry_trigger_text, '触发条件', 'entry_trigger')
    const invalidation = parseTrigger(draft.invalidation_text, '失效条件', 'invalidation')
    const exit = parseTrigger(draft.exit_trigger_text, '退出条件', 'exit_trigger')
    if (entry) override.entry_trigger = entry
    if (invalidation) override.invalidation = invalidation
    if (exit) override.exit_trigger = exit
    if (Object.keys(override).length > 1) overrides.push(override)
  }
  if (!overrides.length) throw new Error('至少修改一项候选条件')
  return { change_note: note, candidate_overrides: overrides }
}

interface RevisionChild {
  id: number
}

interface PlanRevisionDependencies {
  revise: (planId: number, revision: TradingPlanRevision) => Promise<RevisionChild>
  reload: () => Promise<unknown>
  select: (planId: number) => void
  success: (child: RevisionChild) => void
  failure: (error: unknown) => void
  updatePending: (pending: boolean) => void
}

export function createPlanRevisionController(dependencies: PlanRevisionDependencies) {
  let pending = false

  async function run(planId: number, revision: TradingPlanRevision) {
    if (pending) return false
    pending = true
    dependencies.updatePending(true)
    try {
      const child = await dependencies.revise(planId, revision)
      if (!Number.isInteger(child.id) || child.id <= 0) {
        throw new Error('修订接口未返回有效子版本')
      }
      await dependencies.reload()
      dependencies.select(child.id)
      dependencies.success(child)
      return true
    } catch (error) {
      dependencies.failure(error)
      return false
    } finally {
      pending = false
      dependencies.updatePending(false)
    }
  }

  return { run }
}
