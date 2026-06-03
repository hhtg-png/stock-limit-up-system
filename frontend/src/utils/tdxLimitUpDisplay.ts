function toFiniteNumber(value: unknown): number | null {
  const numberValue = Number(value)
  return Number.isFinite(numberValue) ? numberValue : null
}

export function pickDisplayChangePct(previous: unknown, next: unknown): number {
  const nextValue = toFiniteNumber(next)
  if (nextValue !== null && nextValue !== 0) return nextValue

  const previousValue = toFiniteNumber(previous)
  if (previousValue !== null && previousValue !== 0) return previousValue

  return nextValue ?? previousValue ?? 0
}

export function formatTdxSealAmount(value: unknown): string {
  const amount = toFiniteNumber(value)
  if (amount === null || amount <= 0) return '--'

  if (amount >= 10_000_000) {
    const wan = amount / 10_000
    if (wan >= 10_000) return `${(wan / 10_000).toFixed(2)}亿`
    return `${wan.toFixed(0)}万`
  }

  if (amount >= 10_000) return `${(amount / 10_000).toFixed(2)}亿`
  return `${amount.toFixed(0)}万`
}

type TdxDisplayStateLike = {
  event_type?: unknown
  event_label?: unknown
  is_sealed?: unknown
  open_count?: unknown
  seal_amount?: unknown
  target_status_label?: unknown
  target_seal_amount?: unknown
}

export function resolveTdxMergedDisplayState(
  previous: TdxDisplayStateLike | null | undefined,
  next: TdxDisplayStateLike | null | undefined
) {
  const nextEventType = String(next?.event_type || '')
  const isTouchOnlyEvent = nextEventType === 'limit_up_touched'
  const stateSource = isTouchOnlyEvent && previous ? previous : next
  const previousSealAmount = toFiniteNumber(previous?.seal_amount) ?? 0
  const nextSealAmount = toFiniteNumber(next?.seal_amount) ?? 0
  const sealAmount = isTouchOnlyEvent && previous && nextSealAmount <= 0
    ? previousSealAmount
    : nextSealAmount
  const targetSealAmount = isTouchOnlyEvent && previous && nextSealAmount <= 0
    ? String(previous.target_seal_amount || formatTdxSealAmount(previousSealAmount))
    : String(next?.target_seal_amount || formatTdxSealAmount(sealAmount))

  return {
    event_type: String(stateSource?.event_type || next?.event_type || previous?.event_type || ''),
    event_label: String(stateSource?.event_label || next?.event_label || previous?.event_label || ''),
    is_sealed: Boolean(stateSource?.is_sealed),
    open_count: toFiniteNumber(stateSource?.open_count) ?? 0,
    seal_amount: sealAmount,
    target_status_label: String(stateSource?.target_status_label || next?.target_status_label || previous?.target_status_label || ''),
    target_seal_amount: targetSealAmount
  }
}
