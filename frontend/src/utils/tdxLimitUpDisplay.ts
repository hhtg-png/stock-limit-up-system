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
