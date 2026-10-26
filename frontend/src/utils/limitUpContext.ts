import type { LimitUpRealtime } from '@/types/limit-up'

export function formatLimitUpContextTime(value: string | undefined | null): string {
  const match = value?.trim().match(/^(?:\d{4}-\d{2}-\d{2}[T ])?(\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?$/)
  if (!match) return '—'
  const [, hours, minutes, seconds = '00'] = match
  if (Number(hours) > 23 || Number(minutes) > 59 || Number(seconds) > 59) return '—'
  return `${hours.padStart(2, '0')}:${minutes}:${seconds}`
}

export function formatOpeningChangePct(value: number | undefined | null): string {
  if (value == null || !Number.isFinite(value)) return '—'
  const rounded = Number(value.toFixed(2))
  return `${rounded > 0 ? '+' : ''}${rounded.toFixed(2)}%`
}

export function sortByYesterdayFinalSealTime<T extends Pick<LimitUpRealtime, 'stock_code' | 'yesterday_final_seal_time'>>(
  items: readonly T[], order: 'asc' | 'desc' = 'asc'
): T[] {
  const sortTime = (value: string | undefined | null): string => {
    const raw = value?.trim() || ''
    return /^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$/.test(raw) ? raw : ''
  }
  return [...items].sort((a, b) => {
    const aTime = sortTime(a.yesterday_final_seal_time)
    const bTime = sortTime(b.yesterday_final_seal_time)
    if (Boolean(aTime) !== Boolean(bTime)) return aTime ? -1 : 1
    const timeOrder = aTime.localeCompare(bTime) * (order === 'desc' ? -1 : 1)
    return timeOrder || a.stock_code.localeCompare(b.stock_code)
  })
}

function formatYi(value: number | undefined | null): string {
  return value == null || value === 0 ? '-' : (value / 10000).toFixed(2)
}

function escapeCsvCell(value: string | number): string {
  const text = String(value)
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text
}

export function buildLimitUpCsv(items: LimitUpRealtime[]): string {
  const headers = [
    '代码', '名称', '查询日期', '行情日期', '昨日日期', '连板', '首封时间', '昨日首封', '昨日终封', '昨日竞价', '今日竞价',
    '状态', '封单(万)', '成交额(亿)', '实际流通值(亿)', '涨停原因'
  ]
  const rows = items.map(item => [
    item.stock_code,
    item.stock_name,
    item.context_date || '',
    item.trade_date || '',
    item.previous_trade_date || '',
    item.continuous_limit_up_days,
    item.first_limit_up_time || '',
    formatLimitUpContextTime(item.yesterday_first_limit_up_time),
    formatLimitUpContextTime(item.yesterday_final_seal_time),
    formatOpeningChangePct(item.yesterday_open_change_pct),
    formatOpeningChangePct(item.today_open_change_pct),
    item.is_sealed ? '封板' : '开板',
    item.seal_amount?.toFixed(0) || '',
    formatYi(item.amount),
    formatYi(item.tradable_market_value),
    item.limit_up_reason || ''
  ])
  return [headers, ...rows].map(row => row.map(escapeCsvCell).join(',')).join('\n')
}
