interface DailyInfoSearchSource {
  title?: string | null
}

interface DailyInfoSearchStock {
  name?: string | null
  code?: string | null
  sector?: string | null
  summary?: string | null
  reason?: string | null
  source_title?: string | null
}

interface DailyInfoSearchSummary {
  overview?: string | null
  main_lines?: unknown
  catalysts?: unknown
  risks?: unknown
  plan?: string | null
  source_titles?: unknown
  mentioned_stocks?: unknown
  stocks?: unknown
}

export interface DailyInfoSearchItem {
  trade_date?: string | null
  summary?: DailyInfoSearchSummary | null
  sources?: DailyInfoSearchSource[] | null
}

export function filterVisibleDailyInfoSearchResults<T extends DailyInfoSearchItem>(
  items: T[],
  keyword: string,
): T[] {
  const normalizedKeyword = keyword.trim().toLowerCase()
  if (!normalizedKeyword) return items
  return items.filter(item => dailyInfoContainsVisibleKeyword(item, normalizedKeyword))
}

export function dailyInfoContainsVisibleKeyword(item: DailyInfoSearchItem, keyword: string): boolean {
  const normalizedKeyword = keyword.trim().toLowerCase()
  if (!normalizedKeyword) return true
  return visibleSearchText(item).toLowerCase().includes(normalizedKeyword)
}

function visibleSearchText(item: DailyInfoSearchItem): string {
  const summary = item.summary || {}
  const parts: string[] = [
    item.trade_date || '',
    summary.overview || '',
    summary.plan || '',
    ...stringList(summary.main_lines),
    ...stringList(summary.catalysts),
    ...stringList(summary.risks),
    ...stringList(summary.source_titles),
    ...stockTextList(summary.mentioned_stocks),
    ...stockTextList(summary.stocks),
    ...(item.sources || []).map(source => source.title || ''),
  ]
  return parts.join('\n')
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.map(item => String(item || '')).filter(Boolean)
}

function stockTextList(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value
    .filter(item => item && typeof item === 'object')
    .flatMap(item => stockText(item as DailyInfoSearchStock))
}

function stockText(stock: DailyInfoSearchStock): string[] {
  return [
    stock.name || '',
    stock.code || '',
    stock.sector || '',
    stock.summary || '',
    stock.reason || '',
    stock.source_title || '',
  ].filter(Boolean)
}
