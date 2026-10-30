import type { DynamicDefinition, DynamicField, DynamicPoint, DynamicRule, DynamicRuleNode } from '../types/dynamic-groups'

export function createRequestGate() {
  let revision = 0
  return { begin: () => ++revision, isCurrent: (request: number) => request === revision, invalidate: () => { revision++ } }
}

export const historicalRuleFields = new Set(['recent_limit_up_count', 'recent_touched_limit_up_count', 'recent_max_continuous_days'])
export function isRuleLeaf(node: DynamicRuleNode): node is DynamicRule { return !!node && 'field' in node }
export function ruleLeaves(nodes: DynamicRuleNode[]): DynamicRule[] { return nodes.flatMap(node => isRuleLeaf(node) ? [node] : ruleLeaves('all' in node ? node.all : node.any)) }
export function hasCompoundRules(nodes: DynamicRuleNode[]) { return nodes.some(node => !isRuleLeaf(node)) }
export function cloneRules(nodes: DynamicRuleNode[]): DynamicRuleNode[] {
  return nodes.map(node => isRuleLeaf(node) ? { field: node.field, op: node.op, value: Array.isArray(node.value) ? [...node.value] : node.value, ...(node.op === 'between' ? { value2: node.value2 } : {}), ...(node.days !== undefined ? { days: node.days } : {}) } : 'all' in node ? { all: cloneRules(node.all) } : { any: cloneRules(node.any) })
}
export function groupDefinition(group: DynamicDefinition): DynamicDefinition {
  const { name, description, folder, color, pinned, enabled, universe, membership_mode, match_mode, rules, strategy } = group
  return { name, description, folder, color, pinned, enabled, universe, membership_mode, match_mode, rules: cloneRules(rules), ...(strategy ? { strategy: { prompt: strategy.prompt, summary: strategy.summary, assumptions: [...strategy.assumptions], ...(strategy.ranking ? { ranking: { ...strategy.ranking } } : {}) } } : {}) }
}

export function intradayChartPoints(points: { time?: string; timestamp?: string; date?: string; value: number | null; version?: number; segment?: number }[]): DynamicPoint[] {
  const result: DynamicPoint[] = []
  let previous: typeof points[number] | undefined
  for (const point of points) {
    const date = point.time || point.timestamp || point.date || ''
    if (previous) {
      const previousDate = previous.time || previous.timestamp || previous.date || ''
      const gap = Date.parse(date) - Date.parse(previousDate)
      if (point.version !== previous.version || point.segment !== previous.segment || gap > 90000) result.push({ date, value: null })
    }
    result.push({ date, value: point.value }); previous = point
  }
  return result
}

export const operatorLabels: Record<string, string> = {
  eq: '等于', ne: '不等于', gte: '大于等于', lte: '小于等于', gt: '大于', lt: '小于', between: '区间', contains: '包含', not_contains: '不包含', in: '属于'
}

export function validateDefinition(definition: Pick<DynamicDefinition, 'name' | 'rules'> & Partial<Pick<DynamicDefinition, 'strategy'>>, fields: DynamicField[]) {
  if (!definition.name.trim()) return '请填写板块名称'
  if (definition.name.trim().length > 80) return '名称最多 80 字'
  let leaves = 0
  function validateNodes(nodes: DynamicRuleNode[], depth = 1): string {
    if (!Array.isArray(nodes) || !nodes.length || depth > 4) return '条件组合最多 4 层，且每组至少需要一个条件'
    for (const rule of nodes) {
      if (!isRuleLeaf(rule)) {
        if (!rule || (('all' in rule) === ('any' in rule))) return '请选择有效的条件组合'
        const invalid = validateNodes('all' in rule ? rule.all : rule.any, depth + 1)
        if (invalid) return invalid
        continue
      }
      leaves++
      if (leaves > 30) return '请设置 1 至 30 条条件'
      const field = fields.find(item => item.key === rule.field)
      if (!field) return '请选择有效的条件字段'
      if (!field.operators.includes(rule.op)) return `${field.label}不支持该运算符`
      if (historicalRuleFields.has(rule.field)) {
        if (!Number.isInteger(rule.days) || rule.days! < 1 || rule.days! > 60) return '历史窗口须为 1 至 60 个已完成交易日'
      } else if (rule.days !== undefined) return `${field.label}不支持历史窗口`
      if (rule.value === '' || rule.value === null || rule.value === undefined || (Array.isArray(rule.value) && !rule.value.length)) return `请填写${field.label}`
      if (field.kind === 'number' && (!Number.isFinite(Number(rule.value)) || typeof rule.value === 'boolean')) return `请填写有效的${field.label}`
      if (rule.op === 'between') {
        if (rule.value2 === '' || rule.value2 === null || rule.value2 === undefined) return `请填写${field.label}区间上限`
        if (field.kind === 'number' && (!Number.isFinite(Number(rule.value2)) || Number(rule.value) > Number(rule.value2))) return `${field.label}区间上限不能小于下限`
        if (field.kind === 'time' && String(rule.value) > String(rule.value2)) return `${field.label}区间上限不能早于下限`
      }
    }
    return ''
  }
  const invalid = validateNodes(definition.rules)
  if (invalid) return invalid
  const ranking = definition.strategy?.ranking
  if (ranking) {
    const field = fields.find(item => item.key === ranking.field)
    if (!field || field.sortable === false || !['number', 'time'].includes(field.kind)) return '请选择可排序的数值或时间字段'
    if (!['asc', 'desc'].includes(ranking.direction)) return '请选择排序方向'
    if (ranking.limit !== undefined && (!Number.isInteger(ranking.limit) || ranking.limit < 1 || ranking.limit > 500)) return '保留数量须为 1 至 500 只'
    if (historicalRuleFields.has(ranking.field)) {
      if (!Number.isInteger(ranking.days) || ranking.days! < 1 || ranking.days! > 60) return '排序历史窗口须为 1 至 60 个已完成交易日'
    } else if (ranking.days !== undefined) return '当前排序字段不支持历史窗口'
  }
  return ''
}

export function historyWindowStart(points: DynamicPoint[], days: number) {
  if (!points.length) return undefined
  if (!days) return points[0].date
  const end = Date.parse(`${points[points.length - 1].date}T00:00:00Z`)
  const boundary = end - days * 86400000
  return points.find(point => Date.parse(`${point.date}T00:00:00Z`) >= boundary)?.date ?? points[0].date
}

export function formatValue(value: unknown, digits = 2) {
  if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return '—'
  return Number(value).toFixed(digits)
}

export function ruleSummary(rules: DynamicRuleNode[], fields: DynamicField[], matchMode = 'all'): string {
  return rules.map(rule => {
    if (!isRuleLeaf(rule)) return `（${ruleSummary('all' in rule ? rule.all : rule.any, fields, 'all' in rule ? 'all' : 'any')}）`
    const field = fields.find(item => item.key === rule.field)
    const valueLabel = (item: string | number | boolean) => field?.options?.find(option => option.value === item)?.label ?? (typeof item === 'boolean' ? (item ? '是' : '否') : item)
    const value = Array.isArray(rule.value) ? rule.value.map(valueLabel).join('、') : valueLabel(rule.value)
    const rawLabel = field?.label ?? rule.field
    const label = rule.days === undefined ? rawLabel : rawLabel.replace(/^近N个已完成交易日/, '')
    return `${rule.days !== undefined ? `近${rule.days}个已完成交易日 · ` : ''}${label} ${operatorLabels[rule.op] ?? rule.op} ${value}${rule.op === 'between' ? ` 至 ${rule.value2}` : ''}${field?.unit ?? ''}`
  }).join(matchMode === 'any' ? ' 或 ' : ' 且 ')
}

export function rankingSummary(definition: DynamicDefinition, fields: DynamicField[]) {
  const ranking = definition.strategy?.ranking
  if (!ranking) return ''
  const rawLabel = fields.find(field => field.key === ranking.field)?.label || ranking.field
  const label = ranking.days ? rawLabel.replace(/^近N个已完成交易日/, '') : rawLabel
  return `按${ranking.days ? `近${ranking.days}个已完成交易日的` : ''}${label}${ranking.direction === 'asc' ? '由低到高' : '由高到低'}${ranking.limit ? `，保留前 ${ranking.limit} 只` : ''}`
}

export function statusLabel(status?: string) {
  const labels: Record<string, string> = { ready: '已更新', ok: '已更新', complete: '已更新', partial: '部分数据待补', pending: '等待更新', queued: '等待更新', retrying: '重试中', building: '历史补充中', loading: '更新中', stale: '行情待更新', failed: '更新失败', error: '更新失败', empty: '暂无成分', unavailable: '数据暂缺', missing: '数据待补', unknown: '待确认', paused: '已暂停' }
  return labels[status ?? ''] ?? '等待更新'
}
