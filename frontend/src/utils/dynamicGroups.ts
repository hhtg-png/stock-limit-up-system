import type { DynamicDefinition, DynamicField, DynamicPoint, DynamicRule } from '../types/dynamic-groups'

export function createRequestGate() {
  let revision = 0
  return { begin: () => ++revision, isCurrent: (request: number) => request === revision, invalidate: () => { revision++ } }
}

export function groupDefinition(group: DynamicDefinition): DynamicDefinition {
  const { name, description, folder, color, pinned, enabled, universe, membership_mode, match_mode, rules } = group
  return { name, description, folder, color, pinned, enabled, universe, membership_mode, match_mode, rules: rules.map(rule => ({ field: rule.field, op: rule.op, value: rule.value, ...(rule.op === 'between' ? { value2: rule.value2 } : {}) })) }
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

export function validateDefinition(definition: Pick<DynamicDefinition, 'name' | 'rules'>, fields: DynamicField[]) {
  if (!definition.name.trim()) return '请填写板块名称'
  if (definition.name.trim().length > 80) return '名称最多 80 字'
  if (!definition.rules.length || definition.rules.length > 30) return '请设置 1 至 30 条条件'
  for (const rule of definition.rules) {
    const field = fields.find(item => item.key === rule.field)
    if (!field) return '请选择有效的条件字段'
    if (!field.operators.includes(rule.op)) return `${field.label}不支持该运算符`
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

export function ruleSummary(rules: DynamicRule[], fields: DynamicField[], matchMode = 'all') {
  return rules.map(rule => {
    const field = fields.find(item => item.key === rule.field)
    const value = typeof rule.value === 'boolean' ? (rule.value ? '是' : '否') : Array.isArray(rule.value) ? rule.value.join('、') : rule.value
    return `${field?.label ?? rule.field} ${operatorLabels[rule.op] ?? rule.op} ${value}${rule.op === 'between' ? ` 至 ${rule.value2}` : ''}${field?.unit ?? ''}`
  }).join(matchMode === 'any' ? '；或 ' : '；')
}

export function statusLabel(status?: string) {
  const labels: Record<string, string> = { ready: '已更新', ok: '已更新', complete: '已更新', partial: '部分数据待补', pending: '等待更新', queued: '等待更新', retrying: '重试中', building: '历史补充中', loading: '更新中', stale: '行情待更新', failed: '更新失败', error: '更新失败', empty: '暂无成分', unavailable: '数据暂缺', missing: '数据待补', unknown: '待确认', paused: '已暂停' }
  return labels[status ?? ''] ?? '等待更新'
}
