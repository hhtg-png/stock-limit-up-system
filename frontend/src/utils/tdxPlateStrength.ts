export interface PlateStrengthChartItem {
  plate_name: string
  strength_score: number
  limit_up_count?: number
  sealed_count?: number
  opened_count?: number
}

export interface PlateStrengthHistoryPoint {
  trade_date: string
  items: PlateStrengthChartItem[]
}

export interface PlateStrengthChartPayload {
  items: PlateStrengthChartItem[]
  history?: PlateStrengthHistoryPoint[]
}

export interface PlateStrengthSelectionPayload {
  source: 'kpl' | 'ths'
  window_days: number
}

export interface PlateStrengthChartData {
  strength: {
    names: string[]
    scores: number[]
  }
  breadth: {
    names: string[]
    sealed: number[]
    opened: number[]
  }
  rotation: {
    dates: string[]
    series: Array<{
      name: string
      values: Array<number | null>
    }>
  }
}

export interface IntradayTagStyleInput {
  label: string
  type: string
}

export type CompactChartMode = 'strength' | 'breadth' | 'rotation'
export type ConstituentScrollDirection = 'up' | 'down'

export function normalizeCompactChartMode(value: string, showTrend: boolean): CompactChartMode {
  if (value === 'breadth') return 'breadth'
  if (value === 'rotation' && showTrend) return 'rotation'
  return 'strength'
}

export function hasRenderableChartSize(width: number, height: number): boolean {
  return width > 0 && height > 0
}

export function intradayTagClass(tag: IntradayTagStyleInput): string {
  if (tag.type === 'dragon') {
    if (tag.label === '龙1') return 'dragon-one'
    if (tag.label === '龙2') return 'dragon-two'
    return 'dragon-other'
  }
  const roleClasses: Record<string, string> = {
    high: 'role-high',
    pioneer: 'role-pioneer',
    core: 'role-core',
    catchup: 'role-catchup',
    opened: 'role-opened'
  }
  return roleClasses[tag.type] || 'role-default'
}

export function normalizePlateStrengthWindow(value: number): number {
  if (!Number.isFinite(value)) return 20
  return Math.max(5, Math.min(120, Math.trunc(value)))
}

export function buildPlateStrengthRequest(source: string, windowDays: number): {
  source: 'kpl' | 'ths'
  window_days: number
} {
  return {
    source: source === 'ths' ? 'ths' : 'kpl',
    window_days: normalizePlateStrengthWindow(windowDays)
  }
}

export function matchesPlateStrengthSelection(
  payload: PlateStrengthSelectionPayload | null | undefined,
  source: string,
  windowDays: number
): boolean {
  if (!payload) return false
  const request = buildPlateStrengthRequest(source, windowDays)
  return payload.source === request.source && payload.window_days === request.window_days
}

export function buildPlateConstituentRequest(plateName: string, source: string): {
  plate_name: string
  source: 'kpl' | 'ths'
} {
  return {
    plate_name: plateName.trim(),
    source: source === 'ths' ? 'ths' : 'kpl'
  }
}

export function nextExpandedPlate(currentPlate: string | null, clickedPlate: string): string | null {
  return currentPlate === clickedPlate ? null : clickedPlate
}

export function nextConstituentScrollTop(
  scrollTop: number,
  clientHeight: number,
  scrollHeight: number,
  direction: ConstituentScrollDirection
): number {
  const maxScrollTop = Math.max(0, scrollHeight - clientHeight)
  const pageDistance = Math.max(34, Math.floor(clientHeight * 0.85))
  const nextScrollTop = direction === 'up'
    ? scrollTop - pageDistance
    : scrollTop + pageDistance
  return Math.min(maxScrollTop, Math.max(0, nextScrollTop))
}

export class PlateStrengthRequestGate {
  private active = false
  private pendingInteractive = false

  begin(silent: boolean): 'start' | 'skip' | 'queue' {
    if (!this.active) {
      this.active = true
      return 'start'
    }
    if (silent) return 'skip'
    this.pendingInteractive = true
    return 'queue'
  }

  finish(): boolean {
    this.active = false
    const shouldReload = this.pendingInteractive
    this.pendingInteractive = false
    return shouldReload
  }
}

export function buildPlateStrengthChartData(
  payload: PlateStrengthChartPayload,
  limit = 12
): PlateStrengthChartData {
  const currentItems = (payload.items || []).slice(0, limit)
  const strengthItems = [...currentItems].reverse()
  const history = payload.history || []

  return {
    strength: {
      names: strengthItems.map(item => item.plate_name),
      scores: strengthItems.map(item => Number(item.strength_score || 0))
    },
    breadth: {
      names: currentItems.map(item => item.plate_name),
      sealed: currentItems.map(item => Number(item.sealed_count || 0)),
      opened: currentItems.map(item => Number(
        item.opened_count ?? Math.max(0, Number(item.limit_up_count || 0) - Number(item.sealed_count || 0))
      ))
    },
    rotation: {
      dates: history.map(point => point.trade_date.slice(5)),
      series: currentItems.slice(0, 6).map(item => ({
        name: item.plate_name,
        values: history.map(point => {
          const match = point.items.find(historyItem => historyItem.plate_name === item.plate_name)
          return match ? Number(match.strength_score || 0) : null
        })
      }))
    }
  }
}
