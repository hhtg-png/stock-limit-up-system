export interface ReviewRangeQuery {
  days: number
  end_date: string
}

export interface ReviewRange {
  startDate: string
  endDate: string
  query: ReviewRangeQuery
}

const QUICK_RANGE_TRADING_DAYS: Record<string, number> = {
  '7': 7,
  '30': 30,
  '3m': 60
}

export function buildReviewRange(timeRange: string, currentDate: string): ReviewRange {
  const parsedDays = Number.parseInt(timeRange, 10)
  const days = QUICK_RANGE_TRADING_DAYS[timeRange] ?? (
    Number.isFinite(parsedDays) && parsedDays > 0 ? parsedDays : 30
  )

  return {
    startDate: currentDate,
    endDate: currentDate,
    query: {
      days,
      end_date: currentDate
    }
  }
}
