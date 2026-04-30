export interface ReviewRangeQuery {
  days: number
  end_date: string
}

export interface ReviewRange {
  startDate: string
  endDate: string
  query: ReviewRangeQuery
}

export function buildReviewRange(timeRange: string, currentDate: string): ReviewRange {
  const parsedDays = Number.parseInt(timeRange, 10)
  const days = Number.isFinite(parsedDays) && parsedDays > 0 ? parsedDays : 30

  return {
    startDate: currentDate,
    endDate: currentDate,
    query: {
      days,
      end_date: currentDate
    }
  }
}
