const CHINA_DATE_FORMATTER = new Intl.DateTimeFormat('en-US', {
  timeZone: 'Asia/Shanghai',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit'
})

export function getChinaDateString(now: Date = new Date()): string {
  const parts = CHINA_DATE_FORMATTER.formatToParts(now)
  const values = new Map(parts.map(part => [part.type, part.value]))
  const year = values.get('year')
  const month = values.get('month')
  const day = values.get('day')

  if (!year || !month || !day) {
    throw new Error('Unable to format Asia/Shanghai date')
  }

  return `${year}-${month}-${day}`
}
