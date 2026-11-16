import { computed, ref } from 'vue'

export function useTradingCalendar() {
  const tradingDates = ref<string[]>([])
  const tradingDateSet = computed(() => new Set(tradingDates.value))
  const calendarReady = computed(() => tradingDates.value.length > 0)

  function setTradingCalendar(dates: string[], today: string) {
    tradingDates.value = [...new Set(dates)].filter(day => day <= today).sort()
  }

  function isTradingDate(day: string) {
    return tradingDateSet.value.has(day)
  }

  function normalizeTradingDate(day: string) {
    return tradingDates.value.filter(date => date <= day).pop() || ''
  }

  function isDateDisabled(date: Date) {
    // Date picker cells represent local calendar dates, independent of the China clock.
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    return !isTradingDate(`${year}-${month}-${day}`)
  }

  return { calendarReady, setTradingCalendar, isTradingDate, normalizeTradingDate, isDateDisabled }
}
