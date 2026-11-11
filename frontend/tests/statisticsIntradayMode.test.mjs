import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import ts from 'typescript'
import * as vue from 'vue'
import dayjs from 'dayjs'

const source = readFileSync(new URL('../src/views/Statistics.vue', import.meta.url), 'utf8')
const transpile = text => ts.transpileModule(text, {
  compilerOptions: { module: ts.ModuleKind.ES2022, target: ts.ScriptTarget.ES2022 }
}).outputText
const { buildReviewRange } = await import(`data:text/javascript,${encodeURIComponent(transpile(
  readFileSync(new URL('../src/utils/reviewRange.ts', import.meta.url), 'utf8')
))}`)
const script = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
const ast = ts.createSourceFile('Statistics.ts', script, ts.ScriptTarget.ES2022, true)
const code = transpile(ast.statements.filter(statement => !ts.isImportDeclaration(statement))
  .map(statement => statement.getText(ast)).join('\n'))

const today = '2026-11-11'
const monday = '2026-11-09'
const friday = '2026-11-06'
const archiveDay = '2026-03-31'
const row = (day, extra = {}) => ({
  trade_date: day, limit_up_count: 3, limit_down_count: 1, continuous_count: 1,
  max_board_height: 2, first_to_second_rate: 20, seal_rate: 66.67,
  market_turnover: 17000, up_count_ex_st: 3100, down_count_ex_st: 1700,
  ...extra
})
const daily = (...rows) => ({ data: { series: rows.map(item => item.trade_date), rows } })
const snapshot = (day, extra = {}) => ({
  ...daily(row(day, extra)), data_status: 'ready', is_fallback: false,
  is_live: day === today, snapshot_time: `${day}T10:06:15+08:00`,
  detail: { trade_date: day, is_fallback: false, stocks: [{ stock_code: day, today_sealed_close: true }] },
  ladder: { trade_date: day, is_fallback: false, ladders: [{ continuous_days: 2 }] }
})
const flush = () => new Promise(resolve => setImmediate(resolve))

function loadReport(auto = true, getMarketReviewArchiveDates = async () => ({ dates: [] })) {
  const mounted = [], unmounted = [], requests = [], intervals = new Map()
  let timerId = 0
  function request(kind, day, response, query) {
    return new Promise(resolve => {
      requests.push({ kind, day, query, resolve })
      if (auto) resolve(response)
    })
  }
  const scope = {
    ...vue, dayjs, buildReviewRange,
    useRouter: () => ({ push() {} }), getChinaDateString: () => today,
    onMounted(callback) { mounted.push(callback) },
    onUnmounted(callback) { unmounted.push(callback) },
    getLimitUpDates: async () => ({ today, dates: [friday, monday, today] }),
    getMarketReviewArchiveDates,
    getMarketReviewDaily: query => request('daily', query.end_date, daily(row(friday)), query),
    getMarketReviewIntraday: day => request('snapshot', day, snapshot(day)),
    getBrokenBoardPerformance: query => request('broken', query.end_date, { points: [] }, query),
    document: { visibilityState: 'visible' },
    window: {
      setInterval(callback, delay) { intervals.set(++timerId, { callback, delay }); return timerId },
      clearInterval(id) { intervals.delete(id) },
      addEventListener() {}, removeEventListener() {}
    }
  }
  const effects = vue.effectScope()
  const state = effects.run(() => new Function(...Object.keys(scope), `${code}\nreturn {
    selectedDate: typeof selectedDate === 'undefined' ? undefined : selectedDate,
    moveDate: typeof moveDate === 'undefined' ? undefined : moveDate,
    goToToday: typeof goToToday === 'undefined' ? undefined : goToToday,
    overviewMetrics: typeof overviewMetrics === 'undefined' ? undefined : overviewMetrics,
    dailyRows, detailStocks, ladderLevels, isLiveIntraday, loading,
    currentDataUnavailable, brokenBoardPoints, availableDates, detailResponse,
    isExchangeTurnoverArchive: typeof isExchangeTurnoverArchive === 'undefined' ? undefined : isExchangeTurnoverArchive
  }`)(...Object.values(scope)))
  return {
    ...state, requests,
    forDate(day, kind) { return requests.find(item => item.day === day && item.kind === kind) },
    async mount() { mounted.forEach(callback => callback()); await flush() },
    async tick() { for (const timer of intervals.values()) {
      assert.equal(timer.delay, 60000)
      timer.callback()
    } await flush() },
    async select(day) {
      assert.ok(state.selectedDate, 'report needs a selected date')
      state.selectedDate.value = day
      await flush()
    },
    dispose() { unmounted.forEach(callback => callback()); effects.stop() }
  }
}

test('selected date scopes trend, snapshot and broken-board comparison, with arrows and return to live today', async () => {
  const state = loadReport()
  try {
    await state.mount()
    assert.equal(state.isLiveIntraday.value, true)
    await state.select(monday)
    assert.deepEqual(state.requests.slice(-3).map(item => [item.kind, item.day]), [
      ['broken', monday], ['daily', monday], ['snapshot', monday]
    ])
    assert.equal(state.detailStocks.value[0].stock_code, monday)
    assert.equal(state.isLiveIntraday.value, false)
    const historicalRequestCount = state.requests.length
    await state.tick()
    assert.equal(state.requests.length, historicalRequestCount, 'historical dates never poll')
    state.moveDate(-1)
    await flush()
    assert.equal(state.selectedDate.value, friday, 'previous skips the weekend')
    state.goToToday()
    await flush()
    assert.equal(state.detailStocks.value[0].stock_code, today)
    await state.tick()
    assert.equal(state.requests.at(-1).day, today)
    assert.equal(state.isLiveIntraday.value, true)
    assert.equal(state.loading.value, false, 'automatic refresh does not hide current data')
  } finally { state.dispose() }
})

test('late responses and fallback details never appear under the newly selected date', async () => {
  const state = loadReport(false)
  try {
    await state.mount()
    await state.select(monday)
    state.forDate(monday, 'daily').resolve(daily(row(friday), row(today)))
    state.forDate(monday, 'snapshot').resolve({ ...snapshot(friday), is_fallback: true })
    state.forDate(monday, 'broken').resolve({ points: [{ trade_date: monday }, { trade_date: today }] })
    await flush()
    assert.deepEqual(state.dailyRows.value.map(item => item.trade_date), [friday])
    assert.deepEqual(state.detailStocks.value, [])
    assert.deepEqual(state.ladderLevels.value, [])
    assert.equal(state.currentDataUnavailable.value, true)
    assert.deepEqual(state.brokenBoardPoints.value.map(item => item.trade_date), [monday])
    state.forDate(today, 'daily').resolve(daily(row(today)))
    state.forDate(today, 'snapshot').resolve(snapshot(today))
    state.forDate(today, 'broken').resolve({ points: [{ trade_date: today }] })
    await flush()
    assert.deepEqual(state.dailyRows.value.map(item => item.trade_date), [friday])
    assert.deepEqual(state.detailStocks.value, [])
    assert.deepEqual(state.brokenBoardPoints.value.map(item => item.trade_date), [monday])
    assert.equal(state.loading.value, false)
  } finally { state.dispose() }
})

test('old monitoring samples only contribute verified selected-day metrics and stay out of market trends', async () => {
  const state = loadReport(false)
  try {
    await state.mount()
    await state.select(monday)
    const sample = { source_status: 'monitor_archive', market_turnover: 0, up_count_ex_st: 0, limit_down_count: 0 }
    state.forDate(monday, 'daily').resolve(daily(row(friday), row(monday, sample)))
    state.forDate(monday, 'snapshot').resolve(snapshot(monday, sample))
    state.forDate(monday, 'broken').resolve({ points: [] })
    await flush()
    assert.equal(state.currentDataUnavailable.value, false)
    assert.deepEqual(state.dailyRows.value.map(item => item.trade_date), [friday])
    assert.deepEqual(state.overviewMetrics.value.map(item => item.label), [
      '触板家数', '封板家数', '炸板家数', '连板家数', '最高板', '封板率'
    ])
    assert.equal(state.detailStocks.value[0].stock_code, monday)
  } finally { state.dispose() }
})

test('navigation merges archive dates without delaying the existing trading calendar', async () => {
  let resolveArchive
  const state = loadReport(true, () => new Promise(resolve => { resolveArchive = resolve }))
  try {
    await state.mount()
    assert.deepEqual(state.availableDates.value, [friday, monday, today])
    assert.equal(typeof resolveArchive, 'function', 'loads the archive dates API')
    resolveArchive({ dates: [archiveDay, friday, '2099-01-01'] })
    await flush()
    assert.deepEqual(state.availableDates.value, [archiveDay, friday, monday, today])
    await state.select(friday)
    state.moveDate(-1)
    await flush()
    assert.equal(state.selectedDate.value, archiveDay)
    state.moveDate(1)
    await flush()
    assert.equal(state.selectedDate.value, friday)
  } finally { state.dispose() }

  const fallback = loadReport(true, async () => { throw Error('archive dates unavailable') })
  try {
    await fallback.mount()
    assert.deepEqual(fallback.availableDates.value, [friday, monday, today])
  } finally { fallback.dispose() }
})

test('verified exchange archive shows only turnover and never becomes a full report', async () => {
  const state = loadReport(false)
  try {
    await state.mount()
    await state.select(archiveDay)
    const archived = { trade_date: archiveDay, source_status: 'exchange_turnover_archive', market_turnover: 14567.89 }
    state.forDate(archiveDay, 'daily').resolve(daily(archived))
    state.forDate(archiveDay, 'snapshot').resolve({
      ...snapshot(archiveDay), ...daily(archived), source_status: archived.source_status,
      detail: { trade_date: archiveDay, is_fallback: false, stocks: [] },
      ladder: { trade_date: archiveDay, is_fallback: false, ladders: [] }
    })
    state.forDate(archiveDay, 'broken').resolve({ points: [] })
    await flush()
    assert.equal(state.currentDataUnavailable.value, false)
    assert.equal(state.isExchangeTurnoverArchive?.value, true)
    assert.deepEqual(state.overviewMetrics.value, [{ label: '沪深成交额（交易所）', value: '14567.89亿' }])
    assert.deepEqual(state.dailyRows.value, [])
    assert.equal(state.detailResponse.value, null, 'no zero-count summary for an unavailable report')
    assert.deepEqual(state.ladderLevels.value, [])
  } finally { state.dispose() }
})
