import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import vm from 'node:vm'
import ts from 'typescript'

const source = readFileSync('src/views/Statistics.vue', 'utf8')
const fetchSource = source.slice(source.indexOf('async function fetchData('), source.indexOf('function goToDetail('))
function setup(intraday, dailyError = false) {
  const ref = value => ({ value })
  const today = '2026-09-11'
  const context = {
    console: { error() {}, warn() {} }, fetchSequence: 0,
    loading: ref(false), isLiveIntraday: ref(false), intradaySnapshotTime: ref(null),
    currentDataUnavailable: ref(false), brokenBoardPoints: ref([]),
    dailySeries: ref([today]), dailyRows: ref([{ trade_date: today }]), dailyHasFallback: ref(false),
    activeStartDate: ref(today), activeEndDate: ref(today), detailResponse: ref({ trade_date: '2026-09-10' }), ladderResponse: ref(null),
    getChinaDateString: () => today, getDateRange: () => ({ startDate: today, endDate: today, query: { days: 30 } }),
    getMarketReviewDaily: async () => { if (dailyError) throw Error('history down'); return { data: { series: ['2026-09-10'], rows: [{ trade_date: '2026-09-10' }] }, end_date: '2026-09-10', is_fallback: true } },
    getMarketReviewIntraday: async () => { if (intraday instanceof Error) throw intraday; return intraday },
    fetchBrokenBoardData() {},
    getBrokenBoardPerformance: async () => ({ points: [] }),
    getMarketReviewDetail: async () => ({ trade_date: '2026-09-10' }),
    getMarketReviewLadder: async () => ({ trade_date: '2026-09-10' }),
    mergeIntradayDailyRows: (series, rows, row) => ({ series: [...series, row.trade_date], rows: [...rows, row] }),
    updateCharts() {}, ElMessage: { error() {} }
  }
  vm.createContext(context)
  vm.runInContext(ts.transpileModule(fetchSource, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText, context)
  return context
}
test('unavailable live snapshot clears old details and keeps current date without fallback', async () => {
  const c = setup({ is_live: true, data_status: 'unavailable', data: { rows: [] } })
  await c.fetchData({ silent: true })
  assert.equal(c.detailResponse.value, null)
  assert.equal(c.activeEndDate.value, '2026-09-11')
  assert.equal(c.currentDataUnavailable.value, true)
  assert.equal(c.dailyHasFallback.value, false)
  assert.equal(c.dailyRows.value.some(row => row.trade_date === '2026-09-11'), false)
})
test('live data still appears when historical request fails', async () => {
  const row = { trade_date: '2026-09-11' }
  const c = setup({ is_live: true, data_status: 'ready', data: { rows: [row] }, detail: { trade_date: row.trade_date }, ladder: { trade_date: row.trade_date } }, true)
  await c.fetchData({ silent: true })
  assert.equal(c.detailResponse.value.trade_date, row.trade_date)
  assert.equal(c.isLiveIntraday.value, true)
})
test('network failure also clears previous current-day state', async () => {
  const c = setup(Error('timeout'))
  await c.fetchData({ silent: true })
  assert.equal(c.detailResponse.value, null)
  assert.equal(c.currentDataUnavailable.value, true)
})
test('broken-board tooltip lists each stock and escapes names', () => {
  const start = source.indexOf('function formatBrokenBoardTooltip(')
  assert.ok(start >= 0)
  const end = source.indexOf('\nfunction ', start + 1)
  const code = source.slice(start, end)
  const c = { brokenBoardPoints: { value: [{ trade_date: '2026-09-11', average_change: 1, sample_count: 2, priced_count: 1,
    stocks: [{ stock_code: '600001', stock_name: '<b>X</b>', previous_board: 3, change_pct: 2 }, { stock_code: '600002', stock_name: 'Y', previous_board: 2, change_pct: null }] }] },
    escapeTooltipHtml: s => s.replaceAll('<', '&lt;').replaceAll('>', '&gt;') }
  vm.createContext(c)
  vm.runInContext(ts.transpileModule(code, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText, c)
  const html = c.formatBrokenBoardTooltip([{ dataIndex: 0 }])
  assert.match(html, /600001/)
  assert.match(html, /600002/)
  assert.match(html, /2.00%/)
  assert.match(html, /暂无数据/)
  assert.doesNotMatch(html, /<b>X/)
  c.brokenBoardPoints.value[0].suspended_count = 1
  c.brokenBoardPoints.value[0].stocks[1].quote_status = 'suspended'
  const suspendedHtml = c.formatBrokenBoardTooltip([{ dataIndex: 0 }])
  assert.match(suspendedHtml, /有效行情 1\/1/)
  assert.match(suspendedHtml, /停牌 1 只/)
  assert.match(suspendedHtml, /停牌（不计入均值）/)
  assert.doesNotMatch(suspendedHtml, /暂无数据/)
})

test('slow cohort history survives minute refresh without duplicate requests', async () => {
  const start = source.indexOf('async function fetchBrokenBoardData(')
  const code = source.slice(start, source.indexOf('async function fetchData(', start))
  let resolve
  let calls = 0
  const c = { brokenBoardRequestKey: '', brokenBoardRequestPending: false, brokenBoardRequestSequence: 0,
    brokenBoardPoints: { value: [] }, updateBrokenBoardChart() {},
    getBrokenBoardPerformance: () => { calls++; return new Promise(done => { resolve = done }) } }
  vm.createContext(c)
  vm.runInContext(ts.transpileModule(code, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText, c)
  const query = { days: 250, end_date: '2026-09-11' }
  const first = c.fetchBrokenBoardData(query)
  await c.fetchBrokenBoardData(query)
  assert.equal(calls, 1)
  resolve({ points: [{ trade_date: '2026-09-11' }] })
  await first
  assert.equal(c.brokenBoardPoints.value.length, 1)
  assert.equal(c.brokenBoardRequestPending, false)
})
