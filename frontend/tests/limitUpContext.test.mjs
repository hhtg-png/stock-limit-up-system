import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import ts from 'typescript'
import { createPinia, setActivePinia } from 'pinia'
import { computed, ref } from 'vue'

const root = resolve(import.meta.dirname, '..')
const helperPath = resolve(root, 'src/utils/limitUpContext.ts')

async function loadTypeScript(path, resolveImports = false) {
  let source = ts.transpileModule(readFileSync(path, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.ES2022, target: ts.ScriptTarget.ES2022 }
  }).outputText
  if (resolveImports) {
    source = source.replace(/from ['"](pinia|vue)['"]/g, (_, name) => `from '${import.meta.resolve(name)}'`)
  }
  return import(`data:text/javascript;charset=utf-8,${encodeURIComponent(source)}`)
}

const helper = existsSync(helperPath) ? await loadTypeScript(helperPath) : null

test('context time renders seconds and missing values without inventing a time', () => {
  assert.ok(helper, 'shared limit-up context formatter is missing')
  for (const [value, expected] of [
    ['09:31:02', '09:31:02'],
    ['9:31:02', '09:31:02'],
    ['2026-10-22T14:53:07', '14:53:07'],
    ['09:25', '09:25:00'],
    [null, '—'],
    [undefined, '—'],
    ['', '—'],
    ['invalid', '—'],
    ['25:00:00', '—']
  ]) {
    assert.equal(helper.formatLimitUpContextTime(value), expected)
  }
})

test('opening percentages retain real zero, sign, precision and missing data', () => {
  assert.ok(helper, 'shared limit-up context formatter is missing')
  for (const [value, expected] of [
    [3.25, '+3.25%'],
    [-1.08, '-1.08%'],
    [2.345, '+2.35%'],
    [0, '0.00%'],
    [-0, '0.00%'],
    [-0.001, '0.00%'],
    [null, '—'],
    [undefined, '—'],
    [Number.NaN, '—'],
    [Number.POSITIVE_INFINITY, '—']
  ]) {
    assert.equal(helper.formatOpeningChangePct(value), expected)
  }
})

test('CSV exports the same context values including zero and missing data', () => {
  assert.ok(helper, 'shared limit-up context formatter is missing')
  const csv = helper.buildLimitUpCsv([
    {
      stock_code: '000001', stock_name: '示例股份', continuous_limit_up_days: 2,
      context_date: '2026-10-23', trade_date: '2026-10-23', previous_trade_date: '2026-10-22',
      first_limit_up_time: '09:32:00', is_sealed: true, seal_amount: 1200,
      amount: 20000, tradable_market_value: 30000, limit_up_reason: '原因,说明',
      yesterday_first_limit_up_time: '9:30:01', yesterday_final_seal_time: '14:56:00',
      yesterday_open_change_pct: -1.08, today_open_change_pct: 3.25
    },
    {
      stock_code: '000002', stock_name: '另一股份', continuous_limit_up_days: 1,
      context_date: '2026-10-23', trade_date: '2026-10-23', previous_trade_date: '2026-10-22',
      is_sealed: false, yesterday_open_change_pct: 0, today_open_change_pct: null
    }
  ])
  assert.equal(csv, [
    '代码,名称,行情日期,昨日日期,连板,首封时间,昨日首封,昨日终封,昨日竞价,今日竞价,状态,封单(万),成交额(亿),实际流通值(亿),涨停原因',
    '000001,示例股份,2026-10-23,2026-10-22,2,09:32:00,09:30:01,14:56:00,-1.08%,+3.25%,封板,1200,2.00,3.00,"原因,说明"',
    '000002,另一股份,2026-10-23,2026-10-22,1,,—,—,0.00%,—,开板,,-,-,'
  ].join('\n'))
})

for (const view of ['Dashboard', 'LimitUpList']) test(`${view} desktop, mobile and CSV use shared context presentation`, () => {
  const desktop = readFileSync(resolve(root, `src/views/${view}.vue`), 'utf8')
  const mobile = readFileSync(resolve(root, 'src/components/stock/MobileLimitUpCards.vue'), 'utf8')
  const fields = [
    ['yesterday_first_limit_up_time', '昨日首封', 'formatLimitUpContextTime'],
    ['yesterday_final_seal_time', '昨日终封', 'formatLimitUpContextTime'],
    ['yesterday_open_change_pct', '昨日竞价', 'formatOpeningChangePct'],
    ['today_open_change_pct', '今日竞价', 'formatOpeningChangePct']
  ]
  for (const [field, label, formatter] of fields) {
    assert.match(desktop, new RegExp(`prop="${field}"[^>]*label="${label}"`), `desktop should expose ${label}`)
    assert.match(desktop, new RegExp(`${formatter}\\(row\\.${field}\\)`), `desktop should format ${label}`)
    assert.match(mobile, new RegExp(`<span>${label}</span>[\\s\\S]*?${formatter}\\(item\\.${field}\\)`), `mobile should format ${label}`)
  }
  assert.match(desktop, /const csv = buildLimitUpCsv\(tableData\.value\)/, 'download should use the tested CSV builder')
  assert.match(desktop, /昨日日期\s*\{\{\s*previousTradeDate\s*\}\}/, 'yesterday values should have an explicit business date')
  assert.match(desktop, /@click="setSortBy\('reseal_time'\)"[^>]*>回封<\/el-button>\s*<el-button[^>]*@click="setSortBy\('yesterday_final_seal_time'\)"[^>]*>昨日终封<\/el-button>/, 'yesterday seal sort should follow the reseal button')
  for (const field of ['yesterday_open_change_pct', 'today_open_change_pct']) {
    assert.match(desktop, new RegExp(`@click="setSortBy\\('${field}'\\)"`), `${field} should have a toolbar sort`)
  }
  assert.doesNotMatch(desktop, /sortable="custom"|@sort-change=/, 'sorting should use the toolbar only')
  assert.doesNotMatch(desktop, /<el-button[^>]*>查询<\/el-button>/, 'filters should query without a query button')
})

for (const view of ['Dashboard', 'LimitUpList']) test(`${view} downloads context CSV named for its effective data date`, async () => {
  const source = readFileSync(resolve(root, `src/views/${view}.vue`), 'utf8')
  const script = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
  const ast = ts.createSourceFile(`${view}.ts`, script, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS)
  const functions = ast.statements
    .filter(node => ts.isFunctionDeclaration(node) && ['exportData', 'formatAmountInYi', 'formatTradableMarketValue'].includes(node.name?.text))
    .map(node => node.getText(ast)).join('\n')
  const executable = ts.transpileModule(functions, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText
  const row = {
    stock_code: '000001', stock_name: '示例股份', continuous_limit_up_days: 1,
    context_date: '2026-10-23', trade_date: '2026-10-23', previous_trade_date: '2026-10-22',
    first_limit_up_time: '09:32:00', is_sealed: true,
    yesterday_first_limit_up_time: '09:30:01', yesterday_final_seal_time: '14:56:00',
    yesterday_open_change_pct: 0, today_open_change_pct: 3.25
  }
  for (const [effectiveDate, expectedDate] of [['2026-10-23', '2026-10-23'], ['', '2026-10-25']]) {
    let downloadBlob, clicked = false
    const revoked = []
    const anchor = { href: '', download: '', click() { clicked = true } }
    const scope = {
      tableData: { value: [row] }, displayTradeDate: { value: effectiveDate }, selectedDate: { value: '2026-10-25' },
      buildLimitUpCsv: helper.buildLimitUpCsv, Blob,
      URL: { createObjectURL(blob) { downloadBlob = blob; return 'blob:context-csv' }, revokeObjectURL(url) { revoked.push(url) } },
      document: { createElement(tag) { assert.equal(tag, 'a'); return anchor } }
    }
    const exportData = new Function(...Object.keys(scope), `${executable}\nreturn exportData`)(...Object.values(scope))
    exportData()
    assert.equal(clicked, true)
    assert.equal(anchor.download, `涨停列表_${expectedDate}.csv`)
    assert.equal(await downloadBlob.text(), [
      '代码,名称,行情日期,昨日日期,连板,首封时间,昨日首封,昨日终封,昨日竞价,今日竞价,状态,封单(万),成交额(亿),实际流通值(亿),涨停原因',
      '000001,示例股份,2026-10-23,2026-10-22,1,09:32:00,09:30:01,14:56:00,0.00%,+3.25%,封板,,-,-,'
    ].join('\n'))
    assert.deepEqual(revoked, ['blob:context-csv'])
  }
})

const { useLimitUpStore } = await loadTypeScript(resolve(root, 'src/stores/limit-up.ts'), true)
const richRow = {
  stock_code: '000001', stock_name: '示例股份', trade_date: '2026-10-23',
  context_date: '2026-10-23', previous_trade_date: '2026-10-22',
  yesterday_first_limit_up_time: '09:30:01', yesterday_final_seal_time: '14:56:00',
  yesterday_open_change_pct: 0, today_open_change_pct: 3.25
}

test('rich snapshots and sparse same-session updates retain all four context fields', () => {
  setActivePinia(createPinia())
  const store = useLimitUpStore()
  store.setRealtimeSnapshot('2026-10-23', [richRow])
  store.applyRealtimeDelta([{ stock_code: '000001', current_price: 12.34 }], [], '2026-10-23')
  store.updateItem('000001', { seal_amount: 2500 })
  assert.deepEqual({ ...store.realtimeList[0] }, { ...richRow, current_price: 12.34, seal_amount: 2500 })

  store.setRealtimeSnapshot('2026-10-23', [{ ...richRow, today_open_change_pct: -1.08 }])
  assert.equal(store.realtimeList[0].yesterday_open_change_pct, 0)
  assert.equal(store.realtimeList[0].today_open_change_pct, -1.08)
  assert.equal(store.realtimeList[0].context_date, '2026-10-23')
  assert.equal(store.realtimeList[0].previous_trade_date, '2026-10-22')
})

test('yesterday seal sorting is stable, nonmutating and keeps missing times last in both directions', () => {
  assert.equal(typeof helper.sortByYesterdayFinalSealTime, 'function', 'shared yesterday seal sorter is missing')
  const rows = Object.freeze([
    { stock_code: '000005', yesterday_final_seal_time: undefined },
    { stock_code: '000004', yesterday_final_seal_time: '14:56:00' },
    { stock_code: '000003', yesterday_final_seal_time: ' 09:25:00 ' },
    { stock_code: '000001', yesterday_final_seal_time: '09:25:00' },
    { stock_code: '000002', yesterday_final_seal_time: null },
    { stock_code: '000006', yesterday_final_seal_time: 'invalid' }
  ])
  assert.deepEqual(helper.sortByYesterdayFinalSealTime(rows).map(row => row.stock_code), ['000001', '000003', '000004', '000002', '000005', '000006'])
  assert.deepEqual(helper.sortByYesterdayFinalSealTime(rows, 'desc').map(row => row.stock_code), ['000004', '000001', '000003', '000002', '000005', '000006'])
  assert.deepEqual(rows.map(row => row.stock_code), ['000005', '000004', '000003', '000001', '000002', '000006'])
})

test('yesterday seal sorting treats non-HH:mm:ss values as missing without changing display formatting', () => {
  for (const value of ['9:25:00', '2026-10-23T09:25:00', '09:25', '09:25:00.000', '24:00:00', '09:60:00', '09:25:60', 'invalid', '']) {
    const rows = [
      { stock_code: '000001', yesterday_final_seal_time: value },
      { stock_code: '000002', yesterday_final_seal_time: '12:00:00' }
    ]
    for (const direction of ['asc', 'desc']) {
      assert.deepEqual(helper.sortByYesterdayFinalSealTime(rows, direction).map(row => row.stock_code), ['000002', '000001'], `${direction} must put malformed ${value} last`)
    }
  }
  assert.equal(helper.formatLimitUpContextTime('9:25:00'), '09:25:00')
  assert.equal(helper.formatLimitUpContextTime('2026-10-23T09:25:00'), '09:25:00')
})

for (const view of ['Dashboard', 'LimitUpList']) test(`${view} keeps yesterday seal sort and business dates after realtime updates`, () => {
  setActivePinia(createPinia())
  const store = useLimitUpStore()
  const source = readFileSync(resolve(root, `src/views/${view}.vue`), 'utf8')
  const script = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
  const ast = ts.createSourceFile(`${view}.ts`, script, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS)
  const declarations = ast.statements.filter(node => ts.isVariableStatement(node) && node.declarationList.declarations.some(
    item => ['tableData', 'apiSort', 'previousTradeDate'].includes(item.name.getText(ast))
  )).map(node => node.getText(ast)).join('\n')
  const executable = ts.transpileModule(declarations, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText
  const sortBy = ref('yesterday_final_seal_time')
  const tableSort = ref({ prop: 'yesterday_final_seal_time', order: 'ascending' })
  const scope = { computed, limitUpStore: store, sortBy, tableSort, sortByYesterdayFinalSealTime: helper.sortByYesterdayFinalSealTime, sortByOpeningChangePct: helper.sortByOpeningChangePct, sortByLimitUpField: helper.sortByLimitUpField }
  const state = new Function(...Object.keys(scope), `${executable}\nreturn { tableData, apiSort, previousTradeDate: typeof previousTradeDate === 'undefined' ? undefined : previousTradeDate }`)(...Object.values(scope))
  store.setSnapshot('2026-10-23', [
    { ...richRow, stock_code: '000003', yesterday_final_seal_time: '11:00:00' },
    { ...richRow, stock_code: '000002', yesterday_final_seal_time: null },
    { ...richRow, stock_code: '000001', yesterday_final_seal_time: '09:25:00' }
  ])
  assert.deepEqual(state.tableData.value.map(row => row.stock_code), ['000001', '000003', '000002'])
  assert.deepEqual(state.apiSort.value, { sortBy: 'yesterday_final_seal_time', sortOrder: 'asc' })
  assert.equal(state.previousTradeDate?.value, '2026-10-22')
  store.applyRealtimeDelta([{ stock_code: '000002', yesterday_final_seal_time: '10:00:00' }], [], '2026-10-23')
  assert.deepEqual(state.tableData.value.map(row => row.stock_code), ['000001', '000002', '000003'])
  assert.equal(state.tableData.value[1].context_date, '2026-10-23')
  for (const field of ['yesterday_open_change_pct', 'today_open_change_pct']) {
    sortBy.value = field
    store.applyRealtimeDelta([
      { stock_code: '000003', [field]: -1 }, { stock_code: '000002', [field]: 0 }, { stock_code: '000001', [field]: null }
    ], [], '2026-10-23')
    assert.deepEqual(state.tableData.value.map(row => row.stock_code), ['000002', '000003', '000001'])
    assert.deepEqual(state.apiSort.value, { sortBy: field, sortOrder: 'desc' })
  }
  sortBy.value = 'time'
  assert.deepEqual(state.tableData.value.map(row => row.stock_code), ['000003', '000002', '000001'])
})

test('opening percentage sorts preserve zero and negatives and always place invalid values last', () => {
  assert.equal(typeof helper.sortByOpeningChangePct, 'function', 'shared opening percentage sorter is missing')
  for (const field of ['yesterday_open_change_pct', 'today_open_change_pct']) {
    const rows = Object.freeze([
      { stock_code: '000005', [field]: undefined }, { stock_code: '000004', [field]: -1.08 },
      { stock_code: '000003', [field]: 0 }, { stock_code: '000001', [field]: 0 },
      { stock_code: '000002', [field]: 3.25 }, { stock_code: '000006', [field]: Number.NaN },
      { stock_code: '000007', [field]: Number.POSITIVE_INFINITY }, { stock_code: '000008', [field]: null }
    ])
    assert.deepEqual(helper.sortByOpeningChangePct(rows, field).map(row => row.stock_code), ['000002', '000001', '000003', '000004', '000005', '000006', '000007', '000008'])
    assert.deepEqual(helper.sortByOpeningChangePct(rows, field, 'asc').map(row => row.stock_code), ['000004', '000001', '000003', '000002', '000005', '000006', '000007', '000008'])
    assert.deepEqual(rows.map(row => row.stock_code), ['000005', '000004', '000003', '000001', '000002', '000006', '000007', '000008'])
  }
})

test('a new trading session never inherits the previous session context', () => {
  setActivePinia(createPinia())
  const store = useLimitUpStore()
  store.setRealtimeSnapshot('2026-10-23', [richRow])
  const nextRow = { stock_code: '000001', trade_date: '2026-10-26', today_open_change_pct: 0 }
  store.applyRealtimeDelta([nextRow], [], '2026-10-26')
  assert.equal(store.tradeDate, '2026-10-26')
  assert.deepEqual({ ...store.realtimeList[0] }, nextRow)
})
