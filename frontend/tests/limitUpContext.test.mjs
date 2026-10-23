import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import ts from 'typescript'
import { createPinia, setActivePinia } from 'pinia'

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
      first_limit_up_time: '09:32:00', is_sealed: true, seal_amount: 1200,
      amount: 20000, tradable_market_value: 30000, limit_up_reason: '原因,说明',
      yesterday_first_limit_up_time: '9:30:01', yesterday_final_seal_time: '14:56:00',
      yesterday_open_change_pct: -1.08, today_open_change_pct: 3.25
    },
    {
      stock_code: '000002', stock_name: '另一股份', continuous_limit_up_days: 1,
      is_sealed: false, yesterday_open_change_pct: 0, today_open_change_pct: null
    }
  ])
  assert.equal(csv, [
    '代码,名称,连板,首封时间,昨日首封,昨日终封,昨日竞价,今日竞价,状态,封单(万),成交额(亿),实际流通值(亿),涨停原因',
    '000001,示例股份,2,09:32:00,09:30:01,14:56:00,-1.08%,+3.25%,封板,1200,2.00,3.00,"原因,说明"',
    '000002,另一股份,1,,—,—,0.00%,—,开板,,-,-,'
  ].join('\n'))
})

test('desktop, mobile and CSV are wired to the shared context presentation', () => {
  const desktop = readFileSync(resolve(root, 'src/views/LimitUpList.vue'), 'utf8')
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
})

const { useLimitUpStore } = await loadTypeScript(resolve(root, 'src/stores/limit-up.ts'), true)
const richRow = {
  stock_code: '000001', stock_name: '示例股份', trade_date: '2026-10-23',
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
