import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import ts from 'typescript'

const source = readFileSync(new URL('../src/views/Statistics.vue', import.meta.url), 'utf8')
const script = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
const ast = ts.createSourceFile('Statistics.ts', script, ts.ScriptTarget.ES2022, true)
const functions = ast.statements.filter(node => ts.isFunctionDeclaration(node)
  && ['getBaseGridOption', 'createEmptyOption', 'updateCharts'].includes(node.name?.text))
  .map(node => node.getText(ast)).join('\n')
const code = ts.transpileModule(functions, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText

test('limit trend plots exact sealed, continuous and down counts, preserving zero and missing data', () => {
  const calls = []
  const dailyRows = { value: [
    { trade_date: '2026-11-12', limit_up_count: 80, sealed_count: 62, continuous_count: 14, limit_down_count: 3 },
    { trade_date: '2026-11-13', limit_up_count: 9, sealed_count: 0, continuous_count: 0, limit_down_count: 0 },
    { trade_date: '2026-11-16', limit_up_count: 71, sealed_count: null, continuous_count: null, limit_down_count: null },
    { trade_date: '2026-11-17', limit_up_count: 100 }
  ] }
  const dailySeries = { value: dailyRows.value.map(row => row.trade_date) }
  const scope = {
    dailyRows, dailySeries, formatDateLabel: day => day.slice(5), updateBrokenBoardChart() {},
    boardHeightChart: null, promotionRateChart: null, yesterdayChangeChart: null,
    breadthChart: null, amountChart: null,
    limitTrendChart: { setOption(option, replace) { calls.push({ option, replace }) } }
  }
  const updateCharts = new Function(...Object.keys(scope), `${code}\nreturn updateCharts`)(...Object.values(scope))
  updateCharts()
  const { option, replace } = calls.at(-1)
  assert.equal(replace, true, 'old touch-count series must be replaced')
  assert.deepEqual(option.xAxis.data, dailySeries.value)
  assert.deepEqual(option.series.map(series => ({ name: series.name, data: series.data })), [
    { name: '封板数', data: [62, 0, null, null] },
    { name: '连板数', data: [14, 0, null, null] },
    { name: '跌停数', data: [3, 0, null, null] }
  ])

  dailyRows.value = [{ trade_date: '2026-11-18', limit_up_count: 28, sealed_count: 19, continuous_count: 4, limit_down_count: 2 }]
  dailySeries.value = ['2026-11-18']
  updateCharts()
  assert.deepEqual(calls.at(-1).option.xAxis.data, ['2026-11-18'])
  assert.deepEqual(calls.at(-1).option.series.map(series => series.data), [[19], [4], [2]])

  dailyRows.value = []
  dailySeries.value = []
  updateCharts()
  assert.deepEqual(calls.at(-1).option.series, [])
  assert.equal(calls.at(-1).replace, true)
})
