import { readFileSync } from 'node:fs'
import test from 'node:test'
import assert from 'node:assert/strict'

const source = readFileSync('src/views/Statistics.vue', 'utf8').replace(/\r\n/g, '\n')

test('board height chart displays stock labels from daily rows', () => {
  assert.match(source, /max_board_label/, 'max board labels should be used in the chart')
  assert.match(source, /second_board_label/, 'second board labels should be used in the chart')
  assert.match(source, /gem_board_label/, 'GEM board labels should be used in the chart')
  assert.match(source, /function getBoardLabelFormatter/, 'chart point labels should have a formatter')
  assert.match(source, /function shouldShowBoardHeightLabel/, 'labels should be filtered to avoid crowding')
  assert.match(source, /function formatBoardHeightTooltip/, 'full labels should remain available in tooltip')
  assert.match(source, /tooltip:\s*\{\s*trigger: 'axis',\s*formatter: formatBoardHeightTooltip/s)
  assert.match(source, /label:\s*getBoardHeightLabelOption\('max_board_label', 'top', \[0, -6\]\)/)
  assert.match(source, /label:\s*getBoardHeightLabelOption\('second_board_label', 'bottom', \[0, 16\]\)/)
  assert.match(source, /label:\s*getBoardHeightLabelOption\('gem_board_label', 'bottom', \[0, 28\]\)/)
})

test('board height labels keep more data with layout controls', () => {
  assert.match(source, /ref="boardHeightChartRef" class="chart-container review-main-chart"/)
  assert.match(source, /ref="promotionRateChartRef" class="chart-container review-main-chart"/)
  assert.match(source, /function isBoardHeightLabelPoint/, 'changed height points should be eligible for labels')
  assert.match(source, /currentHeight !== prevHeight \|\| currentHeight !== nextHeight/)
  assert.match(source, /labelLayout:\s*\{\s*hideOverlap: true/s)
  assert.match(source, /getBoardHeightLabelOption\('max_board_label', 'top', \[0, -6\]\)/)
  assert.match(source, /getBoardHeightLabelOption\('second_board_label', 'bottom', \[0, 16\]\)/)
  assert.match(source, /if \(lines\.length <= 3\) \{[\s\S]*return lines\.join\('\\n'\)/)
  assert.match(source, /return `\$\{lines\.slice\(0, 3\)\.join\('\\n'\)\}\\n等\$\{lines\.length\}只`/)
  assert.match(source, /\.review-main-chart\s*\{[\s\S]*height: 380px/)
  const boardLabelOption = source.match(/function getBoardHeightLabelOption[\s\S]*?function getPercentPointLabelOption/)
  assert.ok(boardLabelOption, 'board label option should exist')
  assert.doesNotMatch(boardLabelOption[0], /backgroundColor|borderRadius|padding/, 'board stock labels should not use a white tag background')
})

test('yesterday feedback chart uses straight line series', () => {
  const match = source.match(/yesterdayChangeChart\?\.setOption\([\s\S]*?limitTrendChart\?\.setOption/)
  assert.ok(match, 'yesterday feedback chart option should exist')
  const optionSource = match[0]

  assert.match(optionSource, /name: '昨日涨停平均涨幅'[\s\S]*?type: 'line'/)
  assert.match(optionSource, /name: '昨日连板平均涨幅'[\s\S]*?type: 'line'/)
  assert.doesNotMatch(optionSource, /smooth: true/, 'yesterday feedback lines should not be smoothed')
  assert.doesNotMatch(optionSource, /type: 'bar'/, 'yesterday feedback chart should not use bars')
})

test('promotion and yesterday feedback charts show direct percent readings', () => {
  const promotionMatch = source.match(/promotionRateChart\?\.setOption\([\s\S]*?yesterdayChangeChart\?\.setOption/)
  assert.ok(promotionMatch, 'promotion rate chart option should exist')
  const promotionSource = promotionMatch[0]

  assert.match(source, /function getPercentPointLabelOption/)
  assert.match(source, /function formatPercentAxisLabel/)
  assert.match(promotionSource, /min:\s*0/)
  assert.match(promotionSource, /max:\s*100/)
  assert.match(promotionSource, /label:\s*getPercentPointLabelOption\('top'\)/)
  assert.match(promotionSource, /labelLayout:\s*\{\s*hideOverlap: true/s)

  const yesterdayMatch = source.match(/yesterdayChangeChart\?\.setOption\([\s\S]*?limitTrendChart\?\.setOption/)
  assert.ok(yesterdayMatch, 'yesterday feedback chart option should exist')
  const yesterdaySource = yesterdayMatch[0]

  assert.match(yesterdaySource, /label:\s*getPercentPointLabelOption\('top', true\)/)
  assert.match(yesterdaySource, /label:\s*getPercentPointLabelOption\('bottom', true\)/)
  assert.match(yesterdaySource, /markLine:\s*\{[\s\S]*yAxis:\s*0/s)
})

test('limit and broken amount chart uses hundred-million unit', () => {
  const match = source.match(/amountChart\?\.setOption\([\s\S]*?\n  \)\n\}/)
  assert.ok(match, 'amount chart option should exist')
  const optionSource = match[0]

  assert.match(source, /function toYiFromWanAmount/)
  assert.match(source, /return Number\(\(value \/ 10000\)\.toFixed\(2\)\)/)
  assert.match(source, /function formatYiAmount/)
  assert.match(optionSource, /name:\s*'亿元'/)
  assert.match(optionSource, /formatter:\s*\(value: number\) => formatYiAmount\(value\)/)
  assert.match(optionSource, /data:\s*dailyRows\.value\.map\(row => toYiFromWanAmount\(row\.limit_up_amount\)\)/)
  assert.match(optionSource, /data:\s*dailyRows\.value\.map\(row => toYiFromWanAmount\(row\.broken_amount\)\)/)
  assert.match(optionSource, /valueFormatter:\s*\(value: unknown\) => formatYiAmount\(Number\(value\)\)/)
})

test('ladder groups show stats in a separate compact metrics row', () => {
  const match = source.match(/<div class="ladder-metrics">[\s\S]*?<\/div>/)
  assert.ok(match, 'ladder metrics row should exist')
  const metricsSource = match[0]

  assert.match(metricsSource, /封板[\s\S]*getSealedCount\(ladder\)/)
  assert.match(metricsSource, /炸板[\s\S]*getOpenedCount\(ladder\)/)
  assert.match(metricsSource, /封板率[\s\S]*getLadderSealRate\(ladder\)/)
  assert.match(metricsSource, /均涨[\s\S]*getLadderAverageChange\(ladder\)/)
  assert.match(source, /function getLadderSealRate\(ladder: MarketReviewLadderLevel\)/)
  assert.match(source, /function getLadderAverageChange\(ladder: MarketReviewLadderLevel\)/)
  assert.match(source, /\.ladder-metrics\s*\{[\s\S]*grid-template-columns: repeat\(4, minmax\(0, 1fr\)\)/)
})

test('ladder seal rate and average change use same-cohort metrics from backend', () => {
  const typeSource = readFileSync('src/types/market.ts', 'utf8').replace(/\r\n/g, '\n')
  assert.match(typeSource, /cohort_count:\s*number/)
  assert.match(typeSource, /cohort_seal_rate:\s*number/)
  assert.match(typeSource, /cohort_avg_change:\s*number \| null/)
  assert.match(source, /return formatRate\(ladder\.cohort_seal_rate\)/)
  assert.match(source, /return ladder\.cohort_avg_change/)
})
