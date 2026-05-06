import { readFileSync } from 'node:fs'
import test from 'node:test'
import assert from 'node:assert/strict'

const source = readFileSync('src/views/Statistics.vue', 'utf8')

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
  assert.match(source, /class="chart-container board-height-chart"/, 'board chart should have more vertical space')
  assert.match(source, /function isBoardHeightLabelPoint/, 'changed height points should be eligible for labels')
  assert.match(source, /currentHeight !== prevHeight \|\| currentHeight !== nextHeight/)
  assert.match(source, /labelLayout:\s*\{\s*hideOverlap: true/s)
  assert.match(source, /getBoardHeightLabelOption\('max_board_label', 'top', \[0, -6\]\)/)
  assert.match(source, /getBoardHeightLabelOption\('second_board_label', 'bottom', \[0, 16\]\)/)
  assert.match(source, /return `\$\{lines\[0\]\} 等\$\{lines\.length\}只`/)
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
