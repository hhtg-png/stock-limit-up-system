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
  assert.match(source, /label:\s*getBoardHeightLabelOption\('max_board_label'\)/)
  assert.match(source, /label:\s*getBoardHeightLabelOption\('second_board_label'\)/)
  assert.match(source, /label:\s*getBoardHeightLabelOption\('gem_board_label', 'bottom'\)/)
})

test('board height labels cap crowded points', () => {
  assert.match(source, /function isDominantBoardHeightLabel/, 'only one board label should be eligible per date')
  assert.match(source, /function isSparseBoardHeightLabelPoint/, 'non-peak labels should be suppressed')
  assert.match(source, /!isDominantBoardHeightLabel\(rowIndex, field\)/)
  assert.match(source, /labelLayout:\s*\{\s*hideOverlap: true/s)
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

test('ladder groups avoid redundant sealed-only stats', () => {
  const match = source.match(/<div class="ladder-header">[\s\S]*?<\/div>/)
  assert.ok(match, 'ladder header should exist')
  const headerSource = match[0]

  assert.match(headerSource, /\{\{\s*ladder\.count\s*\}\}只/)
  assert.doesNotMatch(headerSource, /封板|炸板|封板率|均涨/)
  assert.doesNotMatch(headerSource, /getSealedCount|getOpenedCount|getLadderSealRate|getLadderAverageChange/)
  assert.doesNotMatch(source, /function getSealedCount/)
  assert.doesNotMatch(source, /function getOpenedCount/)
  assert.doesNotMatch(source, /function getLadderSealRate/)
  assert.doesNotMatch(source, /function getLadderAverageChange/)
})
