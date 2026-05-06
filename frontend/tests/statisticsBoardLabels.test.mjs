import { readFileSync } from 'node:fs'
import test from 'node:test'
import assert from 'node:assert/strict'

const source = readFileSync('src/views/Statistics.vue', 'utf8')

test('board height chart displays stock labels from daily rows', () => {
  assert.match(source, /max_board_label/, 'max board labels should be used in the chart')
  assert.match(source, /second_board_label/, 'second board labels should be used in the chart')
  assert.match(source, /gem_board_label/, 'GEM board labels should be used in the chart')
  assert.match(source, /function getBoardLabelFormatter/, 'chart point labels should have a formatter')
  assert.match(source, /label:\s*getBoardHeightLabelOption\('max_board_label'\)/)
  assert.match(source, /label:\s*getBoardHeightLabelOption\('second_board_label'\)/)
  assert.match(source, /label:\s*getBoardHeightLabelOption\('gem_board_label', 'bottom'\)/)
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
