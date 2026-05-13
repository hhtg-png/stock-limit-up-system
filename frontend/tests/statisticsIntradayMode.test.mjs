import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

const statisticsSource = readFileSync('src/views/Statistics.vue', 'utf8')
const apiSource = readFileSync('src/api/review.ts', 'utf8')
const typeSource = readFileSync('src/types/market.ts', 'utf8')

test('market review API exposes intraday snapshot with detail and ladder payloads', () => {
  assert.match(apiSource, /getMarketReviewIntraday/, 'intraday API function should be exported')
  assert.match(apiSource, /\/statistics\/review\/intraday/, 'intraday API should call the backend intraday endpoint')
  assert.match(typeSource, /interface MarketReviewIntradayResponse/, 'intraday response type should be explicit')
  assert.match(typeSource, /is_live:\s*boolean/, 'intraday response should tell whether the snapshot is live market data')
  assert.match(typeSource, /detail:\s*MarketReviewDetailResponse/, 'intraday response should carry live detail data')
  assert.match(typeSource, /ladder:\s*MarketReviewLadderResponse/, 'intraday response should carry live ladder data')
})

test('statistics view automatically merges live intraday data into the selected daily range', () => {
  assert.doesNotMatch(statisticsSource, /reviewMode/, 'manual close-review/intraday mode state should be removed')
  assert.doesNotMatch(statisticsSource, /label="intraday"/, 'manual intraday button should not be rendered')
  assert.doesNotMatch(statisticsSource, /label="daily"/, 'manual daily button should not be rendered')
  assert.match(statisticsSource, /<el-radio-group v-model="timeRange" size="small">/, 'range picker should always be available')
  assert.match(statisticsSource, /getMarketReviewDaily\(query\)/, 'daily history should be fetched for the selected range')
  assert.match(statisticsSource, /getMarketReviewIntraday\(today\)/, 'today intraday snapshot should be checked automatically')
  assert.match(
    statisticsSource,
    /if \(intradayResult\.is_live && intradayResult\.data\.rows\.length\)/,
    'live intraday data should only be merged during live market time'
  )
  assert.match(statisticsSource, /mergeIntradayDailyRows/, 'today intraday row should be merged into historical rows')
  assert.match(statisticsSource, /detailResponse\.value = intradayResult\.detail/)
  assert.match(statisticsSource, /ladderResponse\.value = intradayResult\.ladder/)
})

test('statistics view refreshes automatically without hiding historical comparison data', () => {
  assert.match(statisticsSource, /window\.setInterval\(refreshReviewSilently, 60000\)/, 'report should re-check market status every minute')
  assert.match(statisticsSource, /fetchData\(\{ silent: true \}\)/, 'timer refresh should not show the full-page loading mask')
  assert.match(statisticsSource, /const shouldShowLoading = !options\.silent/)
  assert.match(statisticsSource, /if \(shouldShowLoading\) \{\s*loading\.value = true\s*\}/)
  assert.match(statisticsSource, /clearReviewRefreshTimer\(\)/, 'timer should be cleared when view unmounts')
  assert.doesNotMatch(statisticsSource, /v-if="reviewMode === 'daily'"/, 'range picker should not be scoped to a removed mode')
  assert.match(statisticsSource, /盘中实时/, 'summary should identify live intraday data')
  assert.match(statisticsSource, /实时更新/, 'summary should display live snapshot update time')
})
