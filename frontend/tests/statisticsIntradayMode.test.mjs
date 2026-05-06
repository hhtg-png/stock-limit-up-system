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
  assert.match(typeSource, /detail:\s*MarketReviewDetailResponse/, 'intraday response should carry live detail data')
  assert.match(typeSource, /ladder:\s*MarketReviewLadderResponse/, 'intraday response should carry live ladder data')
})

test('statistics view has a distinct intraday mode that refreshes live report data', () => {
  assert.match(statisticsSource, /const reviewMode = ref<'daily' \| 'intraday'>\('daily'\)/)
  assert.match(statisticsSource, /label="intraday"[\s\S]*?盘中实时/)
  assert.match(statisticsSource, /getMarketReviewIntraday\(dayjs\(\)\.format\('YYYY-MM-DD'\)\)/)
  assert.match(statisticsSource, /detailResponse\.value = dailyResult\.detail/)
  assert.match(statisticsSource, /ladderResponse\.value = dailyResult\.ladder/)
  assert.match(statisticsSource, /window\.setInterval\(fetchData, 60000\)/, 'intraday mode should refresh every minute')
  assert.match(statisticsSource, /clearIntradayRefreshTimer\(\)/, 'timer should be cleared when mode changes or view unmounts')
})

test('daily range controls remain scoped to close-review mode', () => {
  assert.match(statisticsSource, /v-if="reviewMode === 'daily'"/, 'range picker should only control daily close-review mode')
  assert.match(statisticsSource, /盘中快照/, 'summary should identify intraday snapshot data')
  assert.match(statisticsSource, /更新时间/, 'summary should display intraday update time')
})
