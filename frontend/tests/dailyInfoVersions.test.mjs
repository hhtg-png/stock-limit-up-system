import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

const viewSource = readFileSync('src/views/DailyInfo.vue', 'utf8')
const apiSource = readFileSync('src/api/intelligence.ts', 'utf8')
const typeSource = readFileSync('src/types/intelligence.ts', 'utf8')

test('daily info renders one merged digest per trade date', () => {
  assert.match(typeSource, /version_id\?: number \| null/, 'daily info response should expose generation version id')
  assert.match(apiSource, /versionId\?: number \| null/, 'daily info API keeps hidden version access for audit links')
  assert.match(apiSource, /version_id: versionId/, 'daily info request should pass version_id to the backend')
  assert.doesNotMatch(viewSource, /sameDateVersions/, 'daily review should not compute same-day generated versions')
  assert.doesNotMatch(viewSource, /handleVersionPageChange/, 'daily review should not page between same-day versions')
  assert.doesNotMatch(viewSource, /<el-pagination[\s\S]*sameDateVersions/, 'daily review should not show same-day version pagination')
  assert.match(viewSource, /history\.trade_date !== item\.trade_date/, 'history upsert should replace the same trade date')
})

test('daily info history sidebar shows one latest row per trade date', () => {
  assert.match(viewSource, /displayHistoryItems/, 'history sidebar should render a display-only deduped list')
  assert.match(viewSource, /latestHistoryByDate/, 'history sidebar should select the latest version per trade date')
  assert.match(viewSource, /v-for="item in displayHistoryItems"/, 'history sidebar should not directly render every same-day version')
  assert.doesNotMatch(viewSource, /sameDateVersions[\s\S]*historyItems\.value/, 'same-day version pagination should not use the full version list')
})

test('daily info stock mentions fall back when mentioned_stocks is an empty array', () => {
  assert.match(viewSource, /stockMentionSource/, 'stock mention source should be selected before normalizing table rows')
  assert.match(viewSource, /firstNonEmptyStockList/, 'stock mentions should use the first non-empty array')
  assert.doesNotMatch(viewSource, /mentioned_stocks \|\| dailyInfo\.value\?\.summary\.stocks/, 'empty mentioned_stocks should not block the stocks fallback')
})

test('daily info history panel is bounded and mobile history scrolls horizontally', () => {
  assert.match(viewSource, /max-height:\s*calc\(100dvh - 120px\)/, 'desktop history panel should not grow past one viewport')
  assert.match(viewSource, /overflow-y:\s*auto/, 'desktop history list should scroll internally')
  assert.match(viewSource, /flex-direction:\s*row/, 'mobile history list should become horizontal')
  assert.match(viewSource, /overflow-x:\s*auto/, 'mobile history list should support horizontal scrolling')
})
