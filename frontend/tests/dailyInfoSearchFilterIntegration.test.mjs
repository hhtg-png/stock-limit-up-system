import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

const viewSource = readFileSync('src/views/DailyInfo.vue', 'utf8')

assert.match(
  viewSource,
  /import \{ filterVisibleDailyInfoSearchResults \} from '@\/utils\/dailyInfoSearch'/,
  'DailyInfo should import the visible search result filter'
)
assert.match(
  viewSource,
  /const filteredItems = filterVisibleDailyInfoSearchResults\(response\.items, keyword\)/,
  'DailyInfo search should filter API candidates before rendering dates'
)
assert.match(
  viewSource,
  /historyItems\.value = filteredItems/,
  'history sidebar should render filtered search candidates'
)
assert.match(
  viewSource,
  /dailyInfo\.value = filteredItems\[0\] \|\| null/,
  'search detail should select the first filtered candidate'
)

console.log('daily info search filter integration checks passed')
