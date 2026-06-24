import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const source = readFileSync(resolve(root, 'src/views/LimitUpList.vue'), 'utf8')

assert.match(source, /label="首板"\s+value="first"/, 'LimitUpList should offer a first-board option')
assert.match(source, /continuous_days:\s*boardFilter\.value\.continuousDays/, 'LimitUpList should pass min continuous-day filter to the backend')
assert.match(source, /continuous_days_exact:\s*boardFilter\.value\.continuousDaysExact/, 'LimitUpList should pass exact continuous-day filter to the backend')
assert.match(source, /reason_category:\s*filters\.reasonCategory/, 'LimitUpList should pass reason filter to the backend')
assert.match(source, /status:\s*filters\.status/, 'LimitUpList should pass status filter to the backend')
assert.match(source, /min_price:\s*normalizedPriceRange\.value\.min/, 'LimitUpList should pass min price filter to the backend')
assert.match(source, /max_price:\s*normalizedPriceRange\.value\.max/, 'LimitUpList should pass max price filter to the backend')
assert.match(source, /sort_by:\s*apiSort\.value\.sortBy/, 'LimitUpList should pass sort field to the backend')
assert.match(source, /sort_order:\s*apiSort\.value\.sortOrder/, 'LimitUpList should pass sort order to the backend')
assert.match(source, /is_one_word[\s\S]*一字/, 'LimitUpList should render an explicit one-word-board marker')
assert.match(source, /1-20[\s\S]*20-50[\s\S]*50-100[\s\S]*100-9999/, 'LimitUpList should render default price range presets')
assert.doesNotMatch(source, /filtered\s*=\s*filtered\.filter\(item => item\.continuous_limit_up_days >= filters\.minContinuousDays!?\)/, 'LimitUpList should not repeat continuous-day filtering locally')
assert.doesNotMatch(source, /filtered\s*=\s*filtered\.filter\(item => item\.limit_up_price/, 'LimitUpList should not repeat price filtering locally')

console.log('limit-up list server filter checks passed')
