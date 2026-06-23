import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const source = readFileSync(resolve(root, 'src/views/LimitUpList.vue'), 'utf8')

assert.match(source, /continuous_days:\s*filters\.minContinuousDays/, 'LimitUpList should pass continuous-day filter to the backend')
assert.match(source, /reason_category:\s*filters\.reasonCategory/, 'LimitUpList should pass reason filter to the backend')
assert.match(source, /status:\s*filters\.status/, 'LimitUpList should pass status filter to the backend')
assert.match(source, /sort_by:\s*apiSort\.value\.sortBy/, 'LimitUpList should pass sort field to the backend')
assert.match(source, /sort_order:\s*apiSort\.value\.sortOrder/, 'LimitUpList should pass sort order to the backend')
assert.match(source, /is_one_word[\s\S]*一字/, 'LimitUpList should render an explicit one-word-board marker')
assert.doesNotMatch(source, /filtered\s*=\s*filtered\.filter\(item => item\.continuous_limit_up_days >= filters\.minContinuousDays!?\)/, 'LimitUpList should not repeat continuous-day filtering locally')

console.log('limit-up list server filter checks passed')
