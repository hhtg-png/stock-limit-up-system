import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const source = readFileSync(resolve(root, 'src/views/LimitUpList.vue'), 'utf8')
const dashboard = readFileSync(resolve(root, 'src/views/Dashboard.vue'), 'utf8')

for (const [name, viewSource] of [['LimitUpList', source], ['Dashboard', dashboard]]) {
  assert.match(viewSource, /label="首板"\s+value="first"/, `${name} should offer a first-board option`)
  assert.match(viewSource, /continuous_days:\s*boardFilter\.value\.continuousDays/, `${name} should pass min continuous-day filter to the backend`)
  assert.match(viewSource, /continuous_days_exact:\s*boardFilter\.value\.continuousDaysExact/, `${name} should pass exact continuous-day filter to the backend`)
  assert.match(viewSource, /reason_category:\s*filters\.reasonCategory/, `${name} should pass reason filter to the backend`)
  assert.match(viewSource, /status:\s*filters\.status/, `${name} should pass status filter to the backend`)
  assert.match(viewSource, /min_price:\s*normalizedPriceRange\.value\.min/, `${name} should pass min price filter to the backend`)
  assert.match(viewSource, /max_price:\s*normalizedPriceRange\.value\.max/, `${name} should pass max price filter to the backend`)
  assert.match(viewSource, /sort_by:\s*apiSort\.value\.sortBy/, `${name} should pass sort field to the backend`)
  assert.match(viewSource, /sort_order:\s*apiSort\.value\.sortOrder/, `${name} should pass sort order to the backend`)
  assert.match(viewSource, /is_one_word[\s\S]*一字/, `${name} should render an explicit one-word-board marker`)
  assert.match(viewSource, /1-20[\s\S]*20-50[\s\S]*50-100[\s\S]*100-9999/, `${name} should render default price range presets`)
  assert.doesNotMatch(viewSource, /filtered\s*=\s*filtered\.filter\(item => item\.continuous_limit_up_days >= filters\.minContinuousDays!?\)/, `${name} should not repeat continuous-day filtering locally`)
  assert.doesNotMatch(viewSource, /filtered\s*=\s*filtered\.filter\(item => item\.limit_up_price/, `${name} should not repeat price filtering locally`)
}

console.log('limit-up list server filter checks passed')
