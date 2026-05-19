import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const types = read('src/types/daily-analysis.ts')
assert.match(types, /DailyAnalysisSession/, 'daily analysis types should expose the session type')
assert.match(types, /session:\s*DailyAnalysisSession/, 'daily analysis rows should include the active session')

const api = read('src/api/daily-analysis.ts')
assert.match(api, /session:\s*DailyAnalysisSession/, 'daily analysis API calls should accept a session parameter')
assert.match(api, /params:\s*\{\s*month,\s*session\s*\}/s, 'month query should pass the session parameter')
assert.match(api, /params:\s*\{\s*session\s*\}/s, 'rebuild and override calls should pass the session parameter')

const view = read('src/views/DailyAnalysis.vue')
assert.match(view, /analysisSession/, 'DailyAnalysis view should track the selected session')
assert.match(view, /盘中/, 'DailyAnalysis view should expose an intraday option')
assert.match(view, /盘后/, 'DailyAnalysis view should expose an after-close option')
assert.match(view, /getDailyAnalysisMonth\(selectedMonth\.value,\s*analysisSession\.value\)/, 'DailyAnalysis view should fetch by session')
assert.match(view, /rebuildDailyAnalysis\(row\.trade_date,\s*analysisSession\.value\)/, 'DailyAnalysis rebuild should target the selected session')
assert.match(view, /updateDailyAnalysisOverrides\([^)]*analysisSession\.value/s, 'DailyAnalysis overrides should target the selected session')

console.log('daily analysis session structure checks passed')
