import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8').replace(/\r\n/g, '\n')
}

const api = read('src/api/intelligence.ts')
const types = read('src/types/intelligence.ts')
const router = read('src/router/index.ts')
const app = read('src/App.vue')
const dailyInfo = read('src/views/DailyInfo.vue')
const dailyAnalysis = read('src/views/DailyAnalysis.vue')
const jiegeMode = read('src/views/JiegeMode.vue')
const industryTrends = read('src/views/IndustryTrends.vue')
const ultraShortSignals = read('src/views/UltraShortSignals.vue')

for (const typeName of [
  'ObsidianStatus',
  'ObsidianExportResponse',
  'IndustryTrend',
  'UltraShortSignal',
  'UltraShortSignalsResponse'
]) {
  assert.match(types, new RegExp(`interface ${typeName}`), `types should expose ${typeName}`)
}

for (const fnName of [
  'getObsidianStatus',
  'exportObsidianKnowledge',
  'getIndustryTrends',
  'getUltraShortSignals'
]) {
  assert.match(api, new RegExp(`function ${fnName}`), `intelligence API should expose ${fnName}`)
}

assert.match(router, /path:\s*'\/industry-trends'/, 'router should expose the industry trends view')
assert.match(router, /path:\s*'\/ultra-short-signals'/, 'router should expose the ultra-short signals view')
assert.match(app, /index="\/industry-trends"/, 'sidebar should link to industry trends')
assert.match(app, /index="\/ultra-short-signals"/, 'sidebar should link to ultra-short signals')
assert.match(app, /path:\s*'\/industry-trends'/, 'mobile nav should include industry trends')
assert.match(app, /path:\s*'\/ultra-short-signals'/, 'mobile nav should include ultra-short signals')

for (const [name, source] of Object.entries({ dailyInfo, dailyAnalysis, jiegeMode })) {
  assert.match(source, /obsidianStatus/, `${name} should track Obsidian status`)
  assert.match(source, /exportObsidianKnowledge/, `${name} should expose Obsidian export`)
  assert.match(source, /Obsidian/, `${name} should render an Obsidian entry point`)
}

assert.match(industryTrends, /getIndustryTrends/, 'IndustryTrends should call the trends API')
assert.match(industryTrends, /trend-list/, 'IndustryTrends should render a compact trend list')
assert.match(industryTrends, /catch \(error\)/, 'IndustryTrends should degrade on API failure')
assert.match(industryTrends, /openObsidianDashboard/, 'IndustryTrends should expose the Obsidian dashboard action')

assert.match(ultraShortSignals, /getUltraShortSignals/, 'UltraShortSignals should call the signals API')
assert.match(ultraShortSignals, /manual_required/, 'UltraShortSignals should surface manual confirmation')
assert.match(ultraShortSignals, /signal-list/, 'UltraShortSignals should render a compact signal list')
assert.match(ultraShortSignals, /catch \(error\)/, 'UltraShortSignals should degrade on API failure')

console.log('obsidian knowledge source checks passed')
