import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const app = read('src/App.vue')
assert.match(app, /class="mobile-bottom-nav"/, 'App.vue should expose a mobile bottom navigation')
assert.match(app, /mobileNavItems/, 'App.vue should define mobile navigation items')
assert.match(app, /path:\s*'\/daily-info'/, 'mobile navigation should include DailyInfo')
assert.match(app, /\{\s*path:\s*'\/daily-info'[\s\S]*icon:\s*Document/, 'DailyInfo should use a different icon from DailyAnalysis')
assert.match(app, /path:\s*'\/jiege-mode'/, 'mobile navigation should include JiegeMode')
assert.match(app, /overflow-x:\s*auto/, 'mobile bottom navigation should support horizontal scrolling')
assert.match(app, /class="mobile-speech-unlock"/, 'mobile header should expose speech unlock action')

const dashboard = read('src/views/Dashboard.vue')
const limitUpList = read('src/views/LimitUpList.vue')
assert.match(dashboard, /MobileLimitUpCards/, 'Dashboard should render mobile stock cards')
assert.match(limitUpList, /MobileLimitUpCards/, 'LimitUpList should render mobile stock cards')

const dailyAnalysis = read('src/views/DailyAnalysis.vue')
assert.match(dailyAnalysis, /mobile-analysis-list/, 'DailyAnalysis should expose a mobile card list')

const dailyInfo = read('src/views/DailyInfo.vue')
assert.match(dailyInfo, /displayHistoryItems/, 'DailyInfo should expose a deduped history list for mobile')
assert.match(dailyInfo, /overflow-x:\s*auto/, 'DailyInfo mobile history should scroll horizontally')
assert.match(dailyInfo, /stock-card-list/, 'DailyInfo should render stock mentions as mobile cards')

const stockDetail = read('src/views/StockDetail.vue')
assert.match(stockDetail, /mobile-detail-anchors/, 'StockDetail should expose mobile section anchors')

const settings = read('src/views/Settings.vue')
assert.match(settings, /:xs="24"/, 'Settings columns should collapse to full width on mobile')

console.log('mobile layout structure checks passed')
