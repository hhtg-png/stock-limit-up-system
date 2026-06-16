import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

function indexOfOrThrow(source, pattern, label) {
  const index = source.search(pattern)
  assert.notEqual(index, -1, `${label} should exist`)
  return index
}

const router = read('src/router/index.ts')
const app = read('src/App.vue')
const api = read('src/api/limit-up.ts')
const types = read('src/types/limit-up.ts')
const viewPath = resolve(root, 'src/views/LimitUpClassification.vue')

assert.equal(existsSync(viewPath), true, 'LimitUpClassification view should exist')

const view = read('src/views/LimitUpClassification.vue')

assert.match(router, /path:\s*'\/limit-up-classification'/, 'router should expose /limit-up-classification')
assert.match(router, /name:\s*'LimitUpClassification'/, 'route should be named LimitUpClassification')
assert.match(router, /LimitUpClassification\.vue/, 'route should lazy-load LimitUpClassification.vue')
assert.match(router, /meta:\s*\{\s*title:\s*'涨停分类'\s*\}/, 'route title should be 涨停分类')

const desktopDailyInfo = indexOfOrThrow(app, /<el-menu-item index="\/daily-info">/, 'desktop DailyInfo nav')
const desktopClassification = indexOfOrThrow(app, /<el-menu-item index="\/limit-up-classification">/, 'desktop classification nav')
const desktopJiege = indexOfOrThrow(app, /<el-menu-item index="\/jiege-mode">/, 'desktop Jiege nav')
assert.ok(desktopDailyInfo < desktopClassification, 'desktop classification nav should be below DailyInfo')
assert.ok(desktopClassification < desktopJiege, 'desktop classification nav should be above JiegeMode')
assert.match(app, /Grid/, 'App should import and use Grid icon for classification nav')

const mobileDailyInfo = indexOfOrThrow(app, /path:\s*'\/daily-info'/, 'mobile DailyInfo nav')
const mobileClassification = indexOfOrThrow(app, /path:\s*'\/limit-up-classification'/, 'mobile classification nav')
const mobileJiege = indexOfOrThrow(app, /path:\s*'\/jiege-mode'/, 'mobile Jiege nav')
assert.ok(mobileDailyInfo < mobileClassification, 'mobile classification nav should follow DailyInfo')
assert.ok(mobileClassification < mobileJiege, 'mobile classification nav should precede JiegeMode')

assert.match(api, /getLimitUpClassification/, 'limit-up API should expose getLimitUpClassification')
assert.match(api, /\/limit-up\/classification/, 'getLimitUpClassification should call /limit-up/classification')
assert.match(api, /force_ai/, 'getLimitUpClassification should support forced AI regeneration')
assert.match(types, /interface LimitUpClassificationResponse/, 'types should define LimitUpClassificationResponse')
assert.match(types, /interface LimitUpClassificationGroup/, 'types should define LimitUpClassificationGroup')
assert.match(types, /interface LimitUpClassificationStock/, 'types should define LimitUpClassificationStock')
assert.match(types, /rule_classified_plate/, 'types should preserve rule classification')
assert.match(types, /classification_method/, 'types should expose classification method')
assert.match(types, /ai_reason_summary/, 'types should expose AI classification summary')

assert.match(view, /class="limit-up-classification"/, 'view should use page root class')
assert.match(view, /<h3>涨停分类<\/h3>/, 'view should render compact title')
assert.match(view, /type="date"/, 'view should include date picker')
assert.match(view, /getLimitUpClassification/, 'view should fetch classification API')
assert.match(view, /重算AI分类/, 'view should expose AI regeneration action')
assert.match(view, /classificationText/, 'view should show classification method')
assert.match(view, /plate_name/, 'view should render plate group names')
assert.match(view, /first_limit_up_time/, 'view should render first seal time')
assert.match(view, /final_seal_time/, 'view should render final seal/reseal time')
assert.match(view, /ai_reason_summary/, 'view should render AI classification reason')
assert.match(view, /classification-card-list/, 'view should expose mobile card list')
assert.doesNotMatch(view, /tdx-plugins/, 'classification page should not reuse TDX plugin API')

console.log('limit-up classification structure checks passed')
