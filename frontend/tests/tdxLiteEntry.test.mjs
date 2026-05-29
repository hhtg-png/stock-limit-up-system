import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

assert.ok(existsSync(resolve(root, 'src/main-full.ts')), 'full app bootstrap should live in main-full.ts')
assert.ok(existsSync(resolve(root, 'src/tdx-main.ts')), 'tdx runtime bootstrap should exist')
assert.ok(existsSync(resolve(root, 'src/TdxApp.vue')), 'tdx runtime shell should exist')

const main = read('src/main.ts')
assert.match(main, /window\.location\.pathname\.startsWith\('\/tdx'\)/, 'main.ts should select tdx runtime by path')
assert.match(main, /import\('\.\/tdx-main'\)/, 'main.ts should lazy-load tdx-main for tdx routes')
assert.match(main, /import\('\.\/main-full'\)/, 'main.ts should lazy-load main-full for normal routes')
assert.doesNotMatch(main, /ElementPlus/, 'main.ts dispatcher should not import Element Plus')
assert.doesNotMatch(main, /@element-plus\/icons-vue/, 'main.ts dispatcher should not import icon library')

const full = read('src/main-full.ts')
assert.match(full, /app\.use\(ElementPlus,\s*\{\s*locale:\s*zhCn\s*\}\)/, 'full runtime should keep Element Plus for normal app pages')
assert.match(full, /Object\.entries\(ElementPlusIconsVue\)/, 'full runtime should keep current global icon registration')
assert.match(full, /createApp\(App\)/, 'full runtime should mount the normal App shell')

const tdxMain = read('src/tdx-main.ts')
assert.match(tdxMain, /createApp\(TdxApp\)/, 'tdx runtime should mount TdxApp')
assert.match(tdxMain, /tdxRouter/, 'tdx runtime should use the tdx-only router')
assert.doesNotMatch(tdxMain, /ElementPlus/, 'tdx runtime should not install Element Plus globally')
assert.doesNotMatch(tdxMain, /ElementPlusIconsVue/, 'tdx runtime should not globally register all icons')
assert.doesNotMatch(tdxMain, /from\s+['"]\.\/App\.vue['"]/, 'tdx runtime should not import the normal app shell')

const tdxApp = read('src/TdxApp.vue')
assert.match(tdxApp, /<router-view\s*\/>/, 'tdx app shell should only render the plugin route')
assert.match(tdxApp, /useWebSocket/, 'tdx app shell should own the websocket connection for plugin pages')
assert.doesNotMatch(tdxApp, /el-container|el-aside|AlertPanel|mobile-bottom-nav/, 'tdx app shell should not include normal app chrome')

console.log('tdx lite entry checks passed')
