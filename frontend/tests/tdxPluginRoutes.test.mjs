import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const router = readFileSync(resolve(root, 'src/router/index.ts'), 'utf8')
const app = readFileSync(resolve(root, 'src/App.vue'), 'utf8')

for (const path of [
  '/tdx',
  '/tdx/ztlive/dark',
  '/tdx/yidong/:code?/dark',
  '/tdx/strong/dark',
  '/tdx/news/dark',
  '/tdx/thsyd/:code?/dark'
]) {
  assert.match(router, new RegExp(`path:\\s*'${path.replace(/[/?]/g, '\\$&')}'`), `router should expose ${path}`)
}

for (const file of [
  'src/views/tdx/TdxPluginCenter.vue',
  'src/views/tdx/TdxLimitUpLive.vue',
  'src/views/tdx/TdxStockMove.vue',
  'src/views/tdx/TdxPlateStrength.vue',
  'src/views/tdx/TdxNewsFeed.vue',
  'src/views/tdx/TdxThsMove.vue',
  'src/components/tdx/TdxPluginShell.vue',
  'src/composables/useTdxStockLink.ts',
  'src/api/tdx-plugins.ts'
]) {
  assert.equal(existsSync(resolve(root, file)), true, `${file} should exist`)
}

assert.match(app, /isTdxRoute/, 'App should detect TDX plugin routes')
assert.match(app, /tdx-standalone/, 'App should render TDX plugin routes without the normal app chrome')

console.log('tdx plugin route structure checks passed')
