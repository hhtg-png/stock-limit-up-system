import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')

assert.match(settings, /通达信看盘插件/, 'Settings should expose a TDX plugin entry')
assert.match(settings, /tdxPluginDialogVisible/, 'Settings should control the TDX plugin modal state')
assert.match(settings, /<el-dialog[\s\S]*tdxPluginDialogVisible/, 'Settings should open TDX plugins inside an Element Plus dialog')
assert.match(settings, /打开插件入口/, 'Settings should include a button that opens the plugin entry modal')

for (const path of [
  '/tdx/ztlive/dark',
  '/tdx/yidong/600589/dark',
  '/tdx/strong/dark',
  '/tdx/news/dark',
  '/tdx/thsyd/600589/dark'
]) {
  assert.match(settings, new RegExp(path.replace(/[/?]/g, '\\$&')), `Settings modal should link to ${path}`)
}

assert.match(settings, /tdx-plugin-modal/, 'Settings should style the modal plugin entry')
assert.match(settings, /plugin-window-card/, 'Settings should render plugin entry cards in the modal')

console.log('tdx settings entry checks passed')
