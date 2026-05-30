import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')

assert.match(settings, /通达信看盘插件/, 'Settings should expose a TDX plugin entry')
assert.match(settings, /tdxPluginDialogVisible/, 'Settings should control the TDX plugin modal state')
assert.match(settings, /<el-dialog[\s\S]*tdxPluginDialogVisible/, 'Settings should open TDX plugins inside an Element Plus dialog')
assert.match(settings, /通达信地址/, 'Settings should describe TDX plugin URLs instead of normal page navigation')
assert.match(settings, /复制插件地址/, 'Settings should copy plugin URLs for Tongdaxin embedding')
assert.match(settings, /buildTdxPluginUrl/, 'Settings should build absolute plugin URLs for Tongdaxin')
assert.match(settings, /fallbackCopyText/, 'Settings should fallback when Clipboard API is unavailable or denied')
assert.match(settings, /document\.execCommand\('copy'\)/, 'Settings should support legacy WebView copy commands')
assert.match(settings, /selectedPluginUrl/, 'Settings should expose the plugin URL for manual selection after copy failures')
assert.doesNotMatch(settings, /router\.push\(path\)/, 'Settings should not navigate away from settings when configuring TDX plugins')
assert.doesNotMatch(settings, /打开插件入口/, 'Settings should not present TDX plugins as normal in-app pages')

for (const path of [
  '/tdx/ztlive/dark#xxxxxx',
  '/tdx/yidong/xxxxxx/dark',
  '/tdx/strong/dark',
  '/tdx/news/dark',
  '/tdx/thsyd/xxxxxx/dark'
]) {
  assert.match(settings, new RegExp(path.replace(/[/?#]/g, '\\$&')), `Settings modal should link to ${path}`)
}

assert.match(settings, /xxxxxx/, 'TDX stock-linked plugin URLs should use the Tongdaxin current-stock placeholder')
assert.doesNotMatch(settings, /\/tdx\/ztlive\/xxxxxx\/dark/, 'combined ztlive plugin should keep current-stock placeholder in hash to avoid a full page navigation')
assert.doesNotMatch(settings, /\/tdx\/yidong\/600589\/dark/, 'Settings should not copy a fixed stock code for stock move linkage')

assert.match(settings, /tdx-plugin-modal/, 'Settings should style the modal plugin entry')
assert.match(settings, /plugin-window-card/, 'Settings should render plugin entry cards in the modal')

console.log('tdx settings entry checks passed')
