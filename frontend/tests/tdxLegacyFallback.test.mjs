import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const html = read('index.html')

assert.doesNotMatch(html, /nomodule[^>]+src="\/tdx-legacy\.js"/, 'TDX fallback must not eagerly run in WebViews with broken nomodule support')
assert.match(html, /loadTdxLegacyFallback/, 'TDX pages should load the fallback if Vue fails to mount')
assert.match(html, /\/\^\\\/tdx\(\[\\\/\]\|\$\)\/\.test\(window\.location\.pathname\)/, 'fallback guard should only apply to TDX routes without startsWith')
assert.match(html, /__TDX_VUE_MOUNTED__/, 'fallback should not overwrite a mounted Vue TDX runtime')

assert.ok(existsSync(resolve(root, 'public/tdx-legacy.js')), 'TDX fallback script should be shipped as a public asset')

const fallback = read('public/tdx-legacy.js')

assert.match(fallback, /XMLHttpRequest/, 'TDX fallback should use XMLHttpRequest for old WebView compatibility')
assert.match(fallback, /\/api\/v1\/tdx-plugins\/limit-up-live\/status/, 'fallback should render the limit-up live status feed')
assert.match(fallback, /http:\/\/www\.treeid\/CODE_/, 'fallback stock clicks should link back to TDX')
assert.doesNotMatch(fallback, /兼容模式/, 'fallback should not show compatibility-mode chrome in TDX')
assert.doesNotMatch(fallback, /=>|\bconst\b|\blet\b/, 'fallback must stay ES5-compatible')

console.log('tdx legacy fallback checks passed')
