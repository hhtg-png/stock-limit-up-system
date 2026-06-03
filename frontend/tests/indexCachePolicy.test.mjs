import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const html = readFileSync(resolve(root, 'index.html'), 'utf8')

assert.match(html, /http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"/, 'HTML should discourage stale TDX cached entrypoints')
assert.match(html, /http-equiv="Pragma" content="no-cache"/, 'HTML should include legacy no-cache pragma for embedded browsers')
assert.match(html, /http-equiv="Expires" content="0"/, 'HTML should include legacy expires policy for embedded browsers')

console.log('index cache policy checks passed')
