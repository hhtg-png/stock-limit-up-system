import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const router = readFileSync(resolve(root, 'src/router/tdx.ts'), 'utf8')

for (const path of [
  '/tdx',
  '/tdx/ztlive/dark',
  '/tdx/yidong/:code?/dark',
  '/tdx/strong/dark',
  '/tdx/news/dark',
  '/tdx/news-voice/dark',
  '/tdx/thsyd/:code?/dark'
]) {
  assert.match(router, new RegExp(`path:\\s*'${path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`), `tdx router should include ${path}`)
}

assert.match(router, /name:\s*'TdxNewsVoice'/, 'tdx router should name the voice-only news route')
assert.match(router, /TdxNewsVoice\.vue/, 'tdx router should lazy-load TdxNewsVoice')
assert.match(router, /document\.title/, 'tdx router should set document title')

console.log('tdx news voice route checks passed')
