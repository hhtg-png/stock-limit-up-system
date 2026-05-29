import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const center = readFileSync(resolve(root, 'src/views/tdx/TdxPluginCenter.vue'), 'utf8')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')

assert.match(center, /聚合快讯语音/, 'tdx plugin center should show the voice-only news plugin')
assert.match(center, /\/tdx\/news-voice\/dark/, 'tdx plugin center should link to news voice route')
assert.match(settings, /聚合快讯语音/, 'settings plugin modal should show the voice-only news plugin')
assert.match(settings, /\/tdx\/news-voice\/dark/, 'settings plugin modal should copy the news voice route')

console.log('tdx news voice entry checks passed')
