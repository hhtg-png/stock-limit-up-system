import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const center = readFileSync(resolve(root, 'src/views/tdx/TdxPluginCenter.vue'), 'utf8')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')
const limitUp = readFileSync(resolve(root, 'src/views/tdx/TdxLimitUpLive.vue'), 'utf8')

assert.doesNotMatch(center, /聚合快讯语音/, 'tdx plugin center should not show a separate voice-only news plugin')
assert.doesNotMatch(center, /\/tdx\/news-voice\/dark/, 'tdx plugin center should not advertise a separate news voice route')
assert.doesNotMatch(settings, /聚合快讯语音/, 'settings plugin modal should not show a separate voice-only news plugin')
assert.doesNotMatch(settings, /\/tdx\/news-voice\/dark/, 'settings plugin modal should not advertise a separate news voice route')
assert.doesNotMatch(limitUp, /news-voice-strip/, 'limit-up plugin should not render a visible aggregate news voice mini window')
assert.doesNotMatch(limitUp, />\s*聚合快讯语音\s*</, 'limit-up plugin should not display a verbose aggregate news voice label')
assert.doesNotMatch(limitUp, /newsVoiceStatusText|newsSpokenCount|recentNewsTitle|latestNewsTime/, 'limit-up plugin should keep aggregate news voice state hidden')

console.log('tdx news voice entry checks passed')
