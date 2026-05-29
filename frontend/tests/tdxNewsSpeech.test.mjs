import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const news = readFileSync(resolve(root, 'src/views/tdx/TdxNewsFeed.vue'), 'utf8')

assert.match(news, /import\s*\{\s*computed,\s*onMounted,\s*onUnmounted,\s*ref,\s*watch\s*\}/, 'news feed should watch realtime and unlock state changes')
assert.match(news, /@change="handleSpeechToggle"/, 'news speech switch should unlock through a handler that can replay visible news')
assert.match(news, /spokenNewsKeys/, 'news feed should keep its own spoken-key guard for visible and realtime items')
assert.match(news, /markKnownNews/, 'news feed should mark the initial snapshot as known before realtime speech starts')
assert.match(news, /speakVisibleNews/, 'news feed should speak currently visible important news after the user enables voice')
assert.match(news, /newsSpeechText/, 'news feed should build speech text from the aggregate news content, not only the title')
assert.match(news, /watch\(\s*realtimeNewsItems/, 'news feed should enqueue new realtime aggregate news')
assert.doesNotMatch(news, /const important = payload\.value\.items\.find/, 'news feed should not only attempt one load-time important item')

console.log('tdx news speech checks passed')
