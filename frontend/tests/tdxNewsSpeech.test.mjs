import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const news = readFileSync(resolve(root, 'src/views/tdx/TdxNewsFeed.vue'), 'utf8')
const speech = readFileSync(resolve(root, 'src/composables/useSpeech.ts'), 'utf8')

assert.match(news, /import\s*\{\s*computed,\s*onMounted,\s*onUnmounted,\s*ref,\s*watch\s*\}/, 'news feed should watch realtime and unlock state changes')
assert.match(news, /@change="handleSpeechToggle"/, 'news speech switch should unlock through a handler that can replay visible news')
assert.doesNotMatch(news, /setTimeout\(\(\) => speakVisibleNews/, 'speech replay should stay inside the user gesture so browser audio playback is not blocked')
assert.match(news, /spokenNewsKeys/, 'news feed should keep its own spoken-key guard for visible and realtime items')
assert.match(news, /markKnownNews/, 'news feed should mark the initial snapshot as known before realtime speech starts')
assert.match(news, /speakVisibleNews/, 'news feed should speak currently visible news after the user enables voice')
assert.match(news, /newsSpeechText/, 'news feed should build speech text from the aggregate news content, not only the title')
assert.match(news, /item\.source === '韭研公社'/, 'JYGS study-publish posts should get a shorter new-post speech format')
assert.match(news, /normalizeNewsSpeechContent/, 'news feed should normalize source-specific speech content before enqueueing')
assert.match(news, /item\.source === '格隆汇'/, 'Gelonghui live news should strip its source/date prefix for speech')
assert.match(news, /!content\.includes\(title\)/, 'speech digest should not repeat a title already contained in the content')
assert.doesNotMatch(news, /IMPORTANT_NEWS_THRESHOLD/, 'aggregate voice news should not be blocked by an importance threshold')
assert.match(news, /function shouldSpeakNews\(item: TdxNewsItem\) \{\s*return Boolean\(item\.news_id && item\.title\)\s*\}/, 'aggregate voice news should enqueue every titled item and rely on spoken-key dedupe')
assert.doesNotMatch(news, /newsItems\.filter\(isAggregateNewsItem\)/, 'JYGS latest study-publish posts should still be eligible for new-post speech')
assert.match(news, /watch\(\s*realtimeNewsItems/, 'news feed should enqueue new realtime aggregate news')
assert.match(news, /enqueuePluginSpeech\(newsSpeechText\(item\),\s*key,\s*\{\s*force:\s*true\s*\}\)/, 'news feed speech should use the plugin voice switch instead of global limit-up alert settings')
assert.doesNotMatch(news, /const important = payload\.value\.items\.find/, 'news feed should not only attempt one load-time important item')

assert.match(speech, /type PluginSpeechOptions/, 'plugin speech should have its own options')
assert.match(speech, /function enqueuePluginSpeech\(text: string,\s*key\?: string,\s*options: PluginSpeechOptions = \{\}\)/, 'plugin speech should accept options')
assert.match(speech, /options\.force \|\| getSpeechEnabled\(\)/, 'forced plugin speech should bypass the original app limit-up alert switch')

console.log('tdx news speech checks passed')
