import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const stockLink = read('src/composables/useTdxStockLink.ts')
assert.match(stockLink, /TdxW\|hong/i, 'TDX stock link should detect Tongdaxin user agents')
assert.match(stockLink, /www\.treeid\/CODE_/, 'TDX stock link should emit treeid CODE URL')
assert.match(stockLink, /padStart\(6,\s*'0'\)/, 'TDX stock link should pad short stock codes before linking')
assert.doesNotMatch(stockLink, /router\.push/, 'TDX plugin stock clicks should not open internal stock detail')
assert.doesNotMatch(stockLink, /StockDetail/, 'TDX plugin stock clicks should not reference the project stock detail route')

const speech = read('src/composables/useSpeech.ts')
assert.match(speech, /enqueuePluginSpeech/, 'useSpeech should expose a plugin speech queue entrypoint')
assert.match(speech, /pluginSpeechKeys/, 'plugin speech should dedupe events by stable keys')
assert.match(speech, /NEWS_SPEECH_SIMILARITY_WINDOW_MS\s*=\s*60\s*\*\s*1000/, 'news speech should use a 1 minute similarity dedupe window')
assert.match(speech, /NEWS_SPEECH_SIMILARITY_THRESHOLD\s*=\s*0\.8/, 'news speech should skip titles above 80% similarity')
assert.match(speech, /isSimilarRecentNewsSpeech/, 'plugin speech should compare recent aggregate news titles before enqueueing')
assert.match(speech, /speechKey\.startsWith\('news-'\)/, 'similarity dedupe should only apply to aggregate news speech keys')
assert.doesNotMatch(speech, /vol:\s*'99'/, 'audio fallback should not use clipping-prone max volume')
assert.match(speech, /targetAudioFallbackVolume/, 'audio fallback should apply a controlled element volume')
assert.match(speech, /NEURAL_TTS_START_TIMEOUT_MS\s*=\s*900/, 'neural TTS should have a short startup timeout for uncached audio')
assert.match(speech, /function shouldUseTargetAudioPlayback\(\)[\s\S]*speechUnlocked\.value[\s\S]*hasAudioFallbackSupport\(\)/, 'unlocked plugin speech should prefer backend neural audio playback')
assert.match(speech, /playWithAudioFallback\([\s\S]*playWithWebSpeech/, 'speech should fall back to Web Speech if neural audio fails')
assert.match(speech, /setTimeout\(\(\)\s*=>[\s\S]*NEURAL_TTS_START_TIMEOUT_MS/, 'speech should not wait indefinitely for uncached neural TTS audio')
assert.match(speech, /audio\.onplaying\s*=\s*markStarted/, 'neural TTS startup timeout should clear once audio starts playing')
assert.match(speech, /audio\.oncanplay\s*=\s*markStarted/, 'neural TTS startup timeout should clear once cached audio can play')
assert.match(speech, /SPEECH_UNLOCK_STORAGE_KEY/, 'plugin speech switch should persist the last enabled state')
assert.match(speech, /readStoredSpeechUnlocked/, 'plugin speech should restore the last enabled state on reload')
assert.match(speech, /persistSpeechUnlocked\(true\)/, 'unlocking plugin speech should save the enabled state')
assert.match(speech, /function lockSpeech/, 'plugin speech should expose a way to turn the remembered switch off')
assert.match(speech, /persistSpeechUnlocked\(false\)/, 'turning plugin speech off should save the disabled state')
assert.match(speech, /function enqueuePluginSpeech[\s\S]*!speechUnlocked\.value[\s\S]*return false/, 'plugin speech queue should respect the remembered voice switch')
assert.match(speech, /urgent\?: boolean/, 'plugin speech should support an urgent low-latency path')
assert.match(speech, /mode:\s*'web-speech'/, 'urgent plugin speech should force browser Web Speech instead of neural TTS')
assert.match(speech, /speechQueue\.unshift/, 'urgent plugin speech should be placed before normal queued news')
assert.match(speech, /stopCurrentSpeechPlayback/, 'urgent plugin speech should be able to interrupt a slower current item')

const websocket = read('src/composables/useWebSocket.ts')
const limitUp = read('src/views/tdx/TdxLimitUpLive.vue')
const composite = read('src/views/tdx/TdxCompositeWatch.vue')
for (const type of ['tdx_limit_up_event', 'tdx_stock_move_event', 'tdx_news_event', 'tdx_plate_strength_update']) {
  assert.match(websocket, new RegExp(`case '${type}':`), `WebSocket should handle ${type}`)
}
assert.match(websocket, /case 'tdx_limit_up_event':[\s\S]*pushTdxLimitUpEvent/, 'limit-up websocket events should update the realtime plugin buffer')
assert.doesNotMatch(websocket, /case 'tdx_limit_up_event':(?:(?!case 'tdx_stock_move_event':)[\s\S])*enqueuePluginSpeech/, 'limit-up websocket speech should be owned by the limit-up page so first-open snapshots can be primed')
assert.match(limitUp, /watch\(\s*realtimeLimitUpEvents/, 'limit-up page should consume realtime limit-up events for low-latency speech')
assert.match(limitUp, /handleStatusEvents\(newRealtimeItems\)/, 'limit-up realtime events should pass through the first-open priming guard')
assert.match(limitUp, /function limitUpSpeechText/, 'pure limit-up page should derive a concise status speech text')
assert.match(limitUp, /urgent:\s*true/, 'pure limit-up speech should use the urgent low-latency queue')
assert.doesNotMatch(limitUp, /enqueuePluginSpeech\(`\$\{item\.stock_name\}\$\{item\.target_status_label \|\| item\.event_label\}`/, 'pure limit-up speech should not speak the raw 封死涨停 label')
assert.match(composite, /function limitUpSpeechText/, 'composite watch should derive a concise status speech text')
assert.match(composite, /urgent:\s*true/, 'composite limit-up speech should use the urgent low-latency queue')
assert.doesNotMatch(composite, /enqueuePluginSpeech\(`\$\{item\.stock_name\}\$\{item\.target_status_label \|\| item\.event_label\}`/, 'composite limit-up speech should not speak the raw 封死涨停 label')
assert.match(limitUp, /lockSpeech/, 'limit-up voice switch should be able to persist the off state')
assert.match(websocket, /case 'tdx_stock_move_event':[\s\S]*enqueuePluginSpeech/, 'stock-move websocket events should enter the speech queue')
assert.match(websocket, /case 'tdx_plate_strength_update':[\s\S]*enqueuePluginSpeech/, 'plate-strength websocket events should enter the speech queue')
assert.doesNotMatch(websocket, /case 'tdx_news_event':(?:(?!case 'tdx_plate_strength_update':)[\s\S])*enqueuePluginSpeech/, 'aggregate news websocket handler should not directly speak in every plugin window')

console.log('tdx plugin speech and stock link checks passed')
