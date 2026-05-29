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
assert.doesNotMatch(speech, /vol:\s*'99'/, 'audio fallback should not use clipping-prone max volume')
assert.match(speech, /targetAudioFallbackVolume/, 'audio fallback should apply a controlled element volume')
assert.match(speech, /function shouldUseTargetAudioPlayback\(\)[\s\S]*speechUnlocked\.value[\s\S]*hasAudioFallbackSupport\(\)/, 'unlocked plugin speech should prefer backend neural audio playback')
assert.match(speech, /playWithAudioFallback\([\s\S]*playWithWebSpeech/, 'speech should fall back to Web Speech if neural audio fails')

const websocket = read('src/composables/useWebSocket.ts')
for (const type of ['tdx_limit_up_event', 'tdx_stock_move_event', 'tdx_news_event', 'tdx_plate_strength_update']) {
  assert.match(websocket, new RegExp(`case '${type}':`), `WebSocket should handle ${type}`)
}
assert.match(websocket, /enqueuePluginSpeech/, 'TDX WebSocket events should enter the plugin speech queue')
assert.match(websocket, /enqueuePluginSpeech\([\s\S]*\{\s*force:\s*true\s*\}/, 'TDX WebSocket plugin events should bypass the original app alert switch after the plugin voice switch is unlocked')

console.log('tdx plugin speech and stock link checks passed')
