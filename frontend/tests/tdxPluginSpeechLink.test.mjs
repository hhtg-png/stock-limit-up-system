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
assert.match(stockLink, /router\.push\(\{ name: 'StockDetail'/, 'normal browsers should open internal stock detail')

const speech = read('src/composables/useSpeech.ts')
assert.match(speech, /enqueuePluginSpeech/, 'useSpeech should expose a plugin speech queue entrypoint')
assert.match(speech, /pluginSpeechKeys/, 'plugin speech should dedupe events by stable keys')

const websocket = read('src/composables/useWebSocket.ts')
for (const type of ['tdx_limit_up_event', 'tdx_stock_move_event', 'tdx_news_event', 'tdx_plate_strength_update']) {
  assert.match(websocket, new RegExp(`case '${type}':`), `WebSocket should handle ${type}`)
}
assert.match(websocket, /enqueuePluginSpeech/, 'TDX WebSocket events should enter the plugin speech queue')

console.log('tdx plugin speech and stock link checks passed')
