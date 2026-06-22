import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const ws = read('src/composables/useWebSocket.ts')
assert.match(ws, /clearTdxPluginRealtime/, 'TDX realtime state should expose an explicit clear hook')
assert.match(ws, /trade_date:\s*String\(data\.trade_date \|\| ''\)/, 'TDX limit-up websocket events should keep their trade date')

for (const path of ['src/views/tdx/TdxLimitUpLive.vue', 'src/views/tdx/TdxCompositeWatch.vue']) {
  const source = read(path)
  assert.match(source, /activeTradeDate\s*=\s*ref\(''\)/, `${path} should track the active payload trade date`)
  assert.match(source, /applyPayloadTradeDate\(next\)/, `${path} should inspect every API payload date`)
  assert.match(source, /function resetDailyState\(tradeDate: string\)/, `${path} should reset page state when the trade date changes`)
  assert.match(source, /seenSpeechKeys\.clear\(\)/, `${path} should clear speech seen keys across trade dates`)
  assert.match(source, /seenTouchedStockCodes\.clear\(\)/, `${path} should clear touched-stock speech state across trade dates`)
  assert.match(source, /spokenLimitUpSpeechAt\.clear\(\)/, `${path} should clear speech dedupe state across trade dates`)
  assert.match(source, /hasPrimedLimitUpSpeech\s*=\s*false/, `${path} should re-prime speech after a trade-date reset`)
  assert.match(source, /limitUpStore\.setRealtimeSnapshot\(tradeDate,\s*\[\]\)/, `${path} should clear stale shared limit-up realtime rows`)
  assert.match(source, /clearTdxPluginRealtime\(\)/, `${path} should clear stale plugin websocket rows`)
  assert.match(source, /function isCurrentTradeDate/, `${path} should have same-day filtering`)
  assert.match(source, /if \(!isCurrentTradeDate\(item\.trade_date\)\) continue/, `${path} should ignore stale realtime rows`)
}

const composite = read('src/views/tdx/TdxCompositeWatch.vue')
assert.match(composite, /stockMoveCache\.clear\(\)/, 'composite plugin should drop stock-move cache across trade dates')
assert.match(composite, /function stockMoveCacheKey\(stockCode: string\)/, 'composite plugin should cache stock-move payloads by trade date')
assert.match(composite, /getTdxStockMove\(stockCode,\s*activeTradeDate\.value \? \{ trade_date: activeTradeDate\.value \} : undefined\)/, 'composite plugin should request stock-move data for the active trade date')

console.log('tdx daily reset checks passed')
