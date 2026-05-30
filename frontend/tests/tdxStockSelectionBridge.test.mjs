import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const bridgePath = 'src/composables/useTdxStockSelection.ts'
assert.equal(existsSync(resolve(root, bridgePath)), true, 'TDX current-stock bridge composable should exist')

const bridge = read(bridgePath)
assert.match(bridge, /readTdxStockCodeFromLocation/, 'bridge should read current stock code from URL path/query/hash')
assert.match(bridge, /extractTdxStockCodeFromMessage/, 'bridge should read current stock code from postMessage payloads')
assert.match(bridge, /installTdxStockSelectionBridge/, 'bridge should install a TDX stock selection listener')
assert.match(bridge, /hashchange/, 'bridge should react to hash-based stock changes')
assert.match(bridge, /popstate/, 'bridge should react to route-based stock changes')
assert.match(bridge, /message/, 'bridge should react to parent/frame stock selection messages')
assert.match(bridge, /setInterval/, 'bridge should poll location because some TDX WebViews mutate URL without router events')
assert.match(bridge, /tdxSelectStock/, 'bridge should expose a global callback for TDX embedding scripts')
assert.match(bridge, /onTdxStockChange/, 'bridge should expose an alternate global callback name')
assert.match(bridge, /stocklink/, 'bridge should accept target-site-style stocklink callback input')
assert.match(bridge, /CODE_000090/, 'bridge should document CODE_ style extraction')
assert.match(bridge, /gpdm=SH600589/, 'bridge should document gpdm-style extraction')

const limitUp = read('src/views/tdx/TdxLimitUpLive.vue')
assert.match(limitUp, /installTdxStockSelectionBridge/, 'limit-up plugin should subscribe to TDX external stock selection')
assert.match(limitUp, /handleExternalStockSelection/, 'limit-up plugin should route external stock selection into the embedded move panel')
assert.doesNotMatch(limitUp, /handleExternalStockSelection[\s\S]{0,220}openStock/, 'external TDX selection should not trigger treeid navigation back to TDX')

const tdxRouter = read('src/router/tdx.ts')
const appRouter = read('src/router/index.ts')
for (const router of [tdxRouter, appRouter]) {
  assert.match(router, /\/tdx\/ztlive\/:code\?\/dark/, 'router should support TDX ztlive URLs that include the active stock code')
}

console.log('tdx stock selection bridge checks passed')
