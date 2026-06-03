import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const limitUp = read('src/views/tdx/TdxLimitUpLive.vue')
const composite = read('src/views/tdx/TdxCompositeWatch.vue')
assert.match(limitUp, /getTdxLimitUpLiveStatus/, '涨停播报 should use a lightweight realtime status endpoint')
assert.match(limitUp, /QUOTE_REFRESH_MS\s*=\s*3000/, '涨停播报 should refresh quote/status fields every 3 seconds like the target page')
assert.match(limitUp, /SNAPSHOT_REFRESH_MS\s*=\s*30000/, '涨停播报 should refresh the full snapshot on a slower count-style cadence')
assert.match(limitUp, /useLimitUpStore/, '涨停播报 should merge WebSocket limit-up snapshot/delta state into the table')
assert.match(limitUp, /useTdxPluginRealtime/, '涨停播报 should merge plugin WebSocket events into the table')
assert.match(limitUp, /snapshotInFlight/, '涨停播报 should prevent overlapping slow snapshot requests')
assert.match(limitUp, /statusInFlight/, '涨停播报 should prevent overlapping fast status requests')
assert.match(limitUp, /function hasSnapshotStructureChanged[\s\S]*if \(!statusItems\.length\) return false[\s\S]*if \(!snapshotItems\.length\) return true/, '涨停播报 should refresh the rich snapshot when realtime status arrives after an empty first snapshot')
assert.match(composite, /function hasSnapshotStructureChanged[\s\S]*if \(!statusItems\.length\) return false[\s\S]*if \(!snapshotItems\.length\) return true/, '复合看盘 should refresh the rich snapshot when realtime status arrives after an empty first snapshot')
assert.match(limitUp, /filterSnapshotItemsByStatusDate\(payload\.value\?\.items \|\| \[\], statusPayload\.value\?\.items \|\| \[\]\)/, '涨停播报 should drop stale snapshot rows once a newer status trade date is available')
assert.match(composite, /filterSnapshotItemsByStatusDate\(payload\.value\?\.items \|\| \[\], statusPayload\.value\?\.items \|\| \[\]\)/, '复合看盘 should drop stale snapshot rows once a newer status trade date is available')
assert.match(limitUp, /function latestEventDatePrefix/, '涨停播报 should compare event date prefixes to avoid mixing yesterday and today rows')
assert.match(composite, /function latestEventDatePrefix/, '复合看盘 should compare event date prefixes to avoid mixing yesterday and today rows')
assert.doesNotMatch(limitUp, /setInterval\(loadData,\s*5000\)/, '涨停播报 should not depend on 5 second full-table polling')

const news = read('src/views/tdx/TdxNewsFeed.vue')
assert.match(news, /useTdxPluginRealtime/, '聚合快讯 should consume plugin WebSocket realtime news')
assert.match(news, /realtimeNewsItems/, '聚合快讯 should place realtime WebSocket news into the rendered feed')
assert.match(news, /NEWS_REFRESH_MS\s*=\s*10000/, '聚合快讯 should use faster polling only as a WebSocket fallback')
assert.doesNotMatch(news, /setInterval\(loadData,\s*30000\)/, '聚合快讯 should not rely on 30 second polling for voice news')

const ws = read('src/composables/useWebSocket.ts')
assert.match(ws, /tdxNewsItems/, 'WebSocket composable should keep shared TDX news events')
assert.match(ws, /tdxLimitUpEvents/, 'WebSocket composable should keep shared TDX limit-up events')
assert.match(ws, /useTdxPluginRealtime/, 'WebSocket composable should expose TDX plugin realtime state')
assert.match(ws, /pushTdxNewsItem/, 'tdx_news_event messages should be normalized into realtime news state')
assert.match(ws, /pushTdxLimitUpEvent/, 'tdx_limit_up_event messages should be normalized into realtime limit-up state')
assert.doesNotMatch(ws, /`news-\$\{item\.news_id \|\| message\.timestamp\}`/, 'tdx_news_event speech should be owned by news pages, not the websocket handler')

console.log('tdx realtime hybrid strategy checks passed')
