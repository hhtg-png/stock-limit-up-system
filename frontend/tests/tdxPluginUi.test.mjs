import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const shell = read('src/components/tdx/TdxPluginShell.vue')
assert.match(shell, /tdx-plugin-shell/, 'TDX shell should provide a stable black plugin wrapper')
assert.match(shell, /#050b12/, 'TDX shell should use the black Tongdaxin-style background')
assert.match(shell, /overflow:\s*auto/, 'TDX shell should allow dense table/list scrolling')
assert.match(shell, /@media\s*\(max-width:\s*640px\)/, 'TDX shell should have a mobile layout')

const limitUp = read('src/views/tdx/TdxLimitUpLive.vue')
assert.match(limitUp, /封死涨停/, 'limit-up plugin should render sealed labels')
assert.match(limitUp, /enqueuePluginSpeech/, 'limit-up plugin should enqueue speech')
assert.match(limitUp, /openStock/, 'limit-up plugin should link stocks through the TDX bridge')
assert.match(limitUp, /errorText/, 'limit-up plugin should expose request errors instead of rendering a blank table')
assert.match(limitUp, /emptyText/, 'limit-up plugin should explain empty data instead of rendering a blank table')
assert.match(limitUp, /ref="plateScroller"/, 'limit-up plate bar should bind a scroller ref for mouse dragging')
assert.match(limitUp, /@mousedown="startPlateDrag"/, 'limit-up plate bar should support mouse drag horizontal scrolling')
assert.match(limitUp, /scrollbar-width:\s*none/, 'limit-up plate bar should hide native horizontal scrollbar')
assert.match(limitUp, /scrollLeft\s*=/, 'limit-up plate drag should move the horizontal scroll position')
assert.match(limitUp, /embedded-move-panel/, 'limit-up plugin should embed stock move analysis in the lower panel')
assert.match(limitUp, /getTdxStockMove/, 'embedded stock move panel should load stock move analysis data')
assert.match(limitUp, /handleStockClick\(item\)/, 'limit-up stock click should update analysis and trigger stock linking')
assert.match(limitUp, /move-panel-resizer/, 'embedded stock move panel should have a vertical resize handle')
assert.match(limitUp, /@pointerdown="startMovePanelResize"/, 'embedded stock move panel should resize by pointer drag')
assert.match(limitUp, /movePanelPercent/, 'embedded stock move panel should keep a user-adjustable height percentage')
assert.match(limitUp, /MOVE_PANEL_STORAGE_KEY/, 'embedded stock move panel should persist the adjusted height')
assert.match(limitUp, /--zt-table-flex/, 'limit-up table body should use a resizable flex share')
assert.match(limitUp, /--move-panel-flex/, 'embedded stock move panel should use a resizable flex share')
assert.match(limitUp, /cursor:\s*row-resize/, 'resize handle should use vertical resize cursor')
assert.match(limitUp, /STOCK_MOVE_CACHE_TTL_MS/, 'embedded stock move panel should cache arbitrary TDX-linked stock analysis')
assert.match(limitUp, /readCachedStockMove/, 'embedded stock move panel should reuse cached stock analysis immediately')
assert.match(limitUp, /refreshSnapshotWhenStructureChanged/, 'limit-up plugin should only refresh the full snapshot when the list structure changes')
assert.doesNotMatch(limitUp, /setInterval\(\(\)\s*=>\s*loadData\(\{\s*silent:\s*true\s*\}\)/, 'limit-up plugin should not replace the whole table on a fixed timer')
assert.doesNotMatch(limitUp, />\s*异动解析\s*</, 'embedded stock move panel should not show a redundant analysis label')
assert.doesNotMatch(limitUp, /点击股票查看异动解析|异动解析加载/, 'embedded stock move panel should not use verbose analysis placeholder text')
assert.doesNotMatch(limitUp, /stockMoveLoading[\s\S]{0,120}>加载中|查询中/, 'embedded stock move panel should not show a query/loading state')

const stockMove = read('src/views/tdx/TdxStockMove.vue')
assert.match(stockMove, /STOCK_MOVE_CACHE_TTL_MS/, 'standalone stock move plugin should cache arbitrary TDX-linked stock analysis')
assert.match(stockMove, /readCachedStockMove/, 'standalone stock move plugin should reuse cached stock analysis immediately')
assert.doesNotMatch(stockMove, />加载中|查询中/, 'standalone stock move plugin should not show a query/loading state')

const plate = read('src/views/tdx/TdxPlateStrength.vue')
assert.match(plate, /strength_score/, 'plate plugin should render strength scores')
assert.match(plate, /core_stocks/, 'plate plugin should render core stocks')

const news = read('src/views/tdx/TdxNewsFeed.vue')
assert.match(news, /importance/, 'news plugin should render importance')
assert.match(news, /enqueuePluginSpeech/, 'news plugin should support speech')
assert.match(news, /v-for="item in aggregateItems"/, 'aggregate news panel should render the target-like quick-news stream')
assert.match(news, /item\.source !== '韭研公社'/, 'aggregate news panel should keep JYGS recognition posts out of the main stream')

console.log('tdx plugin UI structure checks passed')
