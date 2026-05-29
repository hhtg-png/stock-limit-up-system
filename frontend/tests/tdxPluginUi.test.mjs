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

const plate = read('src/views/tdx/TdxPlateStrength.vue')
assert.match(plate, /strength_score/, 'plate plugin should render strength scores')
assert.match(plate, /core_stocks/, 'plate plugin should render core stocks')

const news = read('src/views/tdx/TdxNewsFeed.vue')
assert.match(news, /importance/, 'news plugin should render importance')
assert.match(news, /enqueuePluginSpeech/, 'news plugin should support speech')
assert.match(news, /v-for="item in aggregateItems"/, 'aggregate news panel should render the target-like quick-news stream')
assert.match(news, /item\.source !== '韭研公社'/, 'aggregate news panel should keep JYGS recognition posts out of the main stream')

console.log('tdx plugin UI structure checks passed')
