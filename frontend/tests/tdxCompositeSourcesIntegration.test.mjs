import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { test } from 'node:test'
import vm from 'node:vm'
import ts from 'typescript'
import * as vue from 'vue'

const frontend = resolve(import.meta.dirname, '..')
const storageKey = 'tdx-composite-speech-sources-v1'
const defaults = { limit_up: true, cls: true, ths: true, jygs: true, stcn: true, glh: true, other: true }

function mountView(saved = {}, storage = new Map([[storageKey, JSON.stringify({ ...defaults, ...saved })]])) {
  const queued = []
  const canceled = []
  const realtimeLimitUpEvents = vue.ref([])
  const realtimeNewsItems = vue.ref([])
  const localStorage = { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) }
  const context = vm.createContext({ console, localStorage, window: {
    localStorage,
    setInterval: () => 0, setTimeout: () => 0, clearTimeout() {}, clearInterval() {},
    addEventListener() {}, removeEventListener() {}
  } })
  const modules = new Map()
  function load(path) {
    if (modules.has(path)) return modules.get(path)
    const exports = {}
    modules.set(path, exports)
    const source = readFileSync(resolve(frontend, path), 'utf8')
    const js = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 } }).outputText
    vm.runInContext(`(function(require,module,exports){${js}\n})`, context)(requireModule, { exports }, exports)
    return exports
  }
  function requireModule(name) {
    if (name === 'vue') return { ...vue, onMounted() {}, onUnmounted() {} }
    if (name === 'pinia') return { storeToRefs: () => ({ realtimeList: vue.ref([]) }) }
    if (name === '@/composables/useSpeech') return { useSpeech: () => ({
      speechUnlocked: vue.ref(true), unlockSpeech() {}, lockSpeech() {},
      enqueuePluginSpeech: (text, key, options) => { queued.push({ text, key, options }); return true },
      cancelPluginSpeech: scope => canceled.push(scope)
    }) }
    if (name === '@/composables/useWebSocket') return {
      clearTdxPluginRealtime() {}, useTdxPluginRealtime: () => ({ realtimeLimitUpEvents, realtimeNewsItems })
    }
    if (name === '@/stores/limit-up') return { useLimitUpStore: () => ({ setRealtimeSnapshot() {} }) }
    if (name === '@/composables/useTdxStockLink') return { useTdxStockLink: () => ({ openStock() {} }) }
    if (name === '@/composables/useTdxStockSelection') return { installTdxStockSelectionBridge() {} }
    if (name === '@/api/tdx-plugins') return {}
    if (name.endsWith('.vue')) return { default: {} }
    if (name.startsWith('@/utils/')) return load(`src/${name.slice(2)}.ts`)
    throw new Error(`Unexpected import: ${name}`)
  }
  const view = readFileSync(resolve(frontend, 'src/views/tdx/TdxCompositeWatch.vue'), 'utf8')
  const script = view.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
  const js = ts.transpileModule(script, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 } }).outputText
  const api = vm.runInContext(`(function(require,exports){${js}\nreturn { speakNews, handleStatusEvents, seenSpeechKeys, seenTouchedStockCodes,
    preferences: () => typeof sourcePreferences === 'undefined' ? null : sourcePreferences };})`, context)(requireModule, {})
  return { api, queued, canceled, storage }
}

test('remembered news source choice gates the real composite speech entry after reopening', () => {
  const first = mountView({ cls: false })
  const cls = { news_id: 'cls-1', source: '财联社', title: '测试财联社新快讯', time: '10:00:00' }
  assert.equal(first.api.speakNews(cls), false)
  assert.equal(first.queued.length, 0)
  assert.equal(first.api.speakNews({ ...cls, news_id: 'ths-1', source: '同花顺' }), true)
  assert.equal(first.queued[0].options.scope, 'tdx-composite:ths')
  const reopened = mountView({}, first.storage)
  assert.equal(reopened.api.speakNews({ ...cls, news_id: 'cls-2' }), false)
  assert.equal(reopened.queued.length, 0)
})

test('muted limit-up events remain seen so re-enabling speaks only new events', () => {
  const { api, queued } = mountView({ limit_up: false })
  const item = { event_id: 'old', stock_code: '600000', stock_name: '测试股', event_type: 'limit_up_touched',
    event_time: '10:00:00', is_sealed: true, board: 1 }
  api.handleStatusEvents([item], { primeOnly: false })
  assert.equal(queued.length, 0)
  assert.equal(api.seenSpeechKeys.has('old'), true)
  assert.equal(api.seenTouchedStockCodes.has('600000'), true)
  api.preferences().value = { ...api.preferences().value, limit_up: true }
  api.handleStatusEvents([item], { primeOnly: false })
  assert.equal(queued.length, 0)
  api.handleStatusEvents([{ ...item, event_id: 'new', event_type: 'limit_up_resealed' }], { primeOnly: false })
  assert.equal(queued.length, 1)
  assert.equal(queued[0].options.scope, 'tdx-composite:limit_up')
})

test('turning off a source saves immediately and cancels only its speech scope', () => {
  const { api, canceled, storage } = mountView()
  api.preferences().value = { ...api.preferences().value, cls: false }
  assert.equal(JSON.parse(storage.get(storageKey)).cls, false)
  assert.deepEqual(canceled, ['tdx-composite:cls'])
  api.preferences().value = { ...api.preferences().value, cls: true }
  assert.deepEqual(canceled, ['tdx-composite:cls'])
})
