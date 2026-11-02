import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import test from 'node:test'
import ts from 'typescript'

const helperPath = resolve(import.meta.dirname, '../src/utils/tdxCompositeSources.ts')
const storageKey = 'tdx-composite-speech-sources-v1'
const defaults = { limit_up: true, cls: true, ths: true, jygs: true, stcn: true, glh: true, other: true }

async function loadHelpers() {
  assert.ok(existsSync(helperPath), 'composite source preferences should be implemented')
  const code = ts.transpileModule(readFileSync(helperPath, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 }
  }).outputText
  return import(`data:text/javascript;charset=utf-8,${encodeURIComponent(code)}`)
}

function withStorage(storage, run) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'localStorage')
  Object.defineProperty(globalThis, 'localStorage', { configurable: true, get: () => storage() })
  try { return run() } finally {
    if (previous) Object.defineProperty(globalThis, 'localStorage', previous)
    else delete globalThis.localStorage
  }
}

test('new preferences enable every source and return independent snapshots', async () => {
  const helpers = await loadHelpers()
  withStorage(() => ({ getItem: () => null }), () => {
    const first = helpers.readCompositeSpeechSources()
    assert.deepEqual(first, defaults)
    first.cls = false
    assert.deepEqual(helpers.readCompositeSpeechSources(), defaults)
  })
})

test('source choices survive storage round trips without touching other preferences', async () => {
  const helpers = await loadHelpers()
  const entries = new Map([['speechEnabled', 'false']])
  const prefs = { limit_up: false, cls: true, ths: false, jygs: true, stcn: false, glh: true, other: false }
  withStorage(() => ({ getItem: key => entries.get(key) ?? null, setItem: (key, value) => entries.set(key, value) }), () => {
    helpers.writeCompositeSpeechSources(prefs)
    assert.deepEqual(JSON.parse(entries.get(storageKey)), prefs)
    assert.deepEqual(helpers.readCompositeSpeechSources(), prefs)
    assert.equal(entries.get('speechEnabled'), 'false')
  })
})

test('stored preferences only accept known boolean fields and fill missing values', async () => {
  const helpers = await loadHelpers()
  const stored = JSON.stringify({ cls: false, ths: 'false', jygs: 0, stcn: null, glh: false, surprise: false })
  withStorage(() => ({ getItem: () => stored }), () => {
    assert.deepEqual(helpers.readCompositeSpeechSources(), { ...defaults, cls: false, glh: false })
  })
})

test('corrupt, inaccessible, and unwritable browser storage never break the watch page', async () => {
  const helpers = await loadHelpers()
  for (const value of ['{broken', 'null', 'false', '[]', '"cls"']) {
    withStorage(() => ({ getItem: () => value }), () => {
      assert.deepEqual(helpers.readCompositeSpeechSources(), defaults)
    })
  }
  withStorage(() => { throw new Error('storage denied') }, () => {
    assert.deepEqual(helpers.readCompositeSpeechSources(), defaults)
    assert.doesNotThrow(() => helpers.writeCompositeSpeechSources(defaults))
  })
  withStorage(() => ({ getItem: () => { throw new Error('read denied') }, setItem: () => { throw new Error('quota exceeded') } }), () => {
    assert.deepEqual(helpers.readCompositeSpeechSources(), defaults)
    assert.doesNotThrow(() => helpers.writeCompositeSpeechSources(defaults))
  })
})

test('news providers resolve from Chinese labels and English keys, with unknown providers kept in other', async () => {
  const helpers = await loadHelpers()
  for (const [source, expected] of [
    ['涨停事件', 'limit_up'], ['limit_up', 'limit_up'],
    ['财联社', 'cls'], [' CLS ', 'cls'], ['同花顺', 'ths'], ['ths', 'ths'],
    ['韭研公社', 'jygs'], ['JYGS', 'jygs'], ['时报快讯', 'stcn'], ['stcn', 'stcn'],
    ['格隆汇', 'glh'], ['glh', 'glh'], ['其他快讯', 'other'], ['other', 'other'],
    ['', 'other'], ['未来快讯', 'other'], ['toString', 'other']
  ]) assert.equal(helpers.resolveCompositeNewsSpeechSource(source), expected, source)
})
