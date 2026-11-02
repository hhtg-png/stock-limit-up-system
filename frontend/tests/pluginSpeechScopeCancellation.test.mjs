import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import vm from 'node:vm'
import ts from 'typescript'

const source = readFileSync(resolve(import.meta.dirname, '../src/composables/useSpeech.ts'), 'utf8')
const compiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 }
}).outputText

function createSpeechHarness({ audioEnabled = false } = {}) {
  const spoken = []
  const storage = new Map()
  const timers = new Map()
  let nextTimer = 1
  let cancellationCount = 0
  const audio = {
    tagName: 'AUDIO', src: '', pauseCount: 0,
    pause() { this.pauseCount += 1 },
    play() {},
    load() {},
    removeAttribute(name) { if (name === 'src') this.src = '' }
  }
  const speechSynthesis = {
    getVoices: () => [],
    addEventListener() {},
    resume() {},
    cancel() { cancellationCount += 1 },
    speak(utterance) { spoken.push(utterance) }
  }
  const exports = {}
  const context = {
    exports,
    require(name) {
      if (name === 'vue') return { ref: value => ({ value }) }
      if (name === '@/stores/config') return {
        useConfigStore: () => ({ config: { alert_limit_up_enabled: true, alert_sound_enabled: true } })
      }
      if (name === '@/stores/alert') return { useAlertStore: () => ({ enabled: true, soundEnabled: true }) }
      throw new Error(`Unexpected dependency: ${name}`)
    },
    window: {
      speechSynthesis,
      SpeechSynthesisUtterance: class {},
      localStorage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
      matchMedia: () => ({ matches: false })
    },
    SpeechSynthesisUtterance: class { constructor(text) { this.text = text } },
    navigator: { maxTouchPoints: 0 },
    URLSearchParams,
    console: { debug() {} },
    setTimeout(callback) { const id = nextTimer++; timers.set(id, callback); return id },
    clearTimeout(id) { timers.delete(id) }
  }
  if (audioEnabled) {
    context.document = {
      getElementById: () => audio,
      createElement: () => audio,
      body: { appendChild() {} }
    }
  }
  vm.runInNewContext(compiled, context, { filename: 'useSpeech.ts' })
  const speech = exports.useSpeech()
  assert.equal(speech.unlockSpeech({ silent: true }), true)
  const enqueue = (text, scope, extra = {}) => speech.enqueuePluginSpeech(text, `event-${text}`, { force: true, scope, ...extra })
  return { speech, enqueue, spoken, storage, audio, timers, get cancellationCount() { return cancellationCount } }
}

test('cancels only matching pending scope while retaining other and legacy unscoped speech', () => {
  const h = createSpeechHarness()
  h.enqueue('active', 'source-a')
  h.enqueue('cancel-pending', 'source-b')
  h.enqueue('other-source', 'source-c')
  h.enqueue('legacy')
  const cancellations = h.cancellationCount

  h.speech.cancelPluginSpeech('source-b')
  assert.equal(h.cancellationCount, cancellations, 'unrelated current playback must continue')
  h.spoken[0].onend()
  assert.deepEqual(h.spoken.map(item => item.text), ['active', 'other-source'])
  h.spoken[1].onend()
  assert.deepEqual(h.spoken.map(item => item.text), ['active', 'other-source', 'legacy'])
  assert.equal(h.enqueue('cancel-pending', 'source-b'), false, 'cancellation must not erase event deduplication')
})

test('cancels current matching scope immediately, clears its pending work, and retains master preference', () => {
  const h = createSpeechHarness()
  h.enqueue('cancel-active', 'source-a')
  h.enqueue('cancel-pending', 'source-a')
  h.enqueue('retained', 'source-b')
  const cancellations = h.cancellationCount

  h.speech.cancelPluginSpeech('source-a')
  assert.equal(h.cancellationCount, cancellations + 1)
  assert.deepEqual(h.spoken.map(item => item.text), ['cancel-active', 'retained'])
  assert.equal(h.speech.speechUnlocked.value, true)
  assert.equal(h.storage.get('tdx-plugin-speech-unlocked'), '1')
  assert.equal(h.enqueue('new-after-cancel', 'source-a'), true, 'source cancellation must not disable the shared speech preference')
  h.spoken[1].onend()
  assert.deepEqual(h.spoken.map(item => item.text), ['cancel-active', 'retained', 'new-after-cancel'])
})

test('empty scopes are no-ops and never cancel unscoped legacy entries', () => {
  const h = createSpeechHarness()
  h.enqueue('legacy-active')
  h.enqueue('legacy-pending')
  const cancellations = h.cancellationCount
  h.speech.cancelPluginSpeech('')
  h.speech.cancelPluginSpeech('   ')
  h.speech.cancelPluginSpeech('unknown')
  assert.equal(h.cancellationCount, cancellations)
  h.spoken[0].onend()
  assert.deepEqual(h.spoken.map(item => item.text), ['legacy-active', 'legacy-pending'])
})

test('late Web Speech callbacks from canceled playback cannot finish the replacement item', () => {
  const h = createSpeechHarness()
  h.enqueue('old', 'source-a')
  const old = h.spoken[0]
  h.enqueue('replacement', 'source-b')
  h.enqueue('after-replacement')
  h.speech.cancelPluginSpeech('source-a')

  old.onend()
  old.onerror({ error: 'canceled' })
  assert.deepEqual(h.spoken.map(item => item.text), ['old', 'replacement'])
  h.spoken[1].onend()
  assert.deepEqual(h.spoken.map(item => item.text), ['old', 'replacement', 'after-replacement'])
})

test('canceling neural audio clears startup timers and stale callbacks cannot pause or replace new audio', () => {
  const h = createSpeechHarness({ audioEnabled: true })
  h.enqueue('old-audio', 'source-a')
  const oldEnded = h.audio.onended
  const oldError = h.audio.onerror
  const oldStarted = h.audio.onplaying
  const oldTimeout = [...h.timers.values()][0]
  h.enqueue('new-audio', 'source-b')
  h.enqueue('last-audio')
  h.speech.cancelPluginSpeech('source-a')

  const nextSource = h.audio.src
  const nextEnded = h.audio.onended
  const nextError = h.audio.onerror
  const pauseCount = h.audio.pauseCount
  assert.match(nextSource, /text=new-audio/)
  assert.equal(h.timers.size, 1, 'canceled audio must not retain a startup timeout')
  oldEnded()
  oldError({ type: 'error' })
  oldStarted()
  oldTimeout()
  assert.equal(h.audio.src, nextSource)
  assert.equal(h.audio.onended, nextEnded)
  assert.equal(h.audio.onerror, nextError)
  assert.equal(h.audio.pauseCount, pauseCount)
  assert.equal(h.spoken.length, 0, 'canceled audio must not fall back to Web Speech')
  nextEnded()
  assert.match(h.audio.src, /text=last-audio/)
})

test('urgent scoped speech retains its scope when interrupting normal speech', () => {
  const h = createSpeechHarness()
  h.enqueue('normal', 'source-a')
  h.enqueue('urgent', 'source-b', { urgent: true })
  h.enqueue('remaining', 'source-c')
  assert.deepEqual(h.spoken.map(item => item.text), ['normal', 'urgent'])
  h.speech.cancelPluginSpeech('source-b')
  assert.deepEqual(h.spoken.map(item => item.text), ['normal', 'urgent', 'remaining'])
})
