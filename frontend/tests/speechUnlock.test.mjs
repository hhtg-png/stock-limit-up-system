import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const speech = readFileSync(resolve(root, 'src/composables/useSpeech.ts'), 'utf8')
const app = readFileSync(resolve(root, 'src/App.vue'), 'utf8')

assert.match(speech, /unlockSpeech/, 'useSpeech should expose a user-gesture speech unlock function')
assert.match(speech, /speechUnlocked/, 'useSpeech should track whether mobile speech has been unlocked')
assert.match(app, /unlockSpeech/, 'App should call speech unlock from a visible mobile action')
assert.match(app, /语音/, 'App should label the mobile speech action clearly')

console.log('speech unlock structure checks passed')
