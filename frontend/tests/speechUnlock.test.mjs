import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const speech = readFileSync(resolve(root, 'src/composables/useSpeech.ts'), 'utf8')
const app = readFileSync(resolve(root, 'src/App.vue'), 'utf8')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')

assert.match(speech, /unlockSpeech/, 'useSpeech should expose a user-gesture speech unlock function')
assert.match(speech, /speechUnlocked/, 'useSpeech should track whether mobile speech has been unlocked')
assert.match(speech, /alert_sound_enabled/, 'speech should respect the configured sound reminder switch')
assert.match(speech, /alertStore\.soundEnabled/, 'speech should respect the live sound store switch')
assert.match(speech, /speakInternal\('语音播报功能正常',\s*true\)/, 'test speech should force playback through the unlocked audio path')
assert.match(speech, /type UnlockSpeechOptions/, 'speech unlock should support silent top-bar activation')
assert.doesNotMatch(speech, /语音播报已启用/, 'unlocking speech should not speak an enable prompt')
assert.match(app, /unlockSpeech/, 'App should call speech unlock from a visible mobile action')
assert.match(app, /unlockSpeech\(\{\s*silent:\s*true\s*\}\)/, 'top alert toggle should unlock speech without double-playing the unlock prompt')
assert.doesNotMatch(app, /announceEnabled\(\)/, 'turning on broadcast should not speak a separate enable prompt')
assert.match(app, /语音/, 'App should label the mobile speech action clearly')
assert.match(settings, /useAlertStore/, 'Settings should sync sound switch changes to the live alert store')
assert.match(settings, /alertStore\.setSoundEnabled\(payload\.alert_sound_enabled/, 'Settings should update live sound state after saving')

console.log('speech unlock structure checks passed')
