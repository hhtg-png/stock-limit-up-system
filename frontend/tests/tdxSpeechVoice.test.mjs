import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const speech = readFileSync(resolve(root, 'src/composables/useSpeech.ts'), 'utf8')

assert.match(speech, /targetSpeechProfile/, 'useSpeech should define a target-like voice profile')
assert.match(speech, /Microsoft Xiaoxiao/, 'voice profile should prefer target-like Mandarin female voices')
assert.match(speech, /Microsoft Huihui/, 'voice profile should include common Windows Mandarin voice fallback')
assert.match(speech, /Google 普通话/, 'voice profile should include Chrome Mandarin voice fallback')
assert.match(speech, /speechPitch/, 'useSpeech should control pitch, not just rate and volume')
assert.match(speech, /utterance\.pitch\s*=\s*speechPitch\.value/, 'speech utterance should apply target-like pitch')
assert.match(speech, /utterance\.voice\s*=/, 'speech utterance should pin a selected Mandarin voice when available')
assert.match(speech, /getVoices\(\)/, 'speech voice selection should inspect browser voices')
assert.match(speech, /voiceschanged/, 'speech voice selection should handle async browser voice loading')

console.log('tdx speech voice checks passed')
