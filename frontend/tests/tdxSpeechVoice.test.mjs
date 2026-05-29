import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const speech = readFileSync(resolve(root, 'src/composables/useSpeech.ts'), 'utf8')

assert.match(speech, /targetSpeechProfile/, 'useSpeech should define a target-like voice profile')
assert.match(speech, /Microsoft Xiaoxiao/, 'voice profile should prefer target-like Mandarin female voices')
assert.match(speech, /Microsoft Huihui/, 'voice profile should include common Windows Mandarin voice fallback')
assert.match(speech, /Google 普通话/, 'voice profile should include Chrome Mandarin voice fallback')
assert.match(speech, /targetNeuralTtsEndpoint/, 'useSpeech should define the project neural TTS endpoint')
assert.match(speech, /\/api\/v1\/tts\/speech/, 'audio playback should use the backend neural TTS proxy')
assert.match(speech, /edge-tts/, 'comments should document the GitHub neural TTS backend choice')
assert.match(speech, /targetNeuralTtsVoice\s*=\s*'zh-CN-XiaoyiNeural'/, 'plugin neural TTS should use the faster Xiaoyi voice')
assert.match(speech, /rate:\s*1\.08/, 'browser fallback speech should be faster for market broadcast')
assert.match(speech, /pitch:\s*1\.05/, 'browser fallback speech should use a slightly brighter pitch')
assert.match(speech, /speechPitch/, 'useSpeech should control pitch, not just rate and volume')
assert.match(speech, /utterance\.pitch\s*=\s*speechPitch\.value/, 'speech utterance should apply target-like pitch')
assert.match(speech, /utterance\.voice\s*=/, 'speech utterance should pin a selected Mandarin voice when available')
assert.match(speech, /getVoices\(\)/, 'speech voice selection should inspect browser voices')
assert.match(speech, /voiceschanged/, 'speech voice selection should handle async browser voice loading')
assert.match(speech, /targetTtsAudioId/, 'useSpeech should create a target-style hidden audio fallback')
assert.doesNotMatch(speech, /tts\.baidu\.com\/text2audio/, 'audio playback should not use crackle-prone Baidu compressed TTS')
assert.match(speech, /playWithNeuralTts/, 'useSpeech should play neural MP3 first')
assert.match(speech, /playWithAudioFallback/, 'useSpeech should play by audio when Web Speech is unavailable')
assert.match(speech, /ensureTargetTtsAudio/, 'useSpeech should reuse a hidden audio element for WebView playback')
assert.match(speech, /document\.createElement\('audio'\)/, 'audio fallback should not rely on WebView exposing HTMLAudioElement')

console.log('tdx speech voice checks passed')
