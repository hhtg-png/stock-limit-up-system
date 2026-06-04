import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

for (const path of [
  'src/views/tdx/TdxLimitUpLive.vue',
  'src/views/tdx/TdxCompositeWatch.vue'
]) {
  const source = read(path)
  assert.match(
    source,
    /LIMIT_UP_SPEECH_DEDUPE_WINDOW_MS\s*=\s*60\s*\*\s*1000/,
    `${path} should dedupe repeated limit-up speech within 1 minute`
  )
  assert.match(
    source,
    /seenSpeechKeys/,
    `${path} should keep the previous event-identity speech guard`
  )
  assert.match(
    source,
    /spokenLimitUpSpeechAt/,
    `${path} should track actual spoken limit-up messages by timestamp`
  )
  assert.match(
    source,
    /function rememberExistingEvents[\s\S]*seenSpeechKeys\.add\(limitUpEventSpeechKey\(item\)\)[\s\S]*rememberTouchedStock\(item\)/,
    `${path} initial snapshots should only mark event ids as seen`
  )
  const rememberBlock = source.match(/function rememberExistingEvents[\s\S]*?\n}\s*\n\s*function announceNewStatusEvents/)?.[0] || ''
  assert.doesNotMatch(
    rememberBlock,
    /spokenLimitUpSpeechAt\.set/,
    `${path} initial snapshots must not mark events as spoken`
  )
  assert.match(
    source,
    /function announceNewStatusEvents\(items: TdxLimitUpEvent\[\]\)/,
    `${path} should keep realtime speech in the visible plugin page`
  )
  assert.match(
    source,
    /if \(!key \|\| seenSpeechKeys\.has\(key\)\) continue[\s\S]*const isFirstTouch = isFirstTouchedStock\(item\)/,
    `${path} should inspect first-touch status before marking a realtime event handled`
  )
  assert.match(
    source,
    /rememberLimitUpSpeech\(item,\s*speechText\)/,
    `${path} should apply 1 minute speech dedupe only when it is about to enqueue`
  )
  assert.match(
    source,
    /if \(!rememberLimitUpSpeech\(item,\s*speechText\)\) \{[\s\S]*seenSpeechKeys\.add\(key\)[\s\S]*continue[\s\S]*\}[\s\S]*enqueuePluginSpeech\(speechText/,
    `${path} should guarantee the first eligible speech is queued before later duplicates are skipped`
  )
  assert.match(
    source,
    /if \(!enqueuePluginSpeech\(speechText,\s*key,\s*\{\s*force:\s*true,\s*urgent:\s*true\s*\}\)\) \{[\s\S]*forgetLimitUpSpeech\(item,\s*speechText\)[\s\S]*continue[\s\S]*\}[\s\S]*seenSpeechKeys\.add\(key\)[\s\S]*rememberTouchedStock\(item\)/,
    `${path} should only mark eligible events handled after they enter the speech queue`
  )
}

console.log('tdx limit-up speech dedupe checks passed')
