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
    /knownLimitUpSpeechKeys/,
    `${path} should separate known snapshot events from spoken speech`
  )
  assert.match(
    source,
    /spokenLimitUpSpeechAt/,
    `${path} should track actual spoken limit-up messages by timestamp`
  )
  assert.match(
    source,
    /function rememberExistingEvents[\s\S]*markKnownLimitUpSpeechKey\(item\)[\s\S]*rememberTouchedStock\(item\)/,
    `${path} initial snapshots should only mark events as known`
  )
  const rememberBlock = source.match(/function rememberExistingEvents[\s\S]*?\n}\s*\n\s*function announceNewStatusEvents/)?.[0] || ''
  assert.doesNotMatch(
    rememberBlock,
    /spokenLimitUpSpeechAt\.set/,
    `${path} initial snapshots must not mark events as spoken`
  )
  assert.match(
    source,
    /function announceNewStatusEvents\(items: TdxLimitUpEvent\[\],\s*source:\s*'snapshot' \| 'realtime'/,
    `${path} should distinguish polling snapshots from realtime events`
  )
  assert.match(
    source,
    /source === 'snapshot' && wasKnown/,
    `${path} should avoid replaying already known snapshot rows`
  )
  assert.match(
    source,
    /rememberLimitUpSpeech\(item,\s*speechText\)/,
    `${path} should apply 1 minute speech dedupe only when it is about to enqueue`
  )
  assert.match(
    source,
    /if \(!rememberLimitUpSpeech\(item,\s*speechText\)\) continue[\s\S]*enqueuePluginSpeech\(speechText/,
    `${path} should guarantee the first eligible speech is queued before later duplicates are skipped`
  )
}

console.log('tdx limit-up speech dedupe checks passed')
