import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import test from 'node:test'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const source = readFileSync(resolve(root, 'src/composables/useWebSocket.ts'), 'utf8')

test('limit_up_alert websocket messages trigger speech announcement', () => {
  const match = source.match(/case 'limit_up_alert':([\s\S]*?)break/)
  assert.ok(match, 'limit_up_alert case should exist')
  assert.match(
    match[1],
    /announceStock\s*\(/,
    'limit_up_alert should call announceStock so the broadcast is spoken'
  )
})
