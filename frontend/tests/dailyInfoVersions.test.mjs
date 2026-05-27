import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

const viewSource = readFileSync('src/views/DailyInfo.vue', 'utf8')
const apiSource = readFileSync('src/api/intelligence.ts', 'utf8')
const typeSource = readFileSync('src/types/intelligence.ts', 'utf8')

test('daily info keeps same-day generated versions selectable', () => {
  assert.match(typeSource, /version_id\?: number \| null/, 'daily info response should expose generation version id')
  assert.match(apiSource, /versionId\?: number \| null/, 'daily info API should accept an optional version id')
  assert.match(apiSource, /version_id: versionId/, 'daily info request should pass version_id to the backend')
  assert.match(viewSource, /sameDateVersions/, 'daily review should compute same-day generated versions')
  assert.match(viewSource, /handleVersionPageChange/, 'daily review should page between same-day versions')
  assert.match(viewSource, /historyKey\(item\)/, 'history rows should use version-aware keys')
  assert.doesNotMatch(viewSource, /history\.trade_date !== item\.trade_date/, 'history upsert must not deduplicate by date')
})
