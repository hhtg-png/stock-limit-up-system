import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const view = readFileSync(resolve(root, 'src/views/JiegeMode.vue'), 'utf8')
const types = readFileSync(resolve(root, 'src/types/intelligence.ts'), 'utf8')

assert.match(view, /昨日预判/, 'JiegeMode should render a yesterday prediction panel')
assert.match(view, /yesterdayPrediction/, 'JiegeMode should expose yesterday prediction computed data')
assert.match(types, /yesterday_prediction/, 'JiegeSignalData should type the backend yesterday prediction payload')

console.log('jiege yesterday prediction structure checks passed')
