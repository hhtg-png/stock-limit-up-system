import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import ts from 'typescript'

const root = resolve(import.meta.dirname, '..')
const helperSource = readFileSync(resolve(root, 'src/utils/tdxLimitUpDisplay.ts'), 'utf8')
const transpiled = ts.transpileModule(helperSource, {
  compilerOptions: {
    module: ts.ModuleKind.ES2022,
    target: ts.ScriptTarget.ES2022
  }
}).outputText
const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

assert.equal(helper.pickDisplayChangePct(9.99, 0), 9.99, 'status refresh zero should not hide a real snapshot change pct')
assert.equal(helper.pickDisplayChangePct(20.21, null), 20.21, 'empty status change pct should preserve snapshot value')
assert.equal(helper.pickDisplayChangePct(0, 10), 10, 'a valid next change pct should be used')
assert.equal(helper.pickDisplayChangePct(undefined, 0), 0, 'zero remains the fallback when no better value exists')

assert.equal(helper.formatTdxSealAmount(0), '--', 'empty seal amount should be shown as placeholder')
assert.equal(helper.formatTdxSealAmount(96775.588), '9.68亿', 'wan-yuan seal amount should display as yi')
assert.equal(helper.formatTdxSealAmount(20375.7605), '2.04亿', 'live status wan-yuan amount should not be divided twice')
assert.equal(helper.formatTdxSealAmount(131.8935), '132万', 'small wan-yuan seal amount should display as wan')
assert.equal(helper.formatTdxSealAmount(50_000_000), '5000万', 'yuan seal amount should remain supported for seeded test data')

const limitUp = readFileSync(resolve(root, 'src/views/tdx/TdxLimitUpLive.vue'), 'utf8')
const composite = readFileSync(resolve(root, 'src/views/tdx/TdxCompositeWatch.vue'), 'utf8')
for (const source of [limitUp, composite]) {
  assert.match(source, /pickDisplayChangePct/, 'TDX plugin merge should preserve real change pct over empty status updates')
  assert.match(source, /formatTdxSealAmount/, 'TDX plugin should format seal amount with the shared TDX unit helper')
  assert.match(source, /target_seal_amount:\s*formatTdxSealAmount/, 'TDX plugin merged target seal display should stay in sync with numeric seal amount')
}

console.log('tdx limit-up display checks passed')
