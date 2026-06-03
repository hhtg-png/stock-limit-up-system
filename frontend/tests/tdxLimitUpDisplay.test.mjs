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

const openedState = helper.resolveTdxMergedDisplayState(
  {
    event_type: 'limit_up_opened',
    event_label: '涨停打开',
    is_sealed: false,
    open_count: 1,
    seal_amount: 0,
    target_status_label: '炸板',
    target_seal_amount: '--'
  },
  {
    event_type: 'limit_up_touched',
    event_label: '摸板',
    is_sealed: true,
    open_count: 0,
    seal_amount: 0,
    target_status_label: '2板',
    target_seal_amount: ''
  }
)
assert.equal(openedState.event_type, 'limit_up_opened', 'touch events should not overwrite a known opened state')
assert.equal(openedState.is_sealed, false, 'touch events should not make opened stocks look sealed again')
assert.equal(openedState.target_status_label, '炸板', 'opened stocks should keep the 炸板 display label')
assert.equal(openedState.target_seal_amount, '--', 'opened stocks should keep an empty seal display')

const sealedState = helper.resolveTdxMergedDisplayState(
  {
    event_type: 'limit_up_sealed',
    event_label: '封死涨停',
    is_sealed: true,
    open_count: 0,
    seal_amount: 44985.3537,
    target_status_label: '首板',
    target_seal_amount: '4.50亿'
  },
  {
    event_type: 'limit_up_touched',
    event_label: '摸板',
    is_sealed: true,
    open_count: 0,
    seal_amount: 0,
    target_status_label: '首板',
    target_seal_amount: ''
  }
)
assert.equal(sealedState.seal_amount, 44985.3537, 'touch events without seal amount should not clear a known live seal amount')
assert.equal(sealedState.target_seal_amount, '4.50亿', 'touch events should not turn a known seal amount into --')

const limitUp = readFileSync(resolve(root, 'src/views/tdx/TdxLimitUpLive.vue'), 'utf8')
const composite = readFileSync(resolve(root, 'src/views/tdx/TdxCompositeWatch.vue'), 'utf8')
for (const source of [limitUp, composite]) {
  assert.match(source, /pickDisplayChangePct/, 'TDX plugin merge should preserve real change pct over empty status updates')
  assert.match(source, /formatTdxSealAmount/, 'TDX plugin should format seal amount with the shared TDX unit helper')
  assert.match(source, /target_seal_amount:\s*formatTdxSealAmount/, 'TDX plugin merged target seal display should stay in sync with numeric seal amount')
}

console.log('tdx limit-up display checks passed')
