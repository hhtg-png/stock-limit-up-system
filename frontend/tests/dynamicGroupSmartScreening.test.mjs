import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'vite'
import { resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const definition = (name = '智能观察') => ({ name, description: '', folder: '', color: '#409eff', pinned: false, enabled: true, universe: 'all_a', membership_mode: 'dynamic', match_mode: 'all', rules: [{ all: [{ field: 'is_st', op: 'eq', value: false }, { any: [{ field: 'change_pct', op: 'gte', value: 5 }, { field: 'recent_limit_up_count', days: 5, op: 'gte', value: 2 }] }] }], strategy: { prompt: '最近5日两次涨停或者今日涨幅大于5，排除ST', summary: '观察活跃强势股', assumptions: ['最近交易日排除今日'], ranking: { field: 'recent_limit_up_count', direction: 'desc', days: 5, limit: 20 } } })
const response = value => ({ status: 'ready', message: '理解完成', definition: value, assumptions: ['最近交易日排除今日'], questions: [], unsupported: [], model: 'test-model' })
function deferred() { let resolve; const promise = new Promise(done => { resolve = done }); return { promise, resolve } }
async function withModules(run) {
  const server = await createServer({ configFile: false, root, logLevel: 'silent', server: { middlewareMode: true }, resolve: { alias: { '@': resolve(root, 'src') } } })
  try { await run(await server.ssrLoadModule('/src/utils/dynamicSmartScreening.ts'), await server.ssrLoadModule('/src/utils/dynamicGroups.ts')) } finally { await server.close() }
}

test('ready interpretation preserves composite logic and ranking window and supports refinement context', async () => {
  await withModules(async ({ createSmartScreeningController }, { groupDefinition }) => {
    const applied = [], calls = []
    const controller = createSmartScreeningController({ interpret: async (prompt, context) => { calls.push({ prompt, context }); return response(definition()) }, apply: value => applied.push(value) })
    controller.setPrompt('最近5日两次涨停或者今日涨幅大于5，排除ST')
    await controller.interpret()
    assert.equal(controller.ready.value, true)
    assert.deepEqual(groupDefinition(applied[0]), definition())
    controller.setPrompt('只保留前十名')
    assert.equal(controller.ready.value, false)
    await controller.interpret(applied[0])
    assert.equal(calls[1].prompt, '只保留前十名')
    assert.deepEqual(calls[1].context, definition())
  })
})

test('questions and unsupported requests keep the prompt and never enable saving or apply partial conditions', async () => {
  await withModules(async ({ createSmartScreeningController }) => {
    const applied = []
    for (const status of ['needs_clarification', 'unsupported', 'error']) {
      const controller = createSmartScreeningController({ interpret: async () => ({ ...response(null), status, message: '请补充说明', questions: ['最近几天？'], unsupported: ['均线金叉'] }), apply: value => applied.push(value) })
      controller.setPrompt('最近活跃而且均线金叉')
      await controller.interpret()
      assert.equal(controller.ready.value, false)
      assert.equal(controller.stale.value, false, 'a fresh clarification response must remain visible')
      assert.equal(controller.state.prompt, '最近活跃而且均线金叉')
      assert.deepEqual(controller.state.result.questions, ['最近几天？'])
    }
    assert.equal(applied.length, 0)
  })
})

test('editing a slow request and completing an older interpretation cannot overwrite the newer draft', async () => {
  await withModules(async ({ createSmartScreeningController }) => {
    const first = deferred(), second = deferred(), applied = [], calls = []
    const controller = createSmartScreeningController({ interpret: prompt => { calls.push(prompt); return calls.length === 1 ? first.promise : second.promise }, apply: value => applied.push(value.name) })
    controller.setPrompt('旧描述'); const old = controller.interpret()
    controller.setPrompt('新的描述'); assert.equal(controller.ready.value, false)
    const latest = controller.interpret(); second.resolve(response(definition('新结果'))); await latest
    first.resolve(response(definition('旧结果'))); await old
    assert.deepEqual(applied, ['新结果'])
    assert.equal(controller.state.prompt, '新的描述')
    assert.equal(controller.ready.value, true)
    controller.setPrompt('仍要调整'); assert.equal(controller.ready.value, false)
  })
})

test('disposal and malformed ready payloads cannot produce an accepted definition', async () => {
  await withModules(async ({ createSmartScreeningController }) => {
    const pending = deferred(), applied = []
    const controller = createSmartScreeningController({ interpret: () => pending.promise, apply: value => applied.push(value) })
    controller.setPrompt('测试'); const request = controller.interpret(); controller.dispose(); pending.resolve(response(definition())); await request
    assert.equal(applied.length, 0)
    const malformed = createSmartScreeningController({ interpret: async () => ({ ...response(definition()), unsupported: ['缺失条件'] }), apply: value => applied.push(value) })
    malformed.setPrompt('测试'); await malformed.interpret(); assert.equal(malformed.ready.value, false); assert.equal(applied.length, 0)
  })
})

test('nested validation and summaries retain AND/OR and completed-session windows', async () => {
  await withModules(async (_controller, { validateDefinition, ruleSummary }) => {
    const fields = [ { key: 'is_st', label: 'ST', kind: 'boolean', operators: ['eq'] }, { key: 'change_pct', label: '涨幅', kind: 'number', operators: ['gte'] }, { key: 'recent_limit_up_count', label: '涨停次数', kind: 'number', operators: ['gte'] } ]
    const value = definition()
    assert.equal(validateDefinition(value, fields), '')
    assert.match(ruleSummary(value.rules, fields, 'all'), /且/)
    assert.match(ruleSummary(value.rules, fields, 'all'), /或/)
    assert.match(ruleSummary(value.rules, fields, 'all'), /近5个已完成交易日/)
    value.rules[0].all[1].any[1].days = 0
    assert.match(validateDefinition(value, fields), /1.*60/)
  })
})
