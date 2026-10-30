import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'vite'
import { resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
async function withHelpers(run) {
  const server = await createServer({ configFile: false, root, logLevel: 'silent', server: { middlewareMode: true }, resolve: { alias: { '@': resolve(root, 'src') } } })
  try { await run(await server.ssrLoadModule('/src/utils/dynamicGroups.ts')) } finally { await server.close() }
}

test('latest request gate prevents an older group or batch response and disposal from committing', async () => {
  await withHelpers(({ createRequestGate }) => {
    const gate = createRequestGate()
    const groupA = gate.begin()
    assert.equal(gate.isCurrent(groupA), true)
    const groupB = gate.begin()
    assert.equal(gate.isCurrent(groupA), false)
    assert.equal(gate.isCurrent(groupB), true)
    gate.invalidate()
    assert.equal(gate.isCurrent(groupB), false)
  })
})

test('definition serialization excludes group metadata required by strict write schemas', async () => {
  await withHelpers(({ groupDefinition }) => {
    const group = { id: 5, version: 3, archived: false, name: '测试', description: '', folder: '竞价', color: '#409eff', pinned: false, enabled: true, universe: 'all_a', membership_mode: 'retain', match_mode: 'all', rules: [{ field: 'is_st', op: 'eq', value: false }], member_count: 3 }
    const result = groupDefinition(group)
    assert.equal(result.id, undefined)
    assert.equal(result.version, undefined)
    assert.equal(result.member_count, undefined)
    assert.equal(result.rules[0].value, false)
    assert.equal(result.pinned, false)
    assert.equal(Object.keys(result).length, 10)
  })
})

test('intraday chart separates rule versions and missing minutes without inventing returns', async () => {
  await withHelpers(({ intradayChartPoints }) => {
    const points = [
      { time: '2026-10-30T09:30:00', value: 1000, version: 1 },
      { time: '2026-10-30T09:31:00', value: 1002, version: 1 },
      { time: '2026-10-30T09:35:00', value: 1003, version: 1 },
      { time: '2026-10-30T09:36:00', value: 1000, version: 2, segment: 1 },
      { time: '2026-10-30T09:37:00', value: 1000, version: 2, segment: 2 }
    ]
    const result = intradayChartPoints(points)
    assert.deepEqual(result.map(point => point.value), [1000, 1002, null, 1003, null, 1000, null, 1000])
    assert.equal(points.length, 5)
  })
})

test('rule validation preserves meaningful false and zero and rejects missing or reversed ranges', async () => {
  await withHelpers(({ validateDefinition }) => {
    const fields = [
      { key: 'turnover_rate', label: '换手率', kind: 'number', operators: ['gte', 'between'] },
      { key: 'is_st', label: 'ST', kind: 'boolean', operators: ['eq'] }
    ]
    const definition = { name: '观察', rules: [{ field: 'turnover_rate', op: 'gte', value: 0 }, { field: 'is_st', op: 'eq', value: false }] }
    assert.equal(validateDefinition(definition, fields), '')
    assert.match(validateDefinition({ ...definition, rules: [{ field: 'turnover_rate', op: 'gte', value: '' }] }, fields), /填写/)
    assert.match(validateDefinition({ ...definition, rules: [{ field: 'turnover_rate', op: 'between', value: 10, value2: 2 }] }, fields), /区间/)
    assert.match(validateDefinition({ ...definition, rules: [{ field: 'unknown', op: 'gte', value: 1 }] }, fields), /条件字段/)
    assert.match(validateDefinition({ ...definition, rules: [{ field: 'is_st', op: 'between', value: false }] }, fields), /运算符/)
  })
})

test('history zoom uses dates without rebasing or substituting zero for gaps', async () => {
  await withHelpers(({ historyWindowStart, formatValue }) => {
    const points = [{ date: '2025-01-01', value: 1000 }, { date: '2026-01-01', value: 1250 }, { date: '2026-01-31', value: null }]
    assert.equal(historyWindowStart(points, 30), '2026-01-01')
    assert.equal(historyWindowStart(points, 0), '2025-01-01')
    assert.equal(points[1].value, 1250)
    assert.equal(formatValue(null), '—')
    assert.equal(formatValue(0), '0.00')
  })
})
