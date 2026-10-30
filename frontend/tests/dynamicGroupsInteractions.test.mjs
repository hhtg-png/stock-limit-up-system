import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import ts from 'typescript'
import * as vue from 'vue'

const root = resolve(import.meta.dirname, '..')
const transpile = source => ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ES2022, target: ts.ScriptTarget.ES2022 } }).outputText
const helpers = await import(`data:text/javascript,${encodeURIComponent(transpile(readFileSync(resolve(root, 'src/utils/dynamicGroups.ts'), 'utf8')))}`)
const flush = () => new Promise(resolve => setImmediate(resolve))
const group = id => ({ id, name: `板块${id}`, rules: [], version: 1, latest_selection_id: null })
const current = (id, batch = null) => ({ group: { ...group(id), latest_selection_id: batch, member_count: 5500 }, items: [{ stock_code: `current-${id}` }], total: 5500, summary: { stock_count: 5500, valid_change_count: 5400, average_change_pct: 2.4, up_count: 4000, down_count: 1400 }, selection_id: batch, status: 'ready', warnings: [] })

function loadView({ deferMembers = false } = {}) {
  const source = readFileSync(resolve(root, 'src/views/DynamicGroups.vue'), 'utf8').match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
  const ast = ts.createSourceFile('DynamicGroups.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS)
  const code = transpile(ast.statements.filter(node => !ts.isImportDeclaration(node)).map(node => node.getText(ast)).join('\n'))
  const requests = [], unmounted = [], intervals = new Set(), timers = new Map()
  let nextTimer = 0
  const deferred = (method, ...args) => {
    let resolvePromise, rejectPromise
    const promise = new Promise((resolve, reject) => { resolvePromise = resolve; rejectPromise = reject })
    requests.push({ method, args, resolve: resolvePromise, reject: rejectPromise })
    return promise
  }
  const api = {
    getDynamicCurrent: (...args) => deferred('current', ...args),
    getDynamicTrend: (...args) => deferred('trend', ...args),
    getDynamicDates: async () => ({ dates: ['2026-10-30'] }),
    getDynamicIntraday: async () => ({ points: [], status: 'empty' }),
    getDynamicSelections: async () => ({ items: [{ id: 7, member_count: 1 }], total: 1 }),
    getDynamicSelection: (...args) => { if (deferMembers) return deferred('members', ...args); requests.push({ method: 'members', args }); return Promise.resolve({ id: args[1], total: 5500, summary: current(1).summary, items: [{ stock_code: `frozen-${args[1]}` }] }) }
  }
  const scope = {
    ...vue, ...helpers, api,
    useRouter: () => ({ push() {} }),
    ElMessage: { error() {}, success() {} }, ElMessageBox: { confirm: async () => {} },
    onMounted() {}, onBeforeUnmount(callback) { unmounted.push(callback) },
    document: { hidden: false },
    setInterval(callback) { intervals.add(callback); return callback }, clearInterval(callback) { intervals.delete(callback) },
    setTimeout(callback) { const id = ++nextTimer; timers.set(id, callback); return id }, clearTimeout(id) { timers.delete(id) }
  }
  const effects = vue.effectScope()
  const state = effects.run(() => new Function(...Object.keys(scope), `${code}\nreturn { selectGroup, loadBatch, mode, selectedDate, changeMode, selectedBatchId, selectedBatch, current, selectedId, activeGroup, trend, poll, batchLoading, detailLoading, memberPage, memberSearch, averageChange, rising, falling, stockCount, memberTotal, pagedMembers, changeMemberPage }`)(...Object.values(scope)))
  return { ...state, requests, async advance() { const pending = [...timers.values()]; timers.clear(); pending.forEach(callback => callback()); await flush() }, dispose() { unmounted.forEach(callback => callback()); effects.stop() }, request(method, ...args) { return requests.find(request => request.method === method && JSON.stringify(request.args.slice(0, args.length)) === JSON.stringify(args)) } }
}

test('server paging keeps full-basket statistics and prevents a slow earlier page from replacing the new page', async () => {
  const view = loadView()
  try {
    const load = view.selectGroup(group(1)); view.request('current', 1).resolve(current(1)); await load
    assert.deepEqual(view.request('current', 1).args[1], { offset: 0, limit: 50, q: '' })
    assert.equal(view.stockCount.value, 5500)
    assert.equal(view.memberTotal.value, 5500)
    assert.equal(view.averageChange.value, 2.4)
    const older = view.changeMemberPage(2), latest = view.changeMemberPage(3)
    const calls = view.requests.filter(request => request.method === 'current')
    assert.deepEqual(calls.at(-1).args[1], { offset: 100, limit: 50, q: '' })
    calls.at(-1).resolve({ ...current(1), items: [{ stock_code: 'page-3' }] }); await latest
    calls.at(-2).resolve({ ...current(1), items: [{ stock_code: 'page-2' }] }); await older
    assert.equal(view.pagedMembers.value[0].stock_code, 'page-3')
    assert.equal(view.averageChange.value, 2.4)
  } finally { view.dispose() }
})

test('member search is debounced into one bounded server query and retains all-member summary', async () => {
  const view = loadView()
  try {
    const load = view.selectGroup(group(1)); view.request('current', 1).resolve(current(1)); await load
    view.memberSearch.value = '测'; await flush(); view.memberSearch.value = '测试'; await flush()
    assert.equal(view.requests.filter(request => request.method === 'current').length, 1)
    await view.advance()
    const request = view.requests.filter(request => request.method === 'current').at(-1)
    assert.deepEqual(request.args[1], { offset: 0, limit: 50, q: '测试' })
    request.resolve({ ...current(1), total: 1, items: [{ stock_code: 'match' }] }); await flush()
    assert.equal(view.memberTotal.value, 1)
    assert.equal(view.stockCount.value, 5500)
    assert.equal(view.rising.value, 4000)
    assert.equal(view.averageChange.value, 2.4)
  } finally { view.dispose() }
})

test('a newly archived current batch keeps the displayed member page aligned with its pager', async () => {
  const view = loadView()
  try {
    const load = view.selectGroup(group(1)); view.request('current', 1).resolve(current(1)); await load
    const page = view.changeMemberPage(3)
    view.requests.filter(request => request.method === 'current').at(-1).resolve({ ...current(1), items: [{ stock_code: 'page-3' }] }); await page
    const poll = view.poll(); await flush()
    view.requests.filter(request => request.method === 'current').at(-1).resolve({ ...current(1, 99), items: [{ stock_code: 'new-page-3' }] }); await flush()
    view.request('trend', 1, 99).resolve({ selection_id: 99, points: [] }); await poll
    assert.equal(view.memberPage.value, 3)
    assert.equal(view.pagedMembers.value[0].stock_code, 'new-page-3')
    assert.deepEqual(view.request('members', 1, 99).args[2], { offset: 0, limit: 1, q: '' }, 'chart caption only needs bounded batch metadata')
  } finally { view.dispose() }
})

test('a shrinking dynamic membership moves an out-of-range page to the last available page', async () => {
  const view = loadView()
  try {
    const load = view.selectGroup(group(1)); view.request('current', 1).resolve(current(1)); await load
    const page = view.changeMemberPage(3)
    view.requests.filter(request => request.method === 'current').at(-1).resolve({ ...current(1), total: 5500, items: [{ stock_code: 'page-3' }] }); await page
    const poll = view.poll(); await flush()
    const old = view.requests.filter(request => request.method === 'current').at(-1)
    old.resolve({ ...current(1), total: 12, items: [] }); await flush()
    const repaired = view.requests.filter(request => request.method === 'current').at(-1)
    assert.notEqual(repaired, old)
    assert.deepEqual(repaired.args[1], { offset: 0, limit: 50, q: '' })
    repaired.resolve({ ...current(1), total: 12, items: [{ stock_code: 'last-page' }] }); await poll
    assert.equal(view.memberPage.value, 1)
    assert.equal(view.pagedMembers.value[0].stock_code, 'last-page')
  } finally { view.dispose() }
})

test('historical page changes query the frozen batch and a current poll cannot replace that page', async () => {
  const view = loadView({ deferMembers: true })
  try {
    const load = view.selectGroup(group(1)); view.request('current', 1).resolve(current(1)); await load
    view.mode.value = 'history'; view.selectedBatchId.value = 7
    const archive = view.loadBatch(1, 7)
    view.request('members', 1, 7).resolve({ id: 7, total: 5500, member_count: 5500, summary: current(1).summary, items: [{ stock_code: 'frozen-7' }] })
    view.request('trend', 1, 7).resolve({ selection_id: 7, points: [] }); await archive
    const page = view.changeMemberPage(2)
    const request = view.requests.filter(request => request.method === 'members').at(-1)
    assert.deepEqual(request.args, [1, 7, { offset: 50, limit: 50, q: '' }])
    request.resolve({ id: 7, total: 5500, member_count: 5500, summary: current(1).summary, items: [{ stock_code: 'frozen-page-2' }] }); await page
    const poll = view.poll(); await flush()
    view.requests.filter(request => request.method === 'current').at(-1).resolve(current(1, 99)); await flush()
    view.requests.filter(request => request.method === 'trend').at(-1).resolve({ selection_id: 7, points: [] }); await poll
    assert.equal(view.pagedMembers.value[0].stock_code, 'frozen-page-2')
    assert.equal(view.memberPage.value, 2)
    assert.equal(view.memberTotal.value, 5500)
  } finally { view.dispose() }
})

test('a late first-group response cannot overwrite a later selected group', async () => {
  const view = loadView()
  try {
    const a = view.selectGroup(group(1)), b = view.selectGroup(group(2))
    view.request('current', 2).resolve(current(2)); await b
    view.request('current', 1).resolve(current(1)); await a
    assert.equal(view.activeGroup.value.id, 2)
    assert.equal(view.current.value.items[0].stock_code, 'current-2')
  } finally { view.dispose() }
})

test('batch switching applies only the last selected batch even when the older chart finishes later', async () => {
  const view = loadView()
  try {
    const load = view.selectGroup(group(1)); view.request('current', 1).resolve(current(1)); await load
    view.mode.value = 'history'; view.selectedBatchId.value = 7
    const old = view.loadBatch(1, 7)
    view.selectedBatchId.value = 8
    const latest = view.loadBatch(1, 8)
    view.request('trend', 1, 8).resolve({ selection_id: 8, points: [{ date: '2026-01-01', value: 1100 }] }); await latest
    view.request('trend', 1, 7).resolve({ selection_id: 7, points: [{ date: '2026-01-01', value: 999 }] }); await old
    assert.equal(view.trend.value.selection_id, 8)
    assert.equal(view.selectedBatch.value.items[0].stock_code, 'frozen-8')
  } finally { view.dispose() }
})

test('automatic current updates preserve an explicitly selected historical archive', async () => {
  const view = loadView()
  try {
    const load = view.selectGroup(group(1)); view.request('current', 1).resolve(current(1)); await load
    view.mode.value = 'history'; view.selectedDate.value = '2026-10-30'
    const archive = view.changeMode(); await flush()
    view.request('trend', 1, 7).resolve({ selection_id: 7, points: [] }); await archive
    const poll = view.poll(); await flush()
    view.requests.filter(request => request.method === 'current').at(-1).resolve(current(1, 99)); await flush()
    view.requests.filter(request => request.method === 'trend').at(-1).resolve({ selection_id: 7, points: [] }); await poll
    assert.equal(view.current.value.selection_id, 99)
    assert.equal(view.selectedBatchId.value, 7)
    assert.equal(view.selectedBatch.value.items[0].stock_code, 'frozen-7')
  } finally { view.dispose() }
})

test('disposal invalidates an outstanding group load', async () => {
  const view = loadView()
  const load = view.selectGroup(group(1))
  view.dispose()
  view.request('current', 1).resolve(current(1)); await load
  assert.equal(view.current.value, null)
})

test('collapsed intraday chart waits for a measurable box and releases observer and chart on disposal', () => {
  const source = readFileSync(resolve(root, 'src/components/dynamic-groups/DynamicGroupChart.vue'), 'utf8').match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
  const ast = ts.createSourceFile('DynamicGroupChart.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS)
  const code = transpile(ast.statements.filter(node => !ts.isImportDeclaration(node)).map(node => node.getText(ast)).join('\n'))
  const mounted = [], unmounted = []
  let observed, resize, creations = 0, disposals = 0, disconnections = 0
  const chart = { setOption() {}, resize() {}, dispose() { disposals++ } }
  const scope = {
    ...vue, ...helpers,
    defineProps: () => ({ points: [], label: '测试' }), withDefaults: (props, defaults) => ({ ...defaults, ...props }),
    onMounted(callback) { mounted.push(callback) }, onBeforeUnmount(callback) { unmounted.push(callback) },
    echarts: { init() { creations++; return chart }, graphic: { LinearGradient: class {} } },
    ResizeObserver: class { constructor(callback) { resize = callback } observe(element) { observed = element } disconnect() { disconnections++ } }
  }
  const effects = vue.effectScope()
  const state = effects.run(() => new Function(...Object.keys(scope), `${code}\nreturn { element }`)(...Object.values(scope)))
  try {
    state.element.value = { clientWidth: 0, clientHeight: 340 }
    mounted.forEach(callback => callback())
    assert.equal(creations, 0, 'a collapsed chart must not initialize with zero width')
    assert.equal(observed.clientWidth, 0)
    state.element.value.clientWidth = 320; resize()
    assert.equal(creations, 1)
    unmounted.forEach(callback => callback())
    assert.equal(disposals, 1)
    assert.equal(disconnections, 1)
    resize()
    assert.equal(creations, 1, 'a queued resize after unmount cannot recreate the chart')
  } finally { effects.stop() }
})
