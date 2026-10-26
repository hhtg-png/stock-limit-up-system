import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import ts from 'typescript'
import * as vue from 'vue'
import { createPinia, setActivePinia } from 'pinia'

const root = resolve(import.meta.dirname, '..')
const transpile = source => ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ES2022, target: ts.ScriptTarget.ES2022 } }).outputText
const storeCode = transpile(readFileSync(resolve(root, 'src/stores/limit-up.ts'), 'utf8'))
  .replace(/from ['"](pinia|vue)['"]/g, (_, name) => `from '${import.meta.resolve(name)}'`)
const { useLimitUpStore } = await import(`data:text/javascript,${encodeURIComponent(storeCode)}`)
const context = await import(`data:text/javascript,${encodeURIComponent(transpile(readFileSync(resolve(root, 'src/utils/limitUpContext.ts'), 'utf8')))}`)
const flush = async () => { await new Promise(resolve => setImmediate(resolve)) }

function loadView(view, autoResolve = true) {
  setActivePinia(createPinia())
  const store = useLimitUpStore()
  const source = readFileSync(resolve(root, `src/views/${view}.vue`), 'utf8')
  const script = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
  const ast = ts.createSourceFile(`${view}.ts`, script, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS)
  const code = transpile(ast.statements.filter(node => !ts.isImportDeclaration(node)).map(node => node.getText(ast)).join('\n'))
  const mounted = [], unmounted = [], requests = [], messages = [], timers = new Map()
  let now = 0, nextTimer = 0, exportedBlob
  const scope = {
    ...vue, ...context,
    onMounted(callback) { mounted.push(callback) }, onUnmounted(callback) { unmounted.push(callback) },
    useLimitUpStore: () => store, useRouter: () => ({ push() {} }), dayjs: () => ({ format: () => '2026-10-26' }),
    ElMessage: Object.fromEntries(['warning', 'error', 'success'].map(type => [type, value => messages.push([type, value])])),
    console: { error() {} }, document: { visibilityState: 'visible', createElement: () => ({ click() {} }) },
    Blob, URL: { createObjectURL(blob) { exportedBlob = blob; return 'blob:csv' }, revokeObjectURL() {} },
    window: {
      setTimeout(callback, delay) { const id = ++nextTimer; timers.set(id, { callback, due: now + delay }); return id },
      clearTimeout(id) { timers.delete(id) }, setInterval() { return ++nextTimer }, clearInterval() {}
    },
    clearInterval() {},
    getRealtimeLimitUp(params) {
      let complete, fail
      const promise = new Promise((resolve, reject) => { complete = resolve; fail = reject })
      requests.push({ params, resolve: complete, reject: fail })
      if (autoResolve) complete({ trade_date: params.trade_date, data: [] })
      return promise
    }
  }
  const effects = vue.effectScope()
  const state = effects.run(() => new Function(...Object.keys(scope), `${code}\nreturn {
    filters, selectedDate, loading, refreshing, tableData, apiSort, applyFilters, applyPriceRange,
    handleCustomPriceChange, handleDateChange, resetFilters, refreshData,
    setSortBy: typeof setSortBy === 'undefined' ? undefined : setSortBy,
    setSortOrder: typeof setSortOrder === 'undefined' ? undefined : setSortOrder,
    effectiveOrder: typeof effectiveOrder === 'undefined' ? undefined : effectiveOrder,
    exportData
  }`)(...Object.values(scope)))
  return {
    ...state, source, requests, messages, store,
    async exportedCodes() { state.exportData(); return (await exportedBlob.text()).split('\n').slice(1).map(line => line.split(',')[0]) },
    async mount() { mounted.forEach(callback => callback()); await flush() },
    async advance(ms) {
      now += ms
      for (const [id, timer] of [...timers]) if (timer.due <= now) { timers.delete(id); timer.callback() }
      await flush()
    },
    dispose() { unmounted.forEach(callback => callback()); effects.stop() }
  }
}

for (const view of ['Dashboard', 'LimitUpList']) {
  test(`${view} queries each filter change once and ignores websocket changes`, async () => {
    const state = loadView(view)
    try {
      for (const name of ['boardScope', 'reasonCategory', 'status']) {
        assert.match(state.source, new RegExp(`<el-select[^>]*v-model="filters\\.${name}"[^>]*@change="applyFilters"`), `${name} must trigger its query handler`)
      }
      await state.mount()
      assert.equal(state.requests.length, 1, 'initial mount should issue one query')
      state.store.applyRealtimeDelta([{ stock_code: '000001', today_open_change_pct: 0 }], [], '2026-10-26')
      await flush()
      assert.equal(state.requests.length, 1, 'websocket data must not trigger HTTP queries')
      for (const [name, value, apiName] of [['boardScope', 'first', 'continuous_days_exact'], ['reasonCategory', '机器人', 'reason_category'], ['status', 'opened', 'status']]) {
        state.filters[name] = value
        await state.applyFilters()
        await flush()
        assert.equal(state.requests.at(-1).params[apiName], name === 'boardScope' ? 1 : value)
      }
      assert.equal(state.requests.length, 4)
      state.selectedDate.value = '2026-10-23'
      await state.handleDateChange()
      await flush()
      assert.equal(state.requests.length, 5)
      assert.equal(state.requests.at(-1).params.trade_date, '2026-10-23')
      assert.equal(state.store.acceptRealtimeUpdates, false)
      for (const [sort, direction] of [['yesterday_final_seal_time', 'asc'], ['yesterday_open_change_pct', 'desc'], ['today_open_change_pct', 'desc']]) {
        assert.equal(typeof state.setSortBy, 'function')
        await state.setSortBy(sort)
        await flush()
        assert.equal(state.requests.at(-1).params.sort_by, sort)
        assert.equal(state.requests.at(-1).params.sort_order, direction)
      }
    } finally { state.dispose() }
  })

  test(`${view} debounces custom prices and cancels pending inputs on preset or reset`, async () => {
    const state = loadView(view)
    try {
      await state.mount()
      state.filters.minPrice = '1'
      state.handleCustomPriceChange()
      state.filters.minPrice = '10'
      state.handleCustomPriceChange()
      state.filters.maxPrice = '20'
      state.handleCustomPriceChange()
      await state.advance(299)
      assert.equal(state.requests.length, 1)
      await state.advance(1)
      assert.equal(state.requests.length, 2)
      assert.equal(state.requests.at(-1).params.min_price, 10)
      assert.equal(state.requests.at(-1).params.max_price, 20)
      state.filters.minPrice = '15'
      state.handleCustomPriceChange()
      await state.applyPriceRange({ label: '20-50', min: 20, max: 50 })
      await flush()
      assert.equal(state.requests.length, 3)
      assert.deepEqual([state.requests.at(-1).params.min_price, state.requests.at(-1).params.max_price], [20, 50])
      await state.advance(300)
      assert.equal(state.requests.length, 3, 'preset should cancel an older custom price timer')
      state.filters.minPrice = '30'
      state.handleCustomPriceChange()
      await state.resetFilters()
      await flush()
      assert.equal(state.requests.length, 4)
      assert.equal(state.requests.at(-1).params.min_price, undefined)
      await state.advance(300)
      assert.equal(state.requests.length, 4, 'reset should issue one query')
    } finally { state.dispose() }
  })

  test(`${view} starts new criteria while busy and never applies an obsolete response`, async () => {
    const state = loadView(view, false)
    try {
      await state.mount()
      state.filters.status = 'sealed'
      state.applyFilters()
      await flush()
      assert.equal(state.requests.length, 2, 'new criteria must not be dropped while an older request runs')
      state.requests[0].resolve({ trade_date: '2026-10-23', data: [{ stock_code: 'old' }] })
      await flush()
      assert.equal(state.store.realtimeList.length, 0)
      assert.equal(state.loading.value, true, 'an older response must not clear newer loading state')
      state.filters.minPrice = '10'
      state.handleCustomPriceChange()
      state.requests[1].resolve({ trade_date: '2026-10-26', data: [{ stock_code: 'also-old' }] })
      await flush()
      assert.equal(state.store.realtimeList.length, 0, 'typing immediately invalidates old requests before debounce expires')
      await state.advance(300)
      assert.equal(state.requests.length, 3)
      state.requests[2].resolve({ trade_date: '2026-10-26', data: [{ stock_code: 'latest' }] })
      await flush()
      assert.equal(state.store.realtimeList[0].stock_code, 'latest')
      assert.equal(state.loading.value, false)
    } finally { state.dispose() }
  })

  test(`${view} deduplicates in-flight refresh, reports failures honestly and stops after disposal`, async () => {
    const state = loadView(view, false)
    await state.mount()
    const refresh = state.refreshData()
    await flush()
    assert.equal(state.requests.length, 1, 'refresh should reuse the matching in-flight query')
    state.requests[0].reject(new Error('offline'))
    await refresh
    assert.equal(state.messages.some(([type]) => type === 'success'), false, 'failed requests must not announce refresh success')
    state.filters.minPrice = '10'
    state.handleCustomPriceChange()
    state.dispose()
    await state.advance(300)
    assert.equal(state.requests.length, 1, 'unmount must clear scheduled price queries')
  })

  test(`${view} never reuses invalidated requests when criteria return to an earlier value`, async () => {
    for (const throughPriceInput of [false, true]) {
      const state = loadView(view, false)
      try {
        await state.mount()
        if (throughPriceInput) {
          state.filters.minPrice = '10'
          state.handleCustomPriceChange()
          state.filters.minPrice = ''
          state.handleCustomPriceChange()
          await state.advance(300)
        } else {
          state.filters.status = 'sealed'
          state.applyFilters()
          await flush()
          state.filters.status = ''
          state.applyFilters()
          await flush()
        }
        assert.equal(state.requests.length, throughPriceInput ? 2 : 3)
        assert.deepEqual(state.requests.at(-1).params, state.requests[0].params)
        state.requests.at(-1).resolve({ trade_date: '2026-10-26', data: [{ stock_code: 'latest-A' }] })
        await flush()
        state.requests[0].resolve({ trade_date: '2026-10-23', data: [{ stock_code: 'obsolete-A' }] })
        if (!throughPriceInput) state.requests[1].reject(new Error('obsolete-B'))
        await flush()
        assert.equal(state.store.realtimeList[0].stock_code, 'latest-A')
        assert.equal(state.messages.some(([type]) => type === 'error'), false)
      } finally { state.dispose() }
    }
  })

  test(`${view} keeps first seal, seal amount and board-count ordering after snapshots and deltas`, async () => {
    const state = loadView(view)
    try {
      await state.mount()
      const rows = [
        { stock_code: '000003', first_limit_up_time: '11:00:00', seal_amount: 100, is_one_word: false, continuous_limit_up_days: 1 },
        { stock_code: '000002', first_limit_up_time: null, seal_amount: 100, is_one_word: false, continuous_limit_up_days: 3 },
        { stock_code: '000001', first_limit_up_time: '09:25:00', seal_amount: 100, is_one_word: true, continuous_limit_up_days: 2 },
        { stock_code: '000004', first_limit_up_time: '09:25:00', seal_amount: null, is_one_word: false, continuous_limit_up_days: null }
      ]
      for (const [sort, before, after] of [
        ['time', ['000001', '000004', '000003', '000002'], ['000004', '000001', '000003', '000002']],
        ['seal_amount', ['000001', '000003', '000002', '000004'], ['000004', '000001', '000003', '000002']],
        ['continuous_days', ['000002', '000001', '000003', '000004'], ['000004', '000002', '000001', '000003']]
      ]) {
        await state.setSortBy(sort)
        state.store.setRealtimeSnapshot('2026-10-26', rows)
        assert.deepEqual(state.tableData.value.map(row => row.stock_code), before, `${sort} should sort a fresh websocket snapshot`)
        assert.deepEqual(state.store.realtimeList.map(row => row.stock_code), ['000003', '000002', '000001', '000004'], 'presentation sorting must not mutate the shared store')
        state.store.applyRealtimeDelta([{ stock_code: '000004', first_limit_up_time: '08:59:00', seal_amount: 200, continuous_limit_up_days: 4 }], [], '2026-10-26')
        assert.deepEqual(state.tableData.value.map(row => row.stock_code), after, `${sort} should sort changed websocket values`)
      }
    } finally { state.dispose() }
  })

  test(`${view} keeps defaults until an explicit direction is chosen, then carries it across fields and resets`, async () => {
    const state = loadView(view)
    try {
      await state.mount()
      for (const [sort, direction] of [['time', 'asc'], ['reseal_time', 'asc'], ['yesterday_final_seal_time', 'asc'], ['yesterday_open_change_pct', 'desc'], ['today_open_change_pct', 'desc'], ['seal_amount', 'desc'], ['continuous_days', 'desc']]) {
        await state.setSortBy(sort)
        assert.equal(state.apiSort.value.sortOrder, direction)
      }
      assert.equal(typeof state.setSortOrder, 'function', 'direction controls must update query state')
      const before = state.requests.length
      await state.setSortOrder('asc')
      assert.equal(state.requests.length, before + 1, 'direction changes should query automatically')
      for (const sort of ['today_open_change_pct', 'seal_amount', 'time']) {
        await state.setSortBy(sort)
        assert.equal(state.apiSort.value.sortOrder, 'asc')
      }
      await state.setSortOrder('desc')
      state.filters.status = 'opened'
      await state.resetFilters()
      assert.equal(state.apiSort.value.sortBy, 'time')
      assert.equal(state.apiSort.value.sortOrder, 'desc', 'reset should retain field and direction')
    } finally { state.dispose() }
  })

  test(`${view} sorts all seven fields in both directions after websocket updates and exports the same order`, async () => {
    const state = loadView(view)
    try {
      await state.mount()
      assert.equal(typeof state.setSortOrder, 'function')
      const rows = [
        { stock_code: '000003', first_limit_up_time: '11:00:00', final_seal_time: '11:00:00', yesterday_final_seal_time: '11:00:00', seal_amount: 100, is_one_word: false, continuous_limit_up_days: 1, yesterday_open_change_pct: -1, today_open_change_pct: -1 },
        { stock_code: '000002', first_limit_up_time: null, final_seal_time: null, yesterday_final_seal_time: null, seal_amount: null, continuous_limit_up_days: null, yesterday_open_change_pct: null, today_open_change_pct: null },
        { stock_code: '000001', first_limit_up_time: '09:25:00', final_seal_time: '09:25:00', yesterday_final_seal_time: '09:25:00', seal_amount: 100, is_one_word: true, continuous_limit_up_days: 2, yesterday_open_change_pct: 0, today_open_change_pct: 0 },
        { stock_code: '000004', first_limit_up_time: '09:25:00', final_seal_time: '09:25:00', yesterday_final_seal_time: '09:25:00', seal_amount: 0, continuous_limit_up_days: 0, yesterday_open_change_pct: 0, today_open_change_pct: 0 },
        { stock_code: '000005', first_limit_up_time: '9:25:00', final_seal_time: '9:25:00', yesterday_final_seal_time: '9:25:00', seal_amount: Infinity, continuous_limit_up_days: Infinity, yesterday_open_change_pct: Infinity, today_open_change_pct: Infinity }
      ].map(row => ({ ...row, stock_name: row.stock_code, trade_date: '2026-10-26' }))
      const cases = [
        ['time', ['000001', '000004', '000003', '000002', '000005'], ['000003', '000001', '000004', '000002', '000005'], ['000002', '000001', '000004', '000003', '000005'], ['000003', '000001', '000004', '000002', '000005']],
        ['reseal_time', ['000001', '000004', '000003', '000002', '000005'], ['000003', '000001', '000004', '000002', '000005'], ['000002', '000001', '000004', '000003', '000005'], ['000003', '000001', '000004', '000002', '000005']],
        ['yesterday_final_seal_time', ['000001', '000004', '000003', '000002', '000005'], ['000003', '000001', '000004', '000002', '000005'], ['000002', '000001', '000004', '000003', '000005'], ['000003', '000001', '000004', '000002', '000005']],
        ['yesterday_open_change_pct', ['000003', '000001', '000004', '000002', '000005'], ['000001', '000004', '000003', '000002', '000005'], ['000002', '000003', '000001', '000004', '000005'], ['000001', '000004', '000003', '000002', '000005']],
        ['today_open_change_pct', ['000003', '000001', '000004', '000002', '000005'], ['000001', '000004', '000003', '000002', '000005'], ['000002', '000003', '000001', '000004', '000005'], ['000001', '000004', '000003', '000002', '000005']],
        ['seal_amount', ['000004', '000001', '000003', '000002', '000005'], ['000001', '000003', '000004', '000002', '000005'], ['000002', '000004', '000001', '000003', '000005'], ['000001', '000003', '000004', '000002', '000005']],
        ['continuous_days', ['000004', '000003', '000001', '000002', '000005'], ['000001', '000003', '000004', '000002', '000005'], ['000002', '000004', '000003', '000001', '000005'], ['000001', '000003', '000004', '000002', '000005']]
      ]
      for (const [sort, asc, desc, changedAsc, changedDesc] of cases) {
        await state.setSortBy(sort)
        for (const [direction, expected, updated] of [['asc', asc, changedAsc], ['desc', desc, changedDesc]]) {
          await state.setSortOrder(direction)
          const requestCount = state.requests.length
          state.store.setRealtimeSnapshot('2026-10-26', rows)
          assert.deepEqual(state.tableData.value.map(row => row.stock_code), expected, `${sort} ${direction} snapshot`)
          assert.equal(state.apiSort.value.sortOrder, direction)
          state.store.applyRealtimeDelta([{ stock_code: '000002', first_limit_up_time: '00:00:00', final_seal_time: '00:00:00', yesterday_final_seal_time: '00:00:00', seal_amount: -5, continuous_limit_up_days: -1, yesterday_open_change_pct: -2, today_open_change_pct: -2 }], [], '2026-10-26')
          assert.deepEqual(state.tableData.value.map(row => row.stock_code), updated, `${sort} ${direction} delta`)
          assert.deepEqual(await state.exportedCodes(), updated, `${sort} ${direction} CSV`)
          assert.equal(state.requests.length, requestCount, 'websocket updates must not create HTTP queries')
        }
      }
    } finally { state.dispose() }
  })

  test(`${view} direction A-B-A changes never reuse or apply an obsolete request`, async () => {
    const state = loadView(view, false)
    try {
      await state.mount()
      assert.equal(typeof state.setSortOrder, 'function')
      state.setSortOrder('desc')
      await flush()
      state.setSortOrder('asc')
      await flush()
      assert.equal(state.requests.length, 3)
      assert.deepEqual(state.requests.map(request => request.params.sort_order), ['asc', 'desc', 'asc'])
      state.requests[2].resolve({ trade_date: '2026-10-26', data: [{ stock_code: 'new' }] })
      await flush()
      state.requests[0].resolve({ trade_date: '2026-10-23', data: [{ stock_code: 'old' }] })
      state.requests[1].reject(new Error('obsolete direction'))
      await flush()
      assert.equal(state.store.realtimeList[0].stock_code, 'new')
      assert.equal(state.messages.some(([type]) => type === 'error'), false)
    } finally { state.dispose() }
  })

  test(`${view} pins an explicitly clicked default direction without issuing a duplicate query`, async () => {
    const state = loadView(view)
    try {
      await state.mount()
      assert.match(state.source, /label="方向"[\s\S]*?@click="setSortOrder\('asc'\)"[^>]*>顺序<\/el-button>[\s\S]*?@click="setSortOrder\('desc'\)"[^>]*>倒序<\/el-button>/)
      assert.equal(state.apiSort.value.sortOrder, 'asc')
      const before = state.requests.length
      await state.setSortOrder('asc')
      assert.equal(state.requests.length, before, 'pinning the current direction does not change query parameters')
      await state.setSortBy('today_open_change_pct')
      assert.equal(state.apiSort.value.sortOrder, 'asc', 'the explicit default click must survive a field with another natural default')
      await state.setSortOrder('desc')
      await state.setSortBy('time')
      assert.equal(state.apiSort.value.sortOrder, 'desc')
    } finally { state.dispose() }
  })
}
