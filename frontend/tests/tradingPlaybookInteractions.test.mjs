import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer } from 'vite'

const root = resolve(import.meta.dirname, '..')

function deferred() {
  let resolvePromise
  let rejectPromise
  const promise = new Promise((resolve, reject) => {
    resolvePromise = resolve
    rejectPromise = reject
  })
  return { promise, resolve: resolvePromise, reject: rejectPromise }
}

async function withHelpers(run) {
  const server = await createServer({
    configFile: false,
    root,
    logLevel: 'silent',
    server: { middlewareMode: true },
    resolve: { alias: { '@': resolve(root, 'src') } }
  })
  try {
    await run(await server.ssrLoadModule('/src/views/trading-playbook/interactions.ts'))
  } finally {
    await server.close()
  }
}

test('review editing stays closed until the selected review detail is loaded and matches', async () => {
  await withHelpers(async helpers => {
    const review = { plan_version_id: 7 }
    assert.equal(helpers.canEditReview(review, null, false), false)
    assert.equal(helpers.canEditReview(review, { id: 7 }, true), false)
    assert.equal(helpers.canEditReview(review, { id: 8 }, false), false)
    assert.equal(helpers.canEditReview(review, { id: 7 }, false), true)
  })
})

test('review save guard never calls replace API without the matching loaded detail', async () => {
  await withHelpers(async helpers => {
    const calls = []
    const save = async () => calls.push('saved')
    const review = { plan_version_id: 7 }

    assert.equal(await helpers.saveReviewIfEditable(review, null, false, save), false)
    assert.equal(await helpers.saveReviewIfEditable(review, { id: 7 }, true, save), false)
    assert.equal(await helpers.saveReviewIfEditable(review, { id: 8 }, false, save), false)
    assert.deepEqual(calls, [])
    assert.equal(await helpers.saveReviewIfEditable(review, { id: 7 }, false, save), true)
    assert.deepEqual(calls, ['saved'])
  })
})

test('older review-domain failure cannot clear a newer successful detail', async () => {
  await withHelpers(async helpers => {
    const reviewRequests = new Map()
    const detailRequests = new Map()
    const state = {
      selectedPlanId: null,
      reviewPlan: null,
      reviewPlanLoading: false,
      reviewPlanError: null
    }
    const controller = helpers.createReviewDomainController({
      loadReviews(date) {
        const request = deferred()
        reviewRequests.set(date, request)
        return request.promise
      },
      loadPlan(planId) {
        const request = deferred()
        detailRequests.set(planId, request)
        return request.promise
      },
      update(patch) {
        Object.assign(state, patch)
      }
    })

    const oldLoad = controller.load('2026-07-14')
    const newLoad = controller.load('2026-07-15')
    reviewRequests.get('2026-07-15').resolve([{ plan_version_id: 15 }])
    await Promise.resolve()
    detailRequests.get(15).resolve({ id: 15, candidates: [] })
    await newLoad
    reviewRequests.get('2026-07-14').reject(new Error('old date failed'))
    await oldLoad

    assert.deepEqual(state, {
      selectedPlanId: 15,
      reviewPlan: { id: 15, candidates: [] },
      reviewPlanLoading: false,
      reviewPlanError: null
    })
  })
})

test('switching to an empty review date invalidates an older in-flight detail', async () => {
  await withHelpers(async helpers => {
    const reviewRequests = new Map()
    const detail = deferred()
    const state = {
      selectedPlanId: null,
      reviewPlan: null,
      reviewPlanLoading: false,
      reviewPlanError: null
    }
    const controller = helpers.createReviewDomainController({
      loadReviews(date) {
        const request = deferred()
        reviewRequests.set(date, request)
        return request.promise
      },
      loadPlan() {
        return detail.promise
      },
      update(patch) {
        Object.assign(state, patch)
      }
    })

    const firstLoad = controller.load('2026-07-14')
    reviewRequests.get('2026-07-14').resolve([{ plan_version_id: 14 }])
    await Promise.resolve()
    assert.equal(state.reviewPlanLoading, true)

    const emptyLoad = controller.load('2026-07-15')
    reviewRequests.get('2026-07-15').resolve([])
    await emptyLoad
    assert.deepEqual(state, {
      selectedPlanId: null,
      reviewPlan: null,
      reviewPlanLoading: false,
      reviewPlanError: null
    })

    detail.resolve({ id: 14, candidates: [{ id: 1 }] })
    await firstLoad
    assert.deepEqual(state, {
      selectedPlanId: null,
      reviewPlan: null,
      reviewPlanLoading: false,
      reviewPlanError: null
    })
  })
})

test('confirm and cancel mutations are mutually exclusive under rapid cross clicks', async () => {
  await withHelpers(async helpers => {
    for (const firstKind of ['confirm', 'cancel']) {
      const request = deferred()
      const calls = []
      const successes = []
      let pending = null
      const controller = helpers.createPlanMutationController({
        confirm: async planId => {
          calls.push(['confirm', planId])
          return request.promise
        },
        cancel: async planId => {
          calls.push(['cancel', planId])
          return request.promise
        },
        reload: async () => calls.push(['reload']),
        success: kind => successes.push(kind),
        failure: error => assert.fail(`unexpected failure: ${error}`),
        updatePending: value => { pending = value }
      })

      const first = controller.run(firstKind, 7)
      const blocked = await controller.run(firstKind === 'confirm' ? 'cancel' : 'confirm', 7)
      assert.equal(blocked, false)
      assert.equal(pending, firstKind)
      assert.deepEqual(calls, [[firstKind, 7]])

      request.resolve({ id: 7 })
      assert.equal(await first, true)
      assert.equal(pending, null)
      assert.deepEqual(calls, [[firstKind, 7], ['reload']])
      assert.deepEqual(successes, [firstKind])
    }
  })
})

test('ack guard allows different alerts concurrently and blocks duplicate alert clicks', async () => {
  await withHelpers(async helpers => {
    const first = deferred()
    const second = deferred()
    const calls = []
    const snapshots = []
    const guard = helpers.createConcurrentIdGuard(ids => snapshots.push([...ids].sort()))

    const firstRun = guard.run(1, async () => {
      calls.push(1)
      await first.promise
    })
    const duplicate = await guard.run(1, async () => calls.push(11))
    const secondRun = guard.run(2, async () => {
      calls.push(2)
      await second.promise
    })

    assert.equal(duplicate, false)
    assert.deepEqual(calls, [1, 2])
    assert.equal(guard.isActive(1), true)
    assert.equal(guard.isActive(2), true)
    second.resolve()
    assert.equal(await secondRun, true)
    assert.equal(guard.isActive(1), true)
    assert.equal(guard.isActive(2), false)
    first.resolve()
    assert.equal(await firstRun, true)
    assert.deepEqual(snapshots, [[1], [1, 2], [1], []])
  })
})

test('successful settings reload clears an older action error but failure preserves it', async () => {
  await withHelpers(async helpers => {
    let actionError = 'old save failed'
    await helpers.runAndClearErrorOnSuccess(
      async () => ({ enabled: true }),
      () => { actionError = null }
    )
    assert.equal(actionError, null)

    actionError = 'still relevant'
    await assert.rejects(
      helpers.runAndClearErrorOnSuccess(
        async () => { throw new Error('settings unavailable') },
        () => { actionError = null }
      ),
      /settings unavailable/
    )
    assert.equal(actionError, 'still relevant')
  })
})

test('review save controller freezes one snapshot, blocks duplicates, and releases after failure', async () => {
  await withHelpers(async helpers => {
    const request = deferred()
    const state = {
      tradeDate: '2026-07-14',
      planId: 7,
      note: '原始备注'
    }
    const calls = []
    let pending = false
    let captures = 0
    const controller = helpers.createReviewSaveController({
      async save(snapshot) {
        await Promise.resolve()
        calls.push(['save', snapshot])
        await request.promise
      },
      reload: async snapshot => calls.push(['reload', snapshot.tradeDate]),
      success: () => calls.push(['success']),
      failure: error => calls.push(['failure', String(error)]),
      updatePending: value => { pending = value }
    })
    const capture = () => {
      captures += 1
      return {
        tradeDate: state.tradeDate,
        planId: state.planId,
        payload: { manual_note: state.note }
      }
    }

    const first = controller.run(capture)
    state.tradeDate = '2026-07-15'
    state.planId = 8
    state.note = '保存中被修改'
    assert.equal(await controller.run(capture), false)
    assert.equal(pending, true)
    assert.equal(captures, 1)
    request.reject(new Error('save failed'))
    assert.equal(await first, false)
    assert.equal(pending, false)
    assert.deepEqual(calls, [
      ['save', { tradeDate: '2026-07-14', planId: 7, payload: { manual_note: '原始备注' } }],
      ['failure', 'Error: save failed']
    ])
  })
})

test('revision builder validates structured candidate overrides and rejects unsafe fields', async () => {
  await withHelpers(async helpers => {
    const context = {
      source_trade_date: '2026-07-14',
      target_trade_date: '2026-07-15'
    }
    const current = {
      current_entry: { price_lte: 13, change_pct_lte: 8 },
      current_invalidation: { change_pct_gte: -10 },
      current_exit: { change_pct_gte: -10 }
    }
    assert.deepEqual(helpers.buildPlanRevision('  竞价后调整触发  ', [
      {
        ...current,
        candidate_id: 7,
        action_trade_date: '2026-07-15',
        manual_note: '  等待板块联动  ',
        entry_trigger_text: '{"label":"突破确认","price_gte":12.3}',
        invalidation_text: '{"change_pct_lte":-3}',
        exit_trigger_text: ''
      }
    ], context), {
      change_note: '竞价后调整触发',
      candidate_overrides: [{
        candidate_id: 7,
        action_trade_date: '2026-07-15',
        manual_note: '等待板块联动',
        entry_trigger: { label: '突破确认', price_gte: 12.3 },
        invalidation: { change_pct_lte: -3 }
      }]
    })
    assert.throws(
      () => helpers.buildPlanRevision('错误字段', [{ ...current, candidate_id: 7, entry_trigger_text: '{"auto_order":true}' }], context),
      /不支持字段 auto_order/
    )
    assert.throws(
      () => helpers.buildPlanRevision('保护刚性止损', [{ ...current, candidate_id: 7, invalidation_text: '{"price_lte":10.8}' }], context),
      /刚性止损不能通过修订覆盖/
    )
    assert.throws(
      () => helpers.buildPlanRevision('只有说明', [{ ...current, candidate_id: 7, entry_trigger_text: '{}' }], context),
      /至少修改一项候选条件/
    )
    assert.throws(
      () => helpers.buildPlanRevision('计划外日期', [{ ...current, candidate_id: 7, action_trade_date: '2026-07-16' }], context),
      /行动交易日只能是预案来源日或目标日/
    )
    assert.throws(
      () => helpers.buildPlanRevision('错误入场方向', [{ ...current, candidate_id: 7, entry_trigger_text: '{"change_pct_gte":-1}' }], context),
      /触发条件.change_pct_gte 必须在 0 到 100/
    )
    assert.throws(
      () => helpers.buildPlanRevision('错误退出方向', [{ ...current, candidate_id: 7, exit_trigger_text: '{"change_pct_lte":1}' }], context),
      /退出条件.change_pct_lte 必须在 -100 到 0/
    )
    for (const [name, trigger] of [
      ['价格边界', '{"price_gte":12,"price_lte":11}'],
      ['涨跌幅边界', '{"change_pct_gte":5,"change_pct_lte":4}']
    ]) {
      assert.throws(
        () => helpers.buildPlanRevision(name, [{ ...current, candidate_id: 7, entry_trigger_text: trigger }], context),
        /下限不能大于上限/
      )
    }
    assert.throws(
      () => helpers.buildPlanRevision('合并后冲突', [{
        ...current,
        current_entry: { price_gte: 12 },
        candidate_id: 7,
        entry_trigger_text: '{"price_lte":11}'
      }], context),
      /触发条件合并后的价格下限不能大于上限/
    )
  })
})

test('revision controller creates and selects the audited child version before confirmation', async () => {
  await withHelpers(async helpers => {
    const calls = []
    let pending = false
    const payload = { change_note: '调整触发', candidate_overrides: [{ candidate_id: 7, manual_note: '等待联动' }] }
    const controller = helpers.createPlanRevisionController({
      revise: async (planId, revision) => {
        calls.push(['revise', planId, revision])
        return { id: 8, parent_plan_version_id: planId, status: 'draft' }
      },
      reload: async () => calls.push(['reload']),
      select: planId => calls.push(['select', planId]),
      success: child => calls.push(['success', child.id]),
      failure: error => assert.fail(`unexpected failure: ${error}`),
      updatePending: value => { pending = value }
    })

    assert.equal(await controller.run(7, payload), true)
    assert.equal(pending, false)
    assert.deepEqual(calls, [
      ['revise', 7, payload],
      ['reload'],
      ['select', 8],
      ['success', 8]
    ])
  })
})

test('revision controller rejects malformed or unrelated child responses without side effects', async () => {
  await withHelpers(async helpers => {
    const invalidChildren = [
      { id: 0, parent_plan_version_id: 7, status: 'draft' },
      { id: 7, parent_plan_version_id: 7, status: 'draft' },
      { id: 8, parent_plan_version_id: 9, status: 'draft' },
      { id: 8, parent_plan_version_id: 7, status: 'active' }
    ]
    for (const child of invalidChildren) {
      const calls = []
      let pending = false
      const controller = helpers.createPlanRevisionController({
        revise: async () => child,
        reload: async () => calls.push('reload'),
        select: planId => calls.push(['select', planId]),
        success: value => calls.push(['success', value.id]),
        failure: error => calls.push(['failure', String(error)]),
        updatePending: value => { pending = value }
      })

      assert.equal(await controller.run(7, { change_note: '调整', candidate_overrides: [] }), false)
      assert.equal(pending, false)
      assert.equal(calls.length, 1)
      assert.match(calls[0][1], /有效.*子版本/)
    }
  })
})
