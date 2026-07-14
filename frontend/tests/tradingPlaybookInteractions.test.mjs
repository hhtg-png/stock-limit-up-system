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

test('revision builder validates structured candidate overrides and rejects unsafe fields', async () => {
  await withHelpers(async helpers => {
    assert.deepEqual(helpers.buildPlanRevision('  竞价后调整触发  ', [
      {
        candidate_id: 7,
        action_trade_date: '2026-07-15',
        manual_note: '  等待板块联动  ',
        entry_trigger_text: '{"label":"突破确认","price_gte":12.3}',
        invalidation_text: '{"change_pct_lte":-3}',
        exit_trigger_text: ''
      }
    ]), {
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
      () => helpers.buildPlanRevision('错误字段', [{ candidate_id: 7, entry_trigger_text: '{"auto_order":true}' }]),
      /不支持字段 auto_order/
    )
    assert.throws(
      () => helpers.buildPlanRevision('保护刚性止损', [{ candidate_id: 7, invalidation_text: '{"price_lte":10.8}' }]),
      /刚性止损不能通过修订覆盖/
    )
    assert.throws(
      () => helpers.buildPlanRevision('只有说明', [{ candidate_id: 7, entry_trigger_text: '{}' }]),
      /至少修改一项候选条件/
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
