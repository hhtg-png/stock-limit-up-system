import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import { createPinia } from 'pinia'
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

function axiosResponse(config, data) {
  return { data, status: 200, statusText: 'OK', headers: {}, config }
}

async function withFrontendModules(run) {
  const server = await createServer({
    configFile: false,
    root,
    logLevel: 'silent',
    server: { middlewareMode: true },
    resolve: { alias: { '@': resolve(root, 'src') } }
  })
  try {
    await run(server)
  } finally {
    await server.close()
  }
}

function alert(id, dedupKey, acknowledgedAt = null) {
  return {
    id,
    plan_version_id: 1,
    candidate_id: null,
    event_type: id === 4 ? 'review_ready' : 'watch',
    severity: 'info',
    dedup_key: dedupKey,
    triggered_at: '2026-07-14T14:40:00+08:00',
    market_snapshot_json: {},
    message: `alert-${id}`,
    channel_status_json: {},
    acknowledged_at: acknowledgedAt
  }
}

test('isolated store deduplicates realtime alerts and tracks unread state', async () => {
  await withFrontendModules(async server => {
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const store = useTradingPlaybookStore(createPinia())

    store.receiveAlert(alert(1, 'same'))
    store.receiveAlert({ ...alert(1, 'same'), message: 'same id refreshed' })
    store.receiveAlert(alert(2, 'same'))
    store.receiveAlert(alert(3, 'new', '2026-07-14T15:00:00+08:00'))

    assert.deepEqual(store.alerts.map(item => item.id), [3, 2])
    assert.equal(store.unreadCount, 1)
  })
})

test('isolated store bounds its realtime inbox to 200 newest alerts', async () => {
  await withFrontendModules(async server => {
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const store = useTradingPlaybookStore(createPinia())

    for (let id = 1; id <= 205; id += 1) {
      store.receiveAlert(alert(id, `dedup-${id}`))
    }

    assert.equal(store.alerts.length, 200)
    assert.equal(store.alerts[0].id, 205)
    assert.equal(store.alerts.at(-1).id, 6)
  })
})

test('isolated store loads active plans and persisted reviews with explicit plan selection', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const calls = []
    api.tradingPlaybookApi.defaults.adapter = async config => {
      calls.push({ url: config.url, params: config.params })
      const data = config.url === '/trading-playbook/plans'
        ? { items: [{ id: 1, status: 'draft' }, { id: 2, status: 'active' }] }
        : { items: [{ id: 8, trade_date: '2026-07-14', plan_version_id: 2, data_quality_json: { status: 'ready' } }] }
      return { data, status: 200, statusText: 'OK', headers: {}, config }
    }

    const store = useTradingPlaybookStore(createPinia())
    await store.loadPlans('2026-07-14')
    await store.loadReviews('2026-07-14', 2)

    assert.equal(store.activePlan?.id, 2)
    assert.equal(store.reviews[0].data_quality_json.status, 'ready')
    assert.deepEqual(calls, [
      { url: '/trading-playbook/plans', params: { trade_date: '2026-07-14' } },
      { url: '/trading-playbook/reviews', params: { trade_date: '2026-07-14', plan_id: 2 } }
    ])
  })
})

test('latest plan request wins across dates and repeated same-date loads', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const requests = []
    api.tradingPlaybookApi.defaults.adapter = config => {
      const request = deferred()
      requests.push({ config, request })
      return request.promise
    }
    const store = useTradingPlaybookStore(createPinia())

    const slowOldDate = store.loadPlans('2026-07-14')
    const fastNewDate = store.loadPlans('2026-07-15')
    assert.deepEqual(store.plans, [])
    assert.equal(store.activePlan, null)
    assert.equal(store.plansLoading, true)
    assert.equal(store.plansRequestedTradeDate, '2026-07-15')
    assert.equal(store.plansLoadedTradeDate, null)
    assert.equal(store.plansError, null)
    requests[1].request.resolve(axiosResponse(requests[1].config, {
      items: [{ id: 15, status: 'active', target_trade_date: '2026-07-15' }]
    }))
    await fastNewDate
    requests[0].request.resolve(axiosResponse(requests[0].config, {
      items: [{ id: 14, status: 'active', target_trade_date: '2026-07-14' }]
    }))
    await slowOldDate
    assert.equal(store.activePlan?.id, 15)
    assert.equal(store.plansRequestedTradeDate, '2026-07-15')
    assert.equal(store.plansLoadedTradeDate, '2026-07-15')
    assert.equal(store.plansLoading, false)
    assert.equal(store.plansError, null)

    const slowRepeat = store.loadPlans('2026-07-15')
    const fastRepeat = store.loadPlans('2026-07-15')
    requests[3].request.resolve(axiosResponse(requests[3].config, {
      items: [{ id: 151, status: 'active', target_trade_date: '2026-07-15' }]
    }))
    await fastRepeat
    requests[2].request.resolve(axiosResponse(requests[2].config, {
      items: [{ id: 150, status: 'active', target_trade_date: '2026-07-15' }]
    }))
    await slowRepeat
    assert.equal(store.activePlan?.id, 151)

    const staleFailure = store.loadPlans('2026-07-16')
    const latestSuccess = store.loadPlans('2026-07-17')
    requests[5].request.resolve(axiosResponse(requests[5].config, {
      items: [{ id: 17, status: 'active', target_trade_date: '2026-07-17' }]
    }))
    await latestSuccess
    requests[4].request.reject(new Error('stale plan failed'))
    await assert.rejects(staleFailure, /stale plan failed/)
    assert.equal(store.activePlan?.id, 17)
    assert.equal(store.plansRequestedTradeDate, '2026-07-17')
    assert.equal(store.plansLoadedTradeDate, '2026-07-17')
    assert.equal(store.plansLoading, false)
    assert.equal(store.plansError, null)
  })
})

test('latest review request wins and a rejected latest request blocks older success', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const requests = []
    api.tradingPlaybookApi.defaults.adapter = config => {
      const request = deferred()
      requests.push({ config, request })
      return request.promise
    }
    const store = useTradingPlaybookStore(createPinia())

    const slowOldDate = store.loadReviews('2026-07-14', 14)
    const fastNewDate = store.loadReviews('2026-07-15', 15)
    requests[1].request.resolve(axiosResponse(requests[1].config, {
      items: [{ id: 15, trade_date: '2026-07-15', plan_version_id: 15 }]
    }))
    await fastNewDate
    requests[0].request.resolve(axiosResponse(requests[0].config, {
      items: [{ id: 14, trade_date: '2026-07-14', plan_version_id: 14 }]
    }))
    await slowOldDate
    assert.equal(store.reviews[0].id, 15)
    assert.equal(store.reviewsRequestedTradeDate, '2026-07-15')
    assert.equal(store.reviewsLoadedTradeDate, '2026-07-15')
    assert.equal(store.reviewsLoading, false)
    assert.equal(store.reviewsError, null)

    const olderSuccess = store.loadReviews('2026-07-16', 16)
    const latestFailure = store.loadReviews('2026-07-17', 17)
    assert.deepEqual(store.reviews, [])
    assert.equal(store.reviewsLoading, true)
    assert.equal(store.reviewsRequestedTradeDate, '2026-07-17')
    assert.equal(store.reviewsLoadedTradeDate, null)
    assert.equal(store.reviewsError, null)
    requests[3].request.reject(new Error('latest review failed'))
    await assert.rejects(latestFailure, /latest review failed/)
    requests[2].request.resolve(axiosResponse(requests[2].config, {
      items: [{ id: 16, trade_date: '2026-07-16', plan_version_id: 16 }]
    }))
    await olderSuccess
    assert.deepEqual(store.reviews, [])
    assert.equal(store.reviewsRequestedTradeDate, '2026-07-17')
    assert.equal(store.reviewsLoadedTradeDate, null)
    assert.equal(store.reviewsLoading, false)
    assert.equal(store.reviewsError, 'latest review failed')
  })
})

test('ack and later websocket refresh merge canonical fields without resurrecting unread state', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    api.tradingPlaybookApi.defaults.adapter = async config => axiosResponse(config, {
      ...alert(1, 'same', '2026-07-14T15:00:00+08:00'),
      message: 'REST acknowledged',
      channel_status_json: { in_app: { status: 'sent' } }
    })
    const store = useTradingPlaybookStore(createPinia())
    store.receiveAlert(alert(1, 'same'))

    await store.acknowledgeAlert(1)
    store.receiveAlert({
      ...alert(1, 'same'),
      message: 'WS delivery refreshed',
      channel_status_json: { in_app: { status: 'delivered' } }
    })

    assert.equal(store.alerts.length, 1)
    assert.equal(store.alerts[0].message, 'WS delivery refreshed')
    assert.deepEqual(store.alerts[0].channel_status_json, { in_app: { status: 'delivered' } })
    assert.equal(store.alerts[0].acknowledged_at, '2026-07-14T15:00:00+08:00')
    assert.equal(store.unreadCount, 0)
  })
})

test('same dedup key with a different id merges the incoming canonical event', async () => {
  await withFrontendModules(async server => {
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const store = useTradingPlaybookStore(createPinia())
    store.receiveAlert({
      ...alert(1, 'same', '2026-07-14T15:00:00+08:00'),
      message: 'older'
    })
    store.receiveAlert({
      ...alert(2, 'same'),
      message: 'new canonical',
      channel_status_json: { in_app: { status: 'sent' } }
    })

    assert.equal(store.alerts.length, 1)
    assert.equal(store.alerts[0].id, 2)
    assert.equal(store.alerts[0].message, 'new canonical')
    assert.equal(store.alerts[0].acknowledged_at, '2026-07-14T15:00:00+08:00')
  })
})

test('latest alert inbox load merges REST history with websocket arrivals', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const requests = []
    api.tradingPlaybookApi.defaults.adapter = config => {
      const request = deferred()
      requests.push({ config, request })
      return request.promise
    }
    const store = useTradingPlaybookStore(createPinia())

    const olderLoad = store.loadAlerts(true)
    const latestLoad = store.loadAlerts(false)
    assert.deepEqual(store.alerts, [])
    assert.equal(store.alertsLoading, true)
    assert.equal(store.alertsRequestedUnreadOnly, false)
    assert.equal(store.alertsLoadedUnreadOnly, null)
    assert.equal(store.alertsError, null)
    store.receiveAlert({ ...alert(3, 'ws-new'), triggered_at: '2026-07-14T15:10:00+08:00' })
    requests[1].request.resolve(axiosResponse(requests[1].config, {
      items: [alert(2, 'rest-new'), alert(1, 'rest-old')],
      limit: 50,
      offset: 0
    }))
    await latestLoad
    requests[0].request.reject(new Error('stale inbox failed'))
    await assert.rejects(olderLoad, /stale inbox failed/)

    assert.deepEqual(store.alerts.map(item => item.id), [3, 2, 1])
    assert.equal(store.alertsLoading, false)
    assert.equal(store.alertsError, null)
    assert.equal(store.alertsRequestedUnreadOnly, false)
    assert.equal(store.alertsLoadedUnreadOnly, false)
    assert.deepEqual(requests.map(item => item.config.params), [
      { unread_only: true },
      { unread_only: false }
    ])
  })
})

test('settings loader exposes standalone reminder settings', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    api.tradingPlaybookApi.defaults.adapter = async config => axiosResponse(config, {
      enabled: true,
      trial_position_pct: 10,
      confirmed_position_pct: 30,
      hard_stop_pct: 5,
      max_action_candidates: 3,
      in_app_enabled: true,
      wechat_enabled: false
    })
    const store = useTradingPlaybookStore(createPinia())

    await store.loadSettings()

    assert.equal(store.settings?.in_app_enabled, true)
    assert.equal(store.settings?.wechat_enabled, false)
  })
})

test('trading_plan_alert websocket routing stays outside global alerts speech and desktop notifications', () => {
  const source = readFileSync(resolve(root, 'src/composables/useWebSocket.ts'), 'utf8')
  const match = source.match(/case ['"]trading_plan_alert['"]:([\s\S]*?)break/)

  assert.ok(match, 'trading_plan_alert case should exist')
  assert.match(match[1], /tradingPlaybookStore\.receiveAlert\(message\.data as TradingAlertEvent\)/)
  assert.doesNotMatch(match[1], /alertStore|useSpeech|Notification|Audio/)
})

test('trading playbook store never imports the global alert store', () => {
  const source = readFileSync(resolve(root, 'src/stores/trading-playbook.ts'), 'utf8')
  assert.match(source, /defineStore\(['"]trading-playbook['"]/)
  assert.doesNotMatch(source, /useAlertStore|stores\/alert/)
})
