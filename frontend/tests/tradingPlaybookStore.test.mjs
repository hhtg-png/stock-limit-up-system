import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import axios from 'axios'
import { createPinia, setActivePinia } from 'pinia'
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

function obsidianSyncStatus(marker, overrides = {}) {
  return {
    enabled: true,
    configured: true,
    vault_exists: true,
    auto_git_enabled: false,
    last_success_at: '2026-07-16T15:31:00+08:00',
    last_trade_date: '2026-07-16',
    last_phase: 'after_close',
    pending_count: 0,
    paused_count: 0,
    failed_count: 0,
    last_error: null,
    recent_files: [`30_TradingPlaybook/Daily/Auto/${marker}.md`],
    dashboard_path: 'Dashboards/交易预案.md',
    dashboard_openable: true,
    ...overrides
  }
}

function obsidianVaultStatus(marker, overrides = {}) {
  return {
    enabled: true,
    vault_configured: true,
    vault_exists: true,
    vault_path: `D:/vault-${marker}`,
    auto_git_enabled: false,
    web_research_enabled: false,
    web_research_allowlist: [],
    ...overrides
  }
}

function obsidianExportResult(tradeDate) {
  return {
    trade_date: tradeDate,
    phase: 'reconcile',
    written_files: [`30_TradingPlaybook/Daily/Auto/${tradeDate}.md`],
    skipped_files: [],
    pending_files: [],
    failed_files: [],
    git_status: { state: 'git_complete', committed: true },
    error_summary: null
  }
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

async function withObsidianStore(run) {
  await withFrontendModules(async server => {
    const originalAdapter = axios.defaults.adapter
    const requests = []
    axios.defaults.adapter = config => {
      const request = deferred()
      requests.push({ config, request })
      return request.promise
    }
    try {
      const { useTradingPlaybookStore } = await server.ssrLoadModule(
        '/src/stores/trading-playbook.ts'
      )
      const store = useTradingPlaybookStore(createPinia())
      await run({ requests, store })
    } finally {
      axios.defaults.adapter = originalAdapter
    }
  })
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
    store.receiveAlert(alert(1, 'existing-ws'))

    const olderLoad = store.loadAlerts(true)
    const latestLoad = store.loadAlerts(false)
    assert.deepEqual(store.alerts.map(item => item.id), [1])
    assert.equal(store.alertsLoading, true)
    assert.equal(store.alertsRequestedUnreadOnly, false)
    assert.equal(store.alertsLoadedUnreadOnly, null)
    assert.equal(store.alertsError, null)
    store.receiveAlert({ ...alert(3, 'ws-new'), triggered_at: '2026-07-14T15:10:00+08:00' })
    requests[1].request.resolve(axiosResponse(requests[1].config, {
      items: [alert(2, 'rest-new')],
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

test('concurrent full and unread alert loads merge every successful response in either completion order', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')

    for (const completionOrder of ['full-first', 'unread-first']) {
      const requests = []
      api.tradingPlaybookApi.defaults.adapter = config => {
        const request = deferred()
        requests.push({ config, request })
        return request.promise
      }
      const store = useTradingPlaybookStore(createPinia())

      const fullLoad = store.loadAlerts(false)
      const unreadLoad = store.loadAlerts(true)
      assert.deepEqual(requests.map(item => item.config.params), [
        { unread_only: false },
        { unread_only: true }
      ])

      const fullResponse = axiosResponse(requests[0].config, {
        items: [alert(1, 'acknowledged-history', '2026-07-14T15:00:00+08:00')],
        limit: 50,
        offset: 0
      })
      const unreadResponse = axiosResponse(requests[1].config, {
        items: [{ ...alert(2, 'unread-latest'), triggered_at: '2026-07-14T15:10:00+08:00' }],
        limit: 50,
        offset: 0
      })

      if (completionOrder === 'full-first') {
        requests[0].request.resolve(fullResponse)
        await fullLoad
        requests[1].request.resolve(unreadResponse)
        await unreadLoad
      } else {
        requests[1].request.resolve(unreadResponse)
        await unreadLoad
        requests[0].request.resolve(fullResponse)
        await fullLoad
      }

      assert.deepEqual(store.alerts.map(item => item.id), [2, 1], completionOrder)
      assert.equal(store.alertsLoading, false, completionOrder)
      assert.equal(store.alertsError, null, completionOrder)
      assert.equal(store.alertsRequestedUnreadOnly, true, completionOrder)
      assert.equal(store.alertsLoadedUnreadOnly, true, completionOrder)
    }
  })
})

test('failed latest alert load preserves existing and in-flight websocket reminders', async () => {
  await withFrontendModules(async server => {
    const api = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
    const request = deferred()
    api.tradingPlaybookApi.defaults.adapter = config => request.promise.then(data => axiosResponse(config, data))
    const store = useTradingPlaybookStore(createPinia())
    store.receiveAlert(alert(1, 'existing-ws'))

    const load = store.loadAlerts(false)
    store.receiveAlert({ ...alert(2, 'in-flight-ws'), triggered_at: '2026-07-14T15:10:00+08:00' })
    request.reject(new Error('inbox unavailable'))
    await assert.rejects(load, /inbox unavailable/)

    assert.deepEqual(store.alerts.map(item => item.id), [2, 1])
    assert.equal(store.alertsLoading, false)
    assert.equal(store.alertsError, 'inbox unavailable')
    assert.equal(store.alertsRequestedUnreadOnly, false)
    assert.equal(store.alertsLoadedUnreadOnly, null)
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

test('Obsidian status loads both APIs in parallel and supports unconfigured vaults', async () => {
  await withObsidianStore(async ({ requests, store }) => {
    const load = store.loadObsidianStatus()

    assert.equal(store.obsidianStatusLoading, true)
    assert.equal(store.obsidianError, null)
    assert.equal(store.obsidianStatus, null)
    assert.equal(store.obsidianVaultStatus, null)
    assert.equal(store.obsidianStatusRequestId, 1)
    assert.deepEqual(requests.map(item => [item.config.method, item.config.url]), [
      ['get', '/trading-playbook/obsidian/status'],
      ['get', '/intelligence/obsidian/status']
    ])

    requests[0].request.resolve(
      axiosResponse(requests[0].config, obsidianSyncStatus('ready'))
    )
    await Promise.resolve()
    assert.equal(store.obsidianStatus, null)
    assert.equal(store.obsidianVaultStatus, null)
    assert.equal(store.obsidianStatusLoading, true)

    requests[1].request.resolve(
      axiosResponse(requests[1].config, obsidianVaultStatus('ready'))
    )
    await load
    assert.equal(store.obsidianStatus.recent_files[0], '30_TradingPlaybook/Daily/Auto/ready.md')
    assert.equal(store.obsidianVaultStatus.vault_path, 'D:/vault-ready')
    assert.equal(store.obsidianStatusLoading, false)
    assert.equal(store.obsidianError, null)

    const unconfiguredLoad = store.loadObsidianStatus()
    requests[2].request.resolve(axiosResponse(requests[2].config, obsidianSyncStatus(
      'unconfigured',
      { enabled: false, configured: false, vault_exists: false, dashboard_openable: false }
    )))
    requests[3].request.resolve(axiosResponse(requests[3].config, obsidianVaultStatus(
      'unconfigured',
      { enabled: false, vault_configured: false, vault_exists: false, vault_path: '' }
    )))
    await unconfiguredLoad

    assert.equal(store.obsidianStatus.enabled, false)
    assert.equal(store.obsidianStatus.configured, false)
    assert.equal(store.obsidianVaultStatus.vault_configured, false)
    assert.equal(store.obsidianVaultStatus.vault_path, '')
    assert.equal(store.obsidianStatusRequestId, 2)
    assert.equal(store.obsidianStatusLoading, false)
    assert.equal(store.obsidianError, null)
  })
})

test('Obsidian status partial failure preserves the last complete pair', async () => {
  await withObsidianStore(async ({ requests, store }) => {
    const initialLoad = store.loadObsidianStatus()
    requests[0].request.resolve(
      axiosResponse(requests[0].config, obsidianSyncStatus('previous'))
    )
    requests[1].request.resolve(
      axiosResponse(requests[1].config, obsidianVaultStatus('previous'))
    )
    await initialLoad

    const failedLoad = store.loadObsidianStatus()
    requests[2].request.resolve(
      axiosResponse(requests[2].config, obsidianSyncStatus('partial-new'))
    )
    requests[3].request.reject(new Error('vault network unavailable'))
    await assert.rejects(failedLoad, /vault network unavailable/)

    assert.equal(store.obsidianStatus.recent_files[0], '30_TradingPlaybook/Daily/Auto/previous.md')
    assert.equal(store.obsidianVaultStatus.vault_path, 'D:/vault-previous')
    assert.equal(store.obsidianStatusLoading, false)
    assert.equal(store.obsidianError, 'vault network unavailable')
  })
})

test('latest Obsidian status pair wins over stale success and stale failure', async () => {
  await withObsidianStore(async ({ requests, store }) => {
    const staleSuccess = store.loadObsidianStatus()
    const latestSuccess = store.loadObsidianStatus()
    requests[2].request.resolve(
      axiosResponse(requests[2].config, obsidianSyncStatus('latest'))
    )
    requests[3].request.resolve(
      axiosResponse(requests[3].config, obsidianVaultStatus('latest'))
    )
    await latestSuccess
    requests[0].request.resolve(
      axiosResponse(requests[0].config, obsidianSyncStatus('stale'))
    )
    requests[1].request.resolve(
      axiosResponse(requests[1].config, obsidianVaultStatus('stale'))
    )
    await staleSuccess

    assert.equal(store.obsidianStatus.recent_files[0], '30_TradingPlaybook/Daily/Auto/latest.md')
    assert.equal(store.obsidianVaultStatus.vault_path, 'D:/vault-latest')
    assert.equal(store.obsidianStatusLoading, false)
    assert.equal(store.obsidianError, null)

    const staleFailure = store.loadObsidianStatus()
    const newestSuccess = store.loadObsidianStatus()
    requests[6].request.resolve(
      axiosResponse(requests[6].config, obsidianSyncStatus('newest'))
    )
    requests[7].request.resolve(
      axiosResponse(requests[7].config, obsidianVaultStatus('newest'))
    )
    await newestSuccess
    requests[4].request.reject(new Error('stale status failed'))
    requests[5].request.resolve(
      axiosResponse(requests[5].config, obsidianVaultStatus('ignored'))
    )
    await assert.rejects(staleFailure, /stale status failed/)

    assert.equal(store.obsidianStatus.recent_files[0], '30_TradingPlaybook/Daily/Auto/newest.md')
    assert.equal(store.obsidianVaultStatus.vault_path, 'D:/vault-newest')
    assert.equal(store.obsidianStatusRequestId, 4)
    assert.equal(store.obsidianStatusLoading, false)
    assert.equal(store.obsidianError, null)
  })
})

test('manual Obsidian export posts strict payload, refreshes both statuses, and preserves them on failure', async () => {
  await withObsidianStore(async ({ requests, store }) => {
    const exported = store.exportToObsidian('2026-07-16', true, true)
    assert.equal(store.obsidianExporting, true)
    assert.equal(store.obsidianError, null)
    assert.equal(requests.length, 1)
    assert.equal(requests[0].config.method, 'post')
    assert.equal(requests[0].config.url, '/trading-playbook/obsidian/export')
    assert.deepEqual(JSON.parse(requests[0].config.data), {
      trade_date: '2026-07-16',
      include_rules: true,
      force: true
    })

    const exportResponse = obsidianExportResult('2026-07-16')
    requests[0].request.resolve(axiosResponse(requests[0].config, exportResponse))
    await new Promise(resolvePromise => setImmediate(resolvePromise))
    assert.equal(requests.length, 3)
    assert.equal(store.obsidianExporting, true)
    assert.equal(store.obsidianStatusLoading, true)
    requests[1].request.resolve(
      axiosResponse(requests[1].config, obsidianSyncStatus('after-export'))
    )
    requests[2].request.resolve(
      axiosResponse(requests[2].config, obsidianVaultStatus('after-export'))
    )

    assert.deepEqual(await exported, exportResponse)
    assert.equal(store.obsidianStatus.recent_files[0], '30_TradingPlaybook/Daily/Auto/after-export.md')
    assert.equal(store.obsidianVaultStatus.vault_path, 'D:/vault-after-export')
    assert.equal(store.obsidianStatusLoading, false)
    assert.equal(store.obsidianExporting, false)
    assert.equal(store.obsidianError, null)

    const failedExport = store.exportToObsidian('2026-07-17')
    assert.deepEqual(JSON.parse(requests[3].config.data), {
      trade_date: '2026-07-17',
      include_rules: false,
      force: false
    })
    requests[3].request.reject(new Error('manual export unavailable'))
    await assert.rejects(failedExport, /manual export unavailable/)

    assert.equal(store.obsidianStatus.recent_files[0], '30_TradingPlaybook/Daily/Auto/after-export.md')
    assert.equal(store.obsidianVaultStatus.vault_path, 'D:/vault-after-export')
    assert.equal(store.obsidianExporting, false)
    assert.equal(store.obsidianError, 'manual export unavailable')
  })
})

test('concurrent Obsidian exports keep loading and errors owned by the newest request', async () => {
  await withObsidianStore(async ({ requests, store }) => {
    const staleFailure = store.exportToObsidian('2026-07-14')
    const latestSuccess = store.exportToObsidian('2026-07-15')
    requests[0].request.reject(new Error('stale export failed'))
    await assert.rejects(staleFailure, /stale export failed/)
    assert.equal(store.obsidianExporting, true)
    assert.equal(store.obsidianError, null)

    requests[1].request.resolve(
      axiosResponse(requests[1].config, obsidianExportResult('2026-07-15'))
    )
    await new Promise(resolvePromise => setImmediate(resolvePromise))
    requests[2].request.resolve(
      axiosResponse(requests[2].config, obsidianSyncStatus('latest-export'))
    )
    requests[3].request.resolve(
      axiosResponse(requests[3].config, obsidianVaultStatus('latest-export'))
    )
    await latestSuccess
    assert.equal(store.obsidianExporting, false)
    assert.equal(store.obsidianError, null)

    const staleSuccess = store.exportToObsidian('2026-07-16')
    const latestFailure = store.exportToObsidian('2026-07-17')
    requests[5].request.reject(new Error('latest export failed'))
    await assert.rejects(latestFailure, /latest export failed/)
    assert.equal(store.obsidianExporting, true)
    assert.equal(store.obsidianError, 'latest export failed')

    requests[4].request.resolve(
      axiosResponse(requests[4].config, obsidianExportResult('2026-07-16'))
    )
    await new Promise(resolvePromise => setImmediate(resolvePromise))
    assert.equal(requests.length, 8)
    assert.equal(store.obsidianExporting, true)
    requests[6].request.resolve(
      axiosResponse(requests[6].config, obsidianSyncStatus('stale-export-refresh'))
    )
    requests[7].request.resolve(
      axiosResponse(requests[7].config, obsidianVaultStatus('stale-export-refresh'))
    )
    await staleSuccess
    assert.equal(store.obsidianExporting, false)
    assert.equal(store.obsidianError, 'latest export failed')
    assert.equal(
      store.obsidianStatus.recent_files[0],
      '30_TradingPlaybook/Daily/Auto/stale-export-refresh.md'
    )
    assert.equal(store.obsidianVaultStatus.vault_path, 'D:/vault-stale-export-refresh')
  })
})

test('trading_plan_alert websocket routing stays outside global alerts speech and desktop notifications', () => {
  const source = readFileSync(resolve(root, 'src/composables/useWebSocket.ts'), 'utf8')
  const match = source.match(/case ['"]trading_plan_alert['"]:([\s\S]*?)break/)

  assert.ok(match, 'trading_plan_alert case should exist')
  assert.match(match[1], /tradingPlaybookStore\.receiveAlert\(message\.data as TradingAlertEvent\)/)
  assert.doesNotMatch(match[1], /alertStore|useSpeech|Notification|Audio/)
})

async function withWebSocketHarness(run) {
  await withFrontendModules(async server => {
    const originalWindow = Object.getOwnPropertyDescriptor(globalThis, 'window')
    const originalWebSocket = Object.getOwnPropertyDescriptor(globalThis, 'WebSocket')
    const originalWarn = console.warn
    const originalError = console.error
    const sockets = []
    const loadCalls = []
    const unhandledRejections = []
    const reconnectCallbacks = []
    const errors = []
    let pingStarts = 0

    class FakeWebSocket {
      static CONNECTING = 0
      static OPEN = 1
      static CLOSED = 3

      get [Symbol.toStringTag]() {
        return 'WebSocket'
      }

      constructor(url) {
        this.url = url
        this.readyState = FakeWebSocket.CONNECTING
        sockets.push(this)
      }

      send() {}
      close() {
        this.readyState = FakeWebSocket.CLOSED
        setImmediate(() => this.onclose?.())
      }
    }

    const onUnhandledRejection = reason => unhandledRejections.push(reason)
    process.on('unhandledRejection', onUnhandledRejection)
    Object.defineProperty(globalThis, 'window', {
      configurable: true,
      value: {
        location: { protocol: 'http:', host: 'example.test' },
        setInterval: () => {
          pingStarts += 1
          return 100 + pingStarts
        },
        setTimeout: callback => {
          reconnectCallbacks.push(callback)
          return reconnectCallbacks.length
        }
      }
    })
    Object.defineProperty(globalThis, 'WebSocket', {
      configurable: true,
      value: FakeWebSocket
    })

    try {
      setActivePinia(createPinia())
      const { useTradingPlaybookStore } = await server.ssrLoadModule('/src/stores/trading-playbook.ts')
      const tradingPlaybookStore = useTradingPlaybookStore()
      tradingPlaybookStore.loadAlerts = unreadOnly => {
        loadCalls.push(unreadOnly)
        return Promise.reject(new Error('inbox temporarily unavailable'))
      }
      const { useWebSocket } = await server.ssrLoadModule('/src/composables/useWebSocket.ts')
      console.warn = () => {}
      const client = useWebSocket()
      console.warn = originalWarn
      console.error = (...args) => errors.push(args)

      await run({
        client,
        errors,
        FakeWebSocket,
        loadCalls,
        reconnectCallbacks,
        sockets,
        tradingPlaybookStore,
        unhandledRejections,
        pingStarts: () => pingStarts
      })
    } finally {
      console.warn = originalWarn
      console.error = originalError
      process.off('unhandledRejection', onUnhandledRejection)
      if (originalWindow) Object.defineProperty(globalThis, 'window', originalWindow)
      else delete globalThis.window
      if (originalWebSocket) Object.defineProperty(globalThis, 'WebSocket', originalWebSocket)
      else delete globalThis.WebSocket
    }
  })
}

test('websocket reconnect reloads unread playbook alerts and contains rejection', async () => {
  await withWebSocketHarness(async ({
    client,
    FakeWebSocket,
    loadCalls,
    reconnectCallbacks,
    sockets,
    unhandledRejections,
    pingStarts
  }) => {
    client.connect()
    sockets[0].readyState = FakeWebSocket.OPEN
    sockets[0].onopen()
    await new Promise(resolvePromise => setImmediate(resolvePromise))

    assert.deepEqual(loadCalls, [])
    assert.equal(client.isConnected.value, true)
    assert.equal(pingStarts(), 1)

    sockets[0].readyState = FakeWebSocket.CLOSED
    sockets[0].onclose()
    assert.equal(client.isConnected.value, false)
    assert.equal(reconnectCallbacks.length, 1)
    reconnectCallbacks[0]()
    assert.equal(sockets.length, 2)
    sockets[1].readyState = FakeWebSocket.OPEN
    sockets[1].onopen()
    await new Promise(resolvePromise => setImmediate(resolvePromise))

    assert.deepEqual(loadCalls, [true])
    assert.deepEqual(unhandledRejections, [])
    assert.equal(client.isConnected.value, true)
    assert.equal(pingStarts(), 2)
  })
})

test('manual websocket disconnect ignores asynchronous close and allows a later explicit reconnect', async () => {
  await withWebSocketHarness(async ({
    client,
    FakeWebSocket,
    loadCalls,
    reconnectCallbacks,
    sockets
  }) => {
    client.connect()
    sockets[0].readyState = FakeWebSocket.OPEN
    sockets[0].onopen()

    client.disconnect()
    await new Promise(resolvePromise => setImmediate(resolvePromise))

    assert.equal(client.ws.value, null)
    assert.equal(client.isConnected.value, false)
    assert.deepEqual(loadCalls, [])
    assert.deepEqual(reconnectCallbacks, [])

    client.connect()
    assert.equal(sockets.length, 2)
    sockets[1].readyState = FakeWebSocket.OPEN
    sockets[1].onopen()
    await new Promise(resolvePromise => setImmediate(resolvePromise))
    assert.deepEqual(loadCalls, [])

    sockets[1].readyState = FakeWebSocket.CLOSED
    sockets[1].onclose()
    assert.equal(reconnectCallbacks.length, 1)
  })
})

test('repeated connect while websocket is connecting creates only one socket', async () => {
  await withWebSocketHarness(async ({ client, FakeWebSocket, sockets }) => {
    client.connect()
    assert.equal(sockets[0].readyState, FakeWebSocket.CONNECTING)

    client.connect()

    assert.equal(sockets.length, 1)
    assert.equal(client.ws.value, sockets[0])
  })
})

test('late callbacks from an obsolete websocket cannot mutate current connection state', async () => {
  await withWebSocketHarness(async ({
    client,
    errors,
    FakeWebSocket,
    loadCalls,
    reconnectCallbacks,
    sockets,
    tradingPlaybookStore,
    pingStarts
  }) => {
    client.connect()
    const obsoleteSocket = sockets[0]
    obsoleteSocket.readyState = FakeWebSocket.CLOSED
    client.connect()
    const currentSocket = sockets[1]
    currentSocket.readyState = FakeWebSocket.OPEN
    currentSocket.onopen()

    obsoleteSocket.onopen()
    obsoleteSocket.onmessage({
      data: JSON.stringify({
        type: 'trading_plan_alert',
        data: alert(99, 'obsolete-socket-alert'),
        timestamp: '2026-07-14T15:20:00+08:00'
      })
    })
    obsoleteSocket.onerror(new Error('obsolete socket error'))
    obsoleteSocket.onclose()
    await new Promise(resolvePromise => setImmediate(resolvePromise))

    assert.equal(client.ws.value, currentSocket)
    assert.equal(client.isConnected.value, true)
    assert.equal(pingStarts(), 1)
    assert.deepEqual(loadCalls, [])
    assert.deepEqual(tradingPlaybookStore.alerts, [])
    assert.deepEqual(reconnectCallbacks, [])
    assert.deepEqual(errors, [])
  })
})

test('trading playbook store never imports the global alert store', () => {
  const source = readFileSync(resolve(root, 'src/stores/trading-playbook.ts'), 'utf8')
  assert.match(source, /defineStore\(['"]trading-playbook['"]/)
  assert.doesNotMatch(source, /useAlertStore|stores\/alert/)
})
