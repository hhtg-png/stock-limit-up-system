import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import { createPinia } from 'pinia'
import { createServer } from 'vite'

const root = resolve(import.meta.dirname, '..')

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
    store.receiveAlert(alert(1, 'different'))
    store.receiveAlert(alert(2, 'same'))
    store.receiveAlert(alert(3, 'new', '2026-07-14T15:00:00+08:00'))

    assert.deepEqual(store.alerts.map(item => item.id), [3, 1])
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
