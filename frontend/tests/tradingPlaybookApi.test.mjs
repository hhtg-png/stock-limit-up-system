import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
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

function jsonBody(value) {
  return typeof value === 'string' ? JSON.parse(value) : value
}

test('trading playbook types preserve backend snake_case contracts', () => {
  const source = readFileSync(resolve(root, 'src/types/trading-playbook.ts'), 'utf8')

  for (const field of [
    'source_trade_date',
    'target_trade_date',
    'parent_plan_version_id',
    'market_state_json',
    'mode_radar_json',
    'data_quality_json',
    'supporting_mode_keys_json',
    'entry_trigger_json',
    'channel_status_json',
    'unplanned_executions'
  ]) {
    assert.match(source, new RegExp(`\\b${field}\\b`), `${field} must remain snake_case`)
  }
  assert.match(source, /interface\s+TradingExecutionReview[\s\S]*?data_quality_json:/)
  assert.match(
    source,
    /type\s+TradingAlertEventType[\s\S]*?['"]review_ready['"]/
  )
  assert.match(
    source,
    /interface\s+TradingRuleSnapshot[\s\S]*?mode_key:\s*string[\s\S]*?version:\s*number[\s\S]*?content_hash:\s*string/
  )
  assert.match(source, /interface\s+TradingRuleSnapshot[\s\S]*?\[key:\s*string\]:\s*unknown/)
  assert.match(source, /rule_snapshot_json:\s*TradingRuleSnapshot\[\]/)
  assert.match(source, /wechat_enabled:\s*false/)
})

test('trading plan API preserves object rule snapshots from the backend', async () => {
  await withFrontendModules(async server => {
    const client = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const ruleSnapshot = {
      mode_key: 'leader_turn_two',
      version: 3,
      content_hash: 'a'.repeat(64),
      name: '龙头弱转强'
    }
    client.tradingPlaybookApi.defaults.adapter = async config => ({
      data: { id: 7, rule_snapshot_json: [ruleSnapshot] },
      status: 200,
      statusText: 'OK',
      headers: {},
      config
    })

    const plan = await client.getTradingPlan(7)

    assert.deepEqual(plan.rule_snapshot_json, [ruleSnapshot])
  })
})

test('trading playbook API sends exact backend routes, params, and payloads', async () => {
  await withFrontendModules(async server => {
    const client = await server.ssrLoadModule('/src/api/trading-playbook.ts')
    const calls = []
    client.tradingPlaybookApi.defaults.adapter = async config => {
      calls.push({
        method: config.method,
        url: config.url,
        params: config.params,
        data: jsonBody(config.data)
      })
      return {
        data: { call: calls.length },
        status: 200,
        statusText: 'OK',
        headers: {},
        config
      }
    }

    const revision = { change_note: '临盘修订', candidate_overrides: [] }
    const review = {
      executions: { '7': { executed: false } },
      unplanned_executions: [{ executed: true, stock_code: '600000', stock_name: '浦发银行' }]
    }
    const settings = { in_app_enabled: false, wechat_enabled: false }

    await client.getTradingRules()
    await client.getTradingPlans('2026-07-14')
    await client.getTradingPlan(7)
    await client.generateTradingPlan('2026-07-14', 'preclose')
    await client.reviseTradingPlan(7, revision)
    await client.confirmTradingPlan(7, 'local-user')
    await client.cancelTradingPlan(7)
    await client.getTradingAlerts(false)
    await client.ackTradingAlert(9)
    await client.getTradingReviews('2026-07-14', 7)
    await client.updateTradingExecutionReview('2026-07-14', review, 7)
    await client.getTradingReviews('2026-07-15')
    await client.updateTradingExecutionReview('2026-07-15', review)
    await client.getTradingPlaybookSettings()
    await client.updateTradingPlaybookSettings(settings)
    await client.getTradingPlaybookPersonalWechatStatus()

    assert.equal(client.tradingPlaybookApi.defaults.baseURL, '/api/v1')
    assert.deepEqual(calls, [
      { method: 'get', url: '/trading-playbook/rules', params: undefined, data: undefined },
      { method: 'get', url: '/trading-playbook/plans', params: { trade_date: '2026-07-14' }, data: undefined },
      { method: 'get', url: '/trading-playbook/plans/7', params: undefined, data: undefined },
      {
        method: 'post',
        url: '/trading-playbook/plans/generate',
        params: undefined,
        data: { source_trade_date: '2026-07-14', stage: 'preclose' }
      },
      { method: 'put', url: '/trading-playbook/plans/7', params: undefined, data: revision },
      { method: 'post', url: '/trading-playbook/plans/7/confirm', params: undefined, data: { confirmed_by: 'local-user' } },
      { method: 'post', url: '/trading-playbook/plans/7/cancel', params: undefined, data: undefined },
      { method: 'get', url: '/trading-playbook/alerts', params: { unread_only: false }, data: undefined },
      { method: 'post', url: '/trading-playbook/alerts/9/ack', params: undefined, data: undefined },
      {
        method: 'get',
        url: '/trading-playbook/reviews',
        params: { trade_date: '2026-07-14', plan_id: 7 },
        data: undefined
      },
      {
        method: 'put',
        url: '/trading-playbook/reviews/2026-07-14',
        params: { plan_id: 7 },
        data: review
      },
      {
        method: 'get',
        url: '/trading-playbook/reviews',
        params: { trade_date: '2026-07-15' },
        data: undefined
      },
      {
        method: 'put',
        url: '/trading-playbook/reviews/2026-07-15',
        params: undefined,
        data: review
      },
      { method: 'get', url: '/trading-playbook/settings', params: undefined, data: undefined },
      { method: 'put', url: '/trading-playbook/settings', params: undefined, data: settings },
      {
        method: 'get',
        url: '/trading-playbook/notifications/personal-wechat/status',
        params: undefined,
        data: undefined
      }
    ])
  })
})
