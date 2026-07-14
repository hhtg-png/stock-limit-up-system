import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer } from 'vite'
import vue from '@vitejs/plugin-vue'

const root = resolve(import.meta.dirname, '..')
const viewPath = resolve(root, 'src/views/TradingPlaybook.vue')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8').replace(/\r\n/g, '\n')
}

async function withFrontendModules(run) {
  const server = await createServer({
    configFile: false,
    root,
    logLevel: 'silent',
    plugins: [vue()],
    server: { middlewareMode: true },
    resolve: { alias: { '@': resolve(root, 'src') } }
  })
  try {
    await run(server)
  } finally {
    await server.close()
  }
}

test('standalone playbook page exposes the complete working sections', () => {
  assert.equal(existsSync(viewPath), true, 'TradingPlaybook view should exist')
  const view = read('src/views/TradingPlaybook.vue')

  for (const text of [
    '交易预案',
    '市场状态',
    '版本时间轴',
    '正式行动计划',
    '全模式雷达',
    '独立提醒',
    '执行复盘',
    '规则来源'
  ]) {
    assert.match(view, new RegExp(text), `page should expose ${text}`)
  }
  assert.match(view, /启用行动提醒/, 'confirmation must describe enabling reminders')
  assert.match(view, /仅生成预案与提醒，不会自动下单\/交易/, 'page should reject auto-trading semantics')
  assert.match(view, /confirmTradingPlan/, 'page should call the confirmation API')
  assert.match(view, /cancelTradingPlan/, 'page should allow cancelling eligible plans')
  assert.match(view, /\.slice\(0,\s*3\)/, 'action candidates should remain capped at three')
})

test('page binds every section to live store or API data and remains isolated', () => {
  const view = read('src/views/TradingPlaybook.vue')

  for (const symbol of [
    'store.loadPlans',
    'store.loadReviews',
    'store.loadAlerts',
    'store.loadSettings',
    'getTradingRules',
    'updateTradingExecutionReview',
    'updateTradingPlaybookSettings',
    'store.acknowledgeAlert'
  ]) {
    assert.match(view, new RegExp(symbol.replace('.', '\\.')), `page should use ${symbol}`)
  }
  assert.match(view, /预案目标交易日/, 'plan date should be labelled as a target date')
  assert.match(view, /复盘交易日/, 'review date should be independently labelled')
  assert.match(view, /acknowledged_at/, 'visible unread alerts must use persisted acknowledgement state')
  assert.doesNotMatch(view, /alertsLoadedUnreadOnly/, 'view must not infer visibility from request metadata')
  assert.doesNotMatch(view, /useAlertStore|useSpeech|Notification\s*\(/, 'page must not enter global alert paths')
})

test('version timeline displays persisted version differences', () => {
  const view = read('src/views/TradingPlaybook.vue')

  assert.match(view, /change_summary_json/, 'timeline should expose the immutable version change summary')
  assert.match(view, /版本变化/, 'timeline should label the persisted change summary')
})

test('page exposes loading, error, and empty states and keeps WeChat unavailable', () => {
  const view = read('src/views/TradingPlaybook.vue')

  assert.match(view, /v-loading=/, 'sections should expose loading state')
  assert.match(view, /el-alert[\s\S]*Error|plansError|reviewsError|alertsError|settingsError/, 'page should render request errors')
  assert.match(view, /<el-empty/, 'collections should render explicit empty states')
  assert.match(view, /微信机器人暂未接入/, 'settings should explain the future WeChat channel')
  assert.match(view, /disabled[\s\S]*微信|微信[\s\S]*disabled/, 'WeChat control should be disabled')
})

test('Vue page compiles as a runtime module', async () => {
  await withFrontendModules(async server => {
    const module = await server.ssrLoadModule('/src/views/TradingPlaybook.vue')
    assert.equal(typeof module.default, 'object')
    assert.equal(typeof module.default.setup, 'function')
  })
})

test('presentation helpers implement confirmation, canonical inbox, and section states', async () => {
  await withFrontendModules(async server => {
    const helpers = await server.ssrLoadModule('/src/views/trading-playbook/presentation.ts')
    const draft = {
      status: 'draft',
      data_quality_json: { status: 'ready' }
    }

    assert.equal(helpers.canEnableActionAlerts(draft), true)
    assert.equal(helpers.canEnableActionAlerts({ ...draft, status: 'active' }), false)
    assert.equal(helpers.canEnableActionAlerts({ ...draft, data_quality_json: { status: 'missing' } }), false)
    assert.equal(helpers.canEnableActionAlerts(null), false)

    const alerts = [
      { id: 1, acknowledged_at: null },
      { id: 2, acknowledged_at: '2026-07-14T15:00:00+08:00' }
    ]
    assert.deepEqual(helpers.filterTradingAlerts(alerts, 'unread').map(item => item.id), [1])
    assert.deepEqual(helpers.filterTradingAlerts(alerts, 'all').map(item => item.id), [1, 2])

    assert.equal(helpers.collectionState(true, 'failed', []), 'loading')
    assert.equal(helpers.collectionState(false, 'failed', []), 'error')
    assert.equal(helpers.collectionState(false, null, []), 'empty')
    assert.equal(helpers.collectionState(false, null, [{}]), 'ready')
  })
})

test('settings and review helpers create safe backend payloads in China time', async () => {
  await withFrontendModules(async server => {
    const helpers = await server.ssrLoadModule('/src/views/trading-playbook/presentation.ts')

    assert.deepEqual(helpers.buildSettingsUpdate({
      enabled: true,
      in_app_enabled: true,
      trial_position_pct: 10,
      confirmed_position_pct: 30,
      hard_stop_pct: 5,
      max_action_candidates: 3,
      wechat_enabled: true
    }), {
      enabled: true,
      in_app_enabled: true,
      trial_position_pct: 10,
      confirmed_position_pct: 30,
      hard_stop_pct: 5,
      max_action_candidates: 3,
      wechat_enabled: false
    })
    assert.throws(
      () => helpers.buildSettingsUpdate({ trial_position_pct: 40, confirmed_position_pct: 30 }),
      /试错仓位不能高于确认仓位/
    )

    const payload = helpers.buildManualExecutionUpdate(
      '2026-07-14',
      {
        '7': { executed: true, execution_price: 12.3, quantity: 100, executed_time: '10:05:00', manual_note: '按计划' },
        '8': { executed: false, execution_price: 8.8, quantity: 200, executed_time: '14:00:00', manual_note: '' }
      },
      [{ stock_code: '600000', stock_name: '浦发银行', execution_price: 9.5, quantity: 300, executed_time: '14:20:00', manual_note: '计划外记录' }]
    )
    assert.deepEqual(payload, {
      executions: {
        '7': {
          executed: true,
          execution_price: 12.3,
          quantity: 100,
          executed_at: '2026-07-14T10:05:00+08:00',
          manual_note: '按计划'
        },
        '8': { executed: false }
      },
      unplanned_executions: [{
        executed: true,
        stock_code: '600000',
        stock_name: '浦发银行',
        execution_price: 9.5,
        quantity: 300,
        executed_at: '2026-07-14T14:20:00+08:00',
        manual_note: '计划外记录'
      }]
    })
    assert.throws(
      () => helpers.buildManualExecutionUpdate('2026-07-14', { '7': { executed: true, executed_time: '12:00:00' } }, []),
      /连续交易时段/
    )
  })
})
