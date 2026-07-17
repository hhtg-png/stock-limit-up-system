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
  assert.match(view, /reviseTradingPlan/, 'page should create an audited child revision before confirmation')
  assert.match(view, /确认前修订/, 'draft plan should expose a revision editor')
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
  assert.match(view, /getLatestTradingPlanTargetDate/, 'page should discover the latest generated target date')
  assert.match(view, /await selectLatestPlanTargetDate\(\)[\s\S]*await loadAll\(\)/, 'latest target date should be selected before initial loading')
  assert.match(view, /复盘交易日/, 'review date should be independently labelled')
  assert.match(view, /acknowledged_at/, 'visible unread alerts must use persisted acknowledgement state')
  assert.match(view, /riskPermissionSummary\(selectedPlan\.risk_settings_json\)/, 'risk permission must use the persisted risk snapshot')
  assert.doesNotMatch(view, /marketValue\(['"]risk_permission['"]\)/, 'market state has no risk_permission field')
  assert.doesNotMatch(view, /alertsLoadedUnreadOnly/, 'view must not infer visibility from request metadata')
  assert.doesNotMatch(view, /useAlertStore|useSpeech|Notification\s*\(/, 'page must not enter global alert paths')
})

test('version timeline displays persisted version differences', () => {
  const view = read('src/views/TradingPlaybook.vue')

  assert.match(view, /change_summary_json/, 'timeline should expose the immutable version change summary')
  assert.match(view, /版本变化/, 'timeline should label the persisted change summary')
})

test('page exposes loading, error, empty states, and personal WeChat status', () => {
  const view = read('src/views/TradingPlaybook.vue')

  assert.match(view, /v-loading=/, 'sections should expose loading state')
  assert.match(view, /el-alert[\s\S]*Error|plansError|reviewsError|alertsError|settingsError/, 'page should render request errors')
  assert.match(view, /<el-empty/, 'collections should render explicit empty states')
  assert.match(view, /维持观望和空仓/, 'a ready plan with no candidate should render an explicit no-trade conclusion')
  assert.match(view, /观望 \/ 空仓预案/, 'no-candidate plans should render a visible plan card')
  assert.match(view, /目标日仓位 0%/, 'no-action plan should state the position explicitly')
  assert.match(view, /禁止动作/, 'no-action plan should state prohibited actions')
  assert.match(view, /重新评估/, 'no-action plan should state when the decision can change')
  assert.match(view, /个人微信提醒/, 'settings should expose the personal WeChat channel')
  assert.match(view, /getTradingPlaybookPersonalWechatStatus/, 'WeChat status should come from the backend')
  assert.match(view, /打开个人微信绑定二维码/, 'settings should expose the secure setup entry')
})

test('page exposes compact Obsidian sync controls without weakening manual boundaries', () => {
  const view = read('src/views/TradingPlaybook.vue')

  for (const text of [
    'Obsidian 同步',
    '导出到 Obsidian',
    '打开交易预案 Dashboard',
    '只导出、不会从 Obsidian 回写',
    '需要人工确认',
    '不会自动交易',
    '待重试',
    '已暂停',
    '失败',
    '最近导出文件'
  ]) {
    assert.match(view, new RegExp(text), `Obsidian card should expose ${text}`)
  }
  for (const binding of [
    'store.obsidianStatusLoading',
    'store.obsidianExporting',
    'store.obsidianError',
    'store.obsidianStatus.pending_count',
    'store.obsidianStatus.paused_count',
    'store.obsidianStatus.failed_count',
    'store.obsidianStatus.last_trade_date',
    'store.obsidianStatus.last_phase',
    'store.obsidianStatus.last_success_at',
    'store.obsidianStatus.recent_files',
    'canExportObsidian',
    'obsidianDashboardUri'
  ]) {
    assert.match(view, new RegExp(binding.replaceAll('.', '\\.')), `Obsidian card should bind ${binding}`)
  }
  for (const readiness of [
    'store.obsidianVaultStatus.enabled',
    'store.obsidianVaultStatus.vault_configured',
    'store.obsidianVaultStatus.vault_exists'
  ]) {
    assert.match(view, new RegExp(readiness.replaceAll('.', '\\.')), `readiness should use intelligence ${readiness}`)
  }
  const readinessBlock = view.slice(
    view.indexOf('const canExportObsidian'),
    view.indexOf('const obsidianDashboardUri')
  )
  assert.doesNotMatch(
    readinessBlock,
    /store\.obsidianStatus\.(?:enabled|configured|vault_exists)/,
    'export readiness must not use trading status configuration fields'
  )
  assert.match(view, /store\.exportToObsidian\(targetPlanDate\.value/, 'manual export should use the selected target date')
  assert.match(view, /if \(!canExportObsidian\.value \|\| store\.obsidianExporting\) return/, 'manual export should reject disabled and duplicate submissions')
  assert.match(view, /Promise\.allSettled\([\s\S]*store\.loadObsidianStatus\(\)/, 'initial and manual refresh should load both Obsidian statuses in parallel')
})

test('Obsidian dashboard helper permits only a safe relative trading dashboard path', async () => {
  await withFrontendModules(async server => {
    const view = await server.ssrLoadModule('/src/views/TradingPlaybook.vue')
    const tradingStatus = {
      dashboard_openable: true,
      dashboard_path: '交易预案/Dashboard.md'
    }
    const vaultStatus = {
      enabled: true,
      vault_configured: true,
      vault_exists: true,
      vault_path: 'C:\\Users\\Administrator\\Documents\\交易 Vault'
    }

    assert.equal(
      view.buildObsidianDashboardUri(tradingStatus, vaultStatus),
      `obsidian://open?vault=${encodeURIComponent('交易 Vault')}&file=${encodeURIComponent('交易预案/Dashboard.md')}`
    )
    for (const path of [
      '',
      '/交易预案/Dashboard.md',
      '\\交易预案\\Dashboard.md',
      'C:/交易预案/Dashboard.md',
      'C:\\交易预案\\Dashboard.md',
      '.',
      '..',
      '交易预案/../Dashboard.md',
      '交易预案/./Dashboard.md',
      '交易预案//Dashboard.md',
      '交易预案\\Dashboard.md',
      '交易预案/\u0000Dashboard.md'
    ]) {
      assert.equal(
        view.buildObsidianDashboardUri({ ...tradingStatus, dashboard_path: path }, vaultStatus),
        null,
        `unsafe dashboard path ${JSON.stringify(path)} must be rejected`
      )
    }
    for (const patch of [
      { dashboard_openable: false },
      { vault: { enabled: false } },
      { vault: { vault_configured: false } },
      { vault: { vault_exists: false } },
      { vault: { vault_path: 'C:\\' } },
      { vault: { vault_path: 'C:\\Vault\\..' } },
      { vault: { vault_path: 'C:\\Vault\\bad\u0001name' } }
    ]) {
      assert.equal(
        view.buildObsidianDashboardUri(
          { ...tradingStatus, ...(patch.dashboard_openable === undefined ? {} : { dashboard_openable: patch.dashboard_openable }) },
          { ...vaultStatus, ...patch.vault }
        ),
        null
      )
    }
    const uri = view.buildObsidianDashboardUri(tradingStatus, vaultStatus)
    assert.doesNotMatch(uri, /Users|Administrator|Documents|C%3A/, 'absolute vault path must never enter the URI')
  })
})

test('Obsidian export result helper reports every count and warns on partial results', async () => {
  await withFrontendModules(async server => {
    const view = await server.ssrLoadModule('/src/views/TradingPlaybook.vue')
    const complete = view.describeObsidianExportResult({
      written_files: ['a.md', 'b.md'],
      skipped_files: ['same.md'],
      pending_files: [],
      failed_files: [],
      git_status: { state: 'git_complete', enabled: true, committed: true },
      error_summary: null
    })
    assert.equal(complete.level, 'success')
    assert.match(complete.message, /写入 2/)
    assert.match(complete.message, /跳过 1/)
    assert.match(complete.message, /待重试 0/)
    assert.match(complete.message, /失败 0/)
    assert.match(complete.message, /Git.*提交完成/)

    const partial = view.describeObsidianExportResult({
      written_files: ['a.md'],
      skipped_files: [],
      pending_files: ['retry.md'],
      failed_files: ['failed.md'],
      git_status: { state: 'git_complete', enabled: true, committed: true },
      error_summary: 'Dashboard 写入失败'
    })
    assert.equal(partial.level, 'warning')
    assert.match(partial.message, /写入 1.*跳过 0.*待重试 1.*失败 1/)
    assert.match(partial.message, /错误摘要：Dashboard 写入失败/)
    assert.doesNotMatch(partial.message, /完整成功|全部成功/)

    for (const git_status of [
      { state: 'not_attempted', enabled: false, committed: false, reason: 'no_written_files' },
      { state: 'not_needed', enabled: true, committed: false, reason: 'content_identical' }
    ]) {
      const result = view.describeObsidianExportResult({
        written_files: [],
        skipped_files: ['same.md'],
        pending_files: [],
        failed_files: [],
        git_status,
        error_summary: null
      })
      assert.equal(result.level, 'success', `${git_status.state} should be an allowed terminal Git state`)
      assert.match(result.message, /Git/)
    }

    for (const [git_status, detail] of [
      [{ state: 'git_error', enabled: true, committed: false, error: 'push failed' }, /Git.*失败.*push failed/],
      [{ state: 'git_pending', enabled: true, committed: false, reason: 'content_changed' }, /Git.*待处理/],
      [{ state: 'git_store_pending', enabled: true, committed: false }, /Git.*待保存.*重试/],
      [{ state: 'write_in_progress', enabled: true, committed: false }, /Git.*写入处理中/],
      [{ state: 'write_failed', enabled: true, committed: false }, /Git.*写入失败/],
      [{ state: 'lease_claimed', enabled: true, committed: false }, /Git.*任务处理中/],
      [{ state: 'future_state', enabled: true, committed: false }, /Git.*未知状态.*future_state/],
      [{}, /Git.*状态缺失/]
    ]) {
      const result = view.describeObsidianExportResult({
        written_files: ['a.md'],
        skipped_files: [],
        pending_files: [],
        failed_files: [],
        git_status,
        error_summary: null
      })
      assert.equal(result.level, 'warning', `${git_status.state || 'missing'} Git state must fail closed`)
      assert.match(result.message, detail)
      assert.doesNotMatch(result.message, /完整成功|全部成功/)
    }

    const missing = view.describeObsidianExportResult({
      written_files: ['a.md'],
      skipped_files: [],
      pending_files: [],
      failed_files: [],
      error_summary: null
    })
    assert.equal(missing.level, 'warning')
    assert.match(missing.message, /Git.*状态缺失/)
  })
})

test('review and revision editors freeze every captured input while saving', () => {
  const view = read('src/views/TradingPlaybook.vue')
  const reviewSection = view.slice(view.indexOf('<section class="panel review-panel"'), view.indexOf('<section class="panel settings-panel"'))
  const disabledReviewBindings = reviewSection.match(/:disabled="[^"]*reviewSaving[^"]*"/g) || []

  assert.match(view, /async function saveExecutionReview\(\)\s*{\s*if \(reviewSaving\.value \|\| !reviewEditorReady\.value\) return/)
  assert.match(view, /function selectReviewRow[\s\S]{0,180}if \(reviewSaving\.value\) return/)
  assert.ok(disabledReviewBindings.length >= 13, 'all review date, switches, fields, and mutation buttons must freeze')
  assert.match(view, /<el-form[^>]*:disabled="revisionSaving"[^>]*class="revision-form"|<el-form[^>]*class="revision-form"[^>]*:disabled="revisionSaving"/)
  assert.match(view, /v-model="targetPlanDate"[\s\S]{0,180}:disabled="revisionSaving"/)
  assert.match(view, /class="timeline-version"[\s\S]{0,180}:disabled="revisionSaving"/)
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
    assert.equal(helpers.canEnableActionAlerts({ ...draft, data_quality_json: { status: 'ready', stale: false } }), true)
    assert.equal(helpers.canEnableActionAlerts({ ...draft, status: 'active' }), false)
    assert.equal(helpers.canEnableActionAlerts({ ...draft, data_quality_json: { status: 'missing' } }), false)
    assert.equal(helpers.canEnableActionAlerts({ ...draft, data_quality_json: { status: 'degraded' } }), false)
    for (const stale of [null, 0, 'false', 'yes', true]) {
      assert.equal(
        helpers.canEnableActionAlerts({ ...draft, data_quality_json: { status: 'ready', stale } }),
        false,
        `explicit dirty stale=${String(stale)} must fail closed`
      )
    }
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
    assert.equal(
      helpers.riskPermissionSummary({ trial: 10, confirmed: 30, hard_stop: 5, max_candidates: 3 }),
      '试错 10% · 确认上限 30% · 刚性止损 5% · 最多 3 只'
    )
    assert.equal(helpers.riskPermissionSummary({}), '-')
  })
})

test('radar presentation translates internal codes and JSON summaries into readable Chinese', async () => {
  await withFrontendModules(async server => {
    const helpers = await server.ssrLoadModule('/src/views/trading-playbook/presentation.ts')
    const row = {
      mode_key: 'alive_theme_snake_arbitrage',
      status: 'not_matched',
      role: 'summary',
      compacted: true,
      summary_counts: {
        scanned: 10,
        matched: 0,
        waiting: 0,
        manual_review: 0,
        not_matched: 10
      }
    }

    assert.equal(helpers.tradingModeLabel(row), '板块未死的蛇形套利')
    assert.equal(helpers.tradingModeLabel(row, { alive_theme_snake_arbitrage: '规则中文名' }), '规则中文名')
    assert.equal(helpers.radarStatusLabel(row.status), '未命中')
    assert.equal(helpers.radarStatusType(row.status), 'info')
    assert.equal(helpers.radarCandidateLabel(row), '全市场汇总')
    assert.equal(
      helpers.radarEvidenceSummary(row),
      '扫描 10 只 · 命中 0 · 等待确认 0 · 人工复核 0 · 未命中 10'
    )
    assert.equal(helpers.marketStateLabel('style', 'chaos_retreat'), '混沌退潮')
    assert.equal(helpers.marketStateLabel('window', 'decline'), '退潮期')
    assert.doesNotMatch(helpers.radarEvidenceSummary(row), /radar_summary|not_matched|[{}\[\]"]/)
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
        '8': { executed: false, execution_price: 8.8, quantity: 200, executed_time: '14:00:00', manual_note: '  放弃追高  ' }
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
        '8': { executed: false, manual_note: '放弃追高' }
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
