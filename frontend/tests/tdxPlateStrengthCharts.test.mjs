import test from 'node:test'
import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import ts from 'typescript'

const helperPath = resolve(import.meta.dirname, '../src/utils/tdxPlateStrength.ts')
const viewPath = resolve(import.meta.dirname, '../src/views/tdx/TdxPlateStrength.vue')

test('builds chart data from current rankings and historical strength', async () => {
  assert.ok(existsSync(helperPath), 'plate-strength chart model helper should exist')

  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  const payload = {
    items: [
      {
        plate_name: 'AI电源',
        strength_score: 98,
        limit_up_count: 3,
        sealed_count: 2,
        opened_count: 1
      },
      {
        plate_name: '机器人',
        strength_score: 75,
        limit_up_count: 2,
        sealed_count: 2,
        opened_count: 0
      }
    ],
    history: [
      {
        trade_date: '2026-05-27',
        items: [
          { plate_name: 'AI电源', strength_score: 60 },
          { plate_name: '机器人', strength_score: 80 }
        ]
      },
      {
        trade_date: '2026-05-28',
        items: [
          { plate_name: 'AI电源', strength_score: 98 },
          { plate_name: '机器人', strength_score: 75 }
        ]
      }
    ]
  }

  assert.deepEqual(helper.buildPlateStrengthChartData(payload), {
    strength: {
      names: ['机器人', 'AI电源'],
      scores: [75, 98]
    },
    breadth: {
      names: ['AI电源', '机器人'],
      sealed: [2, 2],
      opened: [1, 0]
    },
    rotation: {
      dates: ['05-27', '05-28'],
      series: [
        { name: 'AI电源', values: [60, 98] },
        { name: '机器人', values: [80, 75] }
      ]
    }
  })
})

test('normalizes custom history windows to the supported range', async () => {
  assert.ok(existsSync(helperPath), 'plate-strength chart model helper should exist')

  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.equal(helper.normalizePlateStrengthWindow(2), 5)
  assert.equal(helper.normalizePlateStrengthWindow(37), 37)
  assert.equal(helper.normalizePlateStrengthWindow(500), 120)
  assert.equal(helper.normalizePlateStrengthWindow(Number.NaN), 20)
})

test('keeps the compact chart selection valid when trend visibility changes', async () => {
  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.equal(helper.normalizeCompactChartMode('rotation', true), 'rotation')
  assert.equal(helper.normalizeCompactChartMode('rotation', false), 'strength')
  assert.equal(helper.normalizeCompactChartMode('breadth', false), 'breadth')
  assert.equal(helper.normalizeCompactChartMode('unknown', true), 'strength')
})

test('does not initialize a chart while its responsive container is hidden', async () => {
  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.equal(helper.hasRenderableChartSize(390, 168), true)
  assert.equal(helper.hasRenderableChartSize(0, 168), false)
  assert.equal(helper.hasRenderableChartSize(390, 0), false)
})

test('uses a chart-free bottom drawer layout only on narrow plugin widths', async () => {
  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.deepEqual(helper.resolvePlateStrengthLayout(390, 770), {
    showCharts: false,
    constituentPresentation: 'bottom-drawer',
    constituentDrawerHeight: 523
  })
  assert.deepEqual(helper.resolvePlateStrengthLayout(520, 770), {
    showCharts: false,
    constituentPresentation: 'bottom-drawer',
    constituentDrawerHeight: 523
  })
  assert.deepEqual(helper.resolvePlateStrengthLayout(521, 770), {
    showCharts: true,
    constituentPresentation: 'inline',
    constituentDrawerHeight: 523
  })
})

test('gives the narrow constituent drawer enough height without covering the whole viewport', async () => {
  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.equal(helper.resolvePlateStrengthLayout(390, 770).constituentDrawerHeight, 523)
  assert.equal(helper.resolvePlateStrengthLayout(390, 1000).constituentDrawerHeight, 560)
  assert.equal(helper.resolvePlateStrengthLayout(390, 240).constituentDrawerHeight, 200)
})

test('builds API parameters for each supported plate source', async () => {
  assert.ok(existsSync(helperPath), 'plate-strength chart model helper should exist')

  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.deepEqual(helper.buildPlateStrengthRequest('ths', 30), {
    source: 'ths',
    window_days: 30
  })
  assert.deepEqual(helper.buildPlateStrengthRequest('unknown', 2), {
    source: 'kpl',
    window_days: 5
  })
})

test('prevents overlapping polling while preserving an interactive reload', async () => {
  assert.ok(existsSync(helperPath), 'plate-strength chart model helper should exist')

  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)
  const gate = new helper.PlateStrengthRequestGate()

  assert.equal(gate.begin(false), 'start')
  assert.equal(gate.begin(true), 'skip')
  assert.equal(gate.begin(false), 'queue')
  assert.equal(gate.finish(), true)
  assert.equal(gate.begin(false), 'start')
  assert.equal(gate.finish(), false)
})

test('rejects a stale payload after the selected source or window changes', async () => {
  assert.ok(existsSync(helperPath), 'plate-strength chart model helper should exist')

  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)
  const payload = { source: 'kpl', window_days: 20 }

  assert.equal(helper.matchesPlateStrengthSelection(payload, 'kpl', 20), true)
  assert.equal(helper.matchesPlateStrengthSelection(payload, 'ths', 20), false)
  assert.equal(helper.matchesPlateStrengthSelection(payload, 'kpl', 30), false)
  assert.equal(helper.matchesPlateStrengthSelection(null, 'kpl', 20), false)
})

test('builds a plate constituent request and keeps only one expanded plate', async () => {
  assert.ok(existsSync(helperPath), 'plate-strength chart model helper should exist')

  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.deepEqual(helper.buildPlateConstituentRequest(' 房地产 ', 'ths'), {
    plate_name: '房地产',
    source: 'ths'
  })
  assert.equal(helper.nextExpandedPlate(null, '房地产'), '房地产')
  assert.equal(helper.nextExpandedPlate('房地产', '房地产'), null)
  assert.equal(helper.nextExpandedPlate('房地产', '半导体'), '半导体')
})

test('pages the constituent scroller without moving beyond either boundary', async () => {
  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.equal(helper.nextConstituentScrollTop(0, 200, 1000, 'down'), 170)
  assert.equal(helper.nextConstituentScrollTop(170, 200, 1000, 'up'), 0)
  assert.equal(helper.nextConstituentScrollTop(790, 200, 1000, 'down'), 800)
  assert.equal(helper.nextConstituentScrollTop(0, 200, 150, 'down'), 0)
})

test('shows the actual constituent membership provenance in the expanded panel', () => {
  const source = readFileSync(viewPath, 'utf8')

  assert.match(source, /source_note/)
  assert.match(source, /成分口径/)
})

test('maps intraday rank and role tags to distinct visual classes', async () => {
  const source = readFileSync(helperPath, 'utf8')
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2020
    }
  }).outputText
  const helper = await import(`data:text/javascript;charset=utf-8,${encodeURIComponent(transpiled)}`)

  assert.equal(helper.intradayTagClass({ label: '龙1', type: 'dragon' }), 'dragon-one')
  assert.equal(helper.intradayTagClass({ label: '龙5', type: 'dragon' }), 'dragon-other')
  assert.equal(helper.intradayTagClass({ label: '中军', type: 'core' }), 'role-core')
  assert.equal(helper.intradayTagClass({ label: '炸板', type: 'opened' }), 'role-opened')
})

test('shows realtime quote-only limit-up state before pool board metadata arrives', () => {
  const source = readFileSync(viewPath, 'utf8')

  assert.match(source, /v-else-if="stock\.is_limit_up"/)
  assert.match(source, />涨停封板</)
})
