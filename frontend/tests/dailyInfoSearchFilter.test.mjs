import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { rmSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
import { resolve } from 'node:path'

const tempDir = '.tmp-daily-info-search'
const tscEntry = 'node_modules/typescript/bin/tsc'

try {
  rmSync(tempDir, { recursive: true, force: true })
  execFileSync(process.execPath, [
    tscEntry,
    'src/utils/dailyInfoSearch.ts',
    '--target',
    'ES2020',
    '--module',
    'ESNext',
    '--moduleResolution',
    'bundler',
    '--outDir',
    tempDir,
    '--noEmit',
    'false',
    '--declaration',
    'false',
    '--skipLibCheck',
    'true',
    '--strict',
    'true',
  ], { stdio: 'inherit' })

  const {
    dailyInfoContainsVisibleKeyword,
    filterVisibleDailyInfoSearchResults,
  } = await import(pathToFileURL(resolve(tempDir, 'dailyInfoSearch.js')).href)

  const hiddenRawDocumentMatch = {
    trade_date: '2026-06-15',
    summary: {
      overview: 'AI硬件和有色资源为主线',
      main_lines: ['PCB', 'MLCC'],
      catalysts: ['涨价扩散'],
      risks: ['高位分化'],
      plan: '等待承接',
      source_titles: ['今天的一些信息整理6.15'],
      mentioned_stocks: [{ name: '胜宏科技', sector: 'PCB', reason: '涨价' }],
    },
    sources: [{ title: '今天的一些信息整理6.15' }],
  }
  const visibleOverviewMatch = {
    trade_date: '2026-05-22',
    summary: {
      overview: '感光干膜和载体铜箔处于布局区',
      main_lines: [],
      catalysts: [],
      risks: [],
      plan: '',
      source_titles: [],
      mentioned_stocks: [],
    },
    sources: [],
  }
  const visibleStockMatch = {
    trade_date: '2026-04-28',
    summary: {
      overview: 'PCB上游材料活跃',
      main_lines: [],
      catalysts: [],
      risks: [],
      plan: '',
      source_titles: [],
      mentioned_stocks: [{ name: '容大感光', sector: '感光干膜', reason: '国产替代' }],
    },
    sources: [],
  }
  const visibleSourceTitleMatch = {
    trade_date: '2026-05-19',
    summary: {
      overview: '材料方向分化',
      main_lines: [],
      catalysts: [],
      risks: [],
      plan: '',
      source_titles: [],
      mentioned_stocks: [],
    },
    sources: [{ title: '感光干膜专题纪要' }],
  }

  const filtered = filterVisibleDailyInfoSearchResults([
    hiddenRawDocumentMatch,
    visibleOverviewMatch,
    visibleStockMatch,
    visibleSourceTitleMatch,
  ], '感光')

  assert.deepEqual(
    filtered.map(item => item.trade_date),
    ['2026-05-22', '2026-04-28', '2026-05-19'],
    'search should drop candidate dates whose visible daily info fields do not contain the keyword'
  )
  assert.equal(dailyInfoContainsVisibleKeyword(hiddenRawDocumentMatch, '感光'), false)
  assert.equal(dailyInfoContainsVisibleKeyword(visibleStockMatch, '容大感光'), true)

  console.log('daily info search filter checks passed')
} finally {
  rmSync(tempDir, { recursive: true, force: true })
}
