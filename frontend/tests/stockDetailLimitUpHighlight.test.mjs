import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

const source = readFileSync(new URL('../src/views/StockDetail.vue', import.meta.url), 'utf8')
const match = source.match(/function isHighlightedLimitUpPoint\(point: KlinePoint\): boolean \{([\s\S]*?)\n\}/)

assert.ok(match, 'StockDetail should define isHighlightedLimitUpPoint')

const body = match[1]
assert.match(
  body,
  /point\.is_limit_up/,
  'K-line limit-up markers should still use backend-confirmed limit-up points'
)
assert.match(
  body,
  /stockInfo\.value\.is_final_sealed/,
  'stock detail fallback marker should require a final sealed limit-up record'
)
assert.doesNotMatch(
  body,
  /point\.is_limit_up\s*\|\|\s*\(stockInfo\.value\.trade_date\s*&&\s*point\.date\s*===\s*stockInfo\.value\.trade_date\)/,
  'opened or broken records must not force a K-line 涨停 marker by trade_date alone'
)
