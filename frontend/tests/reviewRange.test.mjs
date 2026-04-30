import test from 'node:test'
import assert from 'node:assert/strict'
import { buildReviewRange } from '../.tmp-review-range/reviewRange.js'

test('builds trading-session query for every quick review range', () => {
  const today = '2026-04-30'

  for (const [range, days] of [
    ['7', 7],
    ['30', 30],
    ['90', 90]
  ]) {
    const result = buildReviewRange(range, today)

    assert.equal(result.endDate, today)
    assert.equal(result.startDate, today)
    assert.deepEqual(result.query, {
      days,
      end_date: today
    })
  }
})
