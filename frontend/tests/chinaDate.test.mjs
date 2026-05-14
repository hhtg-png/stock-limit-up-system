import test from 'node:test'
import assert from 'node:assert/strict'
import { getChinaDateString } from '../.tmp-china-date/chinaDate.js'

test('returns Asia Shanghai date when the browser local date is still previous day', () => {
  const usEveningDuringChinaOpen = new Date('2026-05-13T21:35:00-04:00')

  assert.equal(getChinaDateString(usEveningDuringChinaOpen), '2026-05-14')
})

test('returns zero padded China calendar date', () => {
  const chinaMorning = new Date('2026-01-02T01:05:00.000Z')

  assert.equal(getChinaDateString(chinaMorning), '2026-01-02')
})
