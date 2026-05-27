import { describe, it, expect } from 'vitest'
import { isLocked, lockedCount, fmtCents, hasMoreLocked } from './gating'

describe('gating helpers', () => {
  it('isLocked reflects the row flag', () => {
    expect(isLocked({ locked: true })).toBe(true)
    expect(isLocked({ ticker: 'AAPL' })).toBe(false)
  })
  it('lockedCount counts locked rows', () => {
    expect(lockedCount([{ locked: true }, { ticker: 'A' }, { locked: true }])).toBe(2)
  })
  it('fmtCents renders dollars', () => {
    expect(fmtCents(10)).toBe('$0.10')
    expect(fmtCents(50)).toBe('$0.50')
    expect(fmtCents(250)).toBe('$2.50')
  })
  it('hasMoreLocked is true when any payable lock exists', () => {
    expect(hasMoreLocked([{ locked: true, reason: 'pay' }])).toBe(true)
    expect(hasMoreLocked([{ locked: true, reason: 'upgrade' }])).toBe(false)
    expect(hasMoreLocked([{ ticker: 'A' }])).toBe(false)
  })
})
