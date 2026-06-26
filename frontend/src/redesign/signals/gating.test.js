import { describe, it, expect } from 'vitest'
import { isLocked, lockedCount, fmtCents, hasMoreLocked, tierOf, tierUnlockCents, allTiersCents, allTiersGrossCents } from './gating'

const RATES = { 1: 30, 2: 20, 3: 10 }

describe('tier pricing helpers', () => {
  it('buckets untiered rows as Promising (3)', () => {
    expect(tierOf({ tier: 1 })).toBe(1)
    expect(tierOf({ tier: 2 })).toBe(2)
    expect(tierOf({ tier: 3 })).toBe(3)
    expect(tierOf({ tier: null })).toBe(3)
    expect(tierOf({})).toBe(3)
  })

  it('prices a tier as rate × locked count', () => {
    expect(tierUnlockCents(2, 3, RATES)).toBe(60)
    expect(tierUnlockCents(3, 11, RATES)).toBe(110)
    expect(tierUnlockCents(1, 0, RATES)).toBe(0)
  })

  it('prices "all" as 40% off the sum, clamped to the priciest tier ($1.70 → $1.10)', () => {
    expect(allTiersCents({ 1: 0, 2: 3, 3: 11 }, RATES)).toBe(110)
  })

  it('grosses the undiscounted tier sum for the "was" anchor', () => {
    expect(allTiersGrossCents({ 1: 0, 2: 3, 3: 11 }, RATES)).toBe(170)
  })

  it('clamps "all" up to the priciest single tier', () => {
    expect(allTiersCents({ 1: 0, 2: 0, 3: 3 }, RATES)).toBe(30)
  })

  it('is zero when nothing is locked', () => {
    expect(allTiersCents({ 1: 0, 2: 0, 3: 0 }, RATES)).toBe(0)
  })
})

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
