import { describe, it, expect } from 'vitest'
import { tierOf, tierUnlockCents, allTiersCents } from './gating'

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

  it('prices "all" as 15% off the sum (today: $1.70 → $1.45)', () => {
    expect(allTiersCents({ 1: 0, 2: 3, 3: 11 }, RATES)).toBe(145)
  })

  it('clamps "all" up to the priciest single tier', () => {
    expect(allTiersCents({ 1: 0, 2: 0, 3: 3 }, RATES)).toBe(30)
  })

  it('is zero when nothing is locked', () => {
    expect(allTiersCents({ 1: 0, 2: 0, 3: 0 }, RATES)).toBe(0)
  })
})
