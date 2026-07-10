import { describe, it, expect } from 'vitest'
import { fmtInt, fmtDays, fmtPercent } from './stats'

describe('stats formatters', () => {
  describe('fmtInt', () => {
    it('renders an em dash for null', () => { expect(fmtInt(null)).toBe('—') })
    it('renders an em dash for undefined', () => { expect(fmtInt(undefined)).toBe('—') })
    it('renders an em dash for NaN', () => { expect(fmtInt(NaN)).toBe('—') })
    it('formats with locale grouping', () => { expect(fmtInt(1234)).toBe('1,234') })
    it('rounds to a whole number (no stray decimals mid count-up animation)', () => {
      expect(fmtInt(42.734)).toBe('43')
    })
    it('formats zero as 0, not an em dash', () => { expect(fmtInt(0)).toBe('0') })
  })

  describe('fmtDays', () => {
    it('renders an em dash for null', () => { expect(fmtDays(null)).toBe('—') })
    it('formats a plain integer', () => { expect(fmtDays(14)).toBe('14') })
  })

  describe('fmtPercent', () => {
    it('renders an em dash for null', () => { expect(fmtPercent(null)).toBe('—') })
    it('divides the API\'s already-x100 value back down for Intl percent style', () => {
      expect(fmtPercent(64.2)).toBe('64.2%')
    })
    it('unsigned mode has no explicit + on positive values', () => {
      expect(fmtPercent(12)).toBe('12%')
    })
    it('signed mode adds an explicit + on positive values', () => {
      expect(fmtPercent(12, 'en', { signed: true })).toBe('+12%')
    })
    it('signed mode keeps the - on negative values (not double-signed)', () => {
      expect(fmtPercent(-8.4, 'en', { signed: true })).toBe('-8.4%')
    })
    it('signed mode shows 0% for exactly zero (signDisplay: exceptZero)', () => {
      expect(fmtPercent(0, 'en', { signed: true })).toBe('0%')
    })
  })
})
