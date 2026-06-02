import { describe, it, expect } from 'vitest'
import { validateHolding } from './holdingForm'

describe('validateHolding', () => {
  const ok = { ticker: 'AAPL', shares: '10', price: '100', date: '2026-01-01' }
  it('accepts valid input', () => expect(validateHolding(ok)).toBe(null))
  it('requires a ticker', () => expect(validateHolding({ ...ok, ticker: '' })).toMatch(/ticker/i))
  it('requires shares > 0', () => expect(validateHolding({ ...ok, shares: '0' })).toMatch(/shares/i))
  it('rejects negative price', () => expect(validateHolding({ ...ok, price: '-1' })).toMatch(/price/i))
  it('rejects future date', () => expect(validateHolding({ ...ok, date: '2999-01-01' })).toMatch(/future/i))
})
