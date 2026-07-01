import { describe, it, expect } from 'vitest'
import { buildCards, filterCards, sortCards } from './watchlistDerive'

const baseArgs = () => ({
  lists: [{ id: 1, name: 'Core Tech' }, { id: 2, name: 'Growth' }],
  tickersByList: {
    1: [{ ticker: 'NVDA', note: '' }, { ticker: 'TSLA', note: '' }],
    2: [{ ticker: 'GOOGL', note: '' }],
  },
  holdingsByList: {
    1: [{ id: 10, ticker: 'NVDA', quantity: 2, buy_price: 100, buy_date: '2026-01-01' }],
    2: [],
  },
  signalsByTicker: {
    NVDA: { ticker: 'NVDA', company: 'NVIDIA', close: 200, fair_value_upside: null },
    TSLA: { ticker: 'TSLA', company: 'Tesla', close: 250, fair_value_upside: 0.10 },
    GOOGL: { ticker: 'GOOGL', company: 'Alphabet', close: 175, fair_value_upside: 0.05 },
  },
  prices: { NVDA: 220, TSLA: 245, GOOGL: 175 },
  comparisonByName: { 'Core Tech': 42.18, 'Growth': 28.04 },
})

describe('buildCards', () => {
  it('marks a ticker with lots as owned and computes cost/pnl/yield', () => {
    const [core] = buildCards(baseArgs())
    const nvda = core.rows.find(r => r.ticker === 'NVDA')
    expect(nvda.owned).toBe(true)
    expect(nvda.lotCount).toBe(1)
    expect(nvda.costBasis).toBe(200)          // 2 * 100
    expect(nvda.price).toBe(220)               // live price wins
    expect(nvda.pnlAbs).toBe(240)              // 2*220 - 200
    expect(Math.round(nvda.yieldPct)).toBe(120) // 240/200*100
    expect(nvda.upsidePct).toBeNull()
  })

  it('marks a ticker with no lots as watch-only and reports analyst upside', () => {
    const [core] = buildCards(baseArgs())
    const tsla = core.rows.find(r => r.ticker === 'TSLA')
    expect(tsla.owned).toBe(false)
    expect(tsla.lotCount).toBe(0)
    expect(tsla.costBasis).toBeNull()
    expect(tsla.pnlAbs).toBeNull()
    expect(tsla.upsidePct).not.toBeNull()      // overlaid vs the live price
  })

  it('carries list metadata and the aggregate yield from the comparison map', () => {
    const cards = buildCards(baseArgs())
    expect(cards.map(c => c.name)).toEqual(['Core Tech', 'Growth'])
    expect(cards[0].tickerCount).toBe(2)
    expect(cards[0].aggregateYield).toBe(42.18)
    expect(cards[1].rows[0].ticker).toBe('GOOGL')
  })

  it('falls back to null aggregateYield when the list has no comparison entry', () => {
    const args = baseArgs()
    args.comparisonByName = {}
    const [core] = buildCards(args)
    expect(core.aggregateYield).toBeNull()
  })
})

describe('filterCards', () => {
  it('matches by list name', () => {
    const cards = buildCards(baseArgs())
    expect(filterCards(cards, 'growth').map(c => c.name)).toEqual(['Growth'])
  })

  it('matches by member ticker symbol', () => {
    const cards = buildCards(baseArgs())
    expect(filterCards(cards, 'nvda').map(c => c.name)).toEqual(['Core Tech'])
  })

  it('returns all cards for an empty query', () => {
    const cards = buildCards(baseArgs())
    expect(filterCards(cards, '  ')).toHaveLength(2)
  })
})

describe('sortCards', () => {
  it('sorts descending by aggregate yield by default', () => {
    const cards = buildCards(baseArgs())
    expect(sortCards(cards, 'desc').map(c => c.name)).toEqual(['Core Tech', 'Growth'])
  })

  it('sorts ascending when asked', () => {
    const cards = buildCards(baseArgs())
    expect(sortCards(cards, 'asc').map(c => c.name)).toEqual(['Growth', 'Core Tech'])
  })

  it('treats a null aggregateYield as the lowest value', () => {
    const args = baseArgs()
    args.comparisonByName = { 'Growth': 5 }   // Core Tech has no entry -> null
    const cards = buildCards(args)
    expect(sortCards(cards, 'desc').map(c => c.name)).toEqual(['Growth', 'Core Tech'])
  })
})
