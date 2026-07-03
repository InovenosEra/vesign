import { describe, it, expect } from 'vitest'
import { buildCards, filterCards, sortCards } from './watchlistDerive'

const baseArgs = () => ({
  lists: [{ id: 1, name: 'Core Tech' }, { id: 2, name: 'Growth' }],
  tickersByList: {
    1: [
      { ticker: 'NVDA', note: '', target_price: 220 },
      { ticker: 'TSLA', note: '', target_price: null },
    ],
    2: [{ ticker: 'GOOGL', note: '', target_price: 300 }],
  },
  signalsByTicker: {
    NVDA: { ticker: 'NVDA', company: 'NVIDIA', close: 200, fair_value_upside: 0.10, signal: 'BUY', health_score: 4 },
    TSLA: { ticker: 'TSLA', company: 'Tesla', close: 250, fair_value_upside: 0.05, signal: 'HOLD', health_score: 3 },
    GOOGL: { ticker: 'GOOGL', company: 'Alphabet', close: 175, fair_value_upside: 0.20, signal: 'BUY', health_score: 5 },
  },
  prices: { NVDA: 220, TSLA: 245, GOOGL: 175 },
})

describe('buildCards', () => {
  it('carries price/day-change/analyst-upside/signal/health/target for every row, no ownership', () => {
    const [core] = buildCards(baseArgs())
    const nvda = core.rows.find(r => r.ticker === 'NVDA')
    expect(nvda.owned).toBeUndefined()
    expect(nvda.price).toBe(220)          // live price wins
    expect(nvda.targetPrice).toBe(220)
    expect(nvda.signal).toBe('BUY')
    expect(nvda.healthScore).toBe(4)
    expect(nvda.upsidePct).not.toBeNull()  // overlaid vs the live price
  })

  it('carries list metadata and a null target_price as null targetPrice', () => {
    const cards = buildCards(baseArgs())
    expect(cards.map(c => c.name)).toEqual(['Core Tech', 'Growth'])
    expect(cards[0].tickerCount).toBe(2)
    const tsla = cards[0].rows.find(r => r.ticker === 'TSLA')
    expect(tsla.targetPrice).toBeNull()
  })

  it('computes signalMix, avgHealth, biggestUpside, and nearTargetCount per card', () => {
    const [core] = buildCards(baseArgs())
    expect(core.signalMix).toEqual({ BUY: 1, HOLD: 1, SELL: 0 })
    expect(core.avgHealth).toBeCloseTo(3.5, 5)     // (4+3)/2
    expect(core.biggestUpside.ticker).toBe('TSLA') // higher upside than NVDA once overlaid on live price
    // NVDA's live price (220) already sits AT its target (220) -> within any
    // positive threshold; TSLA has no target -> only NVDA counts.
    expect(core.nearTargetCount).toBe(1)
  })

  it('computes avgUpside per card from each row\'s upsidePct', () => {
    const cards = buildCards(baseArgs())
    expect(cards[1].rows[0].ticker).toBe('GOOGL')
    expect(cards[1].avgUpside).not.toBeNull()
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
  it('sorts descending by avg upside by default', () => {
    const cards = buildCards(baseArgs())
    const sorted = sortCards(cards, 'desc')
    expect(sorted[0].avgUpside).toBeGreaterThanOrEqual(sorted[1].avgUpside)
  })

  it('sorts ascending when asked', () => {
    const cards = buildCards(baseArgs())
    const sorted = sortCards(cards, 'asc')
    expect(sorted[0].avgUpside).toBeLessThanOrEqual(sorted[1].avgUpside)
  })

  it('treats a null avgUpside as the lowest value', () => {
    const args = baseArgs()
    args.signalsByTicker.GOOGL.fair_value_upside = null
    args.signalsByTicker.GOOGL.close = null
    const cards = sortCards(buildCards(args), 'desc')
    expect(cards[cards.length - 1].name).toBe('Growth')
  })
})
