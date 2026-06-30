import { describe, it, expect } from 'vitest'
import { concentration, signalMix, avgHealthWeighted, topUpside, weakestHealth, computeRows, buildBridge } from './derive'

const H = (o) => ({ ticker: 'X', total_qty: 1, total_cost: 100, latest_close: 100, prev_close: 100, ...o })

describe('derive', () => {
  it('computeRows adds weight %', () => {
    const { rows, totals } = computeRows([
      H({ ticker: 'A', total_qty: 1, latest_close: 100, total_cost: 50 }),
      H({ ticker: 'B', total_qty: 1, latest_close: 300, total_cost: 50 }),
    ])
    const a = rows.find(r => r.ticker === 'A')
    expect(Math.round(a.weight)).toBe(25)         // 100 / 400
    expect(totals.totalValue).toBe(400)
  })
  it('concentration classifies by top-5 weight', () => {
    const rows = [H({ value: 80 }), H({ value: 20 })].map((r, i) => ({ ...r, ticker: 'T' + i, value: r.value }))
    const c = concentration(rows, 100)
    expect(c.label).toBe('Concentrated')         // top5 = 100% ≥ 70
    expect(Math.round(c.topPct)).toBe(80)
    expect(c.positions).toBe(2)
  })
  it('signalMix counts signals', () => {
    expect(signalMix([H({ signal: 'BUY' }), H({ signal: 'BUY' }), H({ signal: 'SELL' }), H({ signal: null })]))
      .toEqual({ BUY: 2, HOLD: 0, SELL: 1 })
  })
  it('avgHealthWeighted weights by value', () => {
    const v = avgHealthWeighted([H({ value: 300, health_score: 5 }), H({ value: 100, health_score: 1 })], 400)
    expect(v).toBeCloseTo(4)                       // (5*300 + 1*100)/400
  })
  it('topUpside picks max analyst upside', () => {
    const t = topUpside([H({ ticker: 'A', latest_close: 100, target_mean_price: 110 }),
                         H({ ticker: 'B', latest_close: 100, target_mean_price: 150 })])
    expect(t.ticker).toBe('B'); expect(Math.round(t.upside)).toBe(50)
  })
  it('weakestHealth picks min score', () => {
    expect(weakestHealth([H({ ticker: 'A', health_score: 4 }), H({ ticker: 'B', health_score: 2 })]).ticker).toBe('B')
  })
  it('buildBridge bridges cost to value via per-holding steps', () => {
    const { rows, totals } = computeRows([
      H({ ticker: 'A', total_qty: 1, latest_close: 150, total_cost: 100 }),  // +50 gain
      H({ ticker: 'B', total_qty: 1, latest_close: 80, total_cost: 100 }),   // -20 loss
    ])
    const { cost, value, segs, peak } = buildBridge(rows, totals)
    expect(cost).toBe(200)
    expect(value).toBe(230)
    expect(segs.map(s => s.ticker)).toEqual(['A', 'B'])     // gainer steps first
    expect(segs[0].kind).toBe('gain')
    expect([segs[0].from, segs[0].to]).toEqual([200, 250])
    expect([segs[1].from, segs[1].to]).toEqual([250, 230])  // loss steps back down
    expect(peak).toBe(250)                                   // running total peaks before the loss
  })
})
