/* Pure portfolio derivations — kept out of the page component so they're unit
 * testable. computeRows mirrors the prior in-page logic, plus a per-row weight %.
 * The rest power the allocation/concentration readout and the "Vesign's Read" panel. */

// One row per holding with derived value/cost/pnl/yld/day/weight. Plus portfolio
// totals and the best/worst/largest summaries used by the net-worth hero.
export function computeRows(holdings) {
  const rows = holdings.map(h => {
    const value = (h.total_qty != null && h.latest_close != null) ? h.total_qty * h.latest_close : null
    const cost = h.total_cost
    const pnl = (value != null && cost != null) ? value - cost : null
    const yld = (pnl != null && cost) ? pnl / cost * 100 : null
    const day = (h.latest_close != null && h.prev_close) ? (h.latest_close - h.prev_close) / h.prev_close * 100 : null
    return { ...h, value, cost, pnl, yld, day }
  }).sort((a, b) => a.ticker.localeCompare(b.ticker))

  const totalCost = rows.reduce((s, r) => s + (r.cost || 0), 0)
  const totalValue = rows.reduce((s, r) => s + (r.value || 0), 0)
  const totalPnl = totalValue - totalCost
  const totalYld = totalCost ? totalPnl / totalCost * 100 : 0
  const todayDelta = rows.reduce((s, r) =>
    s + ((r.latest_close != null && r.prev_close != null) ? (r.latest_close - r.prev_close) * (r.total_qty || 0) : 0), 0)
  const todayPct = (totalValue - todayDelta) ? todayDelta / (totalValue - todayDelta) * 100 : 0

  // Per-row weight % of total value (second pass — needs totalValue).
  rows.forEach(r => { r.weight = (totalValue && r.value != null) ? r.value / totalValue * 100 : null })

  const best = rows.slice().sort((a, b) => (b.yld || -1e9) - (a.yld || -1e9))[0]
  const worst = rows.slice().sort((a, b) => (a.yld || 1e9) - (b.yld || 1e9))[0]
  const largestRow = rows.length
    ? rows.reduce((a, b) => ((b.value || 0) > (a.value || 0) ? b : a))
    : null
  const largest = largestRow
    ? { ...largestRow, pctOfValue: totalValue ? largestRow.value / totalValue * 100 : 0 }
    : null

  return {
    rows,
    totals: { totalCost, totalValue, totalPnl, totalYld, todayDelta, todayPct, count: rows.length },
    best, worst, largest,
  }
}

// Concentration / diversification read. label by top-5 weight: ≥70 Concentrated,
// ≥40 Balanced, else Diversified.
export function concentration(rows, totalValue) {
  const valued = rows.filter(r => r.value).slice().sort((a, b) => b.value - a.value)
  const tv = totalValue || valued.reduce((s, r) => s + r.value, 0)
  if (!valued.length || !tv) {
    return { topPct: 0, top5Pct: 0, positions: rows.length, topSectorPct: 0, label: 'Diversified' }
  }
  const topPct = valued[0].value / tv * 100
  const top5Pct = valued.slice(0, 5).reduce((s, r) => s + r.value, 0) / tv * 100
  const bySector = new Map()
  valued.forEach(r => {
    const k = r.sector || 'Other'
    bySector.set(k, (bySector.get(k) || 0) + r.value)
  })
  const topSectorPct = Math.max(...bySector.values()) / tv * 100
  const label = top5Pct >= 70 ? 'Concentrated' : top5Pct >= 40 ? 'Balanced' : 'Diversified'
  return { topPct, top5Pct, positions: valued.length, topSectorPct, label }
}

// Counts of current BUY/HOLD/SELL across holdings (unrated holdings ignored).
export function signalMix(rows) {
  const out = { BUY: 0, HOLD: 0, SELL: 0 }
  rows.forEach(r => { if (r.signal && out[r.signal] != null) out[r.signal] += 1 })
  return out
}

// Value-weighted mean company-health score across holdings that have one.
export function avgHealthWeighted(rows) {
  let num = 0, den = 0
  rows.forEach(r => {
    if (r.health_score != null && r.value) { num += r.health_score * r.value; den += r.value }
  })
  return den ? num / den : null
}

// Holding with the largest analyst-target upside vs its live price.
export function topUpside(rows) {
  let best = null
  rows.forEach(r => {
    if (r.target_mean_price != null && r.latest_close) {
      const upside = (r.target_mean_price - r.latest_close) / r.latest_close * 100
      if (best == null || upside > best.upside) best = { ticker: r.ticker, upside }
    }
  })
  return best
}

// Holding with the lowest company-health score (the watch-out).
export function weakestHealth(rows) {
  let worst = null
  rows.forEach(r => {
    if (r.health_score != null && (worst == null || r.health_score < worst.health_score)) worst = r
  })
  return worst
}
