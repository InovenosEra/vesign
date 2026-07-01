import { overlayLive, overlayUpside } from '../fmt'

// One view-model per watchlist: each row is either "owned" (has lots in
// THIS list, so it carries cost/pnl/yield) or "watch-only" (no lots, so it
// carries the analyst upside instead) — a ticker can be owned in one list
// and watch-only in another, since lots are attributed per watchlist_id.
export function buildCards({ lists, tickersByList, holdingsByList, signalsByTicker, prices, comparisonByName }) {
  return lists.map(list => {
    const members = tickersByList[list.id] || []
    const lots = holdingsByList[list.id] || []
    const lotsByTicker = {}
    for (const l of lots) (lotsByTicker[l.ticker] ??= []).push(l)

    const rows = members.map(({ ticker }) => {
      const sig = signalsByTicker[ticker] || {}
      const close = sig.close ?? null
      const livePrice = prices[ticker] ?? null
      const { price, change: dayPct } = overlayLive(close, null, livePrice)

      const myLots = lotsByTicker[ticker] || []
      const owned = myLots.length > 0
      let costBasis = null, pnlAbs = null, yieldPct = null
      if (owned) {
        const qty = myLots.reduce((s, l) => s + l.quantity, 0)
        costBasis = myLots.reduce((s, l) => s + l.quantity * l.buy_price, 0)
        if (price != null) {
          pnlAbs = qty * price - costBasis
          yieldPct = costBasis ? (pnlAbs / costBasis) * 100 : null
        }
      }

      let upsidePct = null
      if (!owned && sig.fair_value_upside != null && close != null) {
        upsidePct = overlayUpside(close, sig.fair_value_upside * 100, livePrice).upside
      }

      return {
        ticker, company: sig.company ?? null, price, dayPct,
        owned, lotCount: myLots.length, costBasis, pnlAbs, yieldPct, upsidePct,
      }
    })

    return {
      id: list.id,
      name: list.name,
      tickerCount: rows.length,
      aggregateYield: comparisonByName[list.name] ?? null,
      rows,
    }
  })
}

export function filterCards(cards, query) {
  const q = (query || '').trim().toLowerCase()
  if (!q) return cards
  return cards.filter(c =>
    c.name.toLowerCase().includes(q) || c.rows.some(r => r.ticker.toLowerCase().includes(q))
  )
}

export function sortCards(cards, dir = 'desc') {
  return cards.slice().sort((a, b) => {
    const av = a.aggregateYield ?? -Infinity
    const bv = b.aggregateYield ?? -Infinity
    return dir === 'desc' ? bv - av : av - bv
  })
}
