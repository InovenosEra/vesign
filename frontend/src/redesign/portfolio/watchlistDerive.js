import { overlayLive, overlayUpside } from '../fmt'
import { signalMix as computeSignalMix } from './derive'

const NEAR_TARGET_PCT = 5   // "within 5% of target" — see design spec

// One view-model per watchlist. No ownership concept at all: every row is
// forward-looking (price + day change + analyst upside + Vesign signal/health
// + a user-set target price). Rows never carry cost/P&L — that's Holdings'
// job, and Holdings is a fully independent, user-scoped concept now.
export function buildCards({ lists, tickersByList, signalsByTicker, prices }) {
  return lists.map(list => {
    const members = tickersByList[list.id] || []

    const rows = members.map(({ ticker, target_price }) => {
      const sig = signalsByTicker[ticker] || {}
      const close = sig.close ?? null
      const livePrice = prices[ticker] ?? null
      const { price, change: dayPct } = overlayLive(close, null, livePrice)

      let upsidePct = null
      if (sig.fair_value_upside != null && close != null) {
        upsidePct = overlayUpside(close, sig.fair_value_upside * 100, livePrice).upside
      }

      return {
        ticker, company: sig.company ?? null, price, dayPct, upsidePct,
        signal: sig.signal ?? null, healthScore: sig.health_score ?? null,
        targetPrice: target_price ?? null,
      }
    })

    const upsideVals = rows.map(r => r.upsidePct).filter(v => v != null)
    const avgUpside = upsideVals.length ? upsideVals.reduce((s, v) => s + v, 0) / upsideVals.length : null

    const healthVals = rows.map(r => r.healthScore).filter(v => v != null)
    const avgHealth = healthVals.length ? healthVals.reduce((s, v) => s + v, 0) / healthVals.length : null

    let biggestUpside = null
    rows.forEach(r => {
      if (r.upsidePct != null && (biggestUpside == null || r.upsidePct > biggestUpside.upside)) {
        biggestUpside = { ticker: r.ticker, upside: r.upsidePct }
      }
    })

    const nearTargetCount = rows.filter(r =>
      r.targetPrice != null && r.price != null &&
      Math.abs((r.price - r.targetPrice) / r.targetPrice) * 100 <= NEAR_TARGET_PCT
    ).length

    return {
      id: list.id,
      name: list.name,
      tickerCount: rows.length,
      avgUpside,
      signalMix: computeSignalMix(rows.map(r => ({ signal: r.signal }))),
      avgHealth,
      biggestUpside,
      nearTargetCount,
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
    const av = a.avgUpside ?? -Infinity
    const bv = b.avgUpside ?? -Infinity
    return dir === 'desc' ? bv - av : av - bv
  })
}
