/* Redesign — non-monetary formatters, ported from the mockup's data.js.
 * Monetary values go through useCurrency().fmtPrice instead, so prices convert
 * with the selected currency. These helpers cover the rest. */

/* Self-hosted logo (absolute, like the mockup) so it resolves regardless of
 * the local backend having the PNGs. */
export const LOGO = (t) => 'https://ve-sign.com/logos/' + encodeURIComponent(t) + '.png'

export const dirClass = (v) => (v == null ? '' : v > 0 ? 'up' : v < 0 ? 'down' : '')

/* Live overlay: endpoints return last stored close + change_pct (vs prior close).
 * Given a live quote, derive prev-close and recompute the live price + change so
 * the UI is live intraday. Falls back to the stored values when no live quote. */
export function overlayLive(close, changePct, livePrice) {
  if (livePrice == null || !isFinite(livePrice)) return { price: close, change: changePct }
  if (changePct == null || close == null) return { price: livePrice, change: changePct }
  const prev = close / (1 + changePct / 100)
  if (!isFinite(prev) || prev === 0) return { price: livePrice, change: changePct }
  return { price: livePrice, change: (livePrice - prev) / prev * 100 }
}

/* Same idea for valuation rows: close + upside (vs analyst target). Live price
 * changes the upside; derive the target, recompute upside against the live price. */
export function overlayUpside(close, upside, livePrice) {
  if (livePrice == null || !isFinite(livePrice)) return { price: close, upside }
  if (upside == null || close == null) return { price: livePrice, upside }
  const target = close * (1 + upside / 100)
  return { price: livePrice, upside: livePrice > 0 ? (target - livePrice) / livePrice * 100 : upside }
}

export function num(n, opts = {}) {
  if (n == null || !isFinite(n)) return '—'
  const fd = opts.fd != null ? opts.fd : 2
  return Number(n).toLocaleString(undefined, {
    minimumFractionDigits: fd, maximumFractionDigits: fd,
  })
}

export function pct(n, opts = {}) {
  if (n == null || !isFinite(n)) return '—'
  const sign = n > 0 ? '+' : ''
  return `${sign}${Number(n).toFixed(opts.fd != null ? opts.fd : 2)}%`
}

export function dateFmt(iso) {
  if (!iso) return ''
  const d = new Date(iso.replace(' ', 'T'))
  if (isNaN(d)) return iso
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
}

export function ago(iso) {
  if (!iso) return ''
  const d = new Date(iso.replace(' ', 'T'))
  if (isNaN(d)) return ''
  const mins = Math.max(0, Math.round((Date.now() - d.getTime()) / 60000))
  if (mins < 1) return 'just now'
  if (mins < 60) return `${mins} min ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  return `${days}d ago`
}

/* SVG path from a list of values, for index-card sparklines.
 * Returns the `d` attribute; viewBox is 0 0 W H. */
export function spark(values, opts = {}) {
  const W = opts.width || 100
  const H = opts.height || 24
  if (!values || values.length < 2) return ''
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min || 1
  const stepX = W / (values.length - 1)
  return values.map((v, i) => {
    const x = (i * stepX).toFixed(2)
    const y = (H - ((v - min) / span) * H).toFixed(2)
    return (i === 0 ? 'M' : 'L') + x + ',' + y
  }).join(' ')
}
