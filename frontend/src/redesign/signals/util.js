/* Shared helpers for the Signals page sections. */
import { WHITE_BG_LOGOS } from '../../api'

/* Last-12-months trade window, ISO yyyy-mm-dd (matches the mockup's
 * fetchHistoricalTrades('US') range used elsewhere in production). */
export function tradeWindow() {
  const now = new Date()
  const end = now.toISOString().slice(0, 10)
  const ago = new Date(now)
  ago.setFullYear(ago.getFullYear() - 1)
  const start = ago.toISOString().slice(0, 10)
  return { start, end }
}

/* Class for the mini logo — white-bg padding for the allowlisted tickers. */
export const logoCls = (t) => 'logo-mini' + (WHITE_BG_LOGOS.has(t) ? ' white-bg' : '')

/* Short month/day/year — mirrors the mockup's ymd(). */
export function ymd(s) {
  if (!s) return '—'
  const d = new Date(String(s).replace(' ', 'T'))
  return isNaN(d) ? s : d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
}
