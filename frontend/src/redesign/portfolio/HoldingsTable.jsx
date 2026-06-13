/* Holdings table — one row per ticker; click opens the SignalModal.
 * Columns + formatting mirror the production (ve-sign.com) holdings table:
 *   logo · Ticker · Company · M. Cap (B) · Qty · Avg Price · Invested ·
 *   <phase-aware price + live change> · Market value · Yield
 * Plus the redesign-only chevron (expand per-lot detail) and CSV export. */
import { useState, Fragment } from 'react'
import { useQuery } from '@tanstack/react-query'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useCurrency } from '../../context/CurrencyContext'
import { getWatchlists, getMarketStatus } from '../../api'
import AddHoldingForm from './AddHoldingForm'
import HoldingLots from './HoldingLots'

// 5-point company-health score rendered as filled/empty dots, color-coded by
// score to match the Signals page (1 red → 5 deep green).
const HEALTH_COLOR = { 1: '#ff4d5c', 2: '#c2660c', 3: '#ff9500', 4: '#00d97e', 5: '#0a8f54' }
const healthDots = (score) => {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  const c = HEALTH_COLOR[n] || '#6b7280'
  return [0, 1, 2, 3, 4].map(i => (
    <span key={i} className={'s' + (i < n ? '' : ' off')} style={i < n ? { background: c } : undefined} />
  ))
}

// Production format for Prediction/ML Score: arrow + absolute value, 1 decimal.
const arrowPct1 = (v) => (v == null ? '—' : `${v >= 0 ? '▲' : '▼'} ${Math.abs(v).toFixed(1)}%`)

export default function HoldingsTable({ rows, subhead }) {
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()

  const [expanded, setExpanded] = useState(() => new Set())
  const [adding, setAdding] = useState(false)
  const { data: watchlists } = useQuery({ queryKey: ['dd-watchlists'], queryFn: getWatchlists })
  const { data: mstat } = useQuery({ queryKey: ['market-status', 'US'], queryFn: () => getMarketStatus('US') })
  const toggle = (t) => setExpanded(prev => {
    const n = new Set(prev); n.has(t) ? n.delete(t) : n.add(t); return n
  })
  const COLS = 13  // chevron + the 12 data columns

  // Phase-aware price-column header (matches production: pre/post, else "Live Price").
  const phase = mstat?.phase
  const priceLabel = phase === 'pre' ? 'Pre-Market'
    : phase === 'post' ? 'Post-Market'
    : 'Live Price'
  const live = phase === 'regular' || phase === 'pre' || phase === 'post'

  return (
    <>
      <div className="section-h">
        <h2>Holdings</h2>
        <span className="sub">{subhead}</span>
        <a className="right" style={{ cursor: 'pointer' }} onClick={() => setAdding(a => !a)}>+ Add holding</a>
      </div>
      {adding && <AddHoldingForm watchlists={watchlists} onDone={() => setAdding(false)} />}
      <table className="data-table">
        <thead>
          <tr>
            <th style={{ width: 28 }}></th>
            <th>Ticker</th>
            <th>Company</th>
            <th className="r">Health</th>
            <th className="r">Prediction</th>
            <th className="r">ML Score</th>
            <th className="r">Qty</th>
            <th className="r">Avg Price</th>
            <th className="r">Last Price</th>
            <th className="r">{priceLabel}</th>
            <th className="r">Invested</th>
            <th className="r">Market value</th>
            <th className="r" style={{ paddingRight: 18 }}>Total P&amp;L</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => {
            const diff = (live && r.latest_close != null && r.prev_close != null)
              ? r.latest_close - r.prev_close : null
            // Prediction = analyst-target upside vs the LIVE price, recomputed
            // intraday like production's "Target" column (not the frozen
            // fair_value_upside, which is stale during a session).
            const upside = (r.target_mean_price != null && r.latest_close)
              ? (r.target_mean_price - r.latest_close) / r.latest_close * 100 : null
            const mlPct = r.prediction_score == null ? null : r.prediction_score * 100
            return (
            <Fragment key={r.ticker}>
              <tr onClick={() => open(r.ticker, r.company || '')}>
                <td style={{ width: 28 }}>
                  <span className="row-chevron" onClick={(e) => { e.stopPropagation(); toggle(r.ticker) }}>
                    {expanded.has(r.ticker) ? '▾' : '▸'}
                  </span>
                </td>
                <td>
                  <div className="ticker-cell">
                    <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
                    <span className="tk">{r.ticker}</span>
                  </div>
                </td>
                <td className="co-cell">{r.company || '—'}</td>
                <td className="r">{r.health_score == null ? '—' : <span className="health">{healthDots(r.health_score)}</span>}</td>
                <td className={'r ' + dirClass(upside)}>{arrowPct1(upside)}</td>
                <td className={'r ' + dirClass(mlPct)}>{arrowPct1(mlPct)}</td>
                <td className="r">{r.total_qty == null ? '—' : num(r.total_qty, { fd: 0 })}</td>
                <td className="r">{r.avg_price == null ? '—' : fmtPrice(r.avg_price)}</td>
                <td className="r">{r.last_close == null ? '—' : fmtPrice(r.last_close)}</td>
                <td className={'r' + (live ? '' : ' muted')}>
                  {!live ? <span className="muted">Closed</span>
                    : r.latest_close == null ? '—' : (
                    <>
                      <div>{fmtPrice(r.latest_close)}</div>
                      {diff != null && (
                        <div className={dirClass(diff)} style={{ fontSize: 11 }}>
                          {diff >= 0 ? '▲' : '▼'} {fmtPrice(Math.abs(diff))} ({Math.abs(r.day).toFixed(2)}%)
                        </div>
                      )}
                    </>
                  )}
                </td>
                <td className="r">{r.cost == null ? '—' : fmtPrice(r.cost)}</td>
                <td className="r">{r.value == null ? '—' : fmtPrice(r.value)}</td>
                <td className={'r ' + dirClass(r.pnl)} style={{ paddingRight: 18 }}>
                  {r.pnl == null ? '—' : (
                    <>
                      <div><strong>{r.pnl >= 0 ? '+' : '-'}{fmtPrice(Math.abs(r.pnl))}</strong></div>
                      <div style={{ fontSize: 11 }}>{pct(r.yld)}</div>
                    </>
                  )}
                </td>
              </tr>
              {expanded.has(r.ticker) && (
                <HoldingLots ticker={r.ticker} latestClose={r.latest_close} watchlists={watchlists} colSpan={COLS} />
              )}
            </Fragment>
            )
          })}
        </tbody>
      </table>
    </>
  )
}
