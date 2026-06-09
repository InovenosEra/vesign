/* Holdings table — one row per ticker; click opens the SignalModal.
 * Columns mirror the production (ve-sign.com) holdings table:
 *   logo · Ticker · Company · Market cap · Shares · Avg cost · Invested ·
 *   <phase-aware price> · Current value · Yield%
 * Plus the redesign-only chevron (expand per-lot detail) and CSV export. */
import { useState, Fragment } from 'react'
import { useQuery } from '@tanstack/react-query'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useCurrency } from '../../context/CurrencyContext'
import { getWatchlists, getMarketStatus } from '../../api'
import AddHoldingForm from './AddHoldingForm'
import HoldingLots from './HoldingLots'

const capB = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'

const csvCell = (v) => {
  const s = v == null ? '' : String(v)
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s
}

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
  const COLS = 10  // chevron + the 9 data columns

  // Phase-aware label for the price column (matches production + the app header).
  const phase = mstat?.phase
  const priceLabel = phase === 'pre' ? 'Pre-market'
    : phase === 'post' ? 'Post-market'
    : phase === 'regular' ? 'Live price'
    : 'Last close'

  const exportCsv = () => {
    const header = ['Ticker', 'Company', 'Market cap', 'Shares', 'Avg cost', 'Invested', priceLabel, 'Current value', 'Yield %']
    const lines = [header.join(',')]
    for (const r of rows) {
      lines.push([r.ticker, r.company || '', r.market_cap, r.total_qty, r.avg_price,
        r.cost, r.latest_close, r.value, r.yld].map(csvCell).join(','))
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = `vesign-holdings-${rows.length}.csv`
    document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url)
  }

  return (
    <>
      <div className="section-h">
        <h2>Holdings</h2>
        <span className="sub">{subhead}</span>
        <a className="right" style={{ cursor: 'pointer' }} onClick={() => setAdding(a => !a)}>+ Add holding</a>
        <a className="right" style={{ cursor: 'pointer', marginRight: 12 }} onClick={exportCsv}>Export CSV →</a>
      </div>
      {adding && <AddHoldingForm watchlists={watchlists} onDone={() => setAdding(false)} />}
      <table className="data-table">
        <thead>
          <tr>
            <th style={{ width: 28 }}></th>
            <th>Ticker</th>
            <th>Company</th>
            <th className="r">Market cap</th>
            <th className="r">Shares</th>
            <th className="r">Avg cost</th>
            <th className="r">Invested</th>
            <th className="r">{priceLabel}</th>
            <th className="r">Current value</th>
            <th className="r" style={{ paddingRight: 18 }}>Yield</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
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
                <td className="r muted">{capB(r.market_cap)}</td>
                <td className="r">{r.total_qty == null ? '—' : num(r.total_qty, { fd: 2 })}</td>
                <td className="r">{r.avg_price == null ? '—' : fmtPrice(r.avg_price)}</td>
                <td className="r muted">{r.cost == null ? '—' : fmtPrice(r.cost)}</td>
                <td className="r">{r.latest_close == null ? '—' : fmtPrice(r.latest_close)}</td>
                <td className="r">{r.value == null ? '—' : fmtPrice(r.value)}</td>
                <td className={'r ' + dirClass(r.yld)} style={{ paddingRight: 18 }}><strong>{pct(r.yld)}</strong></td>
              </tr>
              {expanded.has(r.ticker) && (
                <HoldingLots ticker={r.ticker} latestClose={r.latest_close} watchlists={watchlists} colSpan={COLS} />
              )}
            </Fragment>
          ))}
        </tbody>
      </table>
    </>
  )
}
