/* Holdings table — the vault's ledger. One row per ticker, click opens the modal.
 * Sortable + searchable; each row carries position math (weight %, invested,
 * value, P&L, day change) and the model's read (signal pill, health, analyst
 * upside, ML). DCA lots expand per row; export to CSV/XLSX/ZIP. */
import { useState, useMemo, Fragment } from 'react'
import { useQuery } from '@tanstack/react-query'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useCurrency } from '../../context/CurrencyContext'
import { getWatchlists, getMarketStatus } from '../../api'
import DownloadButton from '../../components/DownloadButton'
import AddHoldingForm from './AddHoldingForm'
import HoldingLots from './HoldingLots'

const HEALTH_COLOR = { 1: '#ff4d5c', 2: '#c2660c', 3: '#ff9500', 4: '#00d97e', 5: '#0a8f54' }
const healthDots = (score) => {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  const c = HEALTH_COLOR[n] || '#6b7280'
  return [0, 1, 2, 3, 4].map(i => (
    <span key={i} className={'s' + (i < n ? '' : ' off')} style={i < n ? { background: c } : undefined} />
  ))
}
const arrowPct1 = (v) => (v == null ? '—' : `${v >= 0 ? '▲' : '▼'} ${Math.abs(v).toFixed(1)}%`)
const capSub = (mc) => mc == null ? null
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(1) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'

// Analyst-target upside vs the live price (recomputed intraday like production).
const upsideOf = (r) => (r.target_mean_price != null && r.latest_close)
  ? (r.target_mean_price - r.latest_close) / r.latest_close * 100 : null
const mlPctOf = (r) => (r.prediction_score == null ? null : r.prediction_score * 100)

// Value a row sorts by for a given column key (handles the computed columns).
const sortVal = (r, key) => key === 'upside' ? upsideOf(r)
  : key === 'mlPct' ? mlPctOf(r)
  : key === 'price' ? (r.latest_close ?? r.last_close)
  : r[key]

function cmpBy(a, b, key, dir) {
  const va = sortVal(a, key), vb = sortVal(b, key)
  if (va == null && vb == null) return 0
  if (va == null) return 1
  if (vb == null) return -1
  const c = (typeof va === 'number' && typeof vb === 'number') ? va - vb : String(va).localeCompare(String(vb))
  return dir === 'asc' ? c : -c
}

// Sortable header cell.
function Th({ label, col, sort, onSort, className, style }) {
  const active = sort.key === col
  return (
    <th onClick={() => onSort(col)} className={'sortable' + (className ? ' ' + className : '')} style={style}>
      {label}{active ? <span className="sort-ar">{sort.dir === 'asc' ? '▲' : '▼'}</span> : null}
    </th>
  )
}

const SearchIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="11" cy="11" r="7" /><path d="M21 21l-4.3-4.3" />
  </svg>
)

const SIG_CLS = { BUY: 'buy', HOLD: 'hold', SELL: 'sell' }

export default function HoldingsTable({ rows, subhead }) {
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()
  const [expanded, setExpanded] = useState(() => new Set())
  const [adding, setAdding] = useState(false)
  const [search, setSearch] = useState('')
  const [sort, setSort] = useState({ key: 'value', dir: 'desc' })   // default: biggest positions first
  const { data: watchlists } = useQuery({ queryKey: ['dd-watchlists'], queryFn: getWatchlists })
  const { data: mstat } = useQuery({ queryKey: ['market-status', 'US'], queryFn: () => getMarketStatus('US') })
  const toggle = (t) => setExpanded(prev => { const n = new Set(prev); n.has(t) ? n.delete(t) : n.add(t); return n })
  const toggleSort = (key) => setSort(s => (s.key === key ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'desc' }))
  const COLS = 13

  const phase = mstat?.phase
  const live = phase === 'regular' || phase === 'pre' || phase === 'post'
  const priceLabel = !live ? 'Last Price' : phase === 'pre' ? 'Pre-Market' : phase === 'post' ? 'Post-Market' : 'Live Price'

  const q = search.trim().toLowerCase()
  const view = useMemo(() => {
    const base = q
      ? rows.filter(r => (r.ticker || '').toLowerCase().includes(q) || (r.company || '').toLowerCase().includes(q))
      : rows
    return base.slice().sort((a, b) => cmpBy(a, b, sort.key, sort.dir))
  }, [rows, q, sort])

  const H = (label, col, className, style) => <Th label={label} col={col} sort={sort} onSort={toggleSort} className={className} style={style} />

  return (
    <>
      <div className="section-h">
        <h2>Holdings</h2>
        <span className="sub">{subhead}</span>
        <div className="hold-controls">
          <span className="hold-search"><SearchIcon /><input placeholder="Search ticker / company" value={search} onChange={e => setSearch(e.target.value)} /></span>
          <DownloadButton url="/api/portfolio/holdings/export" filenameFallback="holdings" label="Export" />
          <a className="hold-add" onClick={() => setAdding(a => !a)}>+ Add holding</a>
        </div>
      </div>
      {adding && <AddHoldingForm watchlists={watchlists} onDone={() => setAdding(false)} />}
      <table className="data-table">
        <thead>
          <tr>
            <th style={{ width: 28 }}></th>
            {H('Ticker', 'ticker')}
            {H('Company', 'company')}
            {H('Weight', 'weight', 'r')}
            {H('Health', 'health_score', 'r')}
            {H('Prediction', 'upside', 'r')}
            {H('ML', 'mlPct', 'r')}
            {H('Qty', 'total_qty', 'r')}
            {H('Avg Price', 'avg_price', 'r')}
            {H(priceLabel, 'price', 'r')}
            {H('Invested', 'cost', 'r')}
            {H('Market value', 'value', 'r')}
            {H('Total P&L', 'pnl', 'r', { paddingRight: 18 })}
          </tr>
        </thead>
        <tbody>
          {view.length === 0
            ? <tr><td colSpan={COLS} className="muted" style={{ textAlign: 'center', padding: 24 }}>{q ? 'No matching holdings.' : 'No holdings.'}</td></tr>
            : view.map(r => {
              const diff = (live && r.latest_close != null && r.prev_close != null) ? r.latest_close - r.prev_close : null
              const upside = upsideOf(r)
              const mlPct = mlPctOf(r)
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
                        {r.signal && <span className={'sig-pill ' + (SIG_CLS[r.signal] || 'hold')}>{r.signal}</span>}
                      </div>
                    </td>
                    <td className="co-cell">
                      <div>{r.company || '—'}</div>
                      {(r.sector || r.market_cap != null) && (
                        <div className="co-sub">{[r.sector, capSub(r.market_cap)].filter(Boolean).join(' · ')}</div>
                      )}
                    </td>
                    <td className="r">
                      {r.weight == null ? '—' : (
                        <div className="wt-wrap">
                          <span className="wt-num">{r.weight.toFixed(1)}%</span>
                          <span className="wt-bar"><span className="wt-fill" style={{ width: Math.min(100, r.weight) + '%' }} /></span>
                        </div>
                      )}
                    </td>
                    <td className="r">{r.health_score == null ? '—' : <span className="health">{healthDots(r.health_score)}</span>}</td>
                    <td className={'r ' + dirClass(upside)}>{arrowPct1(upside)}</td>
                    <td className={'r ' + dirClass(mlPct)}>{arrowPct1(mlPct)}</td>
                    <td className="r">{r.total_qty == null ? '—' : num(r.total_qty, { fd: 0 })}</td>
                    <td className="r">{r.avg_price == null ? '—' : fmtPrice(r.avg_price)}</td>
                    <td className="r">
                      {live && r.latest_close != null ? (
                        <>
                          <div>{fmtPrice(r.latest_close)}</div>
                          {diff != null && (
                            <div className={dirClass(diff)} style={{ fontSize: 11 }}>
                              {diff >= 0 ? '▲' : '▼'} {fmtPrice(Math.abs(diff))} ({Math.abs(r.day).toFixed(2)}%)
                            </div>
                          )}
                        </>
                      ) : r.last_close != null ? (
                        <><div className="muted">{fmtPrice(r.last_close)}</div><div className="muted" style={{ fontSize: 10 }}>at close</div></>
                      ) : '—'}
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
