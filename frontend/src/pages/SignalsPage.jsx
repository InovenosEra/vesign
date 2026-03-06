import { useState, useMemo, useContext, useRef, useEffect, useLayoutEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSignalsToday, getSignals, getPriceHistory, getSignalMarkers } from '../api'
import { MarketContext } from '../context/MarketContext'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
}

function fmtDate(str) {
  if (!str) return '—'
  const d = new Date(str)
  const dd = String(d.getDate()).padStart(2, '0')
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const yy = String(d.getFullYear()).slice(2)
  return `${dd}/${mm}/${yy}`
}

const _HEALTH_COLORS = ['', '#e74c3c', '#e67e22', '#f1c40f', '#2ecc71', '#1a9e55']
const _HEALTH_LABELS = ['', 'Weak', 'Fair', 'Good', 'Great', 'Excellent']

function HealthCell({ score }) {
  if (!score) return <td style={{ color: 'var(--muted)' }}>—</td>
  return (
    <td title={_HEALTH_LABELS[score]} style={{ whiteSpace: 'nowrap' }}>
      <div style={{ display: 'flex', gap: 3, alignItems: 'center' }}>
        {[1,2,3,4,5].map(i => (
          <div key={i} style={{
            width: 10, height: 10, borderRadius: 2,
            background: i <= score ? _HEALTH_COLORS[score] : 'var(--border)',
          }} />
        ))}
        <span style={{ fontSize: 11, color: _HEALTH_COLORS[score], marginLeft: 3 }}>
          {_HEALTH_LABELS[score]}
        </span>
      </div>
    </td>
  )
}

function PredictionCell({ value }) {
  if (value == null) return <td>—</td>
  const pct = (value * 100).toFixed(2)
  return <td className={value > 0 ? 'up' : 'down'}>{value > 0 ? '▲' : '▼'} {Math.abs(pct).toFixed(2)}%</td>
}

function LivePriceCell({ ticker, closePrice, prices, marketOpen }) {
  const style = { whiteSpace: 'nowrap' }
  if (marketOpen === null) return <td style={{ ...style, color: 'var(--muted)' }}>—</td>
  if (!marketOpen) return <td style={{ ...style, color: 'var(--muted)', fontSize: 12 }}>Closed</td>
  const live = prices[ticker]
  if (live == null) return <td style={{ ...style, color: 'var(--muted)' }}>—</td>
  const diff  = live - closePrice
  const pct   = closePrice ? (diff / closePrice) * 100 : 0
  const cls   = diff >= 0 ? 'up' : 'down'
  const arrow = diff >= 0 ? '▲' : '▼'
  return (
    <td style={style}>
      <div>{live.toFixed(2)}</div>
      <div className={cls} style={{ fontSize: 11 }}>
        {arrow} {Math.abs(diff).toFixed(2)} ({Math.abs(pct).toFixed(2)}%)
      </div>
    </td>
  )
}

// Sortable <th> — used by client-side tables
// Server-side sortable <th> — used by the paginated all-signals table
function ServerTh({ label, col, sortBy, sortDir, onSort }) {
  const active = sortBy === col
  return (
    <th onClick={() => onSort(col)} style={{ cursor: 'pointer' }}>
      {label}
      <span className={`sort-icon ${active ? 'sort-active' : ''}`}>
        {active ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ' ⇅'}
      </span>
    </th>
  )
}

function Pagination({ page, pages, onChange }) {
  if (pages <= 1) return null
  return (
    <div className="pagination">
      <button disabled={page === 1} onClick={() => onChange(1)}>«</button>
      <button disabled={page === 1} onClick={() => onChange(page - 1)}>‹ Prev</button>
      <span>{page} / {pages}</span>
      <button disabled={page === pages} onClick={() => onChange(page + 1)}>Next ›</button>
      <button disabled={page === pages} onClick={() => onChange(pages)}>»</button>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Signal popup modal — 12M chart with vertical line at signal date
// ---------------------------------------------------------------------------

function SignalModal({ row, onClose }) {
  const { market } = useContext(MarketContext)
  const currency  = market === 'IL' ? '₪' : '$'
  const today     = new Date().toISOString().slice(0, 10)
  const target12m = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); return d.toISOString().slice(0, 10) })()
  const start12m  = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()

  const { data: history = [], isLoading } = useQuery({
    queryKey: ['price-history-signal', row.ticker],
    queryFn: () => getPriceHistory(row.ticker, { start: start12m, end: today }),
    staleTime: 300_000,
  })

  const { data: markers = [] } = useQuery({
    queryKey: ['signal-markers', row.ticker],
    queryFn: () => getSignalMarkers(row.ticker, 13),
    staleTime: 300_000,
  })

  const base12m  = history.filter(d => d.date <= target12m).at(-1)
  const yield12m = base12m && history.length > 0
    ? ((history.at(-1).close - base12m.close) / base12m.close) * 100
    : null

  const minPrice = history.length ? Math.min(...history.map(d => d.close)) * 0.97 : 0
  const maxPrice = history.length ? Math.max(...history.map(d => d.close)) * 1.03 : 0

  // General column ref — measures height to constrain right column
  const generalColRef = useRef(null)
  const [generalColH, setGeneralColH] = useState(null)
  useLayoutEffect(() => {
    if (generalColRef.current) setGeneralColH(generalColRef.current.offsetHeight)
  })

  // Wrapper ref for SVG overlay positioning
  const wrapperRef = useRef(null)
  const [wrapperWidth, setWrapperWidth] = useState(0)
  useEffect(() => {
    if (!wrapperRef.current) return
    const obs = new ResizeObserver(entries => setWrapperWidth(entries[0].contentRect.width))
    obs.observe(wrapperRef.current)
    return () => obs.disconnect()
  }, [isLoading])

  // Mirror Recharts' point scale
  function dateToX(dateStr) {
    const idx = history.findIndex(d => d.date === dateStr)
    if (idx < 0 || history.length <= 1) return null
    const plotLeft  = 8 + 48
    const plotWidth = wrapperWidth - plotLeft - 70
    return plotLeft + (idx / (history.length - 1)) * plotWidth
  }

  // Build BUY/SELL pairs and detect open position
  const pairs = []
  let pendingBuy = null
  for (const m of markers) {
    if (m.signal === 'BUY') {
      pendingBuy = m
    } else if (m.signal === 'SELL' && pendingBuy) {
      pairs.push({ buy: pendingBuy, sell: m })
      pendingBuy = null
    }
  }
  const openBuy = pendingBuy  // last BUY with no subsequent SELL

  const PLOT_TOP    = 70
  const PLOT_BOTTOM = 332

  function priceBox(cx, value, color, byOverride) {
    const px = 8, fs = 11
    const bw = value.length * 6.8 + px * 2
    const bh = fs + 10
    const by = byOverride !== undefined ? byOverride : PLOT_TOP - bh - 4
    return (
      <g>
        <rect x={cx - bw / 2} y={by} width={bw} height={bh} rx={4} fill="var(--surface)" stroke={color} strokeWidth={1.5} />
        <text x={cx} y={by + bh / 2} textAnchor="middle" dominantBaseline="central"
          fontSize={fs} style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
          {value}
        </text>
      </g>
    )
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header" style={{ alignItems: 'flex-start' }}>
          {row.logo_url
            ? <img src={row.logo_url} alt="" style={{ width: 96, height: 96, borderRadius: 10, objectFit: 'contain', flexShrink: 0 }} onError={e => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex'; }} />
            : null}
          <div style={{
            width: 96, height: 96, flexShrink: 0, borderRadius: 10,
            background: 'var(--surface)', border: '1px solid var(--border)',
            display: row.logo_url ? 'none' : 'flex',
            alignItems: 'center', justifyContent: 'center',
            fontSize: 13, fontWeight: 'bold', color: 'var(--text)',
          }}>
            {row.ticker}
          </div>
          <div ref={generalColRef} style={{ display: 'flex', flexDirection: 'column', gap: 4, flexShrink: 0, width: 300 }}>
            <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>General</div>
            <div style={{ padding: '8px 0px', border: '1px solid var(--border)', borderRadius: 8 }}>
              <table style={{ fontSize: 12, borderCollapse: 'collapse', width: '100%', margin: 0, tableLayout: 'fixed' }}>
                <tbody>
                  {[
                    ['Ticker',     <strong>{row.ticker ?? '—'}</strong>],
                    ['Company',    row.company ?? '—'],
                    ['Industry',   row.industry ?? '—'],
                    ['Market Cap', row.market_cap != null ? `$${(row.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}B` : '—'],
                    ['Signal',     row.signal ? <span className={`badge badge-${row.signal}`}>{row.signal}</span> : '—'],
                    ['Price',      row.close != null ? `${currency}${fmt(row.close)}` : '—'],
                    ['RSI',        row.rsi != null ? row.rsi.toFixed(1) : '—'],
                    ['Prediction', row.fair_value_upside != null ? <span className={row.fair_value_upside >= 0 ? 'up' : 'down'}>{row.fair_value_upside >= 0 ? '+' : ''}{fmt(row.fair_value_upside * 100)}%</span> : '—'],
                    ['12M Yield',  yield12m != null ? <span className={yield12m >= 0 ? 'up' : 'down'}>{yield12m >= 0 ? '+' : ''}{fmt(yield12m)}%</span> : '—'],
                  ].map(([label, value]) => (
                    <tr key={label} style={{ height: 22 }}>
                      <td style={{ color: 'var(--muted)', paddingRight: 8, verticalAlign: 'middle', whiteSpace: 'nowrap', width: 90 }}>{label}</td>
                      <td style={{ verticalAlign: 'middle', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          {(row.description_short || row.description || row.health_score) && (
            <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4, overflow: 'hidden', ...(generalColH ? { height: generalColH } : {}) }}>
              {(row.description_short || row.description) && (<>
                <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>Description</div>
                <div style={{ fontSize: 12, lineHeight: 1.6, overflowY: 'auto', flex: 1, minHeight: 0, padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8 }}>
                  {row.description_short || row.description}
                </div>
              </>)}
              {row.health_score && (() => {
                const labels = ['', 'Weak', 'Fair', 'Good', 'Great', 'Excellent']
                const colors = ['', '#e74c3c', '#e67e22', '#f1c40f', '#2ecc71', '#1a9e55']
                const score  = row.health_score
                return (
                  <div style={{ padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
                    <div style={{ fontSize: 13, color: 'var(--muted)', fontWeight: 'bold', marginBottom: 6 }}>Company Health</div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                      {[1,2,3,4,5].map(i => (
                        <div key={i} style={{ width: 28, height: 12, borderRadius: 4, background: i <= score ? colors[score] : 'var(--border)' }} />
                      ))}
                      <span style={{ fontSize: 12, fontWeight: 'bold', color: colors[score], marginLeft: 4 }}>{labels[score]}</span>
                    </div>
                    {row.health_reason && <div style={{ fontSize: 12, lineHeight: 1.6, flex: 1, minHeight: 0, overflowY: 'auto' }}>{row.health_reason}</div>}
                  </div>
                )
              })()}
            </div>
          )}
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Chart */}
        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>Loading chart…</p>
        ) : (
          <div ref={wrapperRef} style={{ position: 'relative', overflow: 'hidden' }}>
            <ResponsiveContainer width="100%" height={340}>
              <LineChart data={history} margin={{ top: 70, right: 70, bottom: 8, left: 8 }}>
                <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
                <XAxis
                  dataKey="date"
                  tick={{ fill: 'var(--muted)', fontSize: 11 }}
                  tickFormatter={d => { const [, m, day] = d.split('-'); return `${day}/${m}` }}
                  interval="preserveStartEnd"
                  minTickGap={50}
                />
                <YAxis
                  domain={[minPrice, maxPrice]}
                  tick={{ fill: 'var(--muted)', fontSize: 11 }}
                  tickFormatter={v => v.toFixed(0)}
                  width={48}
                />
                <Tooltip
                  contentStyle={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12 }}
                  labelStyle={{ color: 'var(--muted)' }}
                  itemStyle={{ color: 'var(--text)' }}
                  labelFormatter={d => { const [y, m, day] = d.split('-'); return `${day}/${m}/${y.slice(2)}` }}
                  formatter={v => [`${currency}${v.toFixed(2)}`, 'Close']}
                />
                <Line type="monotone" dataKey="close" stroke="var(--accent)" dot={false} strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>

            {/* SVG overlay: BUY/SELL signal lines */}
            {wrapperWidth > 0 && history.length > 1 && (
              <svg style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: 340, pointerEvents: 'none', overflow: 'visible' }}>

                {/* Completed BUY→SELL pairs */}
                {pairs.map((p, i) => {
                  const buyX  = dateToX(p.buy.date)
                  const sellX = dateToX(p.sell.date)
                  const pct   = p.buy.close && p.sell.close
                    ? ((p.sell.close - p.buy.close) / p.buy.close) * 100
                    : null
                  const color = pct != null && pct >= 0 ? 'var(--green)' : 'var(--red)'
                  const lineY = PLOT_TOP + 18
                  return (
                    <g key={i}>
                      {buyX  != null && <>
                        <line x1={buyX}  y1={PLOT_TOP} x2={buyX}  y2={PLOT_BOTTOM} style={{ stroke: 'var(--green)', strokeWidth: 2 }} />
                        {p.buy.close  != null && priceBox(buyX,  currency + fmt(p.buy.close,  1), 'var(--green)')}
                      </>}
                      {sellX != null && <>
                        <line x1={sellX} y1={PLOT_TOP} x2={sellX} y2={PLOT_BOTTOM} style={{ stroke: 'var(--red)',   strokeWidth: 2 }} />
                        {p.sell.close != null && priceBox(sellX, currency + fmt(p.sell.close, 1), 'var(--red)')}
                      </>}
                      {buyX != null && sellX != null && pct != null && (() => {
                        const label = `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`
                        const bw = label.length * 6.8 + 16
                        const plotRight = wrapperWidth - 24
                        const cx = Math.min(Math.max((buyX + sellX) / 2, 56 + bw / 2), plotRight - bw / 2)
                        const bh = 18
                        return <>
                          <line x1={buyX} y1={lineY} x2={sellX} y2={lineY}
                            style={{ stroke: color, strokeWidth: 1.5, strokeDasharray: '4 3' }} />
                          <rect x={cx - bw / 2} y={lineY + 4} width={bw} height={bh} rx={3}
                            fill="var(--surface)" stroke={color} strokeWidth={1.5} />
                          <text x={cx} y={lineY + 4 + bh / 2} textAnchor="middle" dominantBaseline="central" fontSize={10}
                            style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                            {label}
                          </text>
                        </>
                      })()}
                    </g>
                  )
                })}

                {/* Open BUY (no subsequent SELL) */}
                {openBuy && (() => {
                  const buyX  = dateToX(openBuy.date)
                  const lastX = dateToX(history.at(-1).date)
                  if (!buyX) return null
                  const currentPrice = history.at(-1).close
                  const pct       = openBuy.close ? ((currentPrice - openBuy.close) / openBuy.close) * 100 : null
                  const gainColor = pct != null && pct >= 0 ? 'var(--green)' : 'var(--red)'
                  const priceText = openBuy.close != null ? currency + fmt(openBuy.close, 1) : ''
                  const yieldText = pct != null ? `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%` : ''

                  const price_bh = 21, gain_bh = 18, gap = 4
                  const bw_price = priceText.length * 6.8 + 16
                  const bw_gain  = yieldText.length * 6.8 + 16

                  // Collision detection: check if price box overlaps a pair price box
                  const pairBoxes = []
                  for (const p of pairs) {
                    const bx = dateToX(p.buy.date)
                    const sx = dateToX(p.sell.date)
                    if (bx != null && p.buy.close != null)
                      pairBoxes.push({ x: bx, w: (currency + fmt(p.buy.close, 1)).length * 6.8 + 16 })
                    if (sx != null && p.sell.close != null)
                      pairBoxes.push({ x: sx, w: (currency + fmt(p.sell.close, 1)).length * 6.8 + 16 })
                  }
                  const hasCollision = pairBoxes.some(b => Math.abs(buyX - b.x) < (bw_price + b.w) / 2)

                  // Price box: same row as pair price boxes; raised one row when colliding
                  const price_by = hasCollision
                    ? PLOT_TOP - price_bh - 4 - price_bh - 6
                    : PLOT_TOP - price_bh - 4
                  // Gain box: separate box sitting above the price box
                  const gain_by  = price_by - gain_bh - gap

                  return (
                    <g>
                      <line x1={buyX} y1={PLOT_TOP} x2={buyX} y2={PLOT_BOTTOM} style={{ stroke: 'var(--green)', strokeWidth: 2 }} />
                      {openBuy.close != null && priceBox(buyX, priceText, 'var(--green)', price_by)}
                      {/* Gain box: yield only, above price box */}
                      {yieldText && <>
                        <rect x={buyX - bw_gain / 2} y={gain_by} width={bw_gain} height={gain_bh} rx={4}
                          fill="var(--surface)" stroke={gainColor} strokeWidth={1.5} />
                        <text x={buyX} y={gain_by + gain_bh / 2} textAnchor="middle" dominantBaseline="central"
                          fontSize={11} style={{ fill: gainColor, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                          {yieldText}
                        </text>
                      </>}
                      {/* "Open" label rotated vertically on the BUY line */}
                      {(() => {
                        const midY = (PLOT_TOP + PLOT_BOTTOM) / 2
                        const tw = 32, th = 16
                        return (
                          <g transform={`rotate(-90, ${buyX}, ${midY})`}>
                            <rect x={buyX - tw / 2} y={midY - th / 2} width={tw} height={th} rx={3}
                              fill="var(--surface)" opacity={0.85} />
                            <text x={buyX} y={midY} textAnchor="middle" dominantBaseline="central"
                              fontSize={10} style={{ fill: 'var(--green)', fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                              Open
                            </text>
                          </g>
                        )
                      })()}
                      {/* Dashed line showing open duration */}
                      {lastX != null && (
                        <line x1={buyX} y1={PLOT_TOP + 18} x2={lastX} y2={PLOT_TOP + 18}
                          style={{ stroke: gainColor, strokeWidth: 1.5, strokeDasharray: '4 3' }} />
                      )}
                    </g>
                  )
                })()}

              </svg>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Today's table — small dataset, client-side sort, live prices
// ---------------------------------------------------------------------------

// Shared fixed widths so BUY and SELL tables align column-by-column
// Columns: logo, ticker, company, mktcap, price, rsi, low, base, high, prediction, live/date
// logo, ticker, company, mktcap, price, rsi, low, base, high, health, prediction, live
const COL_WIDTHS = ['44px', '70px', '150px', '90px', '70px', '55px', '80px', '80px', '80px', '120px', '85px', '100px']

function TodayTableBody({ rows, prices, marketOpen, onRowClick }) {
  return (
    <table style={{ tableLayout: 'fixed', width: '100%', minWidth: 1024 }}>
      <colgroup>{COL_WIDTHS.map((w, i) => <col key={i} style={{ width: w }} />)}</colgroup>
      <thead>
        <tr>
          <th></th>
          <th>Ticker</th>
          <th>Company</th>
          <th>Market Cap (B)</th>
          <th>Price</th>
          <th>RSI</th>
          <th>Low Price</th>
          <th>Base Price</th>
          <th>High Price</th>
          <th>Health Score</th>
          <th>Prediction</th>
          <th>Live Price</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
            <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" onError={e => e.target.style.display = 'none'} /> : null}</td>
            <td><strong>{r.ticker ?? '—'}</strong></td>
            <td>{r.company ?? '—'}</td>
            <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'}</td>
            <td>{r.close != null ? fmt(r.close) : '—'}</td>
            <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
            <td>{r.target_low_price != null ? fmt(r.target_low_price) : '—'}</td>
            <td>{r.target_mean_price != null ? fmt(r.target_mean_price) : '—'}</td>
            <td>{r.target_high_price != null ? fmt(r.target_high_price) : '—'}</td>
            <HealthCell score={r.health_score} />
            <PredictionCell value={r.fair_value_upside} />
            <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} marketOpen={marketOpen} />
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function TodayTable({ rows, prices, marketOpen, onRowClick }) {
  const { sorted } = useSort(rows, 'market_cap', 'desc')

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  return (
    <div className="data-table-wrap">
      <TodayTableBody rows={sorted} prices={prices} marketOpen={marketOpen} onRowClick={onRowClick} />
    </div>
  )
}

function TodaySellTable({ rows, prices, marketOpen, onRowClick }) {
  const { sorted } = useSort(rows, 'market_cap', 'desc')
  const [page, setPage]         = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [search, setSearch]     = useState('')

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  const filtered = search
    ? sorted.filter(r =>
        r.ticker?.toLowerCase().includes(search.toLowerCase()) ||
        r.company?.toLowerCase().includes(search.toLowerCase())
      )
    : sorted

  const pages     = Math.max(1, Math.ceil(filtered.length / pageSize))
  const paginated = filtered.slice((page - 1) * pageSize, page * pageSize)

  function handlePageSize(val) { setPageSize(Number(val)); setPage(1) }
  function handleSearch(val)   { setSearch(val);           setPage(1) }

  return (
    <>
      <div className="controls">
        <input
          placeholder="🔍 Search ticker or company"
          value={search}
          onChange={e => handleSearch(e.target.value)}
          style={{ width: 240 }}
        />
        {search && <button onClick={() => handleSearch('')}>Clear</button>}
        <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
          <label style={{ color: 'var(--muted)', fontSize: 13 }}>Rows</label>
          <select value={pageSize} onChange={e => handlePageSize(e.target.value)}>
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </span>
      </div>
      <div className="data-table-wrap">
        <TodayTableBody rows={paginated} prices={prices} marketOpen={marketOpen} onRowClick={onRowClick} />
      </div>
      <Pagination page={page} pages={pages} onChange={setPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// All-signals table — server-side sort + pagination
// ---------------------------------------------------------------------------

function AllSignalsTable({ result, sortBy, sortDir, onSort, page, onPage, onRowClick, prices, marketOpen }) {
  if (!result) return null
  const { data: rows, pages } = result

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  const th = (label, col) =>
    <ServerTh label={label} col={col} sortBy={sortBy} sortDir={sortDir} onSort={onSort} />

  return (
    <>
      <div className="data-table-wrap">
        <table style={{ tableLayout: 'fixed', width: '100%', minWidth: 1174 }}>
          <colgroup>{['80px','44px','70px','150px','90px','70px','70px','55px','80px','80px','80px','120px','85px','100px'].map((w, i) => <col key={i} style={{ width: w }} />)}</colgroup>
          <thead>
            <tr>
              {th('Date',           'date')}
              <th></th>
              {th('Ticker',         'ticker')}
              {th('Company',        'company')}
              {th('Market Cap (B)', 'market_cap')}
              {th('Signal',         'signal')}
              {th('Price',          'close')}
              {th('RSI',            'rsi')}
              {th('Low Price',      'target_low_price')}
              {th('Base Price',     'target_mean_price')}
              {th('High Price',     'target_high_price')}
              <th>Health Score</th>
              {th('Prediction',     'fair_value_upside')}
              <th>Live Price</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
                <td>{r.date ? (() => { const [y,m,d] = r.date.slice(0,10).split('-'); return `${d}/${m}/${y.slice(2)}` })() : '—'}</td>
                <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" onError={e => e.target.style.display = 'none'} /> : null}</td>
                <td><strong>{r.ticker ?? '—'}</strong></td>
                <td>{r.company ?? '—'}</td>
                <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'}</td>
                <td>{r.signal ? <span className={`badge badge-${r.signal}`}>{r.signal}</span> : '—'}</td>
                <td>{r.close != null ? fmt(r.close) : '—'}</td>
                <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
                <td>{r.target_low_price != null ? fmt(r.target_low_price) : '—'}</td>
                <td>{r.target_mean_price != null ? fmt(r.target_mean_price) : '—'}</td>
                <td>{r.target_high_price != null ? fmt(r.target_high_price) : '—'}</td>
                <HealthCell score={r.health_score} />
                <PredictionCell value={r.fair_value_upside} />
                <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} marketOpen={marketOpen} />
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pagination page={page} pages={pages} onChange={onPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function SignalsPage() {
  const { market } = useContext(MarketContext)
  const [signalFilter, setSignalFilter] = useState('ALL')
  const [search, setSearch]             = useState('')
  const [page, setPage]                 = useState(1)
  const [pageSize, setPageSize]         = useState(10)
  const [sortBy, setSortBy]             = useState('date')
  const [sortDir, setSortDir]           = useState('desc')
  const [selected, setSelected]         = useState(null)

  function handleSort(col) {
    if (col === sortBy) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(col)
      setSortDir('asc')
    }
    setPage(1)
  }

  function handleSearch(val) { setSearch(val); setPage(1) }
  function handleFilter(val) { setSignalFilter(val); setPage(1) }
  function handlePageSize(val) { setPageSize(Number(val)); setPage(1) }

  const { data: todayBuy,  isLoading: loadingBuy  } = useQuery({
    queryKey: ['signals-today', 'BUY', market],
    queryFn: () => getSignalsToday('BUY', market),
    refetchInterval: 120_000,
  })

  const { data: todaySell, isLoading: loadingSell } = useQuery({
    queryKey: ['signals-today', 'SELL', market],
    queryFn: () => getSignalsToday('SELL', market),
    refetchInterval: 120_000,
  })

  const { data: allResult, isLoading: loadingAll } = useQuery({
    queryKey: ['signals', signalFilter, search, page, pageSize, sortBy, sortDir, market],
    queryFn: () => getSignals({
      signal:    signalFilter === 'ALL' ? undefined : signalFilter,
      search:    search || undefined,
      page,
      page_size: pageSize,
      sort_by:   sortBy,
      sort_dir:  sortDir,
      market,
    }),
    keepPreviousData: true,
  })

  const allTickers = useMemo(() => {
    const set = new Set()
    todayBuy?.forEach(r => r.ticker && set.add(r.ticker))
    todaySell?.forEach(r => r.ticker && set.add(r.ticker))
    ;(allResult?.data ?? []).forEach(r => r.ticker && set.add(r.ticker))
    return [...set]
  }, [todayBuy, todaySell, allResult])

  const { prices, marketOpen } = useLivePrices(allTickers)

  return (
    <div>
      <div className="section">
        <p className="section-title">
          Today's BUY Signals ({loadingBuy ? '…' : (todayBuy?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingBuy
          ? <p className="loading">Loading…</p>
          : <TodayTable rows={todayBuy} prices={prices} marketOpen={marketOpen} onRowClick={setSelected} />}
      </div>

      <div className="section">
        <p className="section-title">
          Today's SELL Signals ({loadingSell ? '…' : (todaySell?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingSell
          ? <p className="loading">Loading…</p>
          : <TodaySellTable rows={todaySell} prices={prices} marketOpen={marketOpen} onRowClick={setSelected} />}
      </div>

      <div className="section">
        <p className="section-title">
          All Signals
          {allResult && <span style={{ color: 'var(--muted)', fontSize: 12, marginLeft: 10 }}>
            {allResult.total.toLocaleString()} rows
          </span>}
        </p>
        <div className="controls">
          <input
            placeholder="🔍 Search ticker or company"
            value={search}
            onChange={e => handleSearch(e.target.value)}
            style={{ width: 240 }}
          />
          {['ALL', 'BUY', 'HOLD', 'SELL'].map(s => (
            <button key={s} onClick={() => handleFilter(s)} className={signalFilter === s ? 'primary' : ''}>
              {s}
            </button>
          ))}
          {search && <button onClick={() => handleSearch('')}>Clear</button>}
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ color: 'var(--muted)', fontSize: 13 }}>Rows</label>
            <select value={pageSize} onChange={e => handlePageSize(e.target.value)}>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </span>
        </div>
        {loadingAll && !allResult
          ? <p className="loading">Loading…</p>
          : <AllSignalsTable
              result={allResult}
              sortBy={sortBy}
              sortDir={sortDir}
              onSort={handleSort}
              page={page}
              onPage={setPage}
              onRowClick={setSelected}
              prices={prices}
              marketOpen={marketOpen}
            />}
      </div>

      {selected && <SignalModal row={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
