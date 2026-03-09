import { useState, useRef, useEffect, useContext } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { getTrades, getPriceHistory } from '../api'
import { useSort } from '../hooks/useSort'
import { MarketContext } from '../context/MarketContext'

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

function isoMonthsAgo(n) {
  const d = new Date()
  d.setMonth(d.getMonth() - n)
  return d.toISOString().slice(0, 10)
}

function countTradingDays(startStr, endStr) {
  let count = 0
  const d = new Date(startStr)
  const end = new Date(endStr)
  while (d <= end) {
    const day = d.getDay()
    if (day !== 0 && day !== 6) count++
    d.setDate(d.getDate() + 1)
  }
  return count
}

function Th({ label, col, sort, onSort }) {
  const active = sort.key === col
  return (
    <th onClick={() => onSort(col)} style={{ cursor: 'pointer' }}>
      {label}
      <span className={`sort-icon ${active ? 'sort-active' : ''}`}>
        {active ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : ' ⇅'}
      </span>
    </th>
  )
}

// ---------------------------------------------------------------------------
// Custom price label for chart reference lines
// ---------------------------------------------------------------------------

function PriceBoxLabel({ viewBox, value, color }) {
  if (!value || !viewBox) return null
  const { x, y } = viewBox
  const px = 8, fontSize = 11
  const boxW = value.length * 6.8 + px * 2
  const boxH = fontSize + 10
  const boxY = y - boxH - 4
  return (
    <g>
      <rect
        x={x - boxW / 2}
        y={boxY}
        width={boxW}
        height={boxH}
        rx={4}
        fill="var(--surface)"
        stroke={color}
        strokeWidth={1.5}
      />
      <text
        x={x}
        y={boxY + boxH / 2}
        textAnchor="middle"
        dominantBaseline="central"
        fill={color}
        fontSize={fontSize}
        fontWeight="700"
        fontFamily="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
      >
        {value}
      </text>
    </g>
  )
}

// ---------------------------------------------------------------------------
// Trade chart modal — supports multiple BUY/SELL pairs
// ---------------------------------------------------------------------------

function TradeModal({ row, start, end, onClose }) {
  const { market } = useContext(MarketContext)
  const currency = market === 'IL' ? '₪' : '$'
  const { data: history = [], isLoading } = useQuery({
    queryKey: ['price-history', row.ticker, start, end],
    queryFn: () => getPriceHistory(row.ticker, { start, end }),
    staleTime: 300_000,
  })

  const end12m    = new Date().toISOString().slice(0, 10)
  const target12m = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); return d.toISOString().slice(0, 10) })()
  const start12m  = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()
  const { data: history12m = [] } = useQuery({
    queryKey: ['price-history-12m', row.ticker],
    queryFn: () => getPriceHistory(row.ticker, { start: start12m, end: end12m }),
    staleTime: 300_000,
  })
  const base12m  = history12m.filter(d => d.date <= target12m).at(-1)
  const yield12m = base12m && history12m.length > 0
    ? ((history12m.at(-1).close - base12m.close) / base12m.close) * 100
    : null

  const minPrice = history.length ? Math.min(...history.map(d => d.close)) * 0.97 : 0
  const maxPrice = history.length ? Math.max(...history.map(d => d.close)) * 1.03 : 0

  // SVG overlay: measure wrapper width so we can calculate x pixel positions
  const wrapperRef = useRef(null)
  const [wrapperWidth, setWrapperWidth] = useState(0)
  useEffect(() => {
    if (!wrapperRef.current) return
    const obs = new ResizeObserver(entries => setWrapperWidth(entries[0].contentRect.width))
    obs.observe(wrapperRef.current)
    return () => obs.disconnect()
  }, [isLoading])

  // Mirror Recharts' point scale: evenly distribute n data points across the plot area
  function dateToX(dateStr) {
    const idx = history.findIndex(d => d.date === dateStr)
    if (idx < 0 || history.length <= 1) return null
    const plotLeft  = 8 + 48                        // margin.left + yAxis.width
    const plotWidth = wrapperWidth - plotLeft - 24  // minus margin.right
    return plotLeft + (idx / (history.length - 1)) * plotWidth
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header">
          {row.logo_url && (
            <img src={row.logo_url} alt="" style={{ width: 96, height: 96, borderRadius: 10, objectFit: 'contain', flexShrink: 0 }} />
          )}
          <table style={{ fontSize: 12, borderCollapse: 'collapse', flex: 1 }}>
            <tbody>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Ticker</td>
                <td style={{ verticalAlign: 'middle' }}><strong>{row.ticker ?? '—'}</strong></td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Company</td>
                <td style={{ verticalAlign: 'middle' }}>{row.company ?? '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Market Cap</td>
                <td style={{ verticalAlign: 'middle' }}>{row.market_cap != null ? `$${(row.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 })}B` : '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>12M Yield (organic)</td>
                <td style={{ verticalAlign: 'middle' }} className={yield12m == null ? '' : yield12m >= 0 ? 'up' : 'down'}>
                  {yield12m != null ? `${yield12m >= 0 ? '+' : ''}${fmt(yield12m)}%` : '—'}
                </td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, verticalAlign: 'middle' }}>12M Yield (Vesign)</td>
                <td style={{ verticalAlign: 'middle' }} className={row.avg_return >= 0 ? 'up' : 'down'}>
                  {row.avg_return != null ? `${row.avg_return >= 0 ? '+' : ''}${fmt(row.avg_return)}%` : '—'}
                </td>
              </tr>
            </tbody>
          </table>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Chart */}
        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>Loading chart…</p>
        ) : (
          <div ref={wrapperRef} style={{ position: 'relative', overflow: 'hidden' }}>
            <ResponsiveContainer width="100%" height={340}>
              <LineChart data={history} margin={{ top: 36, right: 24, bottom: 8, left: 8 }}>
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

            {/* Absolute SVG overlay: vertical buy/sell lines, price boxes, range line + % */}
            {wrapperWidth > 0 && history.length > 1 && (
              <svg style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: 340, pointerEvents: 'none', overflow: 'visible' }}>
                {row.trades.map((t, i) => {
                  const buyX  = t.buy_date  ? dateToX(t.buy_date.slice(0, 10))  : null
                  const sellX = t.sell_date ? dateToX(t.sell_date.slice(0, 10)) : null
                  const PLOT_TOP    = 36
                  const PLOT_BOTTOM = 332  // 340 - margin.bottom(8)

                  // Inline price box renderer
                  const priceBox = (cx, value, color) => {
                    const px = 8, fs = 11
                    const bw = value.length * 6.8 + px * 2
                    const bh = fs + 10
                    const by = PLOT_TOP - bh - 4
                    return (
                      <g>
                        <rect x={cx - bw / 2} y={by} width={bw} height={bh} rx={4}
                          style={{ fill: 'var(--surface)' }} />
                        <text x={cx} y={by + bh / 2} textAnchor="middle" dominantBaseline="central"
                          fontSize={fs} style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                          {value}
                        </text>
                      </g>
                    )
                  }

                  return (
                    <g key={i}>
                      {/* Buy vertical line + price box */}
                      {buyX != null && <>
                        <line x1={buyX} y1={PLOT_TOP} x2={buyX} y2={PLOT_BOTTOM}
                          style={{ stroke: 'var(--green)', strokeWidth: 2 }} />
                        {t.buy_price != null && priceBox(buyX, currency + fmt(t.buy_price, 1), 'var(--green)')}
                      </>}
                      {/* Sell vertical line + price box */}
                      {sellX != null && <>
                        <line x1={sellX} y1={PLOT_TOP} x2={sellX} y2={PLOT_BOTTOM}
                          style={{ stroke: 'var(--red)', strokeWidth: 2 }} />
                        {t.sell_price != null && priceBox(sellX, currency + fmt(t.sell_price, 1), 'var(--red)')}
                      </>}
                      {/* Horizontal dotted range line + % gain label */}
                      {buyX != null && sellX != null && t.buy_price != null && t.sell_price != null && (() => {
                        const pct   = ((t.sell_price - t.buy_price) / t.buy_price) * 100
                        const color = pct >= 0 ? 'var(--green)' : 'var(--red)'
                        const lineY = PLOT_TOP + 18
                        return <>
                          <line x1={buyX} y1={lineY} x2={sellX} y2={lineY}
                            style={{ stroke: color, strokeWidth: 1.5, strokeDasharray: '4 3' }} />
                          <text x={(buyX + sellX) / 2} y={lineY - 6}
                            textAnchor="middle" fontSize={10}
                            style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                            {pct >= 0 ? '+' : ''}{pct.toFixed(1)}%
                          </text>
                        </>
                      })()}
                    </g>
                  )
                })}
              </svg>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function TradesPage() {
  const { market } = useContext(MarketContext)
  const oneYearAgo = new Date()
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1)

  const [start, setStart]       = useState(oneYearAgo.toISOString().slice(0, 10))
  const [end,   setEnd]         = useState(new Date().toISOString().slice(0, 10))
  const [selected, setSelected] = useState(null)
  const [search, setSearch]     = useState('')

  const { data: trades, isLoading, isError } = useQuery({
    queryKey: ['trades', start, end, market],
    queryFn: () => getTrades({ start, end, market }),
  })

  const { sorted, sort, toggle } = useSort(trades, 'avg_return', 'desc')

  const total     = trades ? trades.length : 0
  const totalPairs = trades ? trades.reduce((s, t) => s + t.trade_count, 0) : 0
  const wins      = trades ? trades.reduce((s, t) => s + t.win_count, 0) : 0
  const avgReturn = total > 0
    ? trades.reduce((s, t) => s + t.avg_return * t.trade_count, 0) / totalPairs
    : null
  const avgDays   = total > 0
    ? trades.reduce((s, t) => s + t.avg_days * t.trade_count, 0) / totalPairs
    : null

  const beatMarket = trades
    ? trades.reduce((n, t) =>
        n + (t.organic_yield != null
          ? t.trades.filter(p => p.return_pct > t.organic_yield).length
          : 0), 0)
    : null

  const th = (label, col) => <Th label={label} col={col} sort={sort} onSort={toggle} />

  return (
    <div>
      <p className="page-title">Historical Trades</p>

      <div className="controls">
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>From</label>
        <input type="date" value={start} onChange={e => setStart(e.target.value)} />
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>To</label>
        <input type="date" value={end} onChange={e => setEnd(e.target.value)} />
        {[3, 6, 12, 24, 36].map(m => (
          <button
            key={m}
            className="period-chip"
            onClick={() => { setStart(isoMonthsAgo(m)); setEnd(new Date().toISOString().slice(0, 10)) }}
          >{m}M</button>
        ))}
        <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8 }}>
          <input
            placeholder="🔍 Search ticker or company"
            value={search}
            onChange={e => setSearch(e.target.value)}
            style={{ width: 220 }}
          />
          {search && <button onClick={() => setSearch('')}>Clear</button>}
        </span>
      </div>

      {isLoading && <p className="loading">Loading…</p>}
      {isError   && <p className="error">Failed to load trades.</p>}

      {trades && total > 0 && (
        <div className="metrics">
          <div className="metric-card">
            <div className="label">Total Trades</div>
            <div className="value">{totalPairs}</div>
          </div>
          <div className="metric-card">
            <div className="label">Win Rate</div>
            <div className="value">{totalPairs > 0 ? ((wins / totalPairs) * 100).toFixed(1) : '—'}%</div>
          </div>
          <div className="metric-card">
            <div className="label">Avg Yield/Trade</div>
            <div className={`value ${avgReturn >= 0 ? 'up' : 'down'}`}>
              {avgReturn >= 0 ? '+' : ''}{fmt(avgReturn)}%
            </div>
          </div>
          <div className="metric-card">
            <div className="label">Avg Days Held</div>
            <div className="value">{avgDays != null ? Math.round(avgDays) : '—'}</div>
          </div>
          <div className="metric-card">
            <div className="label">Period Yield</div>
            {(() => {
              const tradingDays = countTradingDays(start, end)
              const annualYield = avgReturn != null && avgDays != null && avgDays > 0
                ? (avgReturn / avgDays) * tradingDays
                : null
              return (
                <div className={`value ${annualYield >= 0 ? 'up' : 'down'}`}>
                  {annualYield != null ? `${annualYield >= 0 ? '+' : ''}${fmt(annualYield)}%` : '—'}
                </div>
              )
            })()}
          </div>
          <div className="metric-card">
            <div className="label">Beat Market</div>
            <div className="value">
              {beatMarket != null ? `${beatMarket} / ${totalPairs}` : '—'}
            </div>
            <div style={{ color: 'var(--muted)', fontSize: 11, marginTop: 2 }}>trades beat organic</div>
          </div>
        </div>
      )}

      {trades && total === 0 && (
        <p className="empty">No completed BUY→SELL trades in the selected period.</p>
      )}

      {trades && total > 0 && (() => {
        const filtered = search
          ? sorted.filter(t =>
              t.ticker?.toLowerCase().includes(search.toLowerCase()) ||
              t.company?.toLowerCase().includes(search.toLowerCase())
            )
          : sorted
        return (
        <div className="data-table-wrap">
          <table>
            <thead>
              <tr>
                <th></th>
                {th('Company',     'company')}
                {th('Ticker',      'ticker')}
                {th('Market Cap (B)', 'market_cap')}
                {th('Trades',      'trade_count')}
                {th('Win Rate',    'win_count')}
                {th('Avg Return',  'avg_return')}
                {th('Avg Days',    'avg_days')}
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0
                ? <tr><td colSpan={8} className="empty" style={{ textAlign: 'center' }}>No matches found.</td></tr>
                : filtered.map((t, i) => {
                const winRate = t.trade_count > 0 ? (t.win_count / t.trade_count) * 100 : 0
                return (
                  <tr key={i} className="clickable-row" onClick={() => setSelected(t)}>
                    <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                    <td>{t.company ?? '—'}</td>
                    <td><strong>{t.ticker}</strong></td>
                    <td>{t.market_cap != null ? (t.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
                    <td>{t.trade_count}</td>
                    <td className={winRate >= 50 ? 'up' : 'down'}>{winRate.toFixed(0)}%</td>
                    <td className={t.avg_return >= 0 ? 'up' : 'down'}>
                      {t.avg_return >= 0 ? '+' : ''}{fmt(t.avg_return)}%
                    </td>
                    <td>{Math.round(t.avg_days)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        )
      })()}

      {selected && <TradeModal row={selected} start={start} end={end} onClose={() => setSelected(null)} />}
    </div>
  )
}
