import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ReferenceLine,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { getTrades, getPriceHistory } from '../api'
import { useSort } from '../hooks/useSort'

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
// Trade chart modal — supports multiple BUY/SELL pairs
// ---------------------------------------------------------------------------

function TradeModal({ row, start, end, onClose }) {
  const { data: history = [], isLoading } = useQuery({
    queryKey: ['price-history', row.ticker, start, end],
    queryFn: () => getPriceHistory(row.ticker, { start, end }),
    staleTime: 300_000,
  })

  const minPrice = history.length ? Math.min(...history.map(d => d.close)) * 0.97 : 0
  const maxPrice = history.length ? Math.max(...history.map(d => d.close)) * 1.03 : 0

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header">
          <span style={{ display: 'flex', alignItems: 'center', fontSize: 16, flex: 1 }}>
            {row.logo_url && <img className="logo" src={row.logo_url} alt="" style={{ marginRight: 8 }} />}
            <strong>{row.ticker}</strong>
            {row.company ? ` — ${row.company}` : ''}
          </span>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Trade summary chips */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, marginBottom: 16 }}>
          {row.trades.map((t, i) => (
            <div key={i} className="trade-chip">
              <span style={{ color: 'var(--muted)', fontSize: 11, marginRight: 6 }}>#{i + 1}</span>
              <span className="up">▲ {fmtDate(t.buy_date)} @ {fmt(t.buy_price)}</span>
              <span style={{ color: 'var(--muted)', margin: '0 4px' }}>→</span>
              <span className="down">▼ {fmtDate(t.sell_date)} @ {fmt(t.sell_price)}</span>
              <span style={{ color: 'var(--muted)', margin: '0 4px' }}>·</span>
              <span className={t.return_pct >= 0 ? 'up' : 'down'}>
                {t.return_pct >= 0 ? '+' : ''}{fmt(t.return_pct)}% · {t.days_held}d
              </span>
            </div>
          ))}
        </div>

        {/* Chart */}
        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>Loading chart…</p>
        ) : (
          <ResponsiveContainer width="100%" height={340}>
            <LineChart data={history} margin={{ top: 24, right: 24, bottom: 8, left: 8 }}>
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
                formatter={v => [`$${v.toFixed(2)}`, 'Close']}
              />
              {row.trades.map((t, i) => [
                t.buy_date && (
                  <ReferenceLine
                    key={`buy-${i}`}
                    x={t.buy_date.slice(0, 10)}
                    stroke="var(--green)"
                    strokeWidth={2}
                    label={{ value: `B${row.trades.length > 1 ? i + 1 : ''}`, position: 'top', fill: 'var(--green)', fontSize: 11, fontWeight: 700 }}
                  />
                ),
                t.sell_date && (
                  <ReferenceLine
                    key={`sell-${i}`}
                    x={t.sell_date.slice(0, 10)}
                    stroke="var(--red)"
                    strokeWidth={2}
                    label={{ value: `S${row.trades.length > 1 ? i + 1 : ''}`, position: 'top', fill: 'var(--red)', fontSize: 11, fontWeight: 700 }}
                  />
                ),
              ])}
              <Line type="monotone" dataKey="close" stroke="var(--accent)" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function TradesPage() {
  const oneYearAgo = new Date()
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1)

  const [start, setStart]       = useState(oneYearAgo.toISOString().slice(0, 10))
  const [end,   setEnd]         = useState(new Date().toISOString().slice(0, 10))
  const [selected, setSelected] = useState(null)

  const { data: trades, isLoading, isError } = useQuery({
    queryKey: ['trades', start, end],
    queryFn: () => getTrades({ start, end }),
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

  const th = (label, col) => <Th label={label} col={col} sort={sort} onSort={toggle} />

  return (
    <div>
      <p className="page-title">Historical Trades</p>

      <div className="controls">
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>From</label>
        <input type="date" value={start} onChange={e => setStart(e.target.value)} />
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>To</label>
        <input type="date" value={end} onChange={e => setEnd(e.target.value)} />
        {[3, 6].map(m => (
          <button
            key={m}
            className="period-chip"
            onClick={() => { setStart(isoMonthsAgo(m)); setEnd(new Date().toISOString().slice(0, 10)) }}
          >{m}M</button>
        ))}
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
            <div className="label">Annual Yield</div>
            {(() => {
              const annualYield = avgReturn != null && avgDays != null && avgDays > 0
                ? (avgReturn / avgDays) * 365
                : null
              return (
                <div className={`value ${annualYield >= 0 ? 'up' : 'down'}`}>
                  {annualYield != null ? `${annualYield >= 0 ? '+' : ''}${fmt(annualYield)}%` : '—'}
                </div>
              )
            })()}
          </div>
        </div>
      )}

      {trades && total === 0 && (
        <p className="empty">No completed BUY→SELL trades in the selected period.</p>
      )}

      {trades && total > 0 && (
        <div className="data-table-wrap">
          <table>
            <thead>
              <tr>
                <th></th>
                {th('Ticker',      'ticker')}
                {th('Company',     'company')}
                {th('Market Cap (B)', 'market_cap')}
                {th('Trades',      'trade_count')}
                {th('Win Rate',    'win_count')}
                {th('Avg Return',  'avg_return')}
                {th('Avg Days',    'avg_days')}
              </tr>
            </thead>
            <tbody>
              {sorted.map((t, i) => {
                const winRate = t.trade_count > 0 ? (t.win_count / t.trade_count) * 100 : 0
                return (
                  <tr key={i} className="clickable-row" onClick={() => setSelected(t)}>
                    <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                    <td><strong>{t.ticker}</strong></td>
                    <td>{t.company ?? '—'}</td>
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
      )}

      {selected && <TradeModal row={selected} start={start} end={end} onClose={() => setSelected(null)} />}
    </div>
  )
}
