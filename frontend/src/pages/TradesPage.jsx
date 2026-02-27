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
// Trade chart modal
// ---------------------------------------------------------------------------

function TradeModal({ trade, onClose }) {
  const { data: history = [], isLoading } = useQuery({
    queryKey: ['price-history', trade.ticker],
    queryFn: () => getPriceHistory(trade.ticker, 12),
    staleTime: 300_000,
  })

  // Normalise buy/sell date strings to YYYY-MM-DD for comparison
  const buyKey  = trade.buy_date  ? trade.buy_date.slice(0, 10)  : null
  const sellKey = trade.sell_date ? trade.sell_date.slice(0, 10) : null

  const minPrice = history.length ? Math.min(...history.map(d => d.close)) * 0.97 : 0
  const maxPrice = history.length ? Math.max(...history.map(d => d.close)) * 1.03 : 0

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <span>
            {trade.logo_url && <img className="logo" src={trade.logo_url} alt="" style={{ marginRight: 8 }} />}
            <strong>{trade.ticker}</strong>
            {trade.company ? ` — ${trade.company}` : ''}
          </span>
          <div className="modal-meta">
            <span className="up">▲ BUY {fmtDate(trade.buy_date)} @ {fmt(trade.buy_price)}</span>
            <span className="down">▼ SELL {fmtDate(trade.sell_date)} @ {fmt(trade.sell_price)}</span>
            <span className={trade.return_pct >= 0 ? 'up' : 'down'}>
              {trade.return_pct >= 0 ? '+' : ''}{fmt(trade.return_pct)}% · {trade.days_held}d
            </span>
          </div>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>Loading chart…</p>
        ) : (
          <ResponsiveContainer width="100%" height={340}>
            <LineChart data={history} margin={{ top: 20, right: 24, bottom: 8, left: 8 }}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis
                dataKey="date"
                tick={{ fill: 'var(--muted)', fontSize: 11 }}
                tickFormatter={d => {
                  const [, m, day] = d.split('-')
                  return `${day}/${m}`
                }}
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
              {buyKey && (
                <ReferenceLine
                  x={buyKey}
                  stroke="var(--green)"
                  strokeWidth={2}
                  label={{ value: 'BUY', position: 'top', fill: 'var(--green)', fontSize: 11, fontWeight: 700 }}
                />
              )}
              {sellKey && (
                <ReferenceLine
                  x={sellKey}
                  stroke="var(--red)"
                  strokeWidth={2}
                  label={{ value: 'SELL', position: 'top', fill: 'var(--red)', fontSize: 11, fontWeight: 700 }}
                />
              )}
              <Line
                type="monotone"
                dataKey="close"
                stroke="var(--accent)"
                dot={false}
                strokeWidth={2}
              />
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

  const { sorted, sort, toggle } = useSort(trades, 'return_pct', 'desc')

  const wins      = trades ? trades.filter(t => t.result === 'Win').length : 0
  const total     = trades ? trades.length : 0
  const avgReturn = total > 0 ? trades.reduce((s, t) => s + t.return_pct, 0) / total : null
  const avgDays   = total > 0 ? trades.reduce((s, t) => s + t.days_held,   0) / total : null

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
            <div className="label">Trades</div>
            <div className="value">{total}</div>
          </div>
          <div className="metric-card">
            <div className="label">Win Rate</div>
            <div className="value">{((wins / total) * 100).toFixed(1)}%</div>
          </div>
          <div className="metric-card">
            <div className="label">Avg Return</div>
            <div className={`value ${avgReturn >= 0 ? 'up' : 'down'}`}>
              {avgReturn >= 0 ? '+' : ''}{fmt(avgReturn)}%
            </div>
          </div>
          <div className="metric-card">
            <div className="label">Avg Days Held</div>
            <div className="value">{avgDays != null ? Math.round(avgDays) : '—'}</div>
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
                {th('Ticker',     'ticker')}
                {th('Company',    'company')}
                {th('Buy Date',   'buy_date')}
                {th('Sell Date',  'sell_date')}
                {th('Buy Price',  'buy_price')}
                {th('Sell Price', 'sell_price')}
                {th('Return',     'return_pct')}
                {th('Days Held',  'days_held')}
                {th('Result',     'result')}
              </tr>
            </thead>
            <tbody>
              {sorted.map((t, i) => (
                <tr
                  key={i}
                  className="clickable-row"
                  onClick={() => setSelected(t)}
                >
                  <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                  <td><strong>{t.ticker}</strong></td>
                  <td>{t.company ?? '—'}</td>
                  <td>{fmtDate(t.buy_date)}</td>
                  <td>{fmtDate(t.sell_date)}</td>
                  <td>{fmt(t.buy_price)}</td>
                  <td>{fmt(t.sell_price)}</td>
                  <td className={t.return_pct >= 0 ? 'up' : 'down'}>
                    {t.return_pct >= 0 ? '▲' : '▼'} {Math.abs(t.return_pct).toFixed(2)}%
                  </td>
                  <td>{t.days_held}</td>
                  <td className={t.result === 'Win' ? 'up' : 'down'}>{t.result}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {selected && <TradeModal trade={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
