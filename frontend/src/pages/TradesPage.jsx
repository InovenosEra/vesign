import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getTrades } from '../api'
import { useSort } from '../hooks/useSort'

function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
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

export default function TradesPage() {
  const oneYearAgo = new Date()
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1)

  const [start, setStart] = useState(oneYearAgo.toISOString().slice(0, 10))
  const [end,   setEnd]   = useState(new Date().toISOString().slice(0, 10))

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
                <tr key={i}>
                  <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                  <td><strong>{t.ticker}</strong></td>
                  <td>{t.company ?? '—'}</td>
                  <td>{t.buy_date}</td>
                  <td>{t.sell_date}</td>
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
    </div>
  )
}
