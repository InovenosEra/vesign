import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getWatchlists, createWatchlist, deleteWatchlist,
  getWatchlistTickers, addTicker, removeTicker,
  getSignalsByTickers, getPriceHistory,
} from '../api'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'

function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
}

function SignalModal({ row, onClose }) {
  const today     = new Date().toISOString().slice(0, 10)
  const end12m    = today
  const target12m = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); return d.toISOString().slice(0, 10) })()
  const start12m  = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()

  const { data: history = [], isLoading } = useQuery({
    queryKey: ['price-history-signal', row.ticker],
    queryFn: () => getPriceHistory(row.ticker, { start: start12m, end: end12m }),
    staleTime: 300_000,
  })

  const base12m  = history.filter(d => d.date <= target12m).at(-1)
  const yield12m = base12m && history.length > 0
    ? ((history.at(-1).close - base12m.close) / base12m.close) * 100
    : null

  const minPrice = history.length ? Math.min(...history.map(d => d.close)) * 0.97 : 0
  const maxPrice = history.length ? Math.max(...history.map(d => d.close)) * 1.03 : 0

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>
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
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Signal</td>
                <td style={{ verticalAlign: 'middle' }}>
                  {row.signal ? <span className={`badge badge-${row.signal}`}>{row.signal}</span> : '—'}
                </td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Price</td>
                <td style={{ verticalAlign: 'middle' }}>{row.close != null ? `$${fmt(row.close)}` : '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>RSI</td>
                <td style={{ verticalAlign: 'middle' }}>{row.rsi != null ? row.rsi.toFixed(1) : '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Prediction</td>
                <td style={{ verticalAlign: 'middle' }} className={row.fair_value_upside == null ? '' : row.fair_value_upside >= 0 ? 'up' : 'down'}>
                  {row.fair_value_upside != null ? `${row.fair_value_upside >= 0 ? '+' : ''}${fmt(row.fair_value_upside * 100)}%` : '—'}
                </td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, verticalAlign: 'middle' }}>12M Yield (organic)</td>
                <td style={{ verticalAlign: 'middle' }} className={yield12m == null ? '' : yield12m >= 0 ? 'up' : 'down'}>
                  {yield12m != null ? `${yield12m >= 0 ? '+' : ''}${fmt(yield12m)}%` : '—'}
                </td>
              </tr>
            </tbody>
          </table>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>
        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>Loading chart…</p>
        ) : (
          <ResponsiveContainer width="100%" height={340}>
            <LineChart data={history} margin={{ top: 16, right: 24, bottom: 8, left: 8 }}>
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
                formatter={v => [`$${v.toFixed(2)}`, 'Close']}
              />
              <Line type="monotone" dataKey="close" stroke="var(--accent)" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
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

function PredictionCell({ value }) {
  if (value == null) return <td>—</td>
  const pct = (value * 100).toFixed(2)
  return <td className={value > 0 ? 'up' : 'down'}>{value > 0 ? '▲' : '▼'} {Math.abs(pct)}%</td>
}

function LivePriceCell({ ticker, closePrice, prices, marketOpen }) {
  if (marketOpen === null) return <td style={{ color: 'var(--muted)' }}>—</td>
  if (!marketOpen) return <td style={{ color: 'var(--muted)', fontSize: 12 }}>Market Closed</td>
  const live = prices[ticker]
  if (live == null) return <td style={{ color: 'var(--muted)' }}>—</td>
  const diff  = live - closePrice
  const pct   = closePrice ? (diff / closePrice) * 100 : 0
  const cls   = diff >= 0 ? 'up' : 'down'
  const arrow = diff >= 0 ? '▲' : '▼'
  return (
    <td>
      <div>{live.toFixed(2)}</div>
      <div className={cls} style={{ fontSize: 11 }}>
        {arrow} {Math.abs(diff).toFixed(2)} ({Math.abs(pct).toFixed(2)}%)
      </div>
    </td>
  )
}

export default function WatchlistPage() {
  const qc = useQueryClient()
  const [selectedId, setSelectedId]   = useState(null)
  const [newListName, setNewListName] = useState('')
  const [newTicker, setNewTicker]     = useState('')
  const [selected, setSelected]       = useState(null)

  const { data: lists = [] } = useQuery({
    queryKey: ['watchlists'],
    queryFn: getWatchlists,
  })

  const { data: tickers = [], isLoading: loadingTickers } = useQuery({
    queryKey: ['watchlist-tickers', selectedId],
    queryFn: () => getWatchlistTickers(selectedId),
    enabled: selectedId != null,
  })

  const tickerSymbols = useMemo(() => tickers.map(t => t.ticker), [tickers])

  const { data: signalData = [] } = useQuery({
    queryKey: ['watchlist-signals', tickerSymbols.join(',')],
    queryFn: () => getSignalsByTickers(tickerSymbols),
    enabled: tickerSymbols.length > 0,
    staleTime: 60_000,
  })

  // Merge watchlist rows with signal data
  const merged = useMemo(() => {
    const sigMap = Object.fromEntries(signalData.map(s => [s.ticker, s]))
    return tickers.map(t => ({ ...t, ...(sigMap[t.ticker] ?? {}) }))
  }, [tickers, signalData])

  const { prices, marketOpen } = useLivePrices(tickerSymbols)
  const { sorted, sort, toggle } = useSort(merged, 'ticker', 'asc')

  const invalidateLists   = () => qc.invalidateQueries({ queryKey: ['watchlists'] })
  const invalidateTickers = () => qc.invalidateQueries({ queryKey: ['watchlist-tickers', selectedId] })

  const createMut = useMutation({
    mutationFn: () => createWatchlist(newListName.trim()),
    onSuccess: (created) => { invalidateLists(); setNewListName(''); setSelectedId(created.id) },
  })

  const deleteMut = useMutation({
    mutationFn: (id) => deleteWatchlist(id),
    onSuccess: (_, id) => { invalidateLists(); if (selectedId === id) setSelectedId(null) },
  })

  const addMut = useMutation({
    mutationFn: () => addTicker(selectedId, newTicker),
    onSuccess: () => { invalidateTickers(); setNewTicker('') },
  })

  const removeMut = useMutation({
    mutationFn: (ticker) => removeTicker(selectedId, ticker),
    onSuccess: invalidateTickers,
  })

  const selectedList = lists.find(l => l.id === selectedId)

  const th = (label, col) => <Th label={label} col={col} sort={sort} onSort={toggle} />

  return (
    <div>
      <p className="page-title">Watchlists</p>
      <div className="watchlist-layout">

        {/* ── Sidebar ── */}
        <div className="watchlist-sidebar">
          <h3>My Lists</h3>
          {lists.length === 0 && <p className="empty" style={{ fontSize: 12 }}>No lists yet.</p>}
          {lists.map(l => (
            <div
              key={l.id}
              className={`list-item ${l.id === selectedId ? 'selected' : ''}`}
              onClick={() => setSelectedId(l.id)}
            >
              <span>{l.name}</span>
              <button
                className="del-btn"
                onClick={e => { e.stopPropagation(); deleteMut.mutate(l.id) }}
                title="Delete list"
              >✕</button>
            </div>
          ))}
          <div className="new-list-row">
            <input
              placeholder="New list name…"
              value={newListName}
              onChange={e => setNewListName(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && newListName.trim() && createMut.mutate()}
            />
            <button
              className="primary"
              onClick={() => createMut.mutate()}
              disabled={!newListName.trim() || createMut.isPending}
            >+</button>
          </div>
          {createMut.isError && (
            <p className="error" style={{ fontSize: 12, marginTop: 6 }}>{createMut.error.message}</p>
          )}
        </div>

        {/* ── Main panel ── */}
        <div>
          {!selectedList ? (
            <p className="empty">Select or create a watchlist.</p>
          ) : (
            <>
              <p className="section-title">
                {selectedList.name}
                {marketOpen && tickerSymbols.length > 0 && (
                  <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>
                )}
              </p>

              {/* Add ticker row */}
              <div className="controls" style={{ marginBottom: 20 }}>
                <input
                  placeholder="Ticker (e.g. AAPL)"
                  value={newTicker}
                  onChange={e => setNewTicker(e.target.value.toUpperCase())}
                  onKeyDown={e => e.key === 'Enter' && newTicker && addMut.mutate()}
                  style={{ width: 120 }}
                />
                <button
                  className="primary"
                  onClick={() => addMut.mutate()}
                  disabled={!newTicker || addMut.isPending}
                >Add</button>
                {addMut.isError && <span className="error">{addMut.error.message}</span>}
              </div>

              {loadingTickers ? (
                <p className="loading">Loading…</p>
              ) : tickers.length === 0 ? (
                <p className="empty">No tickers yet. Add one above.</p>
              ) : (
                <div className="data-table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th></th>
                        {th('Ticker',         'ticker')}
                        {th('Company',        'company')}
                        {th('Signal',         'signal')}
                        {th('Market Cap (B)', 'market_cap')}
                        {th('Price',          'close')}
                        {th('RSI',            'rsi')}
                        {th('Prediction',     'fair_value_upside')}
                        <th>Live Price</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {sorted.map(t => (
                        <tr key={t.ticker} className="clickable-row" onClick={() => setSelected(t)}>
                          <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                          <td><strong>{t.ticker}</strong></td>
                          <td>{t.company ?? '—'}</td>
                          <td>{t.signal ? <span className={`badge badge-${t.signal}`}>{t.signal}</span> : '—'}</td>
                          <td>{t.market_cap != null ? (t.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
                          <td>{t.close != null ? t.close.toFixed(2) : '—'}</td>
                          <td>{t.rsi != null ? t.rsi.toFixed(1) : '—'}</td>
                          <PredictionCell value={t.fair_value_upside} />
                          <LivePriceCell
                            ticker={t.ticker}
                            closePrice={t.close}
                            prices={prices}
                            marketOpen={marketOpen}
                          />
                          <td onClick={e => e.stopPropagation()}>
                            <button
                              className="danger"
                              style={{ padding: '4px 10px', fontSize: 12 }}
                              onClick={() => removeMut.mutate(t.ticker)}
                            >Remove</button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </div>
      </div>
      {selected && <SignalModal row={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
