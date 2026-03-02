import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getWatchlists, createWatchlist, deleteWatchlist,
  getWatchlistTickers, addTicker, removeTicker,
  getSignalsByTickers,
} from '../api'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'

function SignalBadge({ signal }) {
  if (!signal) return <span style={{ color: 'var(--muted)' }}>—</span>
  return <span className={`badge badge-${signal}`}>{signal}</span>
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
  if (!marketOpen) return <td style={{ color: 'var(--muted)', fontSize: 12 }}>Market Close</td>
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
                        {th('Ticker',      'ticker')}
                        {th('Company',     'company')}
                        {th('Market Cap (B)', 'market_cap')}
                        {th('Signal',      'signal')}
                        {th('Price',       'close')}
                        <th>Live Price</th>
                        {th('RSI',         'rsi')}
                        {th('Prediction',  'fair_value_upside')}
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {sorted.map(t => (
                        <tr key={t.ticker}>
                          <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                          <td><strong>{t.ticker}</strong></td>
                          <td>{t.company ?? '—'}</td>
                          <td>{t.market_cap != null ? (t.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
                          <td><SignalBadge signal={t.signal} /></td>
                          <td>{t.close != null ? t.close.toFixed(2) : '—'}</td>
                          <LivePriceCell
                            ticker={t.ticker}
                            closePrice={t.close}
                            prices={prices}
                            marketOpen={marketOpen}
                          />
                          <td>{t.rsi != null ? t.rsi.toFixed(1) : '—'}</td>
                          <PredictionCell value={t.fair_value_upside} />
                          <td>
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
    </div>
  )
}
