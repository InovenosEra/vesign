import { useState, useMemo, useRef, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getWatchlists, createWatchlist, deleteWatchlist,
  getWatchlistTickers, addTicker, removeTicker,
  getSignalsByTickers, searchTickers,
} from '../api'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'
import SignalModal from '../components/SignalModal'

function tickerMarket(ticker) { return ticker?.endsWith('.TA') ? 'IL' : 'US' }

function fmtPrice(n, ticker) {
  if (n == null) return '—'
  const isIL = tickerMarket(ticker) === 'IL'
  const val = isIL ? n / 100 : n
  return Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function fmtMktCap(n, ticker) {
  if (n == null) return '—'
  return (n / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 })
}

function displayTicker(ticker) { return ticker ? ticker.replace(/\.TA$/, '') : '—' }

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

function UpsideCell({ targetMean, close, prices, ticker }) {
  const currentPrice = (prices && prices[ticker]) || close
  if (targetMean == null || !currentPrice) return <td>—</td>
  const pct = ((targetMean - currentPrice) / currentPrice) * 100
  return <td className={pct >= 0 ? 'up' : 'down'}>{pct >= 0 ? '▲' : '▼'} {Math.abs(pct).toFixed(1)}%</td>
}

function LivePriceCell({ ticker, closePrice, prices, marketOpen }) {
  const isIL = tickerMarket(ticker) === 'IL'
  const isOpen = isIL
    ? (marketOpen !== false)   // TASE: open unless explicitly false
    : marketOpen
  if (isOpen === null) return <td style={{ color: 'var(--muted)' }}>—</td>
  if (!isOpen) return <td style={{ color: 'var(--muted)', fontSize: 12 }}>Market Closed</td>
  const live = prices[ticker]
  if (live == null) return <td style={{ color: 'var(--muted)' }}>—</td>
  const displayLive  = isIL ? live / 100 : live
  const displayClose = isIL ? closePrice / 100 : closePrice
  const diff = displayLive - displayClose
  const pct  = displayClose ? (diff / displayClose) * 100 : 0
  const cls  = diff >= 0 ? 'up' : 'down'
  const arrow = diff >= 0 ? '▲' : '▼'
  return (
    <td>
      <div>{displayLive.toFixed(2)}</div>
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
  const [newTicker, setNewTicker]         = useState('')
  const [tickerResults, setTickerResults] = useState([])
  const [tickerDropdown, setTickerDropdown] = useState(false)
  const [tickerActiveIdx, setTickerActiveIdx] = useState(-1)
  const tickerInputRef   = useRef(null)
  const tickerDropdownRef = useRef(null)
  const tickerDebounceRef = useRef(null)
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
    mutationFn: (ticker) => addTicker(selectedId, ticker ?? newTicker),
    onSuccess: () => { invalidateTickers(); setNewTicker(''); setTickerResults([]); setTickerDropdown(false) },
  })

  // Close dropdown on outside click
  useEffect(() => {
    function onMouseDown(e) {
      if (
        tickerDropdownRef.current && !tickerDropdownRef.current.contains(e.target) &&
        tickerInputRef.current    && !tickerInputRef.current.contains(e.target)
      ) setTickerDropdown(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  function handleTickerChange(e) {
    const val = e.target.value.toUpperCase()
    setNewTicker(val)
    setTickerActiveIdx(-1)
    if (tickerDebounceRef.current) clearTimeout(tickerDebounceRef.current)
    if (!val.trim()) { setTickerResults([]); setTickerDropdown(false); return }
    tickerDebounceRef.current = setTimeout(async () => {
      try {
        const data = await searchTickers(val.trim())
        setTickerResults(data)
        setTickerDropdown(true)
      } catch { setTickerResults([]) }
    }, 300)
  }

  function handleTickerKeyDown(e) {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setTickerDropdown(true)
      setTickerActiveIdx(i => Math.min(i + 1, tickerResults.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setTickerActiveIdx(i => Math.max(i - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      if (tickerActiveIdx >= 0 && tickerResults[tickerActiveIdx]) {
        const t = tickerResults[tickerActiveIdx].ticker
        setNewTicker(t)
        setTickerDropdown(false)
        addMut.mutate(t)
      } else if (newTicker) {
        addMut.mutate()
      }
    } else if (e.key === 'Escape') {
      setTickerDropdown(false)
    }
  }

  function handleTickerSelect(ticker) {
    setNewTicker(ticker)
    setTickerDropdown(false)
    addMut.mutate(ticker)
  }

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
                <div style={{ position: 'relative' }}>
                  <input
                    ref={tickerInputRef}
                    placeholder="Ticker or company…"
                    value={newTicker}
                    onChange={handleTickerChange}
                    onKeyDown={handleTickerKeyDown}
                    onFocus={() => tickerResults.length > 0 && setTickerDropdown(true)}
                    style={{ width: 200 }}
                  />
                  {tickerDropdown && tickerResults.length > 0 && (
                    <div
                      ref={tickerDropdownRef}
                      style={{
                        position: 'absolute',
                        top: 'calc(100% + 6px)',
                        left: 0,
                        width: 300,
                        background: 'var(--surface)',
                        border: '1px solid var(--border)',
                        borderRadius: 10,
                        zIndex: 1000,
                        boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
                        overflow: 'hidden',
                      }}
                    >
                      {tickerResults.map((r, i) => (
                        <div
                          key={r.ticker}
                          onMouseDown={() => handleTickerSelect(r.ticker)}
                          onMouseEnter={() => setTickerActiveIdx(i)}
                          onMouseLeave={() => setTickerActiveIdx(-1)}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 10,
                            padding: '8px 14px',
                            cursor: 'pointer',
                            background: i === tickerActiveIdx ? 'rgba(79,142,247,0.15)' : 'transparent',
                            borderBottom: i < tickerResults.length - 1 ? '1px solid var(--border)' : 'none',
                          }}
                        >
                          {r.logo_url
                            ? <img src={r.logo_url} alt="" style={{ width: 28, height: 28, borderRadius: 6, objectFit: 'contain', flexShrink: 0 }} onError={e => e.target.style.display = 'none'} />
                            : <div style={{ width: 28, height: 28, borderRadius: 6, background: 'var(--border)', flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 9, fontWeight: 'bold', color: 'var(--muted)' }}>{r.ticker.slice(0, 4)}</div>
                          }
                          <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{ fontWeight: 700, fontSize: 13 }}>{r.ticker}</div>
                            <div style={{ fontSize: 11, color: 'var(--muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.company}</div>
                          </div>
                          {r.signal && (
                            <span className={`badge badge-${r.signal}`} style={{ flexShrink: 0, fontSize: 10 }}>{r.signal}</span>
                          )}
                          {r.close != null && (
                            <span style={{ fontSize: 12, color: 'var(--muted)', flexShrink: 0 }}>${r.close.toFixed(2)}</span>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
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
                        <th>Health Score</th>
                        <th>Prediction</th>
                        <th>Live Price</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {sorted.map(t => (
                        <tr key={t.ticker} className="clickable-row" onClick={() => setSelected(t)}>
                          <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                          <td><strong>{displayTicker(t.ticker)}</strong></td>
                          <td>{t.company ?? '—'}</td>
                          <td>{t.signal ? <span className={`badge badge-${t.signal}`}>{t.signal}</span> : '—'}</td>
                          <td>{fmtMktCap(t.market_cap, t.ticker)}</td>
                          <td>{fmtPrice(t.close, t.ticker)}</td>
                          <td>{t.rsi != null ? t.rsi.toFixed(1) : '—'}</td>
                          <HealthCell score={t.health_score} />
                          <UpsideCell targetMean={t.target_mean_price} close={t.close} prices={prices} ticker={t.ticker} />
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
