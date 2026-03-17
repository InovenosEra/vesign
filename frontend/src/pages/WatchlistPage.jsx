import { useState, useMemo, useRef, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getWatchlists, createWatchlist, deleteWatchlist,
  getWatchlistTickers, addTicker, removeTicker,
  getSignalsByTickers, searchTickers,
  getHoldings, addHolding, deleteHolding,
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
    <td title={_HEALTH_LABELS[score]}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2, alignItems: 'flex-start' }}>
        <div style={{ display: 'flex', gap: 3 }}>
          {[1,2,3,4,5].map(i => (
            <div key={i} style={{
              width: 10, height: 10, borderRadius: 2,
              background: i <= score ? _HEALTH_COLORS[score] : 'var(--border)',
            }} />
          ))}
        </div>
        <span style={{ fontSize: 11, color: _HEALTH_COLORS[score] }}>
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

  const [expandedTickers, setExpandedTickers] = useState({})
  const [newLot, setNewLot] = useState({})   // { [ticker]: { quantity, buy_price, buy_date } }

  const { data: holdings = [] } = useQuery({
    queryKey: ['watchlist-holdings', selectedId],
    queryFn: () => getHoldings(selectedId),
    enabled: selectedId != null,
  })

  // Group holdings by ticker
  const holdingsByTicker = useMemo(() => {
    const map = {}
    for (const h of holdings) {
      if (!map[h.ticker]) map[h.ticker] = []
      map[h.ticker].push(h)
    }
    return map
  }, [holdings])

  const invalidateLists    = () => qc.invalidateQueries({ queryKey: ['watchlists'] })
  const invalidateTickers  = () => qc.invalidateQueries({ queryKey: ['watchlist-tickers', selectedId] })
  const invalidateHoldings = () => qc.invalidateQueries({ queryKey: ['watchlist-holdings', selectedId] })

  const addHoldingMut = useMutation({
    mutationFn: (body) => addHolding(selectedId, body),
    onSuccess: (_, body) => {
      invalidateHoldings()
      setNewLot(prev => ({ ...prev, [body.ticker]: { quantity: '', buy_price: '', buy_date: '' } }))
    },
  })

  const deleteHoldingMut = useMutation({
    mutationFn: (hid) => deleteHolding(selectedId, hid),
    onSuccess: invalidateHoldings,
  })

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

      {/* ── List selector row ── */}
      <div className="controls" style={{ marginBottom: 20 }}>
        <select
          value={selectedId ?? ''}
          onChange={e => setSelectedId(e.target.value ? Number(e.target.value) : null)}
          style={{ minWidth: 180, fontSize: 15, fontWeight: 600 }}
        >
          <option value="">— select a list —</option>
          {lists.map(l => <option key={l.id} value={l.id}>{l.name}</option>)}
        </select>
        {marketOpen && tickerSymbols.length > 0 && (
          <span style={{ color: 'var(--green)', fontSize: 12 }}>● live</span>
        )}
        {selectedId && (
          <button
            className="danger"
            style={{ padding: '4px 10px', fontSize: 12 }}
            onClick={() => {
              if (window.confirm(`Delete "${selectedList?.name}"? This cannot be undone.`)) {
                deleteMut.mutate(selectedId)
              }
            }}
          >Delete list</button>
        )}
        <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8 }}>
          <input
            placeholder="New list name…"
            value={newListName}
            onChange={e => setNewListName(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && newListName.trim() && createMut.mutate()}
            style={{ width: 180 }}
          />
          <button
            className="primary"
            onClick={() => createMut.mutate()}
            disabled={!newListName.trim() || createMut.isPending}
          >+ Create</button>
        </span>
        {createMut.isError && (
          <span className="error" style={{ fontSize: 12 }}>{createMut.error.message}</span>
        )}
      </div>

      {!selectedList ? (
        <p className="empty">Select a list or create a new one above.</p>
      ) : (
        <>

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
              ) : (() => {
                // ── Portfolio summary ──────────────────────────────────────
                let totalInvested = 0, totalValue = 0
                for (const t of sorted) {
                  const lots = holdingsByTicker[t.ticker] || []
                  const isIL = t.ticker?.endsWith('.TA')
                  const currentPrice = (() => {
                    const live = prices[t.ticker]
                    const raw = live ?? t.close
                    return raw != null ? (isIL ? raw / 100 : raw) : null
                  })()
                  for (const lot of lots) {
                    const cost = lot.quantity * lot.buy_price
                    totalInvested += cost
                    if (currentPrice != null) totalValue += lot.quantity * currentPrice
                  }
                }
                const totalPnlAbs = totalValue - totalInvested
                const totalPnlPct = totalInvested > 0 ? (totalPnlAbs / totalInvested) * 100 : null
                const hasHoldings = totalInvested > 0

                return (<>
                  {hasHoldings && (
                    <div className="metrics" style={{ marginBottom: 16 }}>
                      <div className="metric-card">
                        <div className="label">Total Invested</div>
                        <div className="value">${totalInvested.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                      </div>
                      <div className="metric-card">
                        <div className="label">Current Value</div>
                        <div className="value">${totalValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                      </div>
                      <div className="metric-card">
                        <div className="label">Total P&L ($)</div>
                        <div className={`value ${totalPnlAbs >= 0 ? 'up' : 'down'}`}>
                          {totalPnlAbs >= 0 ? '+' : ''}${Math.abs(totalPnlAbs).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </div>
                      </div>
                      <div className="metric-card">
                        <div className="label">Total P&L (%)</div>
                        <div className={`value ${totalPnlPct >= 0 ? 'up' : 'down'}`}>
                          {totalPnlPct != null ? `${totalPnlPct >= 0 ? '+' : ''}${totalPnlPct.toFixed(2)}%` : '—'}
                        </div>
                      </div>
                    </div>
                  )}

                  <div className="data-table-wrap">
                    <table style={{ tableLayout: 'fixed', width: '100%' }}>
                      <colgroup>
                        <col style={{ width: '3%' }} />   {/* logo */}
                        <col style={{ width: '5%' }} />   {/* ticker */}
                        <col style={{ width: '12%' }} />  {/* company */}
                        <col style={{ width: '6%' }} />   {/* signal */}
                        <col style={{ width: '7%' }} />   {/* market cap */}
                        <col style={{ width: '6%' }} />   {/* price */}
                        <col style={{ width: '5%' }} />   {/* rsi */}
                        <col style={{ width: '6%' }} />   {/* health */}
                        <col style={{ width: '6%' }} />   {/* upside */}
                        <col style={{ width: '6%' }} />   {/* ml score */}
                        <col style={{ width: '8%' }} />   {/* live price */}
                        <col style={{ width: '5%' }} />   {/* qty */}
                        <col style={{ width: '6%' }} />   {/* avg price */}
                        <col style={{ width: '7%' }} />   {/* invested */}
                        <col style={{ width: '5%' }} />   {/* yield */}
                        <col style={{ width: '6%' }} />   {/* actions */}
                      </colgroup>
                      <thead>
                        <tr>
                          <th></th>
                          {th('Ticker',         'ticker')}
                          {th('Company',        'company')}
                          {th('Signal',         'signal')}
                          {th('Mkt Cap (B)',    'market_cap')}
                          {th('Price',          'close')}
                          {th('RSI',            'rsi')}
                          <th>Health</th>
                          <th>Upside</th>
                          <th>ML Score</th>
                          <th>Live Price</th>
                          <th>Qty</th>
                          <th>Avg Price</th>
                          <th>Invested</th>
                          <th>Yield</th>
                          <th></th>
                        </tr>
                      </thead>
                      <tbody>
                        {sorted.map(t => {
                          const lots = holdingsByTicker[t.ticker] || []
                          const isIL = t.ticker?.endsWith('.TA')
                          const currentPrice = (() => {
                            const live = prices[t.ticker]
                            const raw = live ?? t.close
                            return raw != null ? (isIL ? raw / 100 : raw) : null
                          })()
                          const totalQty = lots.reduce((s, l) => s + l.quantity, 0)
                          const totalCost = lots.reduce((s, l) => s + l.quantity * l.buy_price, 0)
                          const avgPrice = totalQty > 0 ? totalCost / totalQty : null
                          const currentVal = currentPrice != null && totalQty > 0 ? currentPrice * totalQty : null
                          const yieldPct = currentVal != null && totalCost > 0 ? ((currentVal - totalCost) / totalCost) * 100 : null
                          const isExpanded = !!expandedTickers[t.ticker]
                          const lot = newLot[t.ticker] || { quantity: '', buy_price: '', buy_date: '' }

                          const clip = { overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }

                          return [
                            <tr key={t.ticker} className="clickable-row" onClick={() => setSelected(t)}>
                              <td>{t.logo_url ? <img className="logo" src={t.logo_url} alt="" /> : null}</td>
                              <td style={clip}><strong>{displayTicker(t.ticker)}</strong></td>
                              <td style={clip}>{t.company ?? '—'}</td>
                              <td>{t.signal ? <span className={`badge badge-${t.signal}`}>{t.signal}</span> : '—'}</td>
                              <td>{fmtMktCap(t.market_cap, t.ticker)}</td>
                              <td>{fmtPrice(t.close, t.ticker)}</td>
                              <td>{t.rsi != null ? t.rsi.toFixed(1) : '—'}</td>
                              <HealthCell score={t.health_score} />
                              <UpsideCell targetMean={t.target_mean_price} close={t.close} prices={prices} ticker={t.ticker} />
                              <td>{t.prediction_score != null ? <span className={t.prediction_score >= 0 ? 'up' : 'down'}>{t.prediction_score >= 0 ? '▲' : '▼'} {Math.abs(t.prediction_score * 100).toFixed(1)}%</span> : '—'}</td>
                              <LivePriceCell ticker={t.ticker} closePrice={t.close} prices={prices} marketOpen={marketOpen} />
                              <td>{totalQty > 0 ? totalQty : '—'}</td>
                              <td>{avgPrice != null ? avgPrice.toFixed(2) : '—'}</td>
                              <td>{totalCost > 0 ? `$${totalCost.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}</td>
                              <td className={yieldPct != null ? (yieldPct >= 0 ? 'up' : 'down') : ''}>
                                {yieldPct != null ? `${yieldPct >= 0 ? '+' : ''}${yieldPct.toFixed(2)}%` : '—'}
                              </td>
                              <td onClick={e => e.stopPropagation()} style={{ whiteSpace: 'nowrap', verticalAlign: 'middle' }}>
                                <button
                                  style={{ padding: '2px 6px', fontSize: 10, marginRight: 4 }}
                                  onClick={() => setExpandedTickers(prev => ({ ...prev, [t.ticker]: !prev[t.ticker] }))}
                                >{isExpanded ? '▲' : '▼'}</button>
                                <button
                                  className="danger"
                                  style={{ padding: '4px 8px', fontSize: 14, border: 'none', background: 'transparent', color: '#e74c3c' }}
                                  onClick={() => removeMut.mutate(t.ticker)}
                                  title="Remove from watchlist"
                                >🗑</button>
                              </td>
                            </tr>,

                            isExpanded && (
                              <tr key={`${t.ticker}-lots`}>
                                <td colSpan={16} style={{ padding: '0 0 0 48px', background: 'var(--bg)' }}>
                                  <div style={{ padding: '12px 16px', borderLeft: '3px solid var(--accent)' }}>
                                    {/* Existing lots */}
                                    {lots.length === 0
                                      ? <p style={{ fontSize: 12, color: 'var(--muted)', margin: '0 0 10px' }}>No lots yet.</p>
                                      : <table style={{ fontSize: 12, borderCollapse: 'collapse', marginBottom: 10, width: 'auto' }}>
                                          <thead>
                                            <tr style={{ color: 'var(--muted)' }}>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>Date</th>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>Qty</th>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>Buy Price</th>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>Cost</th>
                                              <th style={{ padding: '2px 0', fontWeight: 500 }}>Yield</th>
                                              <th></th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {lots.map(l => {
                                              const lotYield = currentPrice != null
                                                ? ((currentPrice - l.buy_price) / l.buy_price) * 100 : null
                                              return (
                                                <tr key={l.id}>
                                                  <td style={{ padding: '3px 16px 3px 0' }}>{l.buy_date}</td>
                                                  <td style={{ padding: '3px 16px 3px 0' }}>{l.quantity}</td>
                                                  <td style={{ padding: '3px 16px 3px 0' }}>${l.buy_price.toFixed(2)}</td>
                                                  <td style={{ padding: '3px 16px 3px 0' }}>${(l.quantity * l.buy_price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                                                  <td className={lotYield != null ? (lotYield >= 0 ? 'up' : 'down') : ''} style={{ padding: '3px 16px 3px 0' }}>
                                                    {lotYield != null ? `${lotYield >= 0 ? '+' : ''}${lotYield.toFixed(2)}%` : '—'}
                                                  </td>
                                                  <td>
                                                    <button className="danger" style={{ padding: '2px 8px', fontSize: 11 }}
                                                      onClick={() => deleteHoldingMut.mutate(l.id)}>✕</button>
                                                  </td>
                                                </tr>
                                              )
                                            })}
                                          </tbody>
                                        </table>
                                    }
                                    {/* Add lot form */}
                                    <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                                      <input
                                        type="number" min="0" step="any"
                                        placeholder="Qty"
                                        value={lot.quantity}
                                        onChange={e => setNewLot(prev => ({ ...prev, [t.ticker]: { ...lot, quantity: e.target.value } }))}
                                        style={{ width: 80 }}
                                      />
                                      <input
                                        type="number" min="0" step="any"
                                        placeholder="Buy price"
                                        value={lot.buy_price}
                                        onChange={e => setNewLot(prev => ({ ...prev, [t.ticker]: { ...lot, buy_price: e.target.value } }))}
                                        style={{ width: 100 }}
                                      />
                                      <input
                                        type="date"
                                        value={lot.buy_date}
                                        onChange={e => setNewLot(prev => ({ ...prev, [t.ticker]: { ...lot, buy_date: e.target.value } }))}
                                        style={{ width: 140 }}
                                      />
                                      <button
                                        className="primary"
                                        style={{ padding: '4px 12px', fontSize: 12 }}
                                        disabled={!lot.quantity || !lot.buy_price || !lot.buy_date || addHoldingMut.isPending}
                                        onClick={() => addHoldingMut.mutate({ ticker: t.ticker, quantity: parseFloat(lot.quantity), buy_price: parseFloat(lot.buy_price), buy_date: lot.buy_date })}
                                      >+ Add lot</button>
                                    </div>
                                  </div>
                                </td>
                              </tr>
                            ),
                          ]
                        })}
                      </tbody>
                    </table>
                  </div>
                </>)
              })()}
        </>
      )}
      {selected && <SignalModal row={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
