import { useState, useMemo, useRef, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import {
  getWatchlists, createWatchlist, deleteWatchlist,
  getWatchlistTickers, addTicker, removeTicker,
  getSignalsByTickers, searchTickers,
  getHoldings, addHolding, deleteHolding,
  WHITE_BG_LOGOS,
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
const _HEALTH_KEYS = ['', 'health.weak', 'health.fair', 'health.good', 'health.great', 'health.excellent']

function HealthCell({ score }) {
  const { t } = useTranslation()
  const label = score ? t(_HEALTH_KEYS[score]) : ''
  if (!score) return <td style={{ color: 'var(--muted)' }}>—</td>
  return (
    <td title={label}>
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
          {label}
        </span>
      </div>
    </td>
  )
}

function Th({ label, col, sort, onSort, className }) {
  const active = sort.key === col
  return (
    <th onClick={() => onSort(col)} style={{ cursor: 'pointer' }} className={className}>
      {label}{active ? <span style={{ marginLeft: 3 }}>{sort.dir === 'asc' ? '▲' : '▼'}</span> : null}
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
  const { t } = useTranslation()
  const isIL = tickerMarket(ticker) === 'IL'
  const isOpen = isIL
    ? (marketOpen !== false)   // TASE: open unless explicitly false
    : marketOpen
  if (isOpen === null) return <td style={{ color: 'var(--muted)' }}>—</td>
  if (!isOpen) return <td style={{ color: 'var(--muted)', fontSize: 12 }}>{t('market.closedShort')}</td>
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
  const { t } = useTranslation()
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
  const [confirmDelete, setConfirmDelete] = useState(null) // { id, name }

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

  const th = (label, col, className) => <Th label={t(label)} col={col} sort={sort} onSort={toggle} className={className} />

  return (
    <div>
      <p className="page-title">{t('watchlist.title')}</p>

      {/* ── List management row ── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20, flexWrap: 'wrap' }}>
        <input
          placeholder={t('watchlist.newListPlaceholder')}
          value={newListName}
          onChange={e => setNewListName(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && newListName.trim() && createMut.mutate()}
          style={{ width: 180, background: 'var(--surface)', border: '1px solid var(--border)', color: 'var(--text)', padding: '7px 12px', borderRadius: 6, fontSize: 14, outline: 'none' }}
        />
        <button
          className="primary"
          onClick={() => createMut.mutate()}
          disabled={!newListName.trim() || createMut.isPending}
        >{t('watchlist.create')}</button>
        {createMut.isError && (
          <span className="error" style={{ fontSize: 12 }}>{createMut.error.message}</span>
        )}

        {lists.map(l => (
          <div
            key={l.id}
            className={`list-card${selectedId === l.id ? ' active' : ''}`}
            onClick={() => setSelectedId(l.id)}
          >
            <span>{l.name}</span>
            <button
              className="card-delete"
              onClick={e => {
                e.stopPropagation()
                setConfirmDelete({ id: l.id, name: l.name })
              }}
              title={t('watchlist.deleteList')}
            >✕</button>
          </div>
        ))}

        {marketOpen && tickerSymbols.length > 0 && (
          <span style={{ color: 'var(--green)', fontSize: 12 }}>● live</span>
        )}
      </div>

      {!selectedList ? (
        <p className="empty">{t('watchlist.empty')}</p>
      ) : (
        <>

          {/* Add ticker row */}
          <div className="controls" style={{ marginBottom: 20 }}>
                <div style={{ position: 'relative' }}>
                  <input
                    ref={tickerInputRef}
                    placeholder={t('watchlist.addTickerPlaceholder')}
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
                            ? <img src={r.logo_url} alt="" style={{ width: 28, height: 28, borderRadius: 6, objectFit: 'contain', flexShrink: 0, ...(WHITE_BG_LOGOS.has(r.ticker) ? { background: '#fff', padding: 2 } : {}) }} onError={e => e.target.style.display = 'none'} />
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
                >{t('watchlist.add')}</button>
                {addMut.isError && <span className="error">{addMut.error.message}</span>}
              </div>

              {loadingTickers ? (
                <p className="loading">{t('table.loading')}</p>
              ) : tickers.length === 0 ? (
                <p className="empty">{t('watchlist.noTickers')}</p>
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
                        <div className="label">{t('watchlist.totalInvested')}</div>
                        <div className="value">${totalInvested.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                      </div>
                      <div className="metric-card">
                        <div className="label">{t('watchlist.currentValue')}</div>
                        <div className="value">${totalValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                      </div>
                      <div className="metric-card">
                        <div className="label">{t('watchlist.totalPnlAbs')}</div>
                        <div className={`value ${totalPnlAbs >= 0 ? 'up' : 'down'}`}>
                          {totalPnlAbs >= 0 ? '+' : ''}${Math.abs(totalPnlAbs).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </div>
                      </div>
                      <div className="metric-card">
                        <div className="label">{t('watchlist.totalPnlPct')}</div>
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
                          {th('col.ticker',         'ticker')}
                          {th('col.company',        'company')}
                          {th('col.signal',         'signal')}
                          {th('col.mktCap',         'market_cap', 'col-hide-sm')}
                          {th('col.price',          'close')}
                          {th('col.rsi',            'rsi')}
                          <th>{t('col.health')}</th>
                          <th>{t('col.upside')}</th>
                          <th className="col-hide-sm">{t('col.mlScore')}</th>
                          <th>{t('col.livePrice')}</th>
                          <th>{t('col.qty')}</th>
                          <th>{t('col.avgPrice')}</th>
                          <th>{t('col.invested')}</th>
                          <th>{t('col.yield')}</th>
                          <th></th>
                        </tr>
                      </thead>
                      <tbody>
                        {sorted.map(row => {
                          const lots = holdingsByTicker[row.ticker] || []
                          const isIL = row.ticker?.endsWith('.TA')
                          const currentPrice = (() => {
                            const live = prices[row.ticker]
                            const raw = live ?? row.close
                            return raw != null ? (isIL ? raw / 100 : raw) : null
                          })()
                          const totalQty = lots.reduce((s, l) => s + l.quantity, 0)
                          const totalCost = lots.reduce((s, l) => s + l.quantity * l.buy_price, 0)
                          const avgPrice = totalQty > 0 ? totalCost / totalQty : null
                          const currentVal = currentPrice != null && totalQty > 0 ? currentPrice * totalQty : null
                          const yieldPct = currentVal != null && totalCost > 0 ? ((currentVal - totalCost) / totalCost) * 100 : null
                          const isExpanded = !!expandedTickers[row.ticker]
                          const lot = newLot[row.ticker] || { quantity: '', buy_price: '', buy_date: '' }

                          const clip = { overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }

                          return [
                            <tr key={row.ticker} className="clickable-row" onClick={() => setSelected(row)}>
                              <td>{row.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(row.ticker) ? ' logo-white-bg' : ''}`} src={row.logo_url} alt="" /> : null}</td>
                              <td style={clip}><strong>{displayTicker(row.ticker)}</strong></td>
                              <td style={clip}>{row.company ?? '—'}</td>
                              <td>{row.signal ? <span className={`badge badge-${row.signal}`}>{row.signal}</span> : '—'}</td>
                              <td className="col-hide-sm">{fmtMktCap(row.market_cap, row.ticker)}</td>
                              <td>{fmtPrice(row.close, row.ticker)}</td>
                              <td>{row.rsi != null ? row.rsi.toFixed(1) : '—'}</td>
                              <HealthCell score={row.health_score} />
                              <UpsideCell targetMean={row.target_mean_price} close={row.close} prices={prices} ticker={row.ticker} />
                              <td className="col-hide-sm">{row.prediction_score != null ? <span className={row.prediction_score >= 0 ? 'up' : 'down'}>{row.prediction_score >= 0 ? '▲' : '▼'} {Math.abs(row.prediction_score * 100).toFixed(1)}%</span> : '—'}</td>
                              <LivePriceCell ticker={row.ticker} closePrice={row.close} prices={prices} marketOpen={marketOpen} />
                              <td>{totalQty > 0 ? totalQty : '—'}</td>
                              <td>{avgPrice != null ? avgPrice.toFixed(2) : '—'}</td>
                              <td>{totalCost > 0 ? `$${totalCost.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}</td>
                              <td className={yieldPct != null ? (yieldPct >= 0 ? 'up' : 'down') : ''}>
                                {yieldPct != null ? `${yieldPct >= 0 ? '+' : ''}${yieldPct.toFixed(2)}%` : '—'}
                              </td>
                              <td onClick={e => e.stopPropagation()} style={{ whiteSpace: 'nowrap', verticalAlign: 'middle' }}>
                                <button
                                  style={{ padding: '2px 6px', fontSize: 10, marginRight: 4 }}
                                  onClick={() => setExpandedTickers(prev => ({ ...prev, [row.ticker]: !prev[row.ticker] }))}
                                >{isExpanded ? '▲' : '▼'}</button>
                                <button
                                  className="danger"
                                  style={{ padding: '4px 8px', fontSize: 14, border: 'none', background: 'transparent', color: '#e74c3c' }}
                                  onClick={() => setConfirmDelete({ ticker: row.ticker })}
                                  title={t('watchlist.removeFromWatchlist')}
                                >🗑</button>
                              </td>
                            </tr>,

                            isExpanded && (
                              <tr key={`${row.ticker}-lots`}>
                                <td colSpan={16} style={{ padding: '0 0 0 48px', background: 'var(--bg)' }}>
                                  <div style={{ padding: '12px 16px', borderLeft: '3px solid var(--accent)' }}>
                                    {/* Existing lots */}
                                    {lots.length === 0
                                      ? <p style={{ fontSize: 12, color: 'var(--muted)', margin: '0 0 10px' }}>{t('watchlist.noLots')}</p>
                                      : <table style={{ fontSize: 12, borderCollapse: 'collapse', marginBottom: 10, width: 'auto' }}>
                                          <thead>
                                            <tr style={{ color: 'var(--muted)' }}>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>{t('col.date')}</th>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>{t('col.qty')}</th>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>{t('col.buyPrice')}</th>
                                              <th style={{ padding: '2px 16px 2px 0', fontWeight: 500 }}>{t('col.cost')}</th>
                                              <th style={{ padding: '2px 0', fontWeight: 500 }}>{t('col.yield')}</th>
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
                                        onChange={e => setNewLot(prev => ({ ...prev, [row.ticker]: { ...lot, quantity: e.target.value } }))}
                                        style={{ width: 80 }}
                                      />
                                      <input
                                        type="number" min="0" step="any"
                                        placeholder="Buy price"
                                        value={lot.buy_price}
                                        onChange={e => setNewLot(prev => ({ ...prev, [row.ticker]: { ...lot, buy_price: e.target.value } }))}
                                        style={{ width: 100 }}
                                      />
                                      <input
                                        type="date"
                                        value={lot.buy_date}
                                        onChange={e => setNewLot(prev => ({ ...prev, [row.ticker]: { ...lot, buy_date: e.target.value } }))}
                                        style={{ width: 140 }}
                                      />
                                      <button
                                        className="primary"
                                        style={{ padding: '4px 12px', fontSize: 12 }}
                                        disabled={!lot.quantity || !lot.buy_price || !lot.buy_date || addHoldingMut.isPending}
                                        onClick={() => addHoldingMut.mutate({ ticker: row.ticker, quantity: parseFloat(lot.quantity), buy_price: parseFloat(lot.buy_price), buy_date: lot.buy_date })}
                                      >{t('watchlist.addLot')}</button>
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

      {confirmDelete && (
        <div style={{
          position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000,
        }} onClick={() => setConfirmDelete(null)}>
          <div style={{
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 14, padding: '28px 32px', minWidth: 320, maxWidth: 400,
            boxShadow: '0 16px 48px rgba(0,0,0,0.6)',
          }} onClick={e => e.stopPropagation()}>
            <p style={{ fontWeight: 700, fontSize: 16, marginBottom: 8 }}>
              {confirmDelete.ticker ? t('watchlist.removeTicker') : t('watchlist.deleteList')}
            </p>
            <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 24 }}>
              {confirmDelete.ticker
                ? <>{t('watchlist.remove')} <strong style={{ color: 'var(--text)' }}>{confirmDelete.ticker}</strong> {t('watchlist.removeTickerSuffix')}?</>
                : <>{t('watchlist.delete')} <strong style={{ color: 'var(--text)' }}>"{confirmDelete.name}"</strong>? {t('watchlist.deleteListSuffix')}</>
              }
            </p>
            <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDelete(null)}>{t('watchlist.cancel')}</button>
              <button className="danger" onClick={() => {
                if (confirmDelete.ticker) removeMut.mutate(confirmDelete.ticker)
                else deleteMut.mutate(confirmDelete.id)
                setConfirmDelete(null)
              }}>
                {confirmDelete.ticker ? t('watchlist.remove') : t('watchlist.delete')}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
