import { useState, useMemo, useRef, useEffect, useContext } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { MarketContext } from '../context/MarketContext'
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, BarChart, Bar } from 'recharts'
import {
  getWatchlists, createWatchlist, renameWatchlist, deleteWatchlist,
  getWatchlistTickers, addTicker, removeTicker,
  getSignalsByTickers, searchTickers,
  getHoldings, addHolding, deleteHolding,
  getPortfolioHoldings, getPortfolioPerformance, getPortfolioComparison,
  WHITE_BG_LOGOS, COMPANY_NAME_OVERRIDES,
} from '../api'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'
import SignalModal from '../components/SignalModal'

const _PIE_COLORS = [
  '#00d2ff', '#3498db', '#2ecc71', '#f39c12', '#e74c3c', '#9b59b6',
  '#1abc9c', '#e67e22', '#34495e', '#16a085', '#8e44ad', '#d35400',
]

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
  const { market } = useContext(MarketContext)
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
  const [renamingId, setRenamingId] = useState(null)
  const [renameValue, setRenameValue] = useState('')

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

  // ── Portfolio-wide data (all lists) ─────────────────────────────────────
  const { data: portfolioHoldings = [] } = useQuery({
    queryKey: ['portfolio-holdings', market],
    queryFn: () => getPortfolioHoldings(market),
    staleTime: 60_000,
  })
  const { data: compData = [] } = useQuery({
    queryKey: ['portfolio-comparison', market],
    queryFn: () => getPortfolioComparison(market),
    staleTime: 300_000,
  })
  const { data: perfData = [] } = useQuery({
    queryKey: ['portfolio-performance', market],
    queryFn: () => getPortfolioPerformance(market),
    staleTime: 300_000,
  })
  const portfolioTickers = useMemo(() => portfolioHoldings.map(h => h.ticker), [portfolioHoldings])
  const { prices: portPrices, marketOpen: portMarketOpen } = useLivePrices(portfolioTickers)

  const portEnriched = useMemo(() => portfolioHoldings.map(h => {
    const livePrice = portPrices[h.ticker]
    const currentPrice = livePrice ?? h.latest_close
    const currentVal = currentPrice != null ? currentPrice * h.total_qty : null
    const pnlPct = currentVal != null && h.total_cost ? ((currentVal - h.total_cost) / h.total_cost) * 100 : null
    // Daily P&L: if market open + live price → change vs last close; else last close vs prev close
    const dailyPnlAbs = portMarketOpen && livePrice != null && h.latest_close != null
      ? (livePrice - h.latest_close) * h.total_qty
      : (h.latest_close != null && h.prev_close != null
          ? (h.latest_close - h.prev_close) * h.total_qty
          : null)
    const dailyBase = portMarketOpen && livePrice != null ? h.latest_close : h.prev_close
    return { ...h, livePrice, currentVal, pnlPct, dailyPnlAbs, dailyBase }
  }), [portfolioHoldings, portPrices, portMarketOpen])

  const portTotalInvested = portEnriched.reduce((s, h) => s + (h.total_cost || 0), 0)
  const portTotalValue    = portEnriched.reduce((s, h) => s + (h.currentVal ?? h.total_cost ?? 0), 0)
  const portPnlAbs        = portTotalValue - portTotalInvested
  const portPnlPct        = portTotalInvested > 0 ? (portPnlAbs / portTotalInvested) * 100 : null
  const portDailyPnlAbs   = portEnriched.every(h => h.dailyPnlAbs == null) ? null
    : portEnriched.reduce((s, h) => s + (h.dailyPnlAbs ?? 0), 0)
  const portBaseTotal     = portEnriched.reduce((s, h) => s + (h.dailyBase != null ? h.dailyBase * h.total_qty : (h.currentVal ?? 0)), 0)
  const portDailyPnlPct   = portDailyPnlAbs != null && portBaseTotal > 0
    ? (portDailyPnlAbs / portBaseTotal) * 100 : null

  const portPieData = useMemo(() => portEnriched
    .map(h => ({ name: h.ticker, value: h.currentVal ?? h.total_cost ?? 0 }))
    .filter(d => d.value > 0)
    .sort((a, b) => b.value - a.value),
  [portEnriched])
  const portPieTotal = portPieData.reduce((s, d) => s + d.value, 0)

  const invalidateLists    = () => qc.invalidateQueries({ queryKey: ['watchlists'] })
  const invalidateTickers  = () => qc.invalidateQueries({ queryKey: ['watchlist-tickers', selectedId] })
  const invalidateHoldings = () => qc.invalidateQueries({ queryKey: ['watchlist-holdings', selectedId] })
  const invalidatePortfolio = () => {
    qc.invalidateQueries({ queryKey: ['portfolio-holdings', market] })
    qc.invalidateQueries({ queryKey: ['portfolio-performance', market] })
    qc.invalidateQueries({ queryKey: ['portfolio-comparison', market] })
  }

  const addHoldingMut = useMutation({
    mutationFn: (body) => addHolding(selectedId, body),
    onSuccess: (_, body) => {
      invalidateHoldings()
      setNewLot(prev => ({ ...prev, [body.ticker]: { quantity: '', buy_price: '', buy_date: '' } }))
    },
  })

  const deleteHoldingMut = useMutation({
    mutationFn: (hid) => deleteHolding(selectedId, hid),
    onSuccess: () => { invalidateHoldings(); invalidatePortfolio() },
  })

  const createMut = useMutation({
    mutationFn: () => createWatchlist(newListName.trim()),
    onSuccess: (created) => { invalidateLists(); setNewListName(''); setSelectedId(created.id) },
  })

  const deleteMut = useMutation({
    mutationFn: (id) => deleteWatchlist(id),
    onSuccess: (_, id) => { invalidateLists(); invalidatePortfolio(); if (selectedId === id) setSelectedId(null) },
  })

  const renameMut = useMutation({
    mutationFn: ({ id, name }) => renameWatchlist(id, name),
    onSuccess: () => { invalidateLists(); invalidatePortfolio(); setRenamingId(null) },
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
    onSuccess: () => { invalidateTickers(); invalidatePortfolio() },
  })

  const selectedList = lists.find(l => l.id === selectedId)

  const th = (label, col, className) => <Th label={t(label)} col={col} sort={sort} onSort={toggle} className={className} />

  return (
    <div>
      <p className="page-title">{t('portfolio.title')}</p>

      {/* ── Portfolio summary (all lists) ── */}
      {portEnriched.length > 0 && (
        /* 5-col grid: [content] [1px divider] [content] [1px divider] [content]
           Both rows (cards + charts) share the same columns so dividers align perfectly */
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'auto 1px 5fr 1px 5fr',
          columnGap: 12,
          rowGap: 16,
          marginBottom: 24,
        }}>
          {/* ── Row 1: summary cards ── */}
          {/* Col 1 */}
          <div style={{ display: 'flex', gap: 16 }}>
            <div className="metric-card">
              <div className="label">{t('watchlist.totalInvested')}</div>
              <div className="value">${portTotalInvested.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
            </div>
            <div className="metric-card">
              <div className="label">{t('watchlist.currentValue')}</div>
              <div className="value">${portTotalValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
            </div>
          </div>
          {/* Col 2: divider */}
          <div style={{ background: 'rgba(255,255,255,0.35)', alignSelf: 'stretch' }} />
          {/* Col 3: all P&L cards in one flex row, second divider inside */}
          <div style={{ display: 'flex', gap: 16, alignItems: 'stretch' }}>
            <div className="metric-card">
              <div className="label">{t('watchlist.totalPnlAbs')}</div>
              <div className={`value ${portPnlAbs >= 0 ? 'up' : 'down'}`}>
                {portPnlAbs >= 0 ? '+' : ''}${Math.abs(portPnlAbs).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
              </div>
            </div>
            <div className="metric-card">
              <div className="label">{t('watchlist.totalPnlPct')}</div>
              <div className={`value ${portPnlPct != null && portPnlPct >= 0 ? 'up' : 'down'}`}>
                {portPnlPct != null ? `${portPnlPct >= 0 ? '+' : ''}${portPnlPct.toFixed(2)}%` : '—'}
              </div>
            </div>
            <div style={{ width: 1, background: 'rgba(255,255,255,0.35)', alignSelf: 'stretch', flexShrink: 0 }} />
            <div className="metric-card">
              <div className="label">{t('portfolio.dailyPnlAbs')}</div>
              <div className={`value ${portDailyPnlAbs != null && portDailyPnlAbs >= 0 ? 'up' : 'down'}`}>
                {portDailyPnlAbs != null ? `${portDailyPnlAbs >= 0 ? '+' : ''}$${Math.abs(portDailyPnlAbs).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}
              </div>
            </div>
            <div className="metric-card">
              <div className="label">{t('portfolio.dailyPnlPct')}</div>
              <div className={`value ${portDailyPnlPct != null && portDailyPnlPct >= 0 ? 'up' : 'down'}`}>
                {portDailyPnlPct != null ? `${portDailyPnlPct >= 0 ? '+' : ''}${portDailyPnlPct.toFixed(2)}%` : '—'}
              </div>
            </div>
          </div>
          {/* Col 4: empty */}
          <div />
          {/* Col 5: empty */}
          <div />

          {/* ── Row 2: charts ── */}
          {/* Col 1: Allocation donut */}
          <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10, padding: '14px 18px', display: 'flex', gap: 20, alignItems: 'center' }}>
            <div style={{ width: 160, height: 160, flexShrink: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={portPieData} dataKey="value" cx="50%" cy="50%" innerRadius={44} outerRadius={74} strokeWidth={1}>
                    {portPieData.map((_, i) => <Cell key={i} fill={_PIE_COLORS[i % _PIE_COLORS.length]} />)}
                  </Pie>
                  <Tooltip
                    contentStyle={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 11 }}
                    labelStyle={{ color: 'var(--text)', fontWeight: 700 }}
                    itemStyle={{ color: 'var(--text)' }}
                    formatter={(v, name) => [`$${v.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`, name]}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div style={{ fontSize: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 5, color: 'var(--muted)', fontSize: 10 }}>
                <span style={{ width: 8, flexShrink: 0 }} />
                <span style={{ minWidth: 50 }}>{t('col.ticker')}</span>
                <span style={{ minWidth: 36, textAlign: 'right' }}>{t('portfolio.allocation').slice(0,5)}%</span>
                <span style={{ minWidth: 42, textAlign: 'right' }}>{t('col.yield')}</span>
              </div>
              {portPieData.slice(0, 8).map((d, i) => {
                const pct = portPieTotal > 0 ? (d.value / portPieTotal) * 100 : 0
                const h = portEnriched.find(x => x.ticker === d.name)
                return (
                  <div key={d.name} style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
                    <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: 2, background: _PIE_COLORS[i % _PIE_COLORS.length], flexShrink: 0 }} />
                    <span style={{ minWidth: 50, fontWeight: 600 }}>{d.name}</span>
                    <span style={{ color: 'var(--muted)', minWidth: 36, textAlign: 'right' }}>{pct.toFixed(1)}%</span>
                    <span className={h?.pnlPct != null ? (h.pnlPct >= 0 ? 'up' : 'down') : ''} style={{ minWidth: 42, textAlign: 'right', fontSize: 11 }}>
                      {h?.pnlPct != null ? `${h.pnlPct >= 0 ? '+' : ''}${h.pnlPct.toFixed(1)}%` : '—'}
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
          {/* Col 2: empty (divider column) */}
          <div />
          {/* Col 3: Performance line chart */}
          {perfData.length > 0
            ? <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10, padding: '14px 18px' }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 10 }}>Performance (last 12 months)</div>
                <div style={{ display: 'flex', gap: 14, marginBottom: 8, fontSize: 11 }}>
                  <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    <span style={{ display: 'inline-block', width: 20, height: 2, background: 'var(--green)', borderRadius: 1 }} />
                    Vesign Signals
                  </span>
                  <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    <span style={{ display: 'inline-block', width: 20, height: 2, background: '#3498db', borderRadius: 1 }} />
                    Your Portfolio
                  </span>
                </div>
                <div style={{ width: '100%', height: 160 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={perfData} margin={{ top: 4, right: 8, left: -10, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                      <XAxis dataKey="week" tick={{ fontSize: 9, fill: 'var(--muted)' }} tickLine={false} interval={7}
                        tickFormatter={v => new Date(v).toLocaleDateString('en-US', { month: 'short' })} />
                      <YAxis tick={{ fontSize: 9, fill: 'var(--muted)' }} tickLine={false} axisLine={false}
                        tickFormatter={v => `${v > 0 ? '+' : ''}${v.toFixed(0)}%`} />
                      <Tooltip
                        contentStyle={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 11 }}
                        labelStyle={{ color: 'var(--muted)', fontSize: 10, marginBottom: 4 }}
                        labelFormatter={v => new Date(v).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                        formatter={(v, name) => v != null ? [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, name] : ['—', name]}
                        itemStyle={{ color: 'var(--text)' }}
                      />
                      <Line type="monotone" dataKey="vesign" name="Vesign" stroke="var(--green)" strokeWidth={1.5} dot={false} connectNulls />
                      <Line type="monotone" dataKey="portfolio" name="Portfolio" stroke="#3498db" strokeWidth={1.5} dot={false} connectNulls />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            : <div />
          }
          {/* Col 4: empty (divider column) */}
          <div />
          {/* Col 5: Comparison bar chart */}
          {compData.length > 0
            ? <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10, padding: '14px 18px' }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 10 }}>{t('portfolio.comparisonTitle')}</div>
                <div style={{ width: '100%', height: 160 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={compData} margin={{ top: 18, right: 8, left: -10, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                      <XAxis dataKey="name" tick={{ fontSize: 10, fill: 'var(--text)' }} tickLine={false} axisLine={false} />
                      <YAxis tick={{ fontSize: 9, fill: 'var(--muted)' }} tickLine={false} axisLine={false}
                        tickFormatter={v => `${v > 0 ? '+' : ''}${v.toFixed(0)}%`} />
                      <Bar dataKey="yield" radius={[4, 4, 0, 0]}
                        label={{ position: 'top', fontSize: 10, fill: 'var(--text)', formatter: v => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%` }}>
                        {compData.map((entry, i) => (
                          <Cell key={i} fill={entry.name === 'Vesign' ? 'var(--green)' : '#3498db'} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            : <div />
          }
        </div>
      )}

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
            onDoubleClick={e => {
              e.stopPropagation()
              setRenamingId(l.id)
              setRenameValue(l.name)
            }}
          >
            {renamingId === l.id ? (
              <input
                autoFocus
                value={renameValue}
                onChange={e => setRenameValue(e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Enter' && renameValue.trim()) {
                    renameMut.mutate({ id: l.id, name: renameValue.trim() })
                  } else if (e.key === 'Escape') {
                    setRenamingId(null)
                  }
                }}
                onBlur={() => {
                  if (renameValue.trim() && renameValue.trim() !== l.name) {
                    renameMut.mutate({ id: l.id, name: renameValue.trim() })
                  } else {
                    setRenamingId(null)
                  }
                }}
                onClick={e => e.stopPropagation()}
                style={{ width: 100, padding: '2px 6px', fontSize: 13, background: 'var(--bg)', border: '1px solid var(--accent)', borderRadius: 4, color: 'var(--text)', outline: 'none' }}
              />
            ) : (
              <span>{l.name}</span>
            )}
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
              ) : (
                  <div className="data-table-wrap">
                    <table style={{ tableLayout: 'fixed', width: '100%' }}>
                      <colgroup>
                        <col style={{ width: '3%' }} />   {/* logo */}
                        <col style={{ width: '5%' }} />   {/* ticker */}
                        <col style={{ width: '12%' }} />  {/* name */}
                        <col style={{ width: '6%' }} />   {/* signal */}
                        <col style={{ width: '6%' }} />   {/* health score */}
                        <col style={{ width: '6%' }} />   {/* target */}
                        <col style={{ width: '6%' }} />   {/* ml score */}
                        <col style={{ width: '6%' }} />   {/* amount */}
                        <col style={{ width: '6%' }} />   {/* avg price */}
                        <col style={{ width: '7%' }} />   {/* current price */}
                        <col style={{ width: '8%' }} />   {/* live price */}
                        <col style={{ width: '7%' }} />   {/* investment */}
                        <col style={{ width: '7%' }} />   {/* total p&l */}
                        <col style={{ width: '6%' }} />   {/* actions */}
                      </colgroup>
                      <thead>
                        <tr>
                          <th></th>
                          {th('col.ticker',         'ticker')}
                          <Th label="Name" col="company" sort={sort} onSort={toggle} />
                          {th('col.signal',         'signal')}
                          <th>Health Score</th>
                          <th>Target</th>
                          <th className="col-hide-sm">{t('col.mlScore')}</th>
                          <th>Amount</th>
                          <th>{t('col.avgPrice')}</th>
                          <Th label="Closed Price" col="close" sort={sort} onSort={toggle} />
                          <th>{t('col.livePrice')}</th>
                          <th>Investment</th>
                          <th>Total P&L (%)</th>
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
                              <td style={clip}>{COMPANY_NAME_OVERRIDES[row.ticker] ?? row.company ?? '—'}</td>
                              <td>{row.signal ? <span className={`badge badge-${row.signal}`}>{row.signal}</span> : '—'}</td>
                              <HealthCell score={row.health_score} />
                              <UpsideCell targetMean={row.target_mean_price} close={row.close} prices={prices} ticker={row.ticker} />
                              <td className="col-hide-sm">{row.prediction_score != null ? <span className={row.prediction_score >= 0 ? 'up' : 'down'}>{row.prediction_score >= 0 ? '▲' : '▼'} {Math.abs(row.prediction_score * 100).toFixed(1)}%</span> : '—'}</td>
                              <td>{totalQty > 0 ? totalQty : '—'}</td>
                              <td>{avgPrice != null ? avgPrice.toFixed(2) : '—'}</td>
                              <td>{fmtPrice(row.close, row.ticker)}</td>
                              <LivePriceCell ticker={row.ticker} closePrice={row.close} prices={prices} marketOpen={marketOpen} />
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
                                <td colSpan={14} style={{ padding: '0 0 0 48px', background: 'var(--bg)' }}>
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
              )}
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
