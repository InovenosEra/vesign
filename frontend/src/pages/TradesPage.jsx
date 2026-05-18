import { useState, useRef, useEffect, useLayoutEffect, useContext, useMemo } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { getTrades, getOpenTrades, getPriceHistory, getNews, WHITE_BG_LOGOS } from '../api'
import SignalChart from '../components/SignalChart'

// ---------- helper: pull full historical trades for one ticker --------------
// Used by TradeModal so its chart shows the full historical trade history,
// independent of the page-level date filter (which defaults to last 12 months
// and would otherwise hide older closed trades from the modal chart).
// 10y covers our 2020-01-02 DB cutoff with margin.
function useFullHistoryTradesForTicker(ticker, market, includeLots = false) {
  const tenYearsAgo = (() => {
    const d = new Date()
    d.setFullYear(d.getFullYear() - 10)
    d.setDate(d.getDate() - 7)
    return d.toISOString().slice(0, 10)
  })()
  const todayStr = new Date().toISOString().slice(0, 10)
  return useQuery({
    queryKey: ['trades-full-by-ticker', ticker, market, includeLots],
    queryFn: () => getTrades({ start: tenYearsAgo, end: todayStr, market, includeLots }),
    staleTime: 5 * 60_000,
    enabled: !!ticker,
  })
}
import { useSort } from '../hooks/useSort'
import { useLivePrices } from '../hooks/useLivePrices'
import { usePersistedState } from '../hooks/usePersistedState'
import { MarketContext } from '../context/MarketContext'
import { useCurrency } from '../context/CurrencyContext'
import DownloadButton from '../components/DownloadButton'

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

// Convert a period sentinel (number of months, or 'ytd') to an ISO start date.
// Used by both selectPeriod and the on-mount effect that restores the
// persisted period after route changes.
function startForPeriod(p) {
  if (p === 'ytd') return `${new Date().getFullYear()}-01-01`
  return isoMonthsAgo(p)
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

function Pagination({ page, pages, onChange }) {
  if (pages <= 1) return null
  return (
    <div className="pagination">
      <button disabled={page === 1} onClick={() => onChange(1)}>«</button>
      <button disabled={page === 1} onClick={() => onChange(page - 1)}>‹ Prev</button>
      <span>{page} / {pages}</span>
      <button disabled={page === pages} onClick={() => onChange(page + 1)}>Next ›</button>
      <button disabled={page === pages} onClick={() => onChange(pages)}>»</button>
    </div>
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

// ---------------------------------------------------------------------------
// Trade chart modal — header/info table on top, shared SignalChart below
// ---------------------------------------------------------------------------

function TradeModal({ row: rowProp, dcaView = false, onClose }) {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const { fmtPrice } = useCurrency()
  const isIL      = rowProp.ticker?.endsWith('.TA') ?? market === 'IL'
  const priceScale = isIL ? 100 : 1

  // Override row.trades with the full 5Y history so the "Yield (Vesign)" calc
  // covers every closed trade for this ticker, not just those in the
  // page-level date filter.
  const { data: fullTrades } = useFullHistoryTradesForTicker(rowProp.ticker, market, dcaView)
  const row = useMemo(() => {
    const all = (fullTrades ?? []).find(tk => tk.ticker === rowProp.ticker)?.trades
    if (!all || all.length === 0) return rowProp
    return { ...rowProp, trades: all.filter(p => p.result !== 'Open') }
  }, [rowProp, fullTrades])

  const [descTab, setDescTab] = useState('info')
  const [chartState, setChartState] = useState({ activePeriod: 12, chartStart: '', chartEnd: '', yieldPeriod: null })

  // 12-month price history only powers the "Current Price" cell; the chart
  // fetches its own range via SignalChart.
  const today = new Date().toISOString().slice(0, 10)
  const start12m = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()
  const { data: history12m = [] } = useQuery({
    queryKey: ['price-history-12m', row.ticker],
    queryFn: () => getPriceHistory(row.ticker, { start: start12m, end: today }),
    staleTime: 300_000,
  })

  const { data: newsData = [], isLoading: newsLoading } = useQuery({
    queryKey: ['news', row.ticker],
    queryFn: () => getNews(row.ticker, 5),
    enabled: descTab === 'news',
    staleTime: 300_000,
  })

  // Vesign yield over the selected chart period — average return of closed
  // trades whose buy and sell both fall inside [chartStart, chartEnd].
  const windowAvgReturn = useMemo(() => {
    if (!chartState.chartStart || !chartState.chartEnd) return null
    const inWindow = (row.trades ?? []).filter(p => {
      if (p.result === 'Open' || !p.buy_date || !p.sell_date) return false
      const buy  = p.buy_date.slice(0, 10)
      const sell = p.sell_date.slice(0, 10)
      return buy >= chartState.chartStart && sell <= chartState.chartEnd
    })
    if (inWindow.length === 0) return null
    const sum = inWindow.reduce((s, p) => {
      if (dcaView && p.avg_cost != null && p.sell_price != null) {
        return s + ((p.sell_price - p.avg_cost) / p.avg_cost) * 100
      }
      return s + (p.return_pct ?? 0)
    }, 0)
    return sum / inWindow.length
  }, [row.trades, chartState.chartStart, chartState.chartEnd, dcaView])

  useEffect(() => {
    const handler = e => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [onClose])

  const generalColRef = useRef(null)
  const [generalColH, setGeneralColH] = useState(null)
  useLayoutEffect(() => {
    if (generalColRef.current) setGeneralColH(generalColRef.current.offsetHeight)
  })

  const healthLabels = ['', t('health.weak'), t('health.fair'), t('health.good'), t('health.great'), t('health.excellent')]
  const healthColors = ['', '#e74c3c', '#e67e22', '#f1c40f', '#2ecc71', '#1a9e55']

  const periodLabel = (() => {
    const labels = { 3:'3M', 6:'6M', 'ytd':'YTD', 12:'1Y', 24:'2Y', 36:'3Y', 60:'5Y' }
    const p = chartState.activePeriod
    return p ? (labels[p] || `${p}M`) : null
  })()

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header" style={{ alignItems: 'flex-start' }}>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6, flexShrink: 0 }}>
            {row.logo_url
              ? (() => {
                  const onErr = (e) => {
                    const wrap = e.target.parentNode?.tagName === 'A' ? e.target.parentNode : e.target
                    wrap.style.display = 'none'
                    if (wrap.nextSibling) wrap.nextSibling.style.display = 'flex'
                  }
                  const img = <img src={row.logo_url} alt="" className="modal-logo" style={{ width: 96, height: 96, borderRadius: 10, objectFit: 'contain', flexShrink: 0, ...(WHITE_BG_LOGOS.has(row.ticker) ? { background: '#fff', padding: 6 } : {}) }} onError={onErr} />
                  const href = row.domain ? `https://${row.domain.replace(/^https?:\/\//, '').replace(/\/$/, '')}` : null
                  return href
                    ? <a href={href} target="_blank" rel="noopener noreferrer" title={href} style={{ lineHeight: 0, cursor: 'pointer' }}>{img}</a>
                    : img
                })()
              : null}
            <div className="modal-logo-placeholder" style={{ width: 96, height: 96, flexShrink: 0, borderRadius: 10, background: 'var(--surface)', border: '1px solid var(--border)', display: row.logo_url ? 'none' : 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 13, fontWeight: 'bold', color: 'var(--text)' }}>
              {row.ticker?.replace(/\.TA$/, '')}
            </div>
            {/* Tab bar below logo */}
            <div style={{ display: 'flex', gap: 4 }}>
              {['info', 'news'].map(tab => (
                <button key={tab}
                  className={`period-chip${descTab === tab ? ' active' : ''}`}
                  onClick={() => setDescTab(tab)}
                  style={{ fontSize: 11, padding: '2px 10px' }}>
                  {tab === 'info' ? t('modal.tabInfo') : t('modal.tabNews')}
                </button>
              ))}
            </div>
          </div>

          {/* General column */}
          <div ref={generalColRef} className="modal-general-col" style={{ display: 'flex', flexDirection: 'column', gap: 4, flexShrink: 0, width: 300 }}>
            <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.general')}</div>
            <div style={{ padding: '8px 0px', border: '1px solid var(--border)', borderRadius: 8 }}>
              <table style={{ fontSize: 12, borderCollapse: 'collapse', width: '100%', margin: 0, tableLayout: 'fixed' }}>
                <tbody>
                  {[
                    [t('modal.ticker'),        <strong>{row.ticker?.replace(/\.TA$/, '') ?? '—'}</strong>],
                    [t('modal.company'),       row.company ?? '—'],
                    [t('modal.industry'),      row.industry ?? '—'],
                    [t('modal.marketCap'),     row.market_cap != null ? (row.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'],
                    [t('modal.currentSignal'), row.current_signal ? <span className={`badge badge-${row.current_signal}`}>{row.current_signal}</span> : '—'],
                    [t('modal.currentPrice'),  history12m.length > 0 ? fmtPrice(history12m.at(-1).close / priceScale) : '—'],
                    [periodLabel ? `${t('modal.yieldPeriod', { label: periodLabel })} (organic)` : `${t('modal.yieldCustom')} (organic)`, chartState.yieldPeriod != null ? <span className={chartState.yieldPeriod >= 0 ? 'up' : 'down'}>{chartState.yieldPeriod >= 0 ? '+' : ''}{fmt(chartState.yieldPeriod)}%</span> : '—'],
                    ...(row.unrealized_pct == null ? [[periodLabel ? `${t('modal.yieldPeriod', { label: periodLabel })} (Vesign)` : `${t('modal.yieldCustom')} (Vesign)`, windowAvgReturn != null ? <span className={windowAvgReturn >= 0 ? 'up' : 'down'}>{windowAvgReturn >= 0 ? '+' : ''}{fmt(windowAvgReturn)}%</span> : '—']] : []),
                    ...(row.unrealized_pct != null ? [[t('modal.yieldSinceBuy'), <span className={row.unrealized_pct >= 0 ? 'up' : 'down'}>{row.unrealized_pct >= 0 ? '+' : ''}{fmt(row.unrealized_pct)}%</span>]] : []),
                  ].map(([label, value]) => (
                    <tr key={label} style={{ height: 22 }}>
                      <td style={{ color: 'var(--muted)', paddingRight: 8, verticalAlign: 'middle', whiteSpace: 'nowrap', width: 120 }}>{label}</td>
                      <td style={{ verticalAlign: 'middle', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Description + Health column (tabbed) */}
          <div className="modal-desc-col" style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4, overflow: 'hidden', ...(generalColH ? { height: generalColH } : {}) }}>
            {/* Info tab */}
            {descTab === 'info' && (<>
              {(row.description_short || row.description) && (<>
                <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.description')}</div>
                <div style={{ fontSize: 12, lineHeight: 1.6, overflowY: 'auto', flex: 1, minHeight: 0, padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8 }}>
                  {row.description_short || row.description}
                </div>
              </>)}
              {row.health_score && (<>
                <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.companyHealth')}</div>
                <div style={{ padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflowY: 'auto' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                    {[1,2,3,4,5].map(i => (
                      <div key={i} style={{ width: 20, height: 8, borderRadius: 3, background: i <= row.health_score ? healthColors[row.health_score] : 'var(--border)' }} />
                    ))}
                    <span style={{ fontSize: 12, fontWeight: 'bold', color: healthColors[row.health_score], marginLeft: 4 }}>{healthLabels[row.health_score]}</span>
                  </div>
                  {row.health_reason && <div style={{ fontSize: 12, lineHeight: 1.6 }}>{row.health_reason}</div>}
                </div>
              </>)}
            </>)}
            {/* News tab */}
            {descTab === 'news' && (<>
              <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.tabNews')}</div>
              <div style={{ fontSize: 12, lineHeight: 1.6, overflowY: 'auto', flex: 1, minHeight: 0, padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8 }}>
                {newsLoading
                  ? <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('table.loading')}</div>
                  : newsData.length === 0
                    ? <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('modal.noNews')}</div>
                    : newsData.map((n, i) => (
                      <div key={i} style={{ paddingBottom: 8, marginBottom: 8, borderBottom: '1px solid var(--border)' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 2 }}>
                          <span style={{ fontSize: 10, color: 'var(--muted)' }}>{(n.date || '').slice(0, 10)}</span>
                          {n.source && <span style={{ fontSize: 10, background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 4, padding: '0 5px' }}>{n.source}</span>}
                        </div>
                        {n.url
                          ? <a href={n.url} target="_blank" rel="noopener noreferrer" style={{ fontSize: 12, fontWeight: 600, color: 'var(--text)', textDecoration: 'none', display: 'block', marginBottom: 2 }}>{n.title}</a>
                          : <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 2 }}>{n.title}</div>
                        }
                        {n.summary && <div style={{ fontSize: 11, color: 'var(--muted)', lineHeight: 1.4, overflow: 'hidden', display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' }}>{n.summary}</div>}
                      </div>
                    ))
                }
              </div>
            </>)}
          </div>

          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Chart */}
        <SignalChart ticker={row.ticker} onPeriodChange={setChartState} />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Open Trades table
// ---------------------------------------------------------------------------

function OpenTradesTable({ data, search, page, pageSize, setPage, onSelect, dcaView = false }) {
  const { t } = useTranslation()
  const { fmtPrice } = useCurrency()
  const { sorted, sort, toggle } = useSort(data, 'buy_date', 'desc')

  const th = (label, col, className) => <Th label={label} col={col} sort={sort} onSort={toggle} className={className} />

  const filtered  = search ? sorted.filter(t =>
    t.ticker?.toLowerCase().includes(search.toLowerCase()) ||
    t.company?.toLowerCase().includes(search.toLowerCase())
  ) : sorted
  const pages     = Math.max(1, Math.ceil(filtered.length / pageSize))
  const paginated = filtered.slice((page - 1) * pageSize, page * pageSize)

  const tickers = paginated.map(t => t.ticker)
  const { prices, phase } = useLivePrices(tickers)

  return (
    <>
      <div className="data-table-wrap">
        <table>
          <thead>
            <tr>
              <th></th>
              {th(t('col.ticker'),       'ticker')}
              {th(t('col.company'),      'company')}
              {th(t('col.marketCap'),    'market_cap',    'col-hide-sm')}
              {th(t('col.buyDate'),      'buy_date')}
              {th(dcaView ? 'Avg Cost'  : t('col.buyPrice'), dcaView ? 'avg_cost' : 'buy_price')}
              {th(t('col.lastDayPrice'), 'current_price', 'col-hide-sm')}
              {th(t('col.daysHeld'),     'days_held')}
              <th>{
                phase === 'pre'  ? t('col.preMarket')  :
                phase === 'post' ? t('col.postMarket') :
                                   t('col.livePrice')
              }</th>
              <th>{t('col.yield')}</th>
            </tr>
          </thead>
          <tbody>
            {paginated.length === 0
              ? <tr><td colSpan={10} className="empty" style={{ textAlign: 'center' }}>{t('trades.noOpen')}</td></tr>
              : paginated.map((trade, i) => {
                const isIL = trade.ticker?.endsWith('.TA')
                const isOpen = phase != null && phase !== 'idle'
                const live = prices[trade.ticker]
                const closePrice = trade.current_price
                const displayLive  = live != null ? (isIL ? live / 100 : live) : null
                const displayClose = closePrice != null ? (isIL ? closePrice / 100 : closePrice) : null
                const diff = displayLive != null && displayClose != null ? displayLive - displayClose : null
                const pct  = diff != null && displayClose ? (diff / displayClose) * 100 : null
                const cls  = diff != null && diff >= 0 ? 'up' : 'down'
                const arrow = diff != null && diff >= 0 ? '▲' : '▼'
                const showAvg = dcaView && trade.avg_cost != null
                const showBadge = dcaView && trade.n_lots > 1
                const displayCost = showAvg ? trade.avg_cost : trade.buy_price

                return (
                  <tr key={i} className="clickable-row" onClick={() => onSelect(trade)}>
                    <td>{trade.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(trade.ticker) ? ' logo-white-bg' : ''}`} src={trade.logo_url} alt="" /> : null}</td>
                    <td><strong>{trade.ticker}</strong></td>
                    <td>{trade.company ?? '—'}</td>
                    <td className="col-hide-sm">{trade.market_cap != null ? (trade.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'}</td>
                    <td>{fmtDate(trade.buy_date)}</td>
                    <td>
                      {fmtPrice(displayCost)}
                      {showBadge && (
                        <span
                          title={trade.lots.map(l => `Lot ${l.seq}: ${l.date} @ ${fmtPrice(l.price)}`).join('\n')}
                          style={{ display: 'inline-block', marginLeft: 6, padding: '1px 7px', borderRadius: 999, background: '#eef2ff', color: '#4338ca', fontSize: 10, fontWeight: 700, cursor: 'help' }}
                        >×{trade.n_lots}</span>
                      )}
                    </td>
                    <td className="col-hide-sm">{fmtPrice(trade.current_price)}</td>
                    <td>{trade.days_held ?? '—'}</td>
                    <td>
                      {phase === null
                        ? <span style={{ color: 'var(--muted)' }}>{displayClose != null ? fmtPrice(displayClose) : '—'}</span>
                        : phase === 'idle'
                          ? <span style={{ color: 'var(--muted)', fontSize: 12 }}>{t('market.closedShort')}</span>
                          : displayLive == null
                            ? <span style={{ color: 'var(--muted)' }}>{displayClose != null ? fmtPrice(displayClose) : '—'}</span>
                            : <div>
                                <div>{fmtPrice(displayLive)}</div>
                                {diff != null && <div className={cls} style={{ fontSize: 11 }}>{arrow} {fmtPrice(Math.abs(diff))} ({Math.abs(pct).toFixed(2)}%)</div>}
                              </div>
                      }
                    </td>
                    {(() => {
                      const priceForYield = (isOpen && displayLive != null) ? displayLive : displayClose
                      const rawBase = showAvg ? trade.avg_cost : trade.buy_price
                      const basePrice = rawBase != null ? (isIL ? rawBase / 100 : rawBase) : null
                      const yieldPct = priceForYield != null && basePrice ? ((priceForYield - basePrice) / basePrice) * 100 : null
                      return (
                        <td className={yieldPct != null ? (yieldPct >= 0 ? 'up' : 'down') : ''}>
                          {yieldPct != null ? `${yieldPct >= 0 ? '+' : ''}${fmt(yieldPct)}%` : '—'}
                        </td>
                      )
                    })()}
                  </tr>
                )
              })
            }
          </tbody>
        </table>
      </div>
      <Pagination page={page} pages={pages} onChange={setPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function TradesPage() {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const { fmtPrice } = useCurrency()
  const oneYearAgo = new Date()
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1)

  const [start, setStart]           = usePersistedState('trades.start', oneYearAgo.toISOString().slice(0, 10))
  const [end,   setEnd]             = usePersistedState('trades.end', new Date().toISOString().slice(0, 10))
  const [activePeriod, setActivePeriod] = usePersistedState('trades.period', 12)
  const [openSearch, setOpenSearch]     = usePersistedState('trades.openSearch', '')
  const [openPage, setOpenPage]         = usePersistedState('trades.openPage', 1)
  const [openPageSize, setOpenPageSize] = usePersistedState('trades.openPageSize', 10)
  const [selected, setSelected]         = useState(null)
  const [selectedOpen, setSelectedOpen] = useState(null)
  const [search, setSearch]         = usePersistedState('trades.search', '')
  const [page, setPage]             = usePersistedState('trades.page', 1)
  const [pageSize, setPageSize]     = usePersistedState('trades.pageSize', 10)
  // Path B: DCA view is now the default for all users — toggle removed.
  const dcaActive = true

  // Re-anchor a chip selection to "today" on each visit (so 1Y always means
  // the trailing 12 months). Custom date ranges (activePeriod=null) are left
  // alone. Default activePeriod=12 → first-ever visit lands on a 1Y window.
  useEffect(() => {
    if (activePeriod) {
      const today = new Date().toISOString().slice(0, 10)
      const s = startForPeriod(activePeriod)
      if (start !== s) setStart(s)
      if (end !== today) setEnd(today)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  function selectPeriod(months) {
    setActivePeriod(months)
    setStart(startForPeriod(months))
    setEnd(new Date().toISOString().slice(0, 10))
  }

  const { data: trades, isLoading, isError } = useQuery({
    queryKey: ['trades', start, end, market, dcaActive],
    queryFn: () => getTrades({ start, end, market, includeLots: dcaActive }),
    staleTime: 300_000,
  })

  // Only the active period loads; other chips lazy-fetch on click.

  const { data: openTrades = [], isLoading: loadingOpen, isError: errorOpen, error: openError } = useQuery({
    queryKey: ['trades-open', market, dcaActive],
    queryFn: () => getOpenTrades(market, dcaActive),
  })

  // Flatten: one row per closed trade (unpivot multi-trade tickers)
  const flatTrades = useMemo(() => (trades ?? []).flatMap(tk =>
    (tk.trades ?? [])
      .filter(p => p.result !== 'Open')
      .map(p => ({
        ...tk,
        buy_date:   p.buy_date,
        sell_date:  p.sell_date,
        buy_price:  p.buy_price,
        sell_price: p.sell_price,
        return_pct: p.return_pct,
        days_held:  p.days_held,
        lots:       p.lots,
        avg_cost:   p.avg_cost,
        n_lots:     p.n_lots,
      }))
  ), [trades])

  const { sorted, sort, toggle } = useSort(flatTrades, 'sell_date', 'desc')

  const yieldOf = (t) => (dcaActive && t.avg_cost && t.sell_price)
    ? ((t.sell_price - t.avg_cost) / t.avg_cost) * 100
    : (t.return_pct ?? 0)
  const totalPairs = flatTrades.length
  const wins       = flatTrades.filter(t => yieldOf(t) > 0).length
  const avgReturn  = totalPairs > 0
    ? flatTrades.reduce((s, t) => s + yieldOf(t), 0) / totalPairs
    : null
  const avgDays    = totalPairs > 0
    ? flatTrades.reduce((s, t) => s + (t.days_held ?? 0), 0) / totalPairs
    : null

  const th = (label, col, className) => <Th label={label} col={col} sort={sort} onSort={toggle} className={className} />

  return (
    <div>
      <p className="page-title">{t('trades.title')}</p>

      <div className="controls">
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('trades.from')}</label>
        <input type="date" value={start} onChange={e => { setStart(e.target.value); setActivePeriod(null) }} />
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('trades.to')}</label>
        <input type="date" value={end} onChange={e => { setEnd(e.target.value); setActivePeriod(null) }} />
        <span style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {[[3, '3M'], [6, '6M'], ['ytd', 'YTD'], [12, '1Y'], [24, '2Y'], [36, '3Y'], [60, '5Y']].map(([m, label]) => (
            <button
              key={m}
              className={`period-chip${activePeriod === m ? ' active' : ''}`}
              onClick={() => selectPeriod(m)}
            >{label}</button>
          ))}
        </span>
      </div>

      {isLoading && <p className="loading">{t('table.loading')}</p>}
      {isError   && <p className="error">{t('trades.failedLoad')}</p>}

      {trades && totalPairs > 0 && (
        <div className="metrics">
          <div className="metric-card">
            <div className="label">{t('trades.totalTrades')}</div>
            <div className="value">{totalPairs}</div>
          </div>
          <div className="metric-card">
            <div className="label">{t('trades.winRate')}</div>
            <div className="value">{totalPairs > 0 ? ((wins / totalPairs) * 100).toFixed(1) : '—'}%</div>
          </div>
          <div className="metric-card">
            <div className="label">{t('trades.avgYield')}</div>
            <div className={`value ${avgReturn >= 0 ? 'up' : 'down'}`}>
              {avgReturn >= 0 ? '+' : ''}{fmt(avgReturn)}%
            </div>
          </div>
          <div className="metric-card">
            <div className="label">{t('trades.avgDays')}</div>
            <div className="value">{avgDays != null ? Math.round(avgDays) : '—'}</div>
          </div>
        </div>
      )}

      {trades && totalPairs === 0 && (
        <p className="empty">{t('trades.noCompleted')}</p>
      )}

      {trades && totalPairs > 0 && (() => {
        const filtered = search
          ? sorted.filter(t =>
              t.ticker?.toLowerCase().includes(search.toLowerCase()) ||
              t.company?.toLowerCase().includes(search.toLowerCase())
            )
          : sorted
        const pages     = Math.max(1, Math.ceil(filtered.length / pageSize))
        const paginated = filtered.slice((page - 1) * pageSize, page * pageSize)
        return (
        <>
        <div className="controls" style={{ marginTop: 0 }}>
          <input
            placeholder={`🔍 ${t('table.search')}`}
            value={search}
            onChange={e => { setSearch(e.target.value); setPage(1) }}
            style={{ width: 240 }}
          />
          {search && <button onClick={() => { setSearch(''); setPage(1) }}>{t('table.clear')}</button>}
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('table.rows')}</label>
            <select value={pageSize} onChange={e => { setPageSize(Number(e.target.value)); setPage(1) }}>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
            <DownloadButton
              url={`/api/trades/export?${new URLSearchParams({
                ...(start  ? { start }  : {}),
                ...(end    ? { end }    : {}),
                ...(market ? { market } : {}),
              }).toString()}`}
              filenameFallback="trades_closed"
            />
          </span>
        </div>
        <div className="data-table-wrap">
          <table>
            <thead>
              <tr>
                <th></th>
                {th(t('col.ticker'),    'ticker')}
                {th(t('col.company'),   'company')}
                {th(t('col.marketCap'), 'market_cap', 'col-hide-sm')}
                {th(t('col.buyDate'),   'buy_date')}
                {th(dcaActive ? 'Avg Cost' : t('col.buyPrice'),  dcaActive ? 'avg_cost' : 'buy_price')}
                {th(t('col.sellDate'),  'sell_date')}
                {th(t('col.sellPrice'), 'sell_price')}
                {th(t('col.daysHeld'),  'days_held', 'col-hide-sm')}
                {th(t('col.yield'),     'return_pct')}
              </tr>
            </thead>
            <tbody>
              {paginated.length === 0
                ? <tr><td colSpan={10} className="empty" style={{ textAlign: 'center' }}>{t('trades.noMatches')}</td></tr>
                : paginated.map((trade, i) => {
                  const showAvg = dcaActive && trade.avg_cost != null
                  const showBadge = dcaActive && trade.n_lots > 1
                  const displayCost = showAvg ? trade.avg_cost : trade.buy_price
                  const dcaYield = dcaActive && trade.avg_cost && trade.sell_price
                    ? ((trade.sell_price - trade.avg_cost) / trade.avg_cost) * 100
                    : trade.return_pct
                  return (
                  <tr key={i} className="clickable-row" onClick={() => setSelected(trade)}>
                    <td>{trade.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(trade.ticker) ? ' logo-white-bg' : ''}`} src={trade.logo_url} alt="" /> : null}</td>
                    <td><strong>{trade.ticker}</strong></td>
                    <td>{trade.company ?? '—'}</td>
                    <td className="col-hide-sm">{trade.market_cap != null ? (trade.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'}</td>
                    <td>{fmtDate(trade.buy_date)}</td>
                    <td>
                      {fmtPrice(displayCost)}
                      {showBadge && (
                        <span
                          title={trade.lots.map(l => `Lot ${l.seq}: ${l.date} @ ${fmtPrice(l.price)}`).join('\n')}
                          style={{ display: 'inline-block', marginLeft: 6, padding: '1px 7px', borderRadius: 999, background: '#eef2ff', color: '#4338ca', fontSize: 10, fontWeight: 700, cursor: 'help' }}
                        >×{trade.n_lots}</span>
                      )}
                    </td>
                    <td>{fmtDate(trade.sell_date)}</td>
                    <td>{fmtPrice(trade.sell_price)}</td>
                    <td className="col-hide-sm">{trade.days_held ?? '—'}</td>
                    <td className={dcaYield >= 0 ? 'up' : 'down'}>
                      {dcaYield >= 0 ? '+' : ''}{fmt(dcaYield)}%
                    </td>
                  </tr>
                  )
                })}
            </tbody>
          </table>
        </div>
        <Pagination page={page} pages={pages} onChange={setPage} />
        </>
        )
      })()}

      {selected && <TradeModal row={selected} dcaView={dcaActive} onClose={() => setSelected(null)} />}
      {selectedOpen && <TradeModal row={selectedOpen} dcaView={dcaActive} onClose={() => setSelectedOpen(null)} />}

      {/* Open Trades */}
      <p className="page-title" style={{ marginTop: 40 }}>{t('trades.openTitle')}</p>

      {loadingOpen && <p className="loading">{t('table.loading')}</p>}
      {errorOpen && <p className="error">Error: {openError?.message}</p>}
      {!loadingOpen && !errorOpen && (() => {
        const filtered = openTrades
        const openYield = (t) => (dcaActive && t.avg_cost && t.current_price)
          ? ((t.current_price - t.avg_cost) / t.avg_cost) * 100
          : (t.unrealized_pct ?? 0)
        const openCount  = filtered.length
        const winCount   = filtered.filter(t => openYield(t) > 0).length
        const winRate    = openCount > 0 ? (winCount / openCount) * 100 : null
        const avgYield   = openCount > 0 ? filtered.reduce((s, t) => s + openYield(t), 0) / openCount : null
        const avgDaysOpen = openCount > 0 ? filtered.reduce((s, t) => s + (t.days_held ?? 0), 0) / openCount : null
        return (
          <>
            {openCount > 0 && (
              <div className="metrics">
                <div className="metric-card">
                  <div className="label">{t('trades.openTrades')}</div>
                  <div className="value">{openCount}</div>
                </div>
                <div className="metric-card">
                  <div className="label">{t('trades.winRate')}</div>
                  <div className="value">{winRate != null ? `${winRate.toFixed(1)}%` : '—'}</div>
                </div>
                <div className="metric-card">
                  <div className="label">{t('trades.avgYield')}</div>
                  <div className={`value ${avgYield >= 0 ? 'up' : 'down'}`}>
                    {avgYield != null ? `${avgYield >= 0 ? '+' : ''}${fmt(avgYield)}%` : '—'}
                  </div>
                </div>
                <div className="metric-card">
                  <div className="label">{t('trades.avgDaysOpen')}</div>
                  <div className="value">{avgDaysOpen != null ? Math.round(avgDaysOpen) : '—'}</div>
                </div>
              </div>
            )}
            <div className="controls" style={{ marginTop: 0 }}>
              <input
                placeholder={`🔍 ${t('table.search')}`}
                value={openSearch}
                onChange={e => { setOpenSearch(e.target.value); setOpenPage(1) }}
                style={{ width: 240 }}
              />
              {openSearch && <button onClick={() => { setOpenSearch(''); setOpenPage(1) }}>{t('table.clear')}</button>}
              <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
                <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('table.rows')}</label>
                <select value={openPageSize} onChange={e => { setOpenPageSize(Number(e.target.value)); setOpenPage(1) }}>
                  <option value={10}>10</option>
                  <option value={25}>25</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                </select>
                <DownloadButton
                  url={`/api/trades/open/export${market ? `?market=${encodeURIComponent(market)}` : ''}`}
                  filenameFallback="trades_open"
                />
              </span>
            </div>
            <OpenTradesTable data={filtered} search={openSearch} page={openPage} pageSize={openPageSize} setPage={setOpenPage} onSelect={setSelectedOpen} dcaView={dcaActive} />
          </>
        )
      })()}
    </div>
  )
}
