import { useState, useMemo, useContext, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { getSignalsToday, getSignals, getDataStatus, WHITE_BG_LOGOS } from '../api'
import { MarketContext } from '../context/MarketContext'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'
import { usePersistedState } from '../hooks/usePersistedState'
import { useCurrency } from '../context/CurrencyContext'
import SignalModal from '../components/SignalModal'
import DownloadButton from '../components/DownloadButton'


// BUY-signal quality tiers (from signals.tier): Prime = vqs 8-9, Strong = vqs 7, Potential = vqs 6.
// Rendered as a glowing star to the LEFT of the ticker logo + a legend by the heading.
// Star colors/glow live in App.css (.tier-star--prime/strong/potential).
const TIER_STAR = {
  1: { label: 'Prime',     cls: 'prime' },     // purple, shining
  2: { label: 'Strong',    cls: 'strong' },    // gold
  3: { label: 'High Potential', cls: 'potential' }, // silver
};

function TierStar({ tier, size = 16 }) {
  const s = TIER_STAR[tier];
  if (!s) return null;
  return (
    <span aria-hidden="true" className={`tier-star tier-star--${s.cls}`} style={{ fontSize: size }}>★</span>
  );
}

// Fixed-width star slot to the left of a logo (keeps logos aligned across BUY/SELL rows).
function TierLogoStar({ tier }) {
  return (
    <span title={tier ? `${TIER_STAR[tier].label} signal` : undefined}
      style={{ width: 16, display: 'inline-flex', justifyContent: 'center', flex: '0 0 auto' }}>
      {tier ? <TierStar tier={tier} size={16} /> : null}
    </span>
  );
}

function TierLegend() {
  return (
    <div style={{ display: 'flex', gap: 16, alignItems: 'center', fontSize: 12, color: 'var(--muted)' }}>
      {[1, 2, 3].map((tr) => (
        <span key={tr} style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
          <TierStar tier={tr} size={13} /> {TIER_STAR[tr].label}
        </span>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
}

// IL prices/targets are stored in agorot (×100). Divide by 100 for display in ₪.
function isILTicker(ticker) { return ticker?.endsWith('.TA') ?? false }

function PriceCell({ value, ticker }) {
  const { fmtPrice } = useCurrency()
  if (value == null) return <>—</>
  const v = isILTicker(ticker) ? value / 100 : value
  return <>{fmtPrice(v)}</>
}

function fmtMktCap(n) {
  if (n == null) return '—'
  return (n / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 })
}

function displayTicker(ticker) {
  return ticker ? ticker.replace(/\.TA$/, '') : '—'
}

function fmtDate(str) {
  if (!str) return '—'
  const d = new Date(str)
  const dd = String(d.getDate()).padStart(2, '0')
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const yy = String(d.getFullYear()).slice(2)
  return `${dd}/${mm}/${yy}`
}

const _HEALTH_COLORS = ['', '#ff4d5c', '#c2660c', '#ff9500', '#00d97e', '#0a8f54']
const _HEALTH_KEYS = ['', 'health.weak', 'health.fair', 'health.good', 'health.great', 'health.excellent']

function HealthCell({ score }) {
  const { t } = useTranslation()
  const label = score ? t(_HEALTH_KEYS[score]) : ''
  if (!score) return <td style={{ color: 'var(--muted)' }}>—</td>
  return (
    <td title={label}>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: 3 }}>
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

function UpsideCell({ targetMean, close }) {
  if (targetMean == null || !close) return <td>—</td>
  const pct = ((targetMean - close) / close) * 100
  return <td className={pct >= 0 ? 'up' : 'down'}>{pct >= 0 ? '▲' : '▼'} {Math.abs(pct).toFixed(1)}%</td>
}

function MLScoreCell({ score, className }) {
  const cls = className || ''
  if (score == null) return <td className={cls} style={{ color: 'var(--muted)' }}>—</td>
  const pct = score * 100
  return <td className={`${cls} ${pct >= 0 ? 'up' : 'down'}`}>{pct >= 0 ? '▲' : '▼'} {Math.abs(pct).toFixed(1)}%</td>
}

function LivePriceCell({ ticker, closePrice, prices, phase }) {
  const { t } = useTranslation()
  const { fmtPrice } = useCurrency()
  const style = { whiteSpace: 'nowrap' }
  const scale = isILTicker(ticker) ? 100 : 1
  // While live prices are still loading on first paint, fall back to the
  // close price so the table is visually complete instead of showing "—".
  if (phase === null) {
    const displayClose = closePrice != null ? closePrice / scale : null
    return <td style={{ ...style, color: 'var(--muted)' }}>{displayClose != null ? fmtPrice(displayClose) : '—'}</td>
  }
  if (phase === 'idle') return <td style={{ ...style, color: 'var(--muted)', fontSize: 12 }}>{t('market.closedShort')}</td>
  const live = prices[ticker]
  if (live == null) {
    const displayClose = closePrice != null ? closePrice / scale : null
    return <td style={{ ...style, color: 'var(--muted)' }}>{displayClose != null ? fmtPrice(displayClose) : '—'}</td>
  }
  const displayLive = live / scale
  const displayClose = closePrice / scale
  const diff  = displayLive - displayClose
  const pct   = displayClose ? (diff / displayClose) * 100 : 0
  const cls   = diff >= 0 ? 'up' : 'down'
  const arrow = diff >= 0 ? '▲' : '▼'
  return (
    <td style={style}>
      <div>{fmtPrice(displayLive)}</div>
      <div className={cls} style={{ fontSize: 11 }}>
        {arrow} {fmtPrice(Math.abs(diff))} ({Math.abs(pct).toFixed(2)}%)
      </div>
    </td>
  )
}

// Sortable <th> — used by client-side tables
// Server-side sortable <th> — used by the paginated all-signals table
function ServerTh({ label, col, sortBy, sortDir, onSort, className }) {
  const active = sortBy === col
  return (
    <th onClick={() => onSort(col)} style={{ cursor: 'pointer' }} className={className}>
      {label}{active ? <span style={{ marginLeft: 3 }}>{sortDir === 'asc' ? '▲' : '▼'}</span> : null}
    </th>
  )
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

// ---------------------------------------------------------------------------
// Today's table — small dataset, client-side sort, live prices
// ---------------------------------------------------------------------------

// Shared fixed widths so BUY and SELL tables align column-by-column
// Columns: logo, ticker, company, mktcap, price, rsi, low, base, high, health, analyst target, ml score, live
const COL_WIDTHS = ['58px', '70px', '150px', '90px', '70px', '55px', '80px', '80px', '80px', '75px', '85px', '75px', '100px']

function TodayTableBody({ rows, prices, phase, onRowClick, market }) {
  const { t } = useTranslation()
  return (
    <table style={{ tableLayout: 'fixed', width: '100%', minWidth: 1024 }}>
      <colgroup>{COL_WIDTHS.map((w, i) => <col key={i} style={{ width: w }} />)}</colgroup>
      <thead>
        <tr>
          <th></th>
          <th>{t('col.ticker')}</th>
          <th>{t('col.company')}</th>
          <th>{t('col.marketCap')}</th>
          <th>{t('col.price')}</th>
          <th>{t('col.rsi')}</th>
          <th className="col-hide-sm">{t('col.lowPrice')}</th>
          <th>{t('col.basePrice')}</th>
          <th className="col-hide-sm">{t('col.highPrice')}</th>
          <th>{t('col.healthScore')}</th>
          <th>{t('col.analystTarget')}</th>
          <th className="col-hide-sm">{t('col.mlScore')}</th>
          <th>{
            phase === 'pre'  ? t('col.preMarket')  :
            phase === 'post' ? t('col.postMarket') :
                               t('col.livePrice')
          }</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
            <td>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 9 }}>
                <TierLogoStar tier={r.tier} />
                {r.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(r.ticker) ? ' logo-white-bg' : ''}`} src={r.logo_url} alt="" onError={e => e.target.style.display = 'none'} /> : null}
              </span>
            </td>
            <td>
              <strong>{displayTicker(r.ticker)}</strong>
              {r.signal === 'BUY' && r.lot_seq > 1 && (
                <span
                  title={`Add-on lot — strengthens the existing position (lot ${r.lot_seq})`}
                  style={{ marginLeft: 6, padding: '1px 6px', borderRadius: 999, background: '#eef2ff', color: '#4338ca', fontSize: 10, fontWeight: 700 }}
                >
                  ×{r.lot_seq}
                </span>
              )}
            </td>
            <td style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.company ?? '—'}</td>
            <td>{fmtMktCap(r.market_cap)}</td>
            <td><PriceCell value={r.close} ticker={r.ticker} /></td>
            <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
            <td className="col-hide-sm"><PriceCell value={r.target_low_price} ticker={r.ticker} /></td>
            <td><PriceCell value={r.target_mean_price} ticker={r.ticker} /></td>
            <td className="col-hide-sm"><PriceCell value={r.target_high_price} ticker={r.ticker} /></td>
            <HealthCell score={r.health_score} />
            <UpsideCell targetMean={r.target_mean_price} close={r.close} />
            <MLScoreCell score={r.prediction_score} className="col-hide-sm" />
            <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} phase={phase} />
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function TodayTable({ rows, prices, phase, onRowClick, market }) {
  const { t } = useTranslation()
  const { sorted } = useSort(rows, 'market_cap', 'desc')

  if (!rows || rows.length === 0) return <p className="empty">{t('signals.noSignals')}</p>

  return (
    <div className="data-table-wrap">
      <TodayTableBody rows={sorted} prices={prices} phase={phase} onRowClick={onRowClick} market={market} />
    </div>
  )
}

function TodaySellTable({ rows, prices, phase, onRowClick, market }) {
  const { t } = useTranslation()
  const { sorted } = useSort(rows, 'market_cap', 'desc')
  const [page, setPage]         = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [search, setSearch]     = useState('')

  if (!rows || rows.length === 0) return <p className="empty">{t('signals.noSignals')}</p>

  const filtered = search
    ? sorted.filter(r =>
        r.ticker?.toLowerCase().includes(search.toLowerCase()) ||
        r.company?.toLowerCase().includes(search.toLowerCase())
      )
    : sorted

  const pages     = Math.max(1, Math.ceil(filtered.length / pageSize))
  const paginated = filtered.slice((page - 1) * pageSize, page * pageSize)

  function handlePageSize(val) { setPageSize(Number(val)); setPage(1) }
  function handleSearch(val)   { setSearch(val);           setPage(1) }

  return (
    <>
      <div className="controls">
        <input
          placeholder={`🔍 ${t('table.search')}`}
          value={search}
          onChange={e => handleSearch(e.target.value)}
          style={{ width: 240 }}
        />
        {search && <button onClick={() => handleSearch('')}>{t('table.clear')}</button>}
        <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
          <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('table.rows')}</label>
          <select value={pageSize} onChange={e => handlePageSize(e.target.value)}>
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </span>
      </div>
      <div className="data-table-wrap">
        <TodayTableBody rows={paginated} prices={prices} phase={phase} onRowClick={onRowClick} market={market} />
      </div>
      <Pagination page={page} pages={pages} onChange={setPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// All-signals table — server-side sort + pagination
// ---------------------------------------------------------------------------

function AllSignalsTable({ result, sortBy, sortDir, onSort, page, onPage, onRowClick, prices, phase, market }) {
  const { t } = useTranslation()
  if (!result) return null
  const { data: rows, pages } = result

  if (!rows || rows.length === 0) return <p className="empty">{t('signals.noSignals')}</p>

  const th = (label, col, className) =>
    <ServerTh label={label} col={col} sortBy={sortBy} sortDir={sortDir} onSort={onSort} className={className} />

  return (
    <>
      <div className="data-table-wrap all-signals-wrap">
        <table style={{ tableLayout: 'fixed', width: '100%', minWidth: 1174 }}>
          <colgroup>{['80px','44px','70px','150px','90px','70px','70px','55px','80px','80px','80px','75px','85px','75px','100px'].map((w, i) => <col key={i} style={{ width: w }} />)}</colgroup>
          <thead>
            <tr>
              {th(t('col.date'),           'date')}
              <th></th>
              {th(t('col.ticker'),         'ticker')}
              {th(t('col.company'),        'company')}
              {th(t('col.marketCap'),      'market_cap')}
              {th(t('col.signal'),         'signal')}
              {th(t('col.price'),          'close')}
              {th(t('col.rsi'),            'rsi')}
              {th(t('col.lowPrice'),       'target_low_price',  'col-hide-sm')}
              {th(t('col.basePrice'),      'target_mean_price')}
              {th(t('col.highPrice'),      'target_high_price', 'col-hide-sm')}
              <th>{t('col.healthScore')}</th>
              <th>{t('col.analystTarget')}</th>
              <th className="col-hide-sm">{t('col.mlScore')}</th>
              <th>{
                phase === 'pre'  ? t('col.preMarket')  :
                phase === 'post' ? t('col.postMarket') :
                                   t('col.livePrice')
              }</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
                <td>{r.date ? (() => { const [y,m,d] = r.date.slice(0,10).split('-'); return `${d}/${m}/${y.slice(2)}` })() : '—'}</td>
                <td>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 9 }}>
                <TierLogoStar tier={r.tier} />
                {r.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(r.ticker) ? ' logo-white-bg' : ''}`} src={r.logo_url} alt="" onError={e => e.target.style.display = 'none'} /> : null}
              </span>
            </td>
                <td><strong>{displayTicker(r.ticker)}</strong></td>
                <td style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.company ?? '—'}</td>
                <td>{fmtMktCap(r.market_cap, market)}</td>
                <td>{r.signal ? <span className={`badge badge-${r.signal}`}>{r.signal}{r.signal === 'BUY' && r.lot_seq > 1 ? ` ×${r.lot_seq}` : ''}</span> : '—'}</td>
                <td><PriceCell value={r.close} ticker={r.ticker} /></td>
                <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
                <td className="col-hide-sm"><PriceCell value={r.target_low_price} ticker={r.ticker} /></td>
                <td><PriceCell value={r.target_mean_price} ticker={r.ticker} /></td>
                <td className="col-hide-sm"><PriceCell value={r.target_high_price} ticker={r.ticker} /></td>
                <HealthCell score={r.health_score} />
                <UpsideCell targetMean={r.target_mean_price} close={r.close} />
                <MLScoreCell score={r.prediction_score} className="col-hide-sm" />
                <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} phase={phase} />
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pagination page={page} pages={pages} onChange={onPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function SignalsPage() {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const [signalFilter, setSignalFilter] = usePersistedState('signals.filter', 'ALL')
  const [search, setSearch]             = usePersistedState('signals.search', '')
  const [debouncedSearch, setDebouncedSearch] = useState(search)
  const [page, setPage]                 = usePersistedState('signals.page', 1)
  const [pageSize, setPageSize]         = usePersistedState('signals.pageSize', 10)
  const [sortBy, setSortBy]             = usePersistedState('signals.sortBy', 'date')
  const [sortDir, setSortDir]           = usePersistedState('signals.sortDir', 'desc')
  const [activePeriod, setActivePeriod] = usePersistedState('signals.period2', '1Y')
  const [startDate, setStartDate]       = usePersistedState('signals.start', '')
  const [endDate,   setEndDate]         = usePersistedState('signals.end',   '')
  const [selected, setSelected]         = useState(null)
  const todayISO = new Date().toISOString().slice(0, 10)
  const PERIOD_CHIPS = ['1D', '1W', '1M', '1Y']

  // Anchor chip ranges on the latest *trading* day (signals are produced
  // overnight against yesterday's close), not today's calendar date —
  // otherwise "1D" picks an empty calendar day on weekends/holidays.
  const { data: dataStatus } = useQuery({
    queryKey: ['data-status'],
    queryFn: getDataStatus,
    staleTime: 5 * 60_000,
  })
  const anchorDate = dataStatus?.latest || todayISO

  // Each chip = N calendar days inclusive of the anchor → subtract (N-1).
  const PERIOD_DAYS = { '1D': 1, '1W': 7, '1M': 30, '1Y': 365 }

  function periodStartISO(key, anchor) {
    const n = PERIOD_DAYS[key]
    if (!n) return null
    if (n === 1) return anchor
    const [y, m, d] = anchor.split('-').map(Number)
    const dt = new Date(y, m - 1, d)  // local-time parse — avoids toISOString TZ shift
    dt.setDate(dt.getDate() - (n - 1))
    const yy = dt.getFullYear()
    const mm = String(dt.getMonth() + 1).padStart(2, '0')
    const dd = String(dt.getDate()).padStart(2, '0')
    return `${yy}-${mm}-${dd}`
  }

  // Re-anchor whenever the latest-trading-day shifts (and bootstrap the
  // first visit). Only applies when a chip is the active selection — custom
  // dates are left alone.
  useEffect(() => {
    if (!activePeriod) return
    const s = periodStartISO(activePeriod, anchorDate)
    if (s == null) return
    if (startDate !== s)          setStartDate(s)
    if (endDate   !== anchorDate) setEndDate(anchorDate)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [anchorDate, activePeriod])

  function selectPeriod(key) {
    const s = periodStartISO(key, anchorDate)
    if (s == null) return
    setActivePeriod(key)
    setStartDate(s)
    setEndDate(anchorDate)
    setPage(1)
  }

  const isDefaultView = !search && signalFilter === 'ALL' && activePeriod === '1Y'

  function resetToDefault() {
    setSearch('')
    setSignalFilter('ALL')
    selectPeriod('1Y')
  }

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 400)
    return () => clearTimeout(t)
  }, [search])

  function handleSort(col) {
    if (col === sortBy) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(col)
      setSortDir('asc')
    }
    setPage(1)
  }

  function handleSearch(val) { setSearch(val); setPage(1) }
  function handleFilter(val) { setSignalFilter(val); setPage(1) }
  function handlePageSize(val) { setPageSize(Number(val)); setPage(1) }

  const { data: todayBuy,  isLoading: loadingBuy  } = useQuery({
    queryKey: ['signals-today', 'BUY', market],
    queryFn: () => getSignalsToday('BUY', market),
    refetchInterval: 120_000,
  })

  const { data: todaySell, isLoading: loadingSell } = useQuery({
    queryKey: ['signals-today', 'SELL', market],
    queryFn: () => getSignalsToday('SELL', market),
    refetchInterval: 120_000,
  })

  const exportUrl = `/api/signals/export?${new URLSearchParams({
    ...(signalFilter && signalFilter !== 'ALL' ? { signal: signalFilter } : {}),
    ...(debouncedSearch ? { search: debouncedSearch } : {}),
    ...(startDate ? { start: startDate } : {}),
    ...(endDate   ? { end:   endDate   } : {}),
    sort_by: sortBy,
    sort_dir: sortDir,
  }).toString()}`

  const { data: allResult, isLoading: loadingAll } = useQuery({
    queryKey: ['signals', signalFilter, debouncedSearch, startDate, endDate, page, pageSize, sortBy, sortDir, market],
    queryFn: () => getSignals({
      signal:    signalFilter === 'ALL' ? undefined : signalFilter,
      search:    debouncedSearch || undefined,
      start:     startDate || undefined,
      end:       endDate   || undefined,
      months:    120,
      page,
      page_size: pageSize,
      sort_by:   sortBy,
      sort_dir:  sortDir,
      market,
    }),
    keepPreviousData: true,
  })

  // Today's tickers: stable key — fires once, not on every page/search change
  const todayTickers = useMemo(() => {
    const set = new Set()
    todayBuy?.forEach(r => r.ticker && set.add(r.ticker))
    todaySell?.forEach(r => r.ticker && set.add(r.ticker))
    return [...set]
  }, [todayBuy, todaySell])

  // All Signals table tickers: separate hook so it doesn't pollute today's cache key
  const allResultTickers = useMemo(() =>
    [...new Set((allResult?.data ?? []).map(r => r.ticker).filter(Boolean))]
  , [allResult])

  const { prices: todayPrices, phase } = useLivePrices(todayTickers)
  const { prices: allPrices }               = useLivePrices(allResultTickers)

  // Merge: today's prices take precedence
  const prices = useMemo(() => ({ ...allPrices, ...todayPrices }), [todayPrices, allPrices])

  return (
    <div>
      <div className="section">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8, marginBottom: 16 }}>
          <p className="section-title" style={{ margin: 0 }}>
            {t('signals.todayBuy', { count: loadingBuy ? '…' : (todayBuy?.length ?? 0) })}
          </p>
          <TierLegend />
        </div>
        {loadingBuy
          ? <p className="loading">{t('table.loading')}</p>
          : <TodayTable rows={todayBuy} prices={prices} phase={phase} onRowClick={setSelected} market={market} />}
      </div>

      <div className="section">
        <p className="section-title">
          {t('signals.todaySell', { count: loadingSell ? '…' : (todaySell?.length ?? 0) })}
        </p>
        {loadingSell
          ? <p className="loading">{t('table.loading')}</p>
          : <TodaySellTable rows={todaySell} prices={prices} phase={phase} onRowClick={setSelected} market={market} />}
      </div>

      <div className="section">
        <p className="section-title">{t('signals.allSignals')}</p>
        <div className="controls">
          <input
            placeholder={`🔍 ${t('table.search')}`}
            value={search}
            onChange={e => handleSearch(e.target.value)}
            style={{ width: 240 }}
          />
          {['ALL', 'BUY', 'HOLD', 'SELL'].map(s => (
            <button key={s} onClick={() => handleFilter(s)} className={signalFilter === s ? 'primary' : ''}>
              {s}
            </button>
          ))}
          {search && <button onClick={() => handleSearch('')}>{t('table.clear')}</button>}
          <span style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginLeft: 24 }}>
            <input type="date" value={startDate} max={endDate || todayISO}
              onChange={e => { setStartDate(e.target.value); setActivePeriod(null); setPage(1) }}
              style={{ fontSize: 12, padding: '6px 8px', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--surface)', color: 'var(--text)', cursor: 'pointer' }} />
            <span style={{ fontSize: 12, color: 'var(--muted)' }}>→</span>
            <input type="date" value={endDate} min={startDate || undefined} max={todayISO}
              onChange={e => { setEndDate(e.target.value); setActivePeriod(null); setPage(1) }}
              style={{ fontSize: 12, padding: '6px 8px', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--surface)', color: 'var(--text)', cursor: 'pointer' }} />
            {PERIOD_CHIPS.map(p => (
              <button key={p}
                className={`period-chip${activePeriod === p ? ' active' : ''}`}
                onClick={() => selectPeriod(p)}>
                {p}
              </button>
            ))}
            {!isDefaultView && (
              <button onClick={resetToDefault} style={{ marginLeft: 6 }}>
                RESET
              </button>
            )}
          </span>
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('table.rows')}</label>
            <select value={pageSize} onChange={e => handlePageSize(e.target.value)}>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
            <DownloadButton url={exportUrl} filenameFallback="signals" />
          </span>
        </div>
        {loadingAll && !allResult
          ? <p className="loading">{t('table.loading')}</p>
          : <AllSignalsTable
              result={allResult}
              sortBy={sortBy}
              sortDir={sortDir}
              onSort={handleSort}
              page={page}
              onPage={setPage}
              onRowClick={setSelected}
              prices={prices}
              phase={phase}
              market={market}
            />}
      </div>

      {selected && <SignalModal row={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
