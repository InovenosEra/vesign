import { useState, useMemo, useContext, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { getSignalsToday, getSignals, WHITE_BG_LOGOS } from '../api'
import { MarketContext } from '../context/MarketContext'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'
import { usePersistedState } from '../hooks/usePersistedState'
import { useCurrency } from '../context/CurrencyContext'
import SignalModal from '../components/SignalModal'
import DownloadXLSXButton from '../components/DownloadXLSXButton'

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

const _HEALTH_COLORS = ['', '#e74c3c', '#e67e22', '#f1c40f', '#2ecc71', '#1a9e55']
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

function LivePriceCell({ ticker, closePrice, prices, marketOpen }) {
  const { t } = useTranslation()
  const { fmtPrice } = useCurrency()
  const style = { whiteSpace: 'nowrap' }
  const scale = isILTicker(ticker) ? 100 : 1
  // While live prices are still loading on first paint, fall back to the
  // close price so the table is visually complete instead of showing "—".
  if (marketOpen === null) {
    const displayClose = closePrice != null ? closePrice / scale : null
    return <td style={{ ...style, color: 'var(--muted)' }}>{displayClose != null ? fmtPrice(displayClose) : '—'}</td>
  }
  if (!marketOpen) return <td style={{ ...style, color: 'var(--muted)', fontSize: 12 }}>{t('market.closedShort')}</td>
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
const COL_WIDTHS = ['44px', '70px', '150px', '90px', '70px', '55px', '80px', '80px', '80px', '75px', '85px', '75px', '100px']

function TodayTableBody({ rows, prices, marketOpen, onRowClick, market }) {
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
          <th>{t('col.livePrice')}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
            <td>{r.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(r.ticker) ? ' logo-white-bg' : ''}`} src={r.logo_url} alt="" onError={e => e.target.style.display = 'none'} /> : null}</td>
            <td>
              <strong>{displayTicker(r.ticker)}</strong>
              {r.vqs === 9 && (
                <span
                  title="Strong BUY"
                  aria-label="Strong BUY"
                  style={{ marginLeft: 6, color: '#f1c40f', fontSize: 14 }}
                >
                  ★
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
            <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} marketOpen={marketOpen} />
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function TodayTable({ rows, prices, marketOpen, onRowClick, market }) {
  const { t } = useTranslation()
  const { sorted } = useSort(rows, 'market_cap', 'desc')

  if (!rows || rows.length === 0) return <p className="empty">{t('signals.noSignals')}</p>

  return (
    <div className="data-table-wrap">
      <TodayTableBody rows={sorted} prices={prices} marketOpen={marketOpen} onRowClick={onRowClick} market={market} />
    </div>
  )
}

function TodaySellTable({ rows, prices, marketOpen, onRowClick, market }) {
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
        <TodayTableBody rows={paginated} prices={prices} marketOpen={marketOpen} onRowClick={onRowClick} market={market} />
      </div>
      <Pagination page={page} pages={pages} onChange={setPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// All-signals table — server-side sort + pagination
// ---------------------------------------------------------------------------

function AllSignalsTable({ result, sortBy, sortDir, onSort, page, onPage, onRowClick, prices, marketOpen, market }) {
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
              <th>{t('col.livePrice')}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
                <td>{r.date ? (() => { const [y,m,d] = r.date.slice(0,10).split('-'); return `${d}/${m}/${y.slice(2)}` })() : '—'}</td>
                <td>{r.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(r.ticker) ? ' logo-white-bg' : ''}`} src={r.logo_url} alt="" onError={e => e.target.style.display = 'none'} /> : null}</td>
                <td><strong>{displayTicker(r.ticker)}</strong></td>
                <td style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.company ?? '—'}</td>
                <td>{fmtMktCap(r.market_cap, market)}</td>
                <td>{r.signal ? <span className={`badge badge-${r.signal}`}>{r.signal}</span> : '—'}</td>
                <td><PriceCell value={r.close} ticker={r.ticker} /></td>
                <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
                <td className="col-hide-sm"><PriceCell value={r.target_low_price} ticker={r.ticker} /></td>
                <td><PriceCell value={r.target_mean_price} ticker={r.ticker} /></td>
                <td className="col-hide-sm"><PriceCell value={r.target_high_price} ticker={r.ticker} /></td>
                <HealthCell score={r.health_score} />
                <UpsideCell targetMean={r.target_mean_price} close={r.close} />
                <MLScoreCell score={r.prediction_score} className="col-hide-sm" />
                <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} marketOpen={marketOpen} />
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
  const [selected, setSelected]         = useState(null)

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

  const exportUrl = `/api/signals/export.xlsx?${new URLSearchParams({
    ...(signalFilter && signalFilter !== 'ALL' ? { signal: signalFilter } : {}),
    ...(debouncedSearch ? { search: debouncedSearch } : {}),
    sort_by: sortBy,
    sort_dir: sortDir,
  }).toString()}`

  const { data: allResult, isLoading: loadingAll } = useQuery({
    queryKey: ['signals', signalFilter, debouncedSearch, page, pageSize, sortBy, sortDir, market],
    queryFn: () => getSignals({
      signal:    signalFilter === 'ALL' ? undefined : signalFilter,
      search:    debouncedSearch || undefined,
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

  const { prices: todayPrices, marketOpen } = useLivePrices(todayTickers)
  const { prices: allPrices }               = useLivePrices(allResultTickers)

  // Merge: today's prices take precedence
  const prices = useMemo(() => ({ ...allPrices, ...todayPrices }), [todayPrices, allPrices])

  return (
    <div>
      <div className="section">
        <p className="section-title">
          {t('signals.todayBuy', { count: loadingBuy ? '…' : (todayBuy?.length ?? 0) })}
        </p>
        {loadingBuy
          ? <p className="loading">{t('table.loading')}</p>
          : <TodayTable rows={todayBuy} prices={prices} marketOpen={marketOpen} onRowClick={setSelected} market={market} />}
      </div>

      <div className="section">
        <p className="section-title">
          {t('signals.todaySell', { count: loadingSell ? '…' : (todaySell?.length ?? 0) })}
        </p>
        {loadingSell
          ? <p className="loading">{t('table.loading')}</p>
          : <TodaySellTable rows={todaySell} prices={prices} marketOpen={marketOpen} onRowClick={setSelected} market={market} />}
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
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('table.rows')}</label>
            <select value={pageSize} onChange={e => handlePageSize(e.target.value)}>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
            <DownloadXLSXButton url={exportUrl} filenameFallback="signals" />
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
              marketOpen={marketOpen}
              market={market}
            />}
      </div>

      {selected && <SignalModal row={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
