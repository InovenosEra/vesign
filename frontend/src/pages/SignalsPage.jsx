import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSignalsToday, getSignals } from '../api'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------


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

// Sortable <th> — used by client-side tables
// Server-side sortable <th> — used by the paginated all-signals table
function ServerTh({ label, col, sortBy, sortDir, onSort }) {
  const active = sortBy === col
  return (
    <th onClick={() => onSort(col)} style={{ cursor: 'pointer' }}>
      {label}
      <span className={`sort-icon ${active ? 'sort-active' : ''}`}>
        {active ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ' ⇅'}
      </span>
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

// Fixed column widths shared by both Today tables so columns align across BUY/SELL sections
const TODAY_COL_WIDTHS = ['44px', '160px', '110px', '80px', '64px', '90px', '90px', '90px', '100px', '130px']

function TodayTableBody({ rows, prices, marketOpen }) {
  return (
    <table style={{ tableLayout: 'fixed', width: '100%' }}>
      <colgroup>
        {TODAY_COL_WIDTHS.map((w, i) => <col key={i} style={{ width: w }} />)}
      </colgroup>
      <thead>
        <tr>
          <th></th>
          <th>Company</th>
          <th>Market Cap (B)</th>
          <th>Price</th>
          <th>RSI</th>
          <th>Low Price</th>
          <th>Base Price</th>
          <th>High Price</th>
          <th>Prediction</th>
          <th>Live Price</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>
            <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" /> : null}</td>
            <td>{r.company ?? '—'}</td>
            <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
            <td>{r.close != null ? r.close.toFixed(2) : '—'}</td>
            <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
            <td>{r.target_low_price != null ? r.target_low_price.toFixed(2) : '—'}</td>
            <td>{r.target_mean_price != null ? r.target_mean_price.toFixed(2) : '—'}</td>
            <td>{r.target_high_price != null ? r.target_high_price.toFixed(2) : '—'}</td>
            <PredictionCell value={r.fair_value_upside} />
            <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} marketOpen={marketOpen} />
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function TodayTable({ rows, prices, marketOpen }) {
  const { sorted } = useSort(rows, 'close', 'desc')

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  return (
    <div className="data-table-wrap">
      <TodayTableBody rows={sorted} prices={prices} marketOpen={marketOpen} />
    </div>
  )
}

function TodaySellTable({ rows, prices, marketOpen }) {
  const { sorted } = useSort(rows, 'market_cap', 'desc')
  const [page, setPage]         = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [search, setSearch]     = useState('')

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

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
          placeholder="🔍 Search ticker or company"
          value={search}
          onChange={e => handleSearch(e.target.value)}
          style={{ width: 240 }}
        />
        {search && <button onClick={() => handleSearch('')}>Clear</button>}
        <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
          <label style={{ color: 'var(--muted)', fontSize: 13 }}>Rows</label>
          <select value={pageSize} onChange={e => handlePageSize(e.target.value)}>
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </span>
      </div>
      <div className="data-table-wrap">
        <TodayTableBody rows={paginated} prices={prices} marketOpen={marketOpen} />
      </div>
      <Pagination page={page} pages={pages} onChange={setPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// All-signals table — server-side sort + pagination
// ---------------------------------------------------------------------------

function AllSignalsTable({ result, sortBy, sortDir, onSort, page, onPage }) {
  if (!result) return null
  const { data: rows, pages } = result

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  const th = (label, col) =>
    <ServerTh label={label} col={col} sortBy={sortBy} sortDir={sortDir} onSort={onSort} />

  return (
    <>
      <div className="data-table-wrap">
        <table style={{ tableLayout: 'fixed', width: '100%' }}>
          <colgroup>
            {TODAY_COL_WIDTHS.map((w, i) => <col key={i} style={{ width: w }} />)}
          </colgroup>
          <thead>
            <tr>
              <th></th>
              {th('Company',        'company')}
              {th('Market Cap (B)', 'market_cap')}
              {th('Price',          'close')}
              {th('RSI',            'rsi')}
              {th('Low Price',      'target_low_price')}
              {th('Base Price',     'target_mean_price')}
              {th('High Price',     'target_high_price')}
              {th('Prediction',     'fair_value_upside')}
              {th('Date',           'date')}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" /> : null}</td>
                <td>{r.company ?? '—'}</td>
                <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
                <td>{r.close != null ? r.close.toFixed(2) : '—'}</td>
                <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
                <td>{r.target_low_price != null ? r.target_low_price.toFixed(2) : '—'}</td>
                <td>{r.target_mean_price != null ? r.target_mean_price.toFixed(2) : '—'}</td>
                <td>{r.target_high_price != null ? r.target_high_price.toFixed(2) : '—'}</td>
                <PredictionCell value={r.fair_value_upside} />
                <td>{r.date ? (() => { const [y,m,d] = r.date.slice(0,10).split('-'); return `${d}/${m}/${y.slice(2)}` })() : '—'}</td>
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
// Success rate table — client-side sort, no pagination needed
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function SignalsPage() {
  const [signalFilter, setSignalFilter] = useState('ALL')
  const [search, setSearch]             = useState('')
  const [page, setPage]                 = useState(1)
  const [pageSize, setPageSize]         = useState(10)
  const [sortBy, setSortBy]             = useState('date')
  const [sortDir, setSortDir]           = useState('desc')

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
    queryKey: ['signals-today', 'BUY'],
    queryFn: () => getSignalsToday('BUY'),
    refetchInterval: 120_000,
  })

  const { data: todaySell, isLoading: loadingSell } = useQuery({
    queryKey: ['signals-today', 'SELL'],
    queryFn: () => getSignalsToday('SELL'),
    refetchInterval: 120_000,
  })

  const { data: allResult, isLoading: loadingAll } = useQuery({
    queryKey: ['signals', signalFilter, search, page, pageSize, sortBy, sortDir],
    queryFn: () => getSignals({
      signal:    signalFilter === 'ALL' ? undefined : signalFilter,
      search:    search || undefined,
      page,
      page_size: pageSize,
      sort_by:   sortBy,
      sort_dir:  sortDir,
    }),
    keepPreviousData: true,
  })

const todayTickers = useMemo(() => {
    const set = new Set()
    todayBuy?.forEach(r => r.ticker && set.add(r.ticker))
    todaySell?.forEach(r => r.ticker && set.add(r.ticker))
    return [...set]
  }, [todayBuy, todaySell])

  const { prices, marketOpen } = useLivePrices(todayTickers)

  return (
    <div>
      <div className="section">
        <p className="section-title">
          Today's BUY Signals ({loadingBuy ? '…' : (todayBuy?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingBuy
          ? <p className="loading">Loading…</p>
          : <TodayTable rows={todayBuy} prices={prices} marketOpen={marketOpen} />}
      </div>

      <div className="section">
        <p className="section-title">
          Today's SELL Signals ({loadingSell ? '…' : (todaySell?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingSell
          ? <p className="loading">Loading…</p>
          : <TodaySellTable rows={todaySell} prices={prices} marketOpen={marketOpen} />}
      </div>

      <div className="section">
        <p className="section-title">
          All Signals
          {allResult && <span style={{ color: 'var(--muted)', fontSize: 12, marginLeft: 10 }}>
            {allResult.total.toLocaleString()} rows
          </span>}
        </p>
        <div className="controls">
          <input
            placeholder="🔍 Search ticker or company"
            value={search}
            onChange={e => handleSearch(e.target.value)}
            style={{ width: 240 }}
          />
          {['ALL', 'BUY', 'HOLD', 'SELL'].map(s => (
            <button key={s} onClick={() => handleFilter(s)} className={signalFilter === s ? 'primary' : ''}>
              {s}
            </button>
          ))}
          {search && <button onClick={() => handleSearch('')}>Clear</button>}
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ color: 'var(--muted)', fontSize: 13 }}>Rows</label>
            <select value={pageSize} onChange={e => handlePageSize(e.target.value)}>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </span>
        </div>
        {loadingAll && !allResult
          ? <p className="loading">Loading…</p>
          : <AllSignalsTable
              result={allResult}
              sortBy={sortBy}
              sortDir={sortDir}
              onSort={handleSort}
              page={page}
              onPage={setPage}
            />}
      </div>

    </div>
  )
}
