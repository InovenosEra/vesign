import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSignalsToday, getSignals, getSuccessRate, runPipeline, getPipelineStatus } from '../api'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

function SignalBadge({ signal }) {
  return <span className={`badge badge-${signal}`}>{signal}</span>
}

function PredictionCell({ value }) {
  if (value == null) return <td>—</td>
  const pct = (value * 100).toFixed(2)
  return <td className={value > 0 ? 'up' : 'down'}>{value > 0 ? '▲' : '▼'} {Math.abs(pct)}%</td>
}

function LivePriceCell({ ticker, closePrice, prices, marketOpen }) {
  if (!marketOpen) return <td style={{ color: 'var(--muted)', fontSize: 12 }}>mkt closed</td>
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

function TodayTable({ rows, prices, marketOpen }) {
  const { sorted, sort, toggle } = useSort(rows, 'close', 'desc')

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  const th = (label, col) => <Th label={label} col={col} sort={sort} onSort={toggle} />

  return (
    <div className="data-table-wrap">
      <table>
        <thead>
          <tr>
            <th></th>
            {th('Ticker',      'ticker')}
            {th('Company',     'company')}
            {th('Mkt Cap (B)', 'market_cap')}
            {th('Price',       'close')}
            <th>Live Price</th>
            {th('RSI',         'rsi')}
            {th('Prediction',  'fair_value_upside')}
            {th('Base Price',  'target_mean_price')}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r, i) => (
            <tr key={i}>
              <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" /> : null}</td>
              <td><strong>{r.ticker}</strong></td>
              <td>{r.company ?? '—'}</td>
              <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
              <td>{r.close != null ? r.close.toFixed(2) : '—'}</td>
              <LivePriceCell ticker={r.ticker} closePrice={r.close} prices={prices} marketOpen={marketOpen} />
              <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
              <PredictionCell value={r.fair_value_upside} />
              <td>{r.target_mean_price != null ? r.target_mean_price.toFixed(2) : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
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
        <table>
          <thead>
            <tr>
              <th></th>
              {th('Ticker',      'ticker')}
              {th('Company',     'company')}
              {th('Mkt Cap (B)', 'market_cap')}
              {th('Date',        'date')}
              {th('Signal',      'signal')}
              {th('Price',       'close')}
              {th('RSI',         'rsi')}
              {th('Prediction',  'fair_value_upside')}
              {th('Base Price',  'target_mean_price')}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" /> : null}</td>
                <td><strong>{r.ticker}</strong></td>
                <td>{r.company ?? '—'}</td>
                <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
                <td>{r.date ? r.date.slice(0, 10) : '—'}</td>
                <td><SignalBadge signal={r.signal} /></td>
                <td>{r.close != null ? r.close.toFixed(2) : '—'}</td>
                <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
                <PredictionCell value={r.fair_value_upside} />
                <td>{r.target_mean_price != null ? r.target_mean_price.toFixed(2) : '—'}</td>
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

function SuccessRateTable({ rows }) {
  const { sorted, sort, toggle } = useSort(rows, 'success_rate', 'desc')

  if (!rows || rows.length === 0) return <p className="empty">No completed trades found.</p>

  const th = (label, col) => <Th label={label} col={col} sort={sort} onSort={toggle} />

  return (
    <div className="data-table-wrap">
      <table>
        <thead>
          <tr>
            <th></th>
            {th('Ticker',      'ticker')}
            {th('Company',     'company')}
            {th('Mkt Cap (B)', 'market_cap')}
            {th('Trades',      'total_trades')}
            {th('Win Rate',    'success_rate')}
            {th('Avg Return',  'avg_return_pct')}
            {th('Avg Days',    'avg_days_held')}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r, i) => (
            <tr key={i}>
              <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" /> : null}</td>
              <td><strong>{r.ticker}</strong></td>
              <td>{r.company ?? '—'}</td>
              <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
              <td>{r.total_trades}</td>
              <td className={r.success_rate >= 50 ? 'up' : 'down'}>{r.success_rate}%</td>
              <td className={r.avg_return_pct >= 0 ? 'up' : 'down'}>
                {r.avg_return_pct >= 0 ? '+' : ''}{r.avg_return_pct.toFixed(2)}%
              </td>
              <td>{r.avg_days_held}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Pipeline bar
// ---------------------------------------------------------------------------

function PipelineBar() {
  const [triggered, setTriggered] = useState(false)
  const { data: status, refetch } = useQuery({
    queryKey: ['pipeline-status'],
    queryFn: getPipelineStatus,
    refetchInterval: triggered ? 3000 : false,
  })

  async function handleRun() {
    try {
      await runPipeline()
      setTriggered(true)
      refetch()
    } catch (e) {
      alert(e.message)
    }
  }

  const running = status?.status === 'running'
  return (
    <div className="pipeline-bar">
      <button className="primary" onClick={handleRun} disabled={running}>
        {running ? 'Running…' : '↺ Run Pipeline'}
      </button>
      {status && status.status !== 'idle' && (
        <div className="pipeline-log">
          <strong style={{ color: status.status === 'error' ? 'var(--red)' : 'var(--muted)' }}>
            {status.status.toUpperCase()}
          </strong>
          {status.log ? '\n' + status.log : ''}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function SignalsPage() {
  const [signalFilter, setSignalFilter] = useState('ALL')
  const [search, setSearch]             = useState('')
  const [page, setPage]                 = useState(1)
  const [pageSize, setPageSize]         = useState(100)
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

  const { data: successRate, isLoading: loadingRate } = useQuery({
    queryKey: ['success-rate'],
    queryFn: () => getSuccessRate(12),
    staleTime: 300_000,
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
      <PipelineBar />

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
          : <TodayTable rows={todaySell} prices={prices} marketOpen={marketOpen} />}
      </div>

      <div className="section">
        <p className="section-title">
          All Signals — last 12 months
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
              <option value={50}>50</option>
              <option value={100}>100</option>
              <option value={250}>250</option>
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

      <div className="section">
        <p className="section-title">
          BUY→SELL Success Rate by Company — last 12 months
          {successRate && <span style={{ color: 'var(--muted)', fontSize: 12, marginLeft: 10 }}>
            {successRate.length} companies
          </span>}
        </p>
        {loadingRate
          ? <p className="loading">Loading…</p>
          : <SuccessRateTable rows={successRate} />}
      </div>
    </div>
  )
}
