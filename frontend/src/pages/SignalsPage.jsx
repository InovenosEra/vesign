import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSignalsToday, getSignals, getPriceHistory } from '../api'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { useLivePrices } from '../hooks/useLivePrices'
import { useSort } from '../hooks/useSort'

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

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

function PredictionCell({ value }) {
  if (value == null) return <td>—</td>
  const pct = (value * 100).toFixed(2)
  return <td className={value > 0 ? 'up' : 'down'}>{value > 0 ? '▲' : '▼'} {Math.abs(pct)}%</td>
}

function LivePriceCell({ ticker, closePrice, prices, marketOpen }) {
  if (marketOpen === null) return <td style={{ color: 'var(--muted)' }}>—</td>
  if (!marketOpen) return <td style={{ color: 'var(--muted)', fontSize: 12 }}>Market Closed</td>
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
// Signal popup modal — 12M chart with vertical line at signal date
// ---------------------------------------------------------------------------

function SignalModal({ row, onClose }) {
  const today    = new Date().toISOString().slice(0, 10)
  const end12m   = today
  const target12m = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); return d.toISOString().slice(0, 10) })()
  const start12m  = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()

  const { data: history = [], isLoading } = useQuery({
    queryKey: ['price-history-signal', row.ticker],
    queryFn: () => getPriceHistory(row.ticker, { start: start12m, end: end12m }),
    staleTime: 300_000,
  })

  const base12m  = history.filter(d => d.date <= target12m).at(-1)
  const yield12m = base12m && history.length > 0
    ? ((history.at(-1).close - base12m.close) / base12m.close) * 100
    : null

  const minPrice = history.length ? Math.min(...history.map(d => d.close)) * 0.97 : 0
  const maxPrice = history.length ? Math.max(...history.map(d => d.close)) * 1.03 : 0


  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header">
          {row.logo_url && (
            <img src={row.logo_url} alt="" style={{ width: 96, height: 96, borderRadius: 10, objectFit: 'contain', flexShrink: 0 }} />
          )}
          <table style={{ fontSize: 12, borderCollapse: 'collapse', flex: 1 }}>
            <tbody>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Ticker</td>
                <td style={{ verticalAlign: 'middle' }}><strong>{row.ticker ?? '—'}</strong></td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Company</td>
                <td style={{ verticalAlign: 'middle' }}>{row.company ?? '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Market Cap</td>
                <td style={{ verticalAlign: 'middle' }}>{row.market_cap != null ? `$${(row.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 })}B` : '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Signal</td>
                <td style={{ verticalAlign: 'middle' }}>
                  {row.signal ? <span className={`badge badge-${row.signal}`}>{row.signal}</span> : '—'}
                </td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Price</td>
                <td style={{ verticalAlign: 'middle' }}>{row.close != null ? `$${fmt(row.close)}` : '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>RSI</td>
                <td style={{ verticalAlign: 'middle' }}>{row.rsi != null ? row.rsi.toFixed(1) : '—'}</td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, paddingBottom: 2, verticalAlign: 'middle' }}>Prediction</td>
                <td style={{ verticalAlign: 'middle' }} className={row.fair_value_upside == null ? '' : row.fair_value_upside >= 0 ? 'up' : 'down'}>
                  {row.fair_value_upside != null ? `${row.fair_value_upside >= 0 ? '+' : ''}${fmt(row.fair_value_upside * 100)}%` : '—'}
                </td>
              </tr>
              <tr>
                <td style={{ color: 'var(--muted)', paddingRight: 16, verticalAlign: 'middle' }}>12M Yield (organic)</td>
                <td style={{ verticalAlign: 'middle' }} className={yield12m == null ? '' : yield12m >= 0 ? 'up' : 'down'}>
                  {yield12m != null ? `${yield12m >= 0 ? '+' : ''}${fmt(yield12m)}%` : '—'}
                </td>
              </tr>
            </tbody>
          </table>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Chart */}
        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>Loading chart…</p>
        ) : (
          <ResponsiveContainer width="100%" height={340}>
            <LineChart data={history} margin={{ top: 16, right: 24, bottom: 8, left: 8 }}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis
                dataKey="date"
                tick={{ fill: 'var(--muted)', fontSize: 11 }}
                tickFormatter={d => { const [, m, day] = d.split('-'); return `${day}/${m}` }}
                interval="preserveStartEnd"
                minTickGap={50}
              />
              <YAxis
                domain={[minPrice, maxPrice]}
                tick={{ fill: 'var(--muted)', fontSize: 11 }}
                tickFormatter={v => v.toFixed(0)}
                width={48}
              />
              <Tooltip
                contentStyle={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12 }}
                labelStyle={{ color: 'var(--muted)' }}
                itemStyle={{ color: 'var(--text)' }}
                labelFormatter={d => { const [y, m, day] = d.split('-'); return `${day}/${m}/${y.slice(2)}` }}
                formatter={v => [`$${v.toFixed(2)}`, 'Close']}
              />
              <Line type="monotone" dataKey="close" stroke="var(--accent)" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Today's table — small dataset, client-side sort, live prices
// ---------------------------------------------------------------------------

// Shared fixed widths so BUY and SELL tables align column-by-column
// Columns: logo, ticker, company, mktcap, price, rsi, low, base, high, prediction, live/date
const COL_WIDTHS = ['44px', '80px', '160px', '110px', '80px', '64px', '90px', '90px', '90px', '100px', '130px']

function TodayTableBody({ rows, prices, marketOpen, onRowClick }) {
  return (
    <table style={{ tableLayout: 'fixed', width: '100%', minWidth: 1038 }}>
      <colgroup>{COL_WIDTHS.map((w, i) => <col key={i} style={{ width: w }} />)}</colgroup>
      <thead>
        <tr>
          <th></th>
          <th>Ticker</th>
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
          <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
            <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" /> : null}</td>
            <td><strong>{r.ticker ?? '—'}</strong></td>
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

function TodayTable({ rows, prices, marketOpen, onRowClick }) {
  const { sorted } = useSort(rows, 'market_cap', 'desc')

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  return (
    <div className="data-table-wrap">
      <TodayTableBody rows={sorted} prices={prices} marketOpen={marketOpen} onRowClick={onRowClick} />
    </div>
  )
}

function TodaySellTable({ rows, prices, marketOpen, onRowClick }) {
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
        <TodayTableBody rows={paginated} prices={prices} marketOpen={marketOpen} onRowClick={onRowClick} />
      </div>
      <Pagination page={page} pages={pages} onChange={setPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// All-signals table — server-side sort + pagination
// ---------------------------------------------------------------------------

function AllSignalsTable({ result, sortBy, sortDir, onSort, page, onPage, onRowClick, prices, marketOpen }) {
  if (!result) return null
  const { data: rows, pages } = result

  if (!rows || rows.length === 0) return <p className="empty">No signals found.</p>

  const th = (label, col) =>
    <ServerTh label={label} col={col} sortBy={sortBy} sortDir={sortDir} onSort={onSort} />

  return (
    <>
      <div className="data-table-wrap">
        <table style={{ tableLayout: 'fixed', width: '100%', minWidth: 1218 }}>
          <colgroup>{['80px','44px','80px','160px','110px','80px','80px','64px','90px','90px','90px','100px','130px'].map((w, i) => <col key={i} style={{ width: w }} />)}</colgroup>
          <thead>
            <tr>
              {th('Date',           'date')}
              <th></th>
              {th('Ticker',         'ticker')}
              {th('Company',        'company')}
              {th('Market Cap (B)', 'market_cap')}
              {th('Signal',         'signal')}
              {th('Price',          'close')}
              {th('RSI',            'rsi')}
              {th('Low Price',      'target_low_price')}
              {th('Base Price',     'target_mean_price')}
              {th('High Price',     'target_high_price')}
              {th('Prediction',     'fair_value_upside')}
              <th>Live Price</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="clickable-row" onClick={() => onRowClick?.(r)}>
                <td>{r.date ? (() => { const [y,m,d] = r.date.slice(0,10).split('-'); return `${d}/${m}/${y.slice(2)}` })() : '—'}</td>
                <td>{r.logo_url ? <img className="logo" src={r.logo_url} alt="" /> : null}</td>
                <td><strong>{r.ticker ?? '—'}</strong></td>
                <td>{r.company ?? '—'}</td>
                <td>{r.market_cap != null ? (r.market_cap / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 }) : '—'}</td>
                <td>{r.signal ? <span className={`badge badge-${r.signal}`}>{r.signal}</span> : '—'}</td>
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
      </div>
      <Pagination page={page} pages={pages} onChange={onPage} />
    </>
  )
}

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
  const [selected, setSelected]         = useState(null)

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

  const allTickers = useMemo(() => {
    const set = new Set()
    todayBuy?.forEach(r => r.ticker && set.add(r.ticker))
    todaySell?.forEach(r => r.ticker && set.add(r.ticker))
    ;(allResult?.data ?? []).forEach(r => r.ticker && set.add(r.ticker))
    return [...set]
  }, [todayBuy, todaySell, allResult])

  const { prices, marketOpen } = useLivePrices(allTickers)

  return (
    <div>
      <div className="section">
        <p className="section-title">
          Today's BUY Signals ({loadingBuy ? '…' : (todayBuy?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingBuy
          ? <p className="loading">Loading…</p>
          : <TodayTable rows={todayBuy} prices={prices} marketOpen={marketOpen} onRowClick={setSelected} />}
      </div>

      <div className="section">
        <p className="section-title">
          Today's SELL Signals ({loadingSell ? '…' : (todaySell?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingSell
          ? <p className="loading">Loading…</p>
          : <TodaySellTable rows={todaySell} prices={prices} marketOpen={marketOpen} onRowClick={setSelected} />}
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
              onRowClick={setSelected}
              prices={prices}
              marketOpen={marketOpen}
            />}
      </div>

      {selected && <SignalModal row={selected} onClose={() => setSelected(null)} />}
    </div>
  )
}
