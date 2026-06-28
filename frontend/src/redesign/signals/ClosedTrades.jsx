/* Closed-trades pane: a date-range control (From/To + period chips), four stat
 * cards computed over the selected range, a ticker/company search box, and the
 * flattened historical-trades table. Mirrors production's TradesPage controls +
 * stats (1Y default, DCA-aware per-trade yield) in the redesign style. */
import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getTrades } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { logoCls, ymd, tradeWindow } from './util'
import PagedTable from './Pager'

// Period sentinels: months, or 'ytd'. Order matches production's chip row.
const PERIODS = [[3, '3M'], [6, '6M'], ['ytd', 'YTD'], [12, '1Y'], [24, '2Y'], [36, '3Y'], [60, '5Y']]
const todayISO = () => new Date().toISOString().slice(0, 10)
function isoMonthsAgo(n) {
  const d = new Date()
  d.setMonth(d.getMonth() - n)
  return d.toISOString().slice(0, 10)
}
// Period sentinel ('ytd' | months) → ISO start date (matches prod startForPeriod).
function startForPeriod(p) {
  if (p === 'ytd') return `${new Date().getFullYear()}-01-01`
  return isoMonthsAgo(p)
}

// Market cap in billions; mirrors OpenTrades' mcapB.
const mcapB = (mc) => (mc == null ? '—' : (mc / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }))

// DCA-aware per-trade yield: dollar-weighted avg_cost when present (multi-lot),
// else the stored return_pct. Matches production's per-row yield AND its stat
// cards (dcaActive = true). Returns null only when there's no basis at all.
const yieldOf = (t) => (t.avg_cost && t.sell_price)
  ? (t.sell_price - t.avg_cost) / t.avg_cost * 100
  : (t.return_pct ?? null)

// Columns mirror production's "Historical Trades" table (TradesPage.jsx) and the
// sibling Open Trades table: logo · Ticker · Company · Market Cap · Buy date ·
// Buy price · Sell date · Sell price · Days held · Yield.
const HEAD = (
  <tr>
    <th></th>
    <th>Ticker</th>
    <th>Company</th>
    <th className="r">Market Cap</th>
    <th className="r">Buy date</th>
    <th className="r">Avg Cost</th>
    <th className="r">Sell date</th>
    <th className="r">Sell price</th>
    <th className="r">Days held</th>
    <th className="r" style={{ paddingRight: 18 }}>Yield</th>
  </tr>
)

function ClosedRow({ t }) {
  const open = useTickerModal()
  const ret = yieldOf(t)
  // Multi-lot (DCA) trades show the dollar-weighted avg cost + an ×N lots badge,
  // matching the server. Single-lot: avg_cost == buy_price, no badge.
  const cost = t.avg_cost != null ? t.avg_cost : t.buy_price
  const multi = t.n_lots > 1
  const lotsTitle = multi ? (t.lots || []).map(l => `Lot ${l.seq}: ${l.date} @ ${num(l.price)}`).join('\n') : undefined
  return (
    <tr data-ticker={t.ticker} data-company={t.company || ''} onClick={() => open(t.ticker, t.company)}>
      <td><img className={logoCls(t.ticker)} src={LOGO(t.ticker)} alt={t.ticker} /></td>
      <td><span className="tk">{t.ticker}</span></td>
      <td><span className="co">{t.company || '—'}</span></td>
      <td className="r">{mcapB(t.market_cap)}</td>
      <td className="r muted">{ymd(t.buy_date)}</td>
      <td className="r">
        <span className="avg-cost">
          {multi && <span className="lot-badge" title={lotsTitle}>×{t.n_lots}</span>}
          <span>{cost == null ? '—' : num(cost)}</span>
        </span>
      </td>
      <td className="r muted">{ymd(t.sell_date)}</td>
      <td className="r">{t.sell_price == null ? '—' : num(t.sell_price)}</td>
      <td className="r muted">{t.days_held ?? '—'}</td>
      <td className={'r ' + dirClass(ret)} style={{ paddingRight: 18 }}><strong>{pct(ret)}</strong></td>
    </tr>
  )
}

function StatCard({ label, value, cls }) {
  return (
    <div className="metric-card">
      <div className="m-label">{label}</div>
      <div className={'m-value' + (cls ? ' ' + cls : '')}>{value}</div>
    </div>
  )
}

const SearchIcon = () => (
  <svg className="ico" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="11" cy="11" r="7" /><path d="M21 21l-4.3-4.3" />
  </svg>
)

export default function ClosedTrades() {
  // Default window = trailing 1Y (matches prod's default activePeriod = 12).
  const [{ start, end }, setRange] = useState(() => tradeWindow())
  const [period, setPeriod] = useState(12)
  const [search, setSearch] = useState('')

  const { data: trades } = useQuery({
    queryKey: ['trades', start, end, 'US', true],
    queryFn: () => getTrades({ start, end, market: 'US', includeLots: true }),
    staleTime: 300_000,
  })

  function selectPeriod(p) {
    setPeriod(p)
    setRange({ start: startForPeriod(p), end: todayISO() })
  }

  // Flatten per-ticker aggregates → one row per closed trade, newest first.
  const flat = useMemo(() => {
    const out = []
    if (Array.isArray(trades)) {
      for (const t of trades) {
        for (const tr of (t.trades || [])) {
          out.push({
            ticker: t.ticker, company: t.company, market_cap: t.market_cap,
            buy_date: tr.buy_date, buy_price: tr.buy_price,
            sell_date: tr.sell_date, sell_price: tr.sell_price,
            days_held: tr.days_held, return_pct: tr.return_pct,
            avg_cost: tr.avg_cost, n_lots: tr.n_lots, lots: tr.lots,
          })
        }
      }
      out.sort((a, b) => String(b.sell_date || '').localeCompare(String(a.sell_date || '')))
    }
    return out
  }, [trades])

  const loaded = Array.isArray(trades)
  const totalPairs = flat.length
  const wins = flat.filter(t => yieldOf(t) > 0).length
  const winRate = totalPairs ? (wins / totalPairs) * 100 : null
  const avgReturn = totalPairs ? flat.reduce((s, t) => s + (yieldOf(t) ?? 0), 0) / totalPairs : null
  const avgDays = totalPairs ? flat.reduce((s, t) => s + (t.days_held ?? 0), 0) / totalPairs : null

  const totalC = loaded ? totalPairs.toLocaleString() : '—'
  const winC = winRate != null ? winRate.toFixed(1) + '%' : '—'
  const yieldC = avgReturn != null ? (avgReturn >= 0 ? '+' : '') + avgReturn.toFixed(2) + '%' : '—'
  const yieldCls = avgReturn != null ? (avgReturn >= 0 ? 'up' : 'down') : ''
  const daysC = avgDays != null ? Math.round(avgDays) : '—'

  // Search filters the table only — the stat cards stay over the full range.
  const q = search.trim().toLowerCase()
  const rows = q
    ? flat.filter(t => (t.ticker || '').toLowerCase().includes(q) || (t.company || '').toLowerCase().includes(q))
    : flat

  return (
    <>
      <div className="ct-controls">
        <label>From</label>
        <input type="date" className="ct-date" value={start}
          onChange={e => { setRange(r => ({ ...r, start: e.target.value })); setPeriod(null) }} />
        <label>To</label>
        <input type="date" className="ct-date" value={end}
          onChange={e => { setRange(r => ({ ...r, end: e.target.value })); setPeriod(null) }} />
        <span className="ct-chips">
          {PERIODS.map(([p, label]) => (
            <button key={label} className={'ct-chip' + (period === p ? ' active' : '')}
              onClick={() => selectPeriod(p)}>{label}</button>
          ))}
        </span>
      </div>

      <div className="metrics">
        <StatCard label="Total trades" value={totalC} />
        <StatCard label="Win rate" value={winC} />
        <StatCard label="Avg yield/trade" value={yieldC} cls={yieldCls} />
        <StatCard label="Avg days held" value={daysC} />
      </div>

      <div className="ct-search">
        <SearchIcon />
        <input type="text" placeholder="Search ticker or company"
          value={search} onChange={e => setSearch(e.target.value)} />
      </div>

      <PagedTable
        key={q}
        head={HEAD}
        rows={rows}
        row={(t, i) => <ClosedRow key={i} t={t} />}
        emptyLabel={loaded ? (q ? 'No matching trades.' : 'No closed trades.') : 'Loading…'}
        colspan={10}
      />
    </>
  )
}
