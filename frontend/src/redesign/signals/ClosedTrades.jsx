/* Closed-trades pane: statistics cards (getStats → /api/stats) + flattened
 * historical-trades table (getTrades over the last 12 months). Ported from the
 * trades-v5.html #pane-closed block, fetchStats(), fetchHistoricalTrades() and
 * closedRow(). */
import { useQuery } from '@tanstack/react-query'
import { getStats, getTrades } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { logoCls, ymd, tradeWindow } from './util'
import PagedTable from './Pager'

// Market cap in billions (raw integer → "12.3"); mirrors OpenTrades' mcapB.
const mcapB = (mc) => (mc == null ? '—' : (mc / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }))

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
    <th className="r">Buy price</th>
    <th className="r">Sell date</th>
    <th className="r">Sell price</th>
    <th className="r">Days held</th>
    <th className="r" style={{ paddingRight: 18 }}>Yield</th>
  </tr>
)

function ClosedRow({ t }) {
  const open = useTickerModal()
  const ret = t.return_pct == null ? null : t.return_pct
  return (
    <tr data-ticker={t.ticker} data-company={t.company || ''} onClick={() => open(t.ticker, t.company)}>
      <td><img className={logoCls(t.ticker)} src={LOGO(t.ticker)} alt={t.ticker} /></td>
      <td><span className="tk">{t.ticker}</span></td>
      <td><span className="co">{t.company || '—'}</span></td>
      <td className="r">{mcapB(t.market_cap)}</td>
      <td className="r muted">{ymd(t.buy_date)}</td>
      <td className="r">{t.buy_price == null ? '—' : num(t.buy_price)}</td>
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

export default function ClosedTrades() {
  const { data: stats } = useQuery({ queryKey: ['stats'], queryFn: getStats })
  const { start, end } = tradeWindow()
  const { data: trades } = useQuery({ queryKey: ['trades', start, end, 'US'], queryFn: () => getTrades({ start, end, market: 'US' }) })

  // Statistics cards (canonical, matches ve-sign.com).
  const total = stats?.closed_trades != null ? stats.closed_trades.toLocaleString() : '—'
  const winRate = stats?.win_rate != null ? pct(stats.win_rate) : '—'
  const avgYield = stats?.avg_yield != null ? pct(stats.avg_yield) : '—'
  const avgYieldCls = stats?.avg_yield != null ? (stats.avg_yield >= 0 ? 'up' : 'down') : 'up'
  const winRateCls = stats?.win_rate != null ? (stats.win_rate >= 50 ? 'up' : 'down') : 'up'
  const avgDays = stats?.avg_hold_days != null
    ? <>{Math.round(stats.avg_hold_days)} <small style={{ color: 'var(--ink-3)', fontSize: 13 }}>days</small></>
    : '—'

  // Flatten per-ticker aggregates → one row per closed trade, newest first.
  const flat = []
  if (Array.isArray(trades)) {
    for (const t of trades) {
      for (const tr of (t.trades || [])) {
        flat.push({
          ticker: t.ticker, company: t.company, market_cap: t.market_cap,
          buy_date: tr.buy_date, buy_price: tr.buy_price,
          sell_date: tr.sell_date, sell_price: tr.sell_price,
          days_held: tr.days_held, return_pct: tr.return_pct,
          result: tr.result,
        })
      }
    }
    flat.sort((a, b) => String(b.sell_date || '').localeCompare(String(a.sell_date || '')))
  }
  const closedCount = Array.isArray(trades) ? flat.length.toLocaleString() : '—'
  const closedSub = Array.isArray(trades) ? `${flat.length} closed trades · newest first` : '—'

  return (
    <>
      <div className="metrics">
        <StatCard label="Total trades" value={total} />
        <StatCard label="Win rate" value={winRate} cls={winRateCls} />
        <StatCard label="Avg yield" value={avgYield} cls={avgYieldCls} />
        <StatCard label="Avg days held" value={avgDays} />
      </div>

      <div className="section-h">
        <h2>Closed trades <span className="sub" style={{ fontFamily: 'var(--mono)', marginLeft: 6 }}>{closedCount}</span></h2>
        <span className="sub">{closedSub}</span>
      </div>
      <PagedTable
        head={HEAD}
        rows={flat}
        row={(t, i) => <ClosedRow key={i} t={t} />}
        emptyLabel="No closed trades."
        colspan={10}
      />
    </>
  )
}
