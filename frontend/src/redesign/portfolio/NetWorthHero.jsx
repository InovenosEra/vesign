/* Net-worth hero — the vault's anchor. A commanding current-value figure with
 * today's move + all-time P&L, and a row of compact stat chips on the right.
 * Replaces the old 5-card KpiStrip but keeps every metric it surfaced. */
import { num, pct } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'

export default function NetWorthHero({ totals, best, worst, largest, vsVesign, watchlistCount }) {
  const { totalCost, totalValue, totalPnl, totalYld, todayDelta, todayPct, count } = totals
  const { symbol, rate } = useCurrency()
  const wc = watchlistCount == null ? 1 : watchlistCount
  const upDown = (v) => (v == null ? '' : v >= 0 ? 'up' : 'down')
  const money = (v) => symbol + num(Math.abs(v) * rate, { fd: 2 })

  return (
    <div className="nw-hero">
      <div className="nw-main">
        <div className="nw-label">Current value</div>
        <div className="nw-value"><span className="s">{symbol}</span>{num(totalValue * rate, { fd: 2 })}</div>
        <div className="nw-sub">
          <span className={'nw-today ' + (todayDelta >= 0 ? 'up' : 'down')}>
            {todayDelta >= 0 ? '▲' : '▼'} {money(todayDelta)} ({pct(todayPct)}) today
          </span>
          <span className="nw-dot">·</span>
          <span className={'nw-alltime ' + (totalPnl >= 0 ? 'up' : 'down')}>
            {totalPnl >= 0 ? '+' : '−'}{money(totalPnl)} · {pct(totalYld)} all-time
          </span>
        </div>
      </div>

      <div className="nw-chips">
        <div className="nw-chip">
          <div className="lbl">Invested</div>
          <div className="val"><span className="s">{symbol}</span>{num(totalCost * rate, { fd: 0 })}</div>
          <div className="sub">{count} holdings · {wc} watchlist{wc === 1 ? '' : 's'}</div>
        </div>
        <div className="nw-chip">
          <div className="lbl">vs Vesign (1Y)</div>
          <div className={'val ' + upDown(vsVesign)}>{vsVesign == null ? '—' : pct(vsVesign)}</div>
          <div className="sub">portfolio vs strategy</div>
        </div>
        <div className="nw-chip">
          <div className="lbl">Best / Worst</div>
          <div className="val nw-bw">
            <div>{best ? <span className="up">{best.ticker} {pct(best.yld)}</span> : '—'}</div>
            <div>{worst ? <span className={worst.yld >= 0 ? 'up' : 'down'}>{worst.ticker} {pct(worst.yld)}</span> : '—'}</div>
          </div>
        </div>
        <div className="nw-chip">
          <div className="lbl">Largest holding</div>
          <div className="val">{largest ? largest.ticker : '—'}</div>
          <div className="sub">{largest ? `${Math.round(largest.pctOfValue)}% of book` : '—'}</div>
        </div>
      </div>
    </div>
  )
}
