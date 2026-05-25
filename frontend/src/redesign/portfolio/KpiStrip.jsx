/* Portfolio KPI strip — Total Invested / Current Value / P&L / vs Vesign / Best-Worst.
 * Ported from portfolio-v1.html's HOLDINGS KPI render block. */
import { num, pct } from '../fmt'

export default function KpiStrip({ totals, best, worst, largest, vsVesign }) {
  const { totalCost, totalValue, totalPnl, totalYld, todayDelta, todayPct, count } = totals
  return (
    <div className="kpi-row">
      <div className="kpi-card">
        <div className="lbl">Total Invested</div>
        <div className="val"><span className="s">$</span><span>{num(totalCost, { fd: 2 })}</span></div>
        <div className="delta"><span className="muted">{count} holdings · 1 watchlist</span></div>
      </div>

      <div className="kpi-card accent">
        <div className="lbl">Current Value</div>
        <div className="val"><span className="s">$</span><span>{num(totalValue, { fd: 2 })}</span></div>
        <div className={'delta ' + (todayDelta >= 0 ? 'up' : 'down')}>
          {todayDelta >= 0 ? '+' : '−'}${num(Math.abs(todayDelta), { fd: 2 })} today
          <span className="abs">{pct(todayPct)}</span>
        </div>
      </div>

      <div className="kpi-card">
        <div className="lbl">Total P&amp;L</div>
        <div className={'val ' + (totalPnl >= 0 ? 'up' : 'down')}>
          <span className="s">{totalPnl >= 0 ? '+$' : '−$'}</span>{num(Math.abs(totalPnl), { fd: 2 })}
        </div>
        <div className={'delta ' + (totalYld >= 0 ? 'up' : 'down')}>
          {pct(totalYld)}<span className="abs">since inception</span>
        </div>
      </div>

      <div className="kpi-card">
        <div className="lbl">vs Vesign (1Y)</div>
        <div className={'val ' + (vsVesign != null ? (vsVesign >= 0 ? 'up' : 'down') : '')}>
          {vsVesign == null ? '—' : pct(vsVesign)}
        </div>
        <div className="delta"><span className="abs">{vsVesign == null ? '—' : 'portfolio vs strategy'}</span></div>
      </div>

      <div className="kpi-card">
        <div className="lbl">Best / Worst</div>
        <div className="val" style={{ fontSize: 14, lineHeight: 1.35 }}>
          <div>{best ? <span className="up">{best.ticker} {pct(best.yld)}</span> : '—'}</div>
          <div style={{ fontSize: 12 }}>
            {worst ? <span className={worst.yld >= 0 ? 'up' : 'down'}>{worst.ticker} {pct(worst.yld)}</span> : '—'}
          </div>
        </div>
        <div className="delta muted" style={{ marginTop: 6 }}>
          {largest ? `Largest holding ${largest.ticker} · ${Math.round(largest.pctOfValue)}%` : '—'}
        </div>
      </div>
    </div>
  )
}
