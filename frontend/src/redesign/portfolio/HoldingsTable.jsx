/* Holdings table — one row per ticker; click opens the SignalModal.
 * Ported verbatim from portfolio-v1.html's HOLDINGS TABLE block. */
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'

export default function HoldingsTable({ rows, subhead }) {
  const open = useTickerModal()
  return (
    <>
      <div className="section-h">
        <h2>Holdings</h2>
        <span className="sub">{subhead}</span>
        <a className="right" href="#">Export CSV →</a>
      </div>
      <table className="data-table">
        <thead>
          <tr>
            <th>Ticker</th>
            <th className="r">Shares</th>
            <th className="r">Avg cost</th>
            <th className="r">Last close</th>
            <th className="r">Day</th>
            <th className="r">Total cost</th>
            <th className="r">Current value</th>
            <th className="r">P&amp;L $</th>
            <th className="r" style={{ paddingRight: 18 }}>Yield</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.ticker} onClick={() => open(r.ticker, r.company || '')}>
              <td>
                <div className="ticker-cell">
                  <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
                  <span className="tk">{r.ticker}</span>
                  <span className="co">{r.company || ''}</span>
                </div>
              </td>
              <td className="r">{r.total_qty == null ? '—' : num(r.total_qty, { fd: 2 })}</td>
              <td className="r">{r.avg_price == null ? '—' : num(r.avg_price)}</td>
              <td className="r">{r.latest_close == null ? '—' : num(r.latest_close)}</td>
              <td className={'r ' + dirClass(r.day)}>{pct(r.day)}</td>
              <td className="r muted">{r.cost == null ? '—' : num(r.cost, { fd: 2 })}</td>
              <td className="r">{r.value == null ? '—' : num(r.value, { fd: 2 })}</td>
              <td className={'r ' + dirClass(r.pnl)}>
                {r.pnl == null ? '—' : (r.pnl >= 0 ? '+$' : '−$') + num(Math.abs(r.pnl), { fd: 2 })}
              </td>
              <td className={'r ' + dirClass(r.yld)} style={{ paddingRight: 18 }}><strong>{pct(r.yld)}</strong></td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
