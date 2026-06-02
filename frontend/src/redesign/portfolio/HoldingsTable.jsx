/* Holdings table — one row per ticker; click opens the SignalModal.
 * Ported verbatim from portfolio-v1.html's HOLDINGS TABLE block. */
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useCurrency } from '../../context/CurrencyContext'

const csvCell = (v) => {
  const s = v == null ? '' : String(v)
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s
}

export default function HoldingsTable({ rows, subhead }) {
  const open = useTickerModal()
  const { fmtPrice, fmtAmount } = useCurrency()

  const exportCsv = () => {
    const header = ['Ticker', 'Company', 'Shares', 'Avg cost', 'Last close', 'Day %', 'Total cost', 'Current value', 'P&L', 'Yield %']
    const lines = [header.join(',')]
    for (const r of rows) {
      lines.push([r.ticker, r.company || '', r.total_qty, r.avg_price, r.latest_close,
        r.day, r.cost, r.value, r.pnl, r.yld].map(csvCell).join(','))
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = `vesign-holdings-${rows.length}.csv`
    document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url)
  }

  return (
    <>
      <div className="section-h">
        <h2>Holdings</h2>
        <span className="sub">{subhead}</span>
        <a className="right" style={{ cursor: 'pointer' }} onClick={exportCsv}>Export CSV →</a>
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
              <td className="r">{r.avg_price == null ? '—' : fmtPrice(r.avg_price)}</td>
              <td className="r">{r.latest_close == null ? '—' : fmtPrice(r.latest_close)}</td>
              <td className={'r ' + dirClass(r.day)}>{pct(r.day)}</td>
              <td className="r muted">{r.cost == null ? '—' : fmtPrice(r.cost)}</td>
              <td className="r">{r.value == null ? '—' : fmtPrice(r.value)}</td>
              <td className={'r ' + dirClass(r.pnl)}>
                {r.pnl == null ? '—' : fmtAmount(r.pnl)}
              </td>
              <td className={'r ' + dirClass(r.yld)} style={{ paddingRight: 18 }}><strong>{pct(r.yld)}</strong></td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
