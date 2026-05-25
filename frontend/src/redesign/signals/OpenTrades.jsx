/* Open trades — full-width table below the BUY/SELL split. Ported from the
 * trades-v5.html open-trades block + openRow(). Data: getOpenTrades('US').
 * Sorted by unrealized P&L desc, paginated. */
import { useQuery } from '@tanstack/react-query'
import { getOpenTrades } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'
import { useTickerModal } from '../TickerModalContext'
import { logoCls, ymd } from './util'
import PagedTable from './Pager'

const HEAD = (
  <tr>
    <th>Ticker</th>
    <th className="r">Bought</th>
    <th className="r">Entry</th>
    <th className="r">Price</th>
    <th className="r">Days held</th>
    <th className="r">Unrealised P&amp;L</th>
    <th className="r" style={{ paddingRight: 18 }}>Yield</th>
  </tr>
)

function OpenRow({ p }) {
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()
  const yld = p.unrealized_pct
  const dollarPnL = p.buy_price && p.current_price
    ? (p.current_price - p.buy_price) * 1000 / p.buy_price // $1k per signal
    : null
  return (
    <tr data-ticker={p.ticker} data-company={p.company || ''} onClick={() => open(p.ticker, p.company)}>
      <td>
        <div className="ticker-cell">
          <img className={logoCls(p.ticker)} src={LOGO(p.ticker)} alt={p.ticker} />
          <span className="tk">{p.ticker}</span>
          <span className="co">{p.company || ''}</span>
        </div>
      </td>
      <td className="r muted">{ymd(p.buy_date)}</td>
      <td className="r">{p.buy_price == null ? '—' : num(p.buy_price)}</td>
      <td className="r">{p.current_price == null ? '—' : num(p.current_price)}</td>
      <td className="r muted">{p.days_held ?? '—'}</td>
      <td className={'r ' + dirClass(dollarPnL)}>
        {dollarPnL == null ? '—' : (dollarPnL >= 0 ? '+' : '-') + fmtPrice(Math.abs(dollarPnL), 0)}
      </td>
      <td className={'r ' + dirClass(yld)} style={{ paddingRight: 18 }}><strong>{pct(yld)}</strong></td>
    </tr>
  )
}

export default function OpenTrades() {
  const { data } = useQuery({ queryKey: ['open-trades', 'US'], queryFn: () => getOpenTrades('US') })
  const positions = Array.isArray(data) ? data : []
  const sorted = positions.slice().sort((a, b) => (b.unrealized_pct || 0) - (a.unrealized_pct || 0))

  return (
    <>
      <div className="section-h">
        <h2>Open trades <span className="sub" style={{ fontFamily: 'var(--mono)', marginLeft: 6 }}>{Array.isArray(data) ? positions.length : '—'}</span></h2>
      </div>
      <PagedTable
        head={HEAD}
        rows={sorted}
        row={(p, i) => <OpenRow key={i} p={p} />}
        emptyLabel="No open positions."
        colspan={7}
      />
    </>
  )
}
