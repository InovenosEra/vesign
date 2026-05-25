/* Analyst changes panel. Target prices use useCurrency().fmtPrice (converts). */
import { useQuery } from '@tanstack/react-query'
import { getTopAnalyst } from '../../api'
import { useCurrency } from '../../context/CurrencyContext'
import { useTickerModal } from '../TickerModalContext'
import { LOGO } from '../fmt'

const kindClass = (k) => k === 'RAISE-TP' ? 'reiterate' : k === 'LOWER-TP' ? 'downgrade' : k === 'INITIATE' ? 'initiate' : ''
const kindLabel = (k) => k === 'RAISE-TP' ? 'Raise TP' : k === 'LOWER-TP' ? 'Lower TP' : k === 'INITIATE' ? 'Initiate' : k

export default function AnalystChanges() {
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()
  const { data } = useQuery({ queryKey: ['market-analyst-top'], queryFn: () => getTopAnalyst(2, 5), refetchInterval: 300_000 })
  const changes = data?.changes || []
  return (
    <div className="news-panel">
      <div className="news-head"><h3>Analyst changes</h3><span className="day">Today</span></div>
      <div className="cal-list">
        {changes.map((c, i) => (
          <div className="an-row" key={i} data-ticker={c.ticker} onClick={() => open(c.ticker, c.company)}>
            <img className="logo-mini" src={LOGO(c.ticker)} alt={c.ticker} />
            <div className="info"><div className="tk">{c.ticker}</div><div className="firm">{c.company || ''}</div></div>
            <span className={'action ' + kindClass(c.kind)}>{kindLabel(c.kind)}</span>
            <div className="target">
              {c.prev_target_mean_price == null
                ? fmtPrice(c.target_mean_price, 0)
                : <><span className="old">{fmtPrice(c.prev_target_mean_price, 0)}</span><span className="arr">→</span>{fmtPrice(c.target_mean_price, 0)}</>}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
