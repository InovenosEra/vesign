/* Upgrades & Downgrades panel — analyst RATING actions (sibling to the
 * Analyst changes panel, which covers target-price moves only). US-only,
 * sourced from FMP's market-wide grades stream. Click a row → ticker modal. */
import { useQuery } from '@tanstack/react-query'
import { getTopGrades } from '../../api'
import { useCurrency } from '../../context/CurrencyContext'
import { useTickerModal } from '../TickerModalContext'
import { LOGO } from '../fmt'

const kindClass = (k) => k === 'UPGRADE' ? 'upgrade' : k === 'DOWNGRADE' ? 'downgrade' : 'initiate'
const kindLabel = (k) => k === 'UPGRADE' ? 'Upgrade' : k === 'DOWNGRADE' ? 'Downgrade' : 'Initiate'

export default function Upgrades() {
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()
  const { data } = useQuery({ queryKey: ['market-grades-top'], queryFn: () => getTopGrades(5), refetchInterval: 300_000 })
  const grades = data?.grades || []
  if (!grades.length) return null
  return (
    <div className="news-panel">
      <div className="news-head"><h3>Upgrades &amp; downgrades</h3><span className="day">Recent</span></div>
      <div className="cal-list">
        {grades.map((g, i) => (
          <div className="an-row" key={i} data-ticker={g.ticker} onClick={() => open(g.ticker, g.company)}>
            <img className="logo-mini" src={LOGO(g.ticker)} alt={g.ticker} />
            <div className="info"><div className="tk">{g.ticker}</div><div className="firm">{g.firm || g.company || ''}</div></div>
            <span className={'action ' + kindClass(g.kind)}>{kindLabel(g.kind)}</span>
            <div className="grade">
              <div>
                {g.from_grade
                  ? <><span className="old">{g.from_grade}</span><span className="arr">→</span>{g.to_grade}</>
                  : g.to_grade}
              </div>
              {g.price_target != null && <div className="g-pt">PT {fmtPrice(g.price_target, 0)}</div>}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
