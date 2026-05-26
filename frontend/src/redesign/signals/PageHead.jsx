/* Signals page-head: context strip (date · NYSE) + Signals / Closed-trades tabs.
 * Ported from the trades-v5.html .page-head block. Tab state is lifted into
 * SignalsPage; counts come from the live queries. */
import { useQuery } from '@tanstack/react-query'
import { getSignalsToday, getTrades } from '../../api'
import { tradeWindow } from './util'

export default function PageHead({ tab, setTab }) {
  const { data: signals } = useQuery({ queryKey: ['signals-today', 'US'], queryFn: () => getSignalsToday(undefined, 'US') })
  const { start, end } = tradeWindow()
  const { data: trades } = useQuery({ queryKey: ['trades', start, end, 'US'], queryFn: () => getTrades({ start, end, market: 'US' }) })

  const sigList = Array.isArray(signals) ? signals : []
  const todayCount = sigList.filter(s => s.signal === 'BUY' || s.signal === 'SELL').length
  let closedCount = 0
  if (Array.isArray(trades)) for (const t of trades) closedCount += t.trade_count || 0

  const day = new Date().toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })

  return (
    <div className="page-head">
      <div className="ph-tabs">
        <a
          className={'ph-tab' + (tab === 'today' ? ' active' : '')}
          href="#"
          onClick={(e) => { e.preventDefault(); setTab('today') }}
        >Signals <span className="count">{sigList.length ? todayCount : '—'}</span></a>
        <a
          className={'ph-tab' + (tab === 'closed' ? ' active' : '')}
          href="#"
          onClick={(e) => { e.preventDefault(); setTab('closed') }}
        >Closed trades <span className="count">{Array.isArray(trades) ? closedCount.toLocaleString() : '—'}</span></a>
        <span className="day ph-inline-day">{day}</span>
      </div>
    </div>
  )
}
