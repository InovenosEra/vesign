/* Economic events (next 7 days) with importance dots. Ported verbatim. */
import { useQuery } from '@tanstack/react-query'
import { getEconomicCal } from '../../api'

function ImpDots({ imp }) {
  const n = imp || 0
  return <>{[0, 1, 2].map(i => <span key={i} className={'d' + (i < n ? '' : ' off')} />)}</>
}

export default function EconomicCalendar() {
  const { data } = useQuery({ queryKey: ['market-economic-cal'], queryFn: () => getEconomicCal(7), refetchInterval: 600_000 })
  const events = (data?.events || []).slice(0, 8)
  return (
    <div className="cal-panel">
      <div className="cal-head"><h3>Economic</h3></div>
      <div className="cal-list">
        {events.map((e, i) => {
          const dt = new Date((e.date || '').replace(' ', 'T') + 'Z')
          const dayHHmm = isNaN(dt) ? (e.date || '')
            : dt.toLocaleString(undefined, { weekday: 'short', hour: '2-digit', minute: '2-digit', hour12: false })
          const meta = [
            e.estimate != null ? `est ${e.estimate}` : null,
            e.prior != null ? `prior ${e.prior}` : null,
          ].filter(Boolean).join(' · ')
          return (
            <div className="cal-row" key={i}>
              <div className="time">{dayHHmm}</div>
              <div className="ev"><div className="name">{e.event}</div><div className="meta">{meta}</div></div>
              <div className="imp"><ImpDots imp={e.importance} /></div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
