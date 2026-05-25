/* Economic events (next 7 days) with importance dots, plus upcoming NYSE market
 * holidays merged in (flagged "markets closed"). */
import { useQuery } from '@tanstack/react-query'
import { getEconomicCal } from '../../api'

function ImpDots({ imp }) {
  const n = imp || 0
  return <>{[0, 1, 2].map(i => <span key={i} className={'d' + (i < n ? '' : ' off')} />)}</>
}

export default function EconomicCalendar() {
  const { data } = useQuery({ queryKey: ['market-economic-cal'], queryFn: () => getEconomicCal(7), refetchInterval: 600_000 })
  const events = data?.events || []
  const holidays = (data?.holidays || []).map(h => ({ date: h.date + ' 00:00:00', event: h.name, holiday: true }))
  const rows = [...events, ...holidays]
    .sort((a, b) => ((a.date || '') < (b.date || '') ? -1 : 1))
    .slice(0, 8)

  return (
    <div className="cal-panel">
      <div className="cal-head"><h3>Economic</h3></div>
      <div className="cal-list">
        {rows.map((e, i) => {
          const dt = new Date((e.date || '').replace(' ', 'T') + 'Z')
          const day = isNaN(dt) ? (e.date || '') : dt.toLocaleString(undefined, { weekday: 'short' })
          const time = (!e.holiday && !isNaN(dt))
            ? dt.toLocaleString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false })
            : ''
          const meta = e.holiday ? 'US markets closed'
            : [e.estimate != null ? `est ${e.estimate}` : null, e.prior != null ? `prior ${e.prior}` : null].filter(Boolean).join(' · ')
          return (
            <div className={'cal-row' + (e.holiday ? ' holiday' : '')} key={i}>
              <div className="time">{day}{time ? ' ' + time : ''}</div>
              <div className="ev"><div className="name">{e.event}</div><div className="meta">{meta}</div></div>
              <div className="imp">{e.holiday ? <span className="hol-tag">Holiday</span> : <ImpDots imp={e.importance} />}</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
