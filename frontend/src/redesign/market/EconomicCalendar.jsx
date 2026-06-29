/* Economic events (next 14 days) with importance dots, plus upcoming NYSE market
 * holidays merged in (flagged "markets closed"). Paginated 10 rows per page,
 * grouped under date headers (weekday dropped from the rows -> time only). */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getEconomicCal } from '../../api'
import { dayGroupHeader } from '../fmt'
import CalPager from './CalPager'

const PAGE_SIZE = 10

function ImpDots({ imp }) {
  const n = imp || 0
  return <>{[0, 1, 2].map(i => <span key={i} className={'d' + (i < n ? '' : ' off')} />)}</>
}

export default function EconomicCalendar() {
  const [page, setPage] = useState(0)
  const { data } = useQuery({ queryKey: ['market-economic-cal', 14], queryFn: () => getEconomicCal(14), refetchInterval: 600_000 })
  const events = data?.events || []
  const holidays = (data?.holidays || []).map(h => ({ date: h.date + ' 00:00:00', event: h.name, holiday: true }))
  const all = [...events, ...holidays]
    .sort((a, b) => ((a.date || '') < (b.date || '') ? -1 : 1))
  const rows = all.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE)

  // Group the current page's rows into consecutive same-day buckets.
  const groups = []
  for (const e of rows) {
    const key = (e.date || '').slice(0, 10)
    let g = groups[groups.length - 1]
    if (!g || g.key !== key) { g = { key, rows: [] }; groups.push(g) }
    g.rows.push(e)
  }

  return (
    <div className="cal-panel">
      <div className="cal-head"><h3>Economic</h3></div>
      <div className="cal-list">
        {groups.map((g) => (
          <div className="cal-daygroup" key={g.key}>
            <div className="cal-dayhead">{dayGroupHeader(g.key)}</div>
            {g.rows.map((e, i) => {
              const dt = new Date((e.date || '').replace(' ', 'T') + 'Z')
              const time = (!e.holiday && !isNaN(dt))
                ? dt.toLocaleString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false })
                : ''
              const meta = e.holiday ? 'US markets closed'
                : [e.estimate != null ? `est ${e.estimate}` : null, e.prior != null ? `prior ${e.prior}` : null].filter(Boolean).join(' · ')
              return (
                <div className={'cal-row' + (e.holiday ? ' holiday' : '')} key={i}>
                  <div className="time">{time || '—'}</div>
                  <div className="ev"><div className="name">{e.event}</div><div className="meta">{meta}</div></div>
                  <div className="imp">{e.holiday ? <span className="hol-tag">Holiday</span> : <ImpDots imp={e.importance} />}</div>
                </div>
              )
            })}
          </div>
        ))}
      </div>
      <CalPager page={page} pageSize={PAGE_SIZE} total={all.length} onPage={setPage} />
    </div>
  )
}
