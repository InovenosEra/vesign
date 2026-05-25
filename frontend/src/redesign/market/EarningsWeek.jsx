/* Earnings this week. Ported from market-v1.html (US-recognizable rows only). */
import { useQuery } from '@tanstack/react-query'
import { getEarningsWeek } from '../../api'
import { num, dateFmt, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'

const DAY_ABBR = { Monday: 'Mon', Tuesday: 'Tue', Wednesday: 'Wed', Thursday: 'Thu', Friday: 'Fri' }

export default function EarningsWeek() {
  const open = useTickerModal()
  const { data } = useQuery({ queryKey: ['market-earnings-week'], queryFn: getEarningsWeek, refetchInterval: 600_000 })
  const all = (data?.earnings || []).filter(r => r.company)
  const rows = all.slice(0, 8)
  const range = all.length ? `${dateFmt(all[0].date)} – ${dateFmt(all[all.length - 1].date)}` : ''
  return (
    <div className="cal-panel">
      <div className="cal-head"><h3>Earnings this week</h3><span className="day">{range}</span></div>
      <div className="cal-list">
        {rows.map((r, i) => {
          const when = `${r.time || ''}${r.time && r.day_of_week ? ' · ' : ''}${DAY_ABBR[r.day_of_week] || ''}`
          return (
            <div className="earn-row" key={i} data-ticker={r.ticker} data-company={r.company || ''} onClick={() => open(r.ticker, r.company)}>
              <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
              <div><div className="tk">{r.ticker}</div><div className="co">{r.company}</div></div>
              <div className="est"><span className="l">EPS est</span>{r.eps_estimated == null ? '—' : num(r.eps_estimated)}</div>
              <div><span className={'when ' + ((r.time || '').toLowerCase())}>{when || '—'}</span></div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
