/* Earnings over the next 14 days (US-recognizable rows only), paginated 10/page,
 * grouped under date headers. Each company is one line: logo · ticker/company ·
 * health dots · market cap · P/E · EPS est, spread across the row width. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getEarningsWeek } from '../../api'
import { num, LOGO, dayGroupHeader } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import CalPager from './CalPager'

const PAGE_SIZE = 10
const HEALTH_COLOR = { 1: '#ff4d5c', 2: '#c2660c', 3: '#ff9500', 4: '#00d97e', 5: '#0a8f54' }

const capFmt = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'
// Negative / zero P/E (loss-makers) shown as em-dash rather than a misleading number.
const peFmt = (pe) => (pe == null || pe <= 0) ? '—' : pe.toFixed(1)

function HealthDots({ score }) {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  const c = HEALTH_COLOR[n] || '#6b7280'
  return (
    <span className="hdots" title={score == null ? 'Health n/a' : `Health ${n}/5`}>
      {[0, 1, 2, 3, 4].map(i => (
        <span key={i} className={'hd' + (i < n ? '' : ' off')} style={i < n ? { background: c } : undefined} />
      ))}
    </span>
  )
}

export default function EarningsWeek() {
  const open = useTickerModal()
  const [page, setPage] = useState(0)
  const { data } = useQuery({ queryKey: ['market-earnings-week', 14], queryFn: () => getEarningsWeek(14), refetchInterval: 600_000 })
  // FMP returns the window DESCENDING; sort ascending by date so the soonest
  // earnings (incl. today's) show first. `date` is an ISO YYYY-MM-DD string,
  // safe to compare lexically.
  const all = (data?.earnings || [])
    .filter(r => r.company)
    .sort((a, b) => (a.date || '').localeCompare(b.date || ''))
  const rows = all.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE)

  // Group the current page's rows into consecutive same-day buckets.
  const groups = []
  for (const r of rows) {
    const key = (r.date || '').slice(0, 10)
    let g = groups[groups.length - 1]
    if (!g || g.key !== key) { g = { key, rows: [] }; groups.push(g) }
    g.rows.push(r)
  }

  return (
    <div className="cal-panel">
      <div className="cal-head"><h3>Earnings</h3></div>
      <div className="cal-list">
        {groups.map((g) => (
          <div className="cal-daygroup" key={g.key}>
            <div className="cal-dayhead">{dayGroupHeader(g.key)}</div>
            {g.rows.map((r, i) => (
              <div className="earn-row" key={i} data-ticker={r.ticker} data-company={r.company || ''} onClick={() => open(r.ticker, r.company)}>
                <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
                <div className="earn-id">
                  <span className="tk">{r.ticker}</span>
                  <span className="co">{r.company}</span>
                </div>
                <div className="earn-metrics">
                  <HealthDots score={r.health_score} />
                  <span className="em em-cap"><i>Cap</i>{capFmt(r.market_cap)}</span>
                  <span className="em em-pe"><i>P/E</i>{peFmt(r.pe_ttm)}</span>
                  <span className="em em-eps"><i>EPS est</i>{r.eps_estimated == null ? '—' : num(r.eps_estimated)}</span>
                  {r.time && <span className={'when ' + r.time.toLowerCase()}>{r.time}</span>}
                </div>
              </div>
            ))}
          </div>
        ))}
      </div>
      <CalPager page={page} pageSize={PAGE_SIZE} total={all.length} onPage={setPage} />
    </div>
  )
}
