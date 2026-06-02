/* Watchlists tab — read-only comparison cards built from the comparison payload
 * (name + 12m yield). Per-ticker membership and watchlist IDs aren't exposed by
 * the portfolio comparison API, so CRUD affordances live on the dedicated
 * watchlist pages, not here. Search + sort-by-yield are wired. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getPortfolioComparison } from '../../api'
import { pct } from '../fmt'

export default function WatchlistsTab({ holdingsCount }) {
  const [q, setQ] = useState('')
  const [sortDesc, setSortDesc] = useState(true)
  const { data: items } = useQuery({ queryKey: ['portfolio-comparison'], queryFn: () => getPortfolioComparison('US') })
  const all = (Array.isArray(items) ? items : []).filter(it => it.name !== 'Vesign')
  const cards = all
    .filter(c => c.name.toLowerCase().includes(q.trim().toLowerCase()))
    .sort((a, b) => sortDesc ? b.yield - a.yield : a.yield - b.yield)

  return (
    <div id="watchlists" className="tab-pane active">
      <div className="wl-toolbar">
        <div className="lead">
          <strong>{all.length}</strong> watchlist{all.length === 1 ? '' : 's'} ·{' '}
          <strong>{holdingsCount}</strong> ticker{holdingsCount === 1 ? '' : 's'} tracked
        </div>
        <div className="spacer"></div>
        <input className="search-input" type="text" placeholder="Search watchlists..."
          value={q} onChange={e => setQ(e.target.value)} />
        <div className="sort-pill" style={{ cursor: 'pointer' }} onClick={() => setSortDesc(d => !d)}>
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M3 6h18M6 12h12M10 18h4" /></svg>
          Sort: Yield {sortDesc ? '↓' : '↑'}
        </div>
      </div>

      <div className="wl-card-grid">
        {cards.map((c, i) => (
          <div className="wl-card" key={i}>
            <div className="wl-card-head">
              <div className="title">
                <div className="nm">{c.name}</div>
              </div>
              <div className="yield-block">
                <div className="l">12m yield</div>
                <div className={'y ' + (c.yield >= 0 ? 'up' : 'down')}>{pct(c.yield)}</div>
              </div>
            </div>
            <div className="wl-card-foot">
              <span className="meta">12-month yield · $1,000 per holding model</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
