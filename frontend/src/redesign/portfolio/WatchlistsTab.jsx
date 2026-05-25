/* Watchlists tab — cards built from the comparison payload (name + 12m yield).
 * The per-ticker watchlist membership isn't exposed by the portfolio API, so the
 * cards show the data we have (name, yield) plus the create-new affordance.
 * Mirrors portfolio-v1.html's WATCHLISTS TAB markup. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getPortfolioComparison } from '../../api'
import { pct } from '../fmt'

export default function WatchlistsTab({ holdingsCount }) {
  const [q, setQ] = useState('')
  const { data: items } = useQuery({ queryKey: ['portfolio-comparison'], queryFn: () => getPortfolioComparison('US') })
  const all = (Array.isArray(items) ? items : []).filter(it => it.name !== 'Vesign')
  const cards = all.filter(c => c.name.toLowerCase().includes(q.trim().toLowerCase()))

  return (
    <div id="watchlists" className="tab-pane active">
      <div className="wl-toolbar">
        <div className="lead">
          <strong>{all.length}</strong> watchlist{all.length === 1 ? '' : 's'} ·{' '}
          <strong>{holdingsCount}</strong> ticker{holdingsCount === 1 ? '' : 's'} tracked
        </div>
        <div className="spacer"></div>
        <input className="search-input" type="text" placeholder="Search watchlists or tickers..."
          value={q} onChange={e => setQ(e.target.value)} />
        <div className="sort-pill">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M3 6h18M6 12h12M10 18h4" /></svg>
          Sort: Yield
        </div>
        <div className="btn-new">
          <span className="plus">+</span> New watchlist
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
              <div className="menu">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="5" r="1.6" /><circle cx="12" cy="12" r="1.6" /><circle cx="12" cy="19" r="1.6" /></svg>
              </div>
            </div>
            <div className="wl-card-foot">
              <span className="add">+ Add ticker</span>
              <span className="meta">12-month yield · $1,000 per holding model</span>
            </div>
          </div>
        ))}

        <div className="wl-card add-new">
          <div className="plus-big">+</div>
          <div className="add-label">Create new watchlist</div>
          <div className="add-sub">Group tickers by theme, strategy, or sector</div>
        </div>
      </div>
    </div>
  )
}
