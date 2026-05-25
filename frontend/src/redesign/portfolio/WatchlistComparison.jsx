/* Watchlist comparison bars — Vesign benchmark (green) vs per-watchlist yield.
 * Ported verbatim from portfolio-v1.html's WATCHLIST COMPARISON block. */
import { useQuery } from '@tanstack/react-query'
import { getPortfolioComparison } from '../../api'
import { pct, dirClass } from '../fmt'

export default function WatchlistComparison() {
  const { data: items } = useQuery({ queryKey: ['portfolio-comparison'], queryFn: () => getPortfolioComparison('US') })
  const list = Array.isArray(items) ? items : []
  const maxAbs = Math.max(...list.map(i => Math.abs(i.yield || 0)), 1)

  return (
    <>
      <div className="section-h">
        <h2>Watchlist comparison</h2>
        <span className="sub">12-month yield · $1,000 per holding model</span>
        <a className="right" href="#watchlists">Manage watchlists →</a>
      </div>
      <div className="wl-grid">
        {list.map((it, i) => {
          const isVesign = it.name === 'Vesign'
          const width = Math.max(2, Math.abs(it.yield || 0) / maxAbs * 100)
          const fillCls = isVesign ? 'green' : (it.yield >= 0 ? '' : 'down')
          return (
            <div className={'wl-row' + (isVesign ? ' vesign' : '')} key={i}>
              <div className={'wl-name' + (isVesign ? ' vesign' : '')}>{it.name}</div>
              <div className="wl-bar"><div className={'fill ' + fillCls} style={{ width: `${width.toFixed(0)}%` }}></div></div>
              <div className={'wl-yield ' + (isVesign ? 'vesign' : dirClass(it.yield))}>{pct(it.yield)}</div>
            </div>
          )
        })}
      </div>
    </>
  )
}
