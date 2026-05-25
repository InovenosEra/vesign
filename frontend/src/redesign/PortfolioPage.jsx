/* Redesign Portfolio page. Rendered inside <AppShell> (which provides the .rd
 * wrapper, tape, header, and the ticker-modal context). Ported from
 * portfolio-v1.html — Holdings tab (KPIs, performance+allocation, watchlist
 * comparison, holdings table) + a Watchlists tab. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getPortfolioHoldings, getPortfolioComparison } from '../api'
import KpiStrip from './portfolio/KpiStrip'
import PerformanceChart from './portfolio/PerformanceChart'
import AllocationDonut from './portfolio/AllocationDonut'
import WatchlistComparison from './portfolio/WatchlistComparison'
import HoldingsTable from './portfolio/HoldingsTable'
import WatchlistsTab from './portfolio/WatchlistsTab'
import './portfolio/portfolio.css'

function computeRows(holdings) {
  const rows = holdings.map(h => {
    const value = (h.total_qty != null && h.latest_close != null) ? h.total_qty * h.latest_close : null
    const cost = h.total_cost
    const pnl = (value != null && cost != null) ? value - cost : null
    const yld = (pnl != null && cost) ? pnl / cost * 100 : null
    const day = (h.latest_close != null && h.prev_close) ? (h.latest_close - h.prev_close) / h.prev_close * 100 : null
    return { ...h, value, cost, pnl, yld, day }
  }).sort((a, b) => (b.value || 0) - (a.value || 0))

  const totalCost = rows.reduce((s, r) => s + (r.cost || 0), 0)
  const totalValue = rows.reduce((s, r) => s + (r.value || 0), 0)
  const totalPnl = totalValue - totalCost
  const totalYld = totalCost ? totalPnl / totalCost * 100 : 0
  const todayDelta = rows.reduce((s, r) =>
    s + ((r.latest_close != null && r.prev_close != null) ? (r.latest_close - r.prev_close) * (r.total_qty || 0) : 0), 0)
  const todayPct = (totalValue - todayDelta) ? todayDelta / (totalValue - todayDelta) * 100 : 0

  const best = rows.slice().sort((a, b) => (b.yld || -1e9) - (a.yld || -1e9))[0]
  const worst = rows.slice().sort((a, b) => (a.yld || 1e9) - (b.yld || 1e9))[0]
  const largest = rows[0]
    ? { ...rows[0], pctOfValue: totalValue ? rows[0].value / totalValue * 100 : 0 }
    : null

  return {
    rows,
    totals: { totalCost, totalValue, totalPnl, totalYld, todayDelta, todayPct, count: rows.length },
    best, worst, largest,
  }
}

export default function PortfolioPage() {
  const [tab, setTab] = useState('holdings')
  const { data: holdings } = useQuery({ queryKey: ['portfolio-holdings'], queryFn: () => getPortfolioHoldings('US') })
  const { data: cmp } = useQuery({ queryKey: ['portfolio-comparison'], queryFn: () => getPortfolioComparison('US') })

  const arr = Array.isArray(holdings) ? holdings : []
  const { rows, totals, best, worst, largest } = computeRows(arr)

  // vs Vesign (1Y) = portfolio "Mine" yield − Vesign benchmark yield.
  let vsVesign = null
  if (Array.isArray(cmp)) {
    const ves = cmp.find(c => c.name === 'Vesign')
    const mine = cmp.find(c => c.name !== 'Vesign')
    if (ves && mine && mine.yield != null && ves.yield != null) vsVesign = mine.yield - ves.yield
  }

  const wlCount = Array.isArray(cmp) ? Math.max(0, cmp.filter(c => c.name !== 'Vesign').length) : 0
  const day = new Date().toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })

  return (
    <>
      <div className="page-head">
        <div className="ph-top">
          <div className="ph-context">
            <span className="day">{day}</span>
          </div>
        </div>
        <div className="ph-tabs">
          <a className={'ph-tab' + (tab === 'holdings' ? ' active' : '')}
            onClick={(e) => { e.preventDefault(); setTab('holdings'); window.scrollTo({ top: 0, behavior: 'instant' }) }}>
            Holdings <span className="count">{rows.length || '—'}</span>
          </a>
          <a className={'ph-tab' + (tab === 'watchlists' ? ' active' : '')}
            onClick={(e) => { e.preventDefault(); setTab('watchlists'); window.scrollTo({ top: 0, behavior: 'instant' }) }}>
            Watchlists <span className="count">{wlCount || '—'}</span>
          </a>
        </div>
      </div>

      <div className="body">
        {tab === 'holdings' && (
          <div id="holdings" className="tab-pane active">
            {!rows.length ? (
              <div className="section-h"><span className="sub">No holdings yet.</span></div>
            ) : (
              <>
                <KpiStrip totals={totals} best={best} worst={worst} largest={largest} vsVesign={vsVesign} />
                <div className="perf-grid">
                  <PerformanceChart />
                  <AllocationDonut rows={rows} totalValue={totals.totalValue} totalYld={totals.totalYld} />
                </div>
                <WatchlistComparison />
                <HoldingsTable rows={rows} subhead={`${rows.length} positions · sorted by value`} />
              </>
            )}
          </div>
        )}

        {tab === 'watchlists' && <WatchlistsTab holdingsCount={rows.length} />}
      </div>
    </>
  )
}
