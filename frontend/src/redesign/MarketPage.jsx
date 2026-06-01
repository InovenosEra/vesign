/* Redesign Market page. Renders inside <AppShell> (.rd wrapper, tape, header,
 * ticker-modal context). Tabbed: Overview / Market Trend / News / Calendar. */
import { useState } from 'react'
import { useQueries } from '@tanstack/react-query'
import {
  getIndices, getCommodities, getCurrencies, getMovers,
  getHighsLows, getValuation, getBreadth, getSectors,
} from '../api'
import PageHead from './market/PageHead'
import Indices from './market/Indices'
import Commodities from './market/Commodities'
import Currencies from './market/Currencies'
import Breadth from './market/Breadth'
import Movers from './market/Movers'
import SectorHeatmap from './market/SectorHeatmap'
import SectorModal from './market/SectorModal'
import NewsFeed from './market/NewsFeed'
import AnalystActivity from './market/AnalystActivity'
import EarningsWeek from './market/EarningsWeek'
import EconomicCalendar from './market/EconomicCalendar'

// Gate the Overview so all sections paint together instead of popping in one by
// one. Runs every section's query in parallel (same queryKeys as the child
// components, so React Query dedupes — no double fetch — and children read from
// cache instantly once revealed). Ready once nothing is on its FIRST load;
// errored queries count as settled, so a failing endpoint can't freeze the page.
function useOverviewReady(enabled) {
  let fx = 'ILS'
  try { fx = localStorage.getItem('rd-fx-base') || 'ILS' } catch { /* ignore */ }
  const defs = [
    [['market-indices'], getIndices],
    [['market-commodities'], getCommodities],
    [['market-currencies', fx], () => getCurrencies(fx)],
    [['market-movers', 'active'], () => getMovers('active', 10)],
    [['market-movers', 'gainers'], () => getMovers('gainers', 10)],
    [['market-movers', 'losers'], () => getMovers('losers', 10)],
    [['market-highs-lows', 'high'], () => getHighsLows('high', 10)],
    [['market-valuation'], () => getValuation(10)],
    [['market-breadth'], getBreadth],
    [['market-sectors'], getSectors],
  ]
  const qs = useQueries({
    queries: defs.map(([queryKey, queryFn]) => ({ queryKey, queryFn, enabled })),
  })
  return !qs.some(q => q.isLoading)
}

function SkelRow({ n, h = 96 }) {
  return (
    <div className="ov-skel-grid">
      {Array.from({ length: n }).map((_, i) => (
        <div key={i} className="rd-skel" style={{ height: h }} />
      ))}
    </div>
  )
}

function OverviewSkeleton() {
  return (
    <div className="ov-skel" aria-busy="true">
      <div className="rd-skel ov-skel-h" />
      <SkelRow n={5} h={120} />
      <div className="rd-skel ov-skel-h" />
      <SkelRow n={8} />
      <div className="rd-skel ov-skel-h" />
      <SkelRow n={5} />
      <div className="rd-skel ov-skel-h" />
      <SkelRow n={5} h={220} />
    </div>
  )
}

export default function MarketPage() {
  const [tab, setTab] = useState('overview')
  const [sector, setSector] = useState(null)
  const ovReady = useOverviewReady(tab === 'overview')
  return (
    <>
      <PageHead tab={tab} setTab={setTab} />
      <div className="body">
        {tab === 'overview' && (
          !ovReady ? <OverviewSkeleton /> : (
          <>
            <Indices />
            <Commodities />
            <Currencies />
            <Movers />
            <Breadth />
            <SectorHeatmap onOpenSector={setSector} />
            <div className="section-h"><h2>Calendar</h2><span className="sub">Next 7 days</span></div>
            <div className="two-col-2">
              <EarningsWeek />
              <EconomicCalendar />
            </div>
          </>
          )
        )}
        {tab === 'news' && (
          <>
            <NewsFeed />
            <AnalystActivity />
          </>
        )}
      </div>
      <SectorModal sector={sector} onClose={() => setSector(null)} />
    </>
  )
}
