/* Redesign Portfolio page. Rendered inside <AppShell> (which provides the .rd
 * wrapper, tape, header, and the ticker-modal context). Ported from
 * portfolio-v1.html — Holdings tab (KPIs, performance+allocation, watchlist
 * comparison, holdings table) + a Watchlists tab. */
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getPortfolioHoldings, getPortfolioComparison, getPortfolioPerformance } from '../api'
import { computeRows } from './portfolio/derive'
import NetWorthHero from './portfolio/NetWorthHero'
import PerformanceChart from './portfolio/PerformanceChart'
import AllocationDonut from './portfolio/AllocationDonut'
import VesignRead from './portfolio/VesignRead'
import CostValueBridge from './portfolio/CostValueBridge'
import HoldingsTable from './portfolio/HoldingsTable'
import WatchlistsTab from './portfolio/WatchlistsTab'
import { useReady, PageSkeleton } from './LoadGate'
import './portfolio/portfolio.css'

const PORTFOLIO_TABS = ['holdings', 'watchlists']   // URL slug == tab id

export default function PortfolioPage() {
  const { tab: slug } = useParams()
  const navigate = useNavigate()
  const tab = PORTFOLIO_TABS.includes(slug) ? slug : 'holdings'
  const setTab = (t) => { navigate('/portfolio/' + t); window.scrollTo({ top: 0, behavior: 'instant' }) }
  const { data: holdings } = useQuery({ queryKey: ['portfolio-holdings'], queryFn: () => getPortfolioHoldings('US'), refetchInterval: 3_000 })
  const { data: cmp } = useQuery({ queryKey: ['portfolio-comparison'], queryFn: () => getPortfolioComparison('US') })
  // Gate the holdings tab so KPIs + performance chart + allocation + comparison +
  // table all appear together (perf chart fetches its own series separately).
  const holdingsReady = useReady(tab === 'holdings', [
    [['portfolio-holdings'], () => getPortfolioHoldings('US')],
    [['portfolio-comparison'], () => getPortfolioComparison('US')],
    [['portfolio-performance'], () => getPortfolioPerformance('US')],
  ])

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
        <div className="ph-tabs">
          <a className={'ph-tab' + (tab === 'holdings' ? ' active' : '')} href="/portfolio/holdings"
            onClick={(e) => { e.preventDefault(); setTab('holdings') }}>
            Holdings <span className="count">{rows.length || '—'}</span>
          </a>
          <a className={'ph-tab' + (tab === 'watchlists' ? ' active' : '')} href="/portfolio/watchlists"
            onClick={(e) => { e.preventDefault(); setTab('watchlists') }}>
            Watchlists <span className="count">{wlCount || '—'}</span>
          </a>
          <span className="day ph-inline-day">{day}</span>
        </div>
      </div>

      <div className="body">
        {tab === 'holdings' && (
          <div id="holdings" className="tab-pane active">
            {!holdingsReady ? (
              <PageSkeleton />
            ) : !rows.length ? (
              <div className="section-h"><span className="sub">No holdings yet.</span></div>
            ) : (
              <>
                <NetWorthHero totals={totals} best={best} worst={worst} largest={largest} vsVesign={vsVesign} watchlistCount={wlCount} />
                <div className="perf-grid">
                  <PerformanceChart />
                  <AllocationDonut rows={rows} totalValue={totals.totalValue} totalYld={totals.totalYld} />
                </div>
                <VesignRead rows={rows} />
                <CostValueBridge rows={rows} totals={totals} />
                <HoldingsTable rows={rows} subhead={`${rows.length} positions`} />
              </>
            )}
          </div>
        )}

        {tab === 'watchlists' && <WatchlistsTab holdingsCount={rows.length} />}
      </div>
    </>
  )
}
