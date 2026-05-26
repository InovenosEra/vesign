/* Redesign Market page. Renders inside <AppShell> (.rd wrapper, tape, header,
 * ticker-modal context). Tabbed: Overview / Market Trend / News / Calendar. */
import { useState } from 'react'
import PageHead from './market/PageHead'
import Indices from './market/Indices'
import Commodities from './market/Commodities'
import Currencies from './market/Currencies'
import Breadth from './market/Breadth'
import Movers from './market/Movers'
import SectorHeatmap from './market/SectorHeatmap'
import SectorModal from './market/SectorModal'
import NewsFeed from './market/NewsFeed'
import AnalystChanges from './market/AnalystChanges'
import Upgrades from './market/Upgrades'
import EarningsWeek from './market/EarningsWeek'
import EconomicCalendar from './market/EconomicCalendar'

export default function MarketPage() {
  const [tab, setTab] = useState('overview')
  const [sector, setSector] = useState(null)
  return (
    <>
      <PageHead tab={tab} setTab={setTab} />
      <div className="body">
        {tab === 'overview' && (
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
        )}
        {tab === 'news' && (
          <>
            <NewsFeed />
            <div className="two-col-2 analyst-cols">
              <AnalystChanges />
              <Upgrades />
            </div>
          </>
        )}
      </div>
      <SectorModal sector={sector} onClose={() => setSector(null)} />
    </>
  )
}
