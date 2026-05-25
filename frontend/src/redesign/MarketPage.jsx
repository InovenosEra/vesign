/* Redesign Market page. Rendered inside <AppShell> (which provides the .rd
 * wrapper, tape, header, and the ticker-modal context). Composes the sections
 * in the mockup's order. */
import PageHead from './market/PageHead'
import Indices from './market/Indices'
import CrossMarket from './market/CrossMarket'
import Movers from './market/Movers'
import SectorHeatmap from './market/SectorHeatmap'
import TopNews from './market/TopNews'
import AnalystChanges from './market/AnalystChanges'
import EarningsWeek from './market/EarningsWeek'
import EconomicCalendar from './market/EconomicCalendar'

export default function MarketPage() {
  return (
    <>
      <PageHead />
      <div className="body">
        <Indices />
        <CrossMarket />
        <Movers />
        <SectorHeatmap />
        <div className="two-col-2" style={{ marginBottom: 28 }}>
          <TopNews />
          <AnalystChanges />
        </div>
        <div className="two-col-2">
          <EarningsWeek />
          <EconomicCalendar />
        </div>
      </div>
    </>
  )
}
