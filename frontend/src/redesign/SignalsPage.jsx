/* Redesign Signals page. Rendered inside <AppShell> (which provides the .rd
 * wrapper, tape, header, and the ticker-modal context). Ported from
 * trades-v5.html: a context+tabs page-head, a Signals pane (BUY/SELL split +
 * open trades) and a Closed-trades pane (stats cards + historical table). */
import { useParams, useNavigate } from 'react-router-dom'
import './signals/signals.css'
import PageHead from './signals/PageHead'
import SignalsSplit from './signals/SignalsSplit'
import OpenTrades from './signals/OpenTrades'
import ClosedTrades from './signals/ClosedTrades'
import { tradeWindow } from './signals/util'
import { useReady, PageSkeleton } from './LoadGate'
import { useMe } from '../context/MeContext'
import { getSignalsToday, getOpenTrades, getStats, getTrades } from '../api'

// URL slug <-> internal tab key. The slug is the last path segment so the URL
// reflects the active sub-page (/signals/active-trades, /signals/closed-trades).
const TAB_BY_SLUG = { 'active-trades': 'today', 'closed-trades': 'closed' }
const SLUG_BY_TAB = { today: 'active-trades', closed: 'closed-trades' }

export default function SignalsPage() {
  const { tab: slug } = useParams()
  const navigate = useNavigate()
  const tab = TAB_BY_SLUG[slug] || 'today'
  const setTab = (t) => navigate('/signals/' + (SLUG_BY_TAB[t] || 'active-trades'))
  const me = useMe()
  const { start, end } = tradeWindow()
  // Both panes mount (display toggled), so all their queries fire on load. Gate
  // each pane so its sections appear together instead of popping in piecemeal.
  // Also wait for `me` so the free/pro gating is decided once — no free→pro flash.
  const todayDataReady = useReady(true, [
    [['signals-today', 'BUY', 'US'], () => getSignalsToday('BUY', 'US')],
    [['signals-today', 'SELL', 'US'], () => getSignalsToday('SELL', 'US')],
    [['open-trades', 'US'], () => getOpenTrades('US')],
  ])
  const todayReady = me.ready && todayDataReady
  const closedReady = useReady(true, [
    [['stats'], getStats],
    [['trades', start, end, 'US'], () => getTrades({ start, end, market: 'US' })],
  ])
  return (
    <>
      <PageHead tab={tab} setTab={setTab} />
      <div className="body">
        <div className={'tab-pane' + (tab === 'today' ? ' active' : '')}>
          {todayReady ? <><SignalsSplit /><OpenTrades /></> : <PageSkeleton />}
        </div>
        <div className={'tab-pane' + (tab === 'closed' ? ' active' : '')}>
          {closedReady ? <ClosedTrades /> : <PageSkeleton />}
        </div>
      </div>
    </>
  )
}
