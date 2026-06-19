/* Redesign Research page — ported from research-v1.html.
 * Rendered inside <AppShell> (which provides the .rd wrapper, tape, top nav,
 * and the ticker-modal context). Two tabs:
 *   • Screener — filter rail + ranked results table (rows open the SignalModal)
 *   • Deep dive — per-ticker in-page detail (hero, chart, fundamentals, analyst,
 *     ML, signal history, news). Tab state lives here; the selected deep-dive
 *     ticker lives in DeepDive itself. */
import { useState } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import ResearchHead from './research/ResearchHead'
import Screener from './research/Screener'
import DeepDive from './research/DeepDive'
import { useReady, PageSkeleton } from './LoadGate'
import { getSignalsToday } from '../api'
import './research/research.css'

export default function ResearchPage() {
  // Sub-page is in the URL: /research = screener, /research/AAPL = deep dive.
  // ?ticker= is kept as a fallback for any older deep-links.
  const { ticker: pathTicker } = useParams()
  const [params] = useSearchParams()
  const navigate = useNavigate()
  const urlTicker = pathTicker || params.get('ticker')
  const tab = urlTicker ? 'deep-dive' : 'screener'
  const deepTicker = (urlTicker || 'NVDA').toUpperCase()
  const setTab = (t) => navigate(t === 'deep-dive' ? '/research/' + deepTicker : '/research')
  const setDeepTicker = (v) => navigate('/research/' + String(v).toUpperCase())
  const [count, setCount] = useState(null)
  // Gate the screener so its filter rail + ranked table appear together.
  const screenerReady = useReady(true, [
    [['signals-today', 'US'], () => getSignalsToday(null, 'US')],
  ])

  return (
    <>
      <ResearchHead tab={tab} setTab={setTab} count={count} />
      {/* Both panes stay mounted (display toggled via .active) so deep-dive
          state + chart survive a tab switch — matching the mockup's tab-pane. */}
      <div className={'tab-pane' + (tab === 'screener' ? ' active' : '')}>
        {screenerReady ? <Screener onCount={setCount} /> : <PageSkeleton />}
      </div>
      <div className={'tab-pane' + (tab === 'deep-dive' ? ' active' : '')}>
        <DeepDive ticker={deepTicker} setTicker={setDeepTicker} />
      </div>
    </>
  )
}
