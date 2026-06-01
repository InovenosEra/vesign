/* Redesign Research page — ported from research-v1.html.
 * Rendered inside <AppShell> (which provides the .rd wrapper, tape, top nav,
 * and the ticker-modal context). Two tabs:
 *   • Screener — filter rail + ranked results table (rows open the SignalModal)
 *   • Deep dive — per-ticker in-page detail (hero, chart, fundamentals, analyst,
 *     ML, signal history, news). Tab state lives here; the selected deep-dive
 *     ticker lives in DeepDive itself. */
import { useState } from 'react'
import ResearchHead from './research/ResearchHead'
import Screener from './research/Screener'
import DeepDive from './research/DeepDive'
import { useReady, PageSkeleton } from './LoadGate'
import { getSignalsToday } from '../api'
import './research/research.css'

export default function ResearchPage() {
  const [tab, setTab] = useState('screener')   // 'screener' | 'deep-dive'
  const [count, setCount] = useState(null)
  // Gate the screener so its filter rail + ranked table appear together.
  const screenerReady = useReady(true, [
    [['signals-today', 'US'], () => getSignalsToday(null, 'US')],
  ])
  // Lets a screener row "drill into" the deep dive instead of opening the modal,
  // mirroring the mockup's per-ticker detail intent. Optional — rows still open
  // the shared SignalModal (matching the mockup's body click handler), but a
  // deep-dive jump is wired for the search/recent pills.
  const [deepTicker, setDeepTicker] = useState('NVDA')

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
