/* Redesign Signals page. Rendered inside <AppShell> (which provides the .rd
 * wrapper, tape, header, and the ticker-modal context). Ported from
 * trades-v5.html: a context+tabs page-head, a Signals pane (BUY/SELL split +
 * open trades) and a Closed-trades pane (stats cards + historical table). */
import { useState } from 'react'
import './signals/signals.css'
import PageHead from './signals/PageHead'
import SignalsSplit from './signals/SignalsSplit'
import OpenTrades from './signals/OpenTrades'
import ClosedTrades from './signals/ClosedTrades'

export default function SignalsPage() {
  const [tab, setTab] = useState('today')
  return (
    <>
      <PageHead tab={tab} setTab={setTab} />
      <div className="body">
        <div className={'tab-pane' + (tab === 'today' ? ' active' : '')}>
          <SignalsSplit />
          <OpenTrades />
        </div>
        <div className={'tab-pane' + (tab === 'closed' ? ' active' : '')}>
          <ClosedTrades />
        </div>
      </div>
    </>
  )
}
