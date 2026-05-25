/* Movers — 4-panel grid: Most Active / Top Gainers / Top Losers / Valuation
 * (Undervalued↔Overvalued toggle vs analyst target). Ported from market-v1.html. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getMovers, getValuation } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'

function MoverRow({ r, chVal }) {
  const open = useTickerModal()
  return (
    <div className="mover-row" data-ticker={r.ticker} data-company={r.company || ''} onClick={() => open(r.ticker, r.company)}>
      <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
      <div><div className="tk">{r.ticker}</div><div className="co">{r.company || ''}</div></div>
      <div className="px">{r.close == null ? '—' : num(r.close, { fd: 2 })}</div>
      <div className={'ch ' + dirClass(chVal)}>{pct(chVal)}</div>
    </div>
  )
}

function Panel({ title, pill, type }) {
  const { data } = useQuery({ queryKey: ['market-movers', type], queryFn: () => getMovers(type, 5), refetchInterval: 60_000 })
  const rows = data?.movers || []
  return (
    <div className="mover-panel">
      <div className="mover-head"><h3>{title}</h3><span className="pill">{pill}</span></div>
      <div className="mover-list">{rows.map((r, i) => <MoverRow key={i} r={r} chVal={r.change_pct} />)}</div>
    </div>
  )
}

function ValuationPanel() {
  const [dir, setDir] = useState('under')
  const { data } = useQuery({ queryKey: ['market-valuation'], queryFn: () => getValuation(5), refetchInterval: 300_000 })
  const rows = (dir === 'under' ? data?.undervalued : data?.overvalued) || []
  return (
    <div className="mover-panel">
      <div className="mover-head val-head">
        <button className={'vt-chip' + (dir === 'under' ? ' active' : '')} onClick={() => setDir('under')}>Undervalued</button>
        <button className={'vt-chip' + (dir === 'over' ? ' active' : '')} onClick={() => setDir('over')}>Overvalued</button>
      </div>
      <div className="mover-list">
        {rows.length
          ? rows.map((r, i) => <MoverRow key={i} r={r} chVal={r.upside} />)
          : <div style={{ padding: 20, textAlign: 'center', color: 'var(--ink-3)', fontSize: 12 }}>No data.</div>}
      </div>
    </div>
  )
}

export default function Movers() {
  return (
    <div className="movers-block">
      <div className="section-h"><h2>Movers</h2><span className="sub">Live · US market</span></div>
      <div className="movers-grid">
        <Panel title="Most Active" pill="●" type="active" />
        <Panel title="Top Gainers" pill="▲" type="gainers" />
        <Panel title="Top Losers" pill="▼" type="losers" />
        <ValuationPanel />
      </div>
    </div>
  )
}
