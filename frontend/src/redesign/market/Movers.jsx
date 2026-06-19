/* Movers — 4-panel grid: Most Active / Top Gainers / Top Losers / Valuation
 * (Undervalued↔Overvalued vs analyst target). Prices AND sort order both come
 * from the server (live snapshot, ~3s), so the displayed % always matches the
 * ranking — no client-side live overlay that would desync the two. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getMovers, getValuation, getHighsLows } from '../../api'
import { num, pct, dirClass, overlayLive, overlayUpside, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'

function MoverRow({ r, kind }) {
  const open = useTickerModal()
  // No live overlay: r.close / r.change_pct (or r.upside) are already live and
  // consistent with the server's sort order. overlayLive with no live price
  // simply returns those server values.
  const o = kind === 'upside'
    ? overlayUpside(r.close, r.upside)
    : overlayLive(r.close, r.change_pct)
  const chVal = kind === 'upside' ? o.upside : o.change
  return (
    <div className="mover-row" data-ticker={r.ticker} data-company={r.company || ''} onClick={() => open(r.ticker, r.company)}>
      <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
      <div className="mname"><div className="tk">{r.ticker}</div><div className="co" title={r.company || ''}>{r.company || ''}</div></div>
      <div className="px">{o.price == null ? '—' : num(o.price, { fd: 2 })}</div>
      <div className={'ch ' + dirClass(chVal)}>{pct(chVal)}</div>
    </div>
  )
}

const PAGE = 10
const FETCH = 30  // depth pulled so the footer can page through up to 3 × 10

// Renders 10 rows at a time with a Prev / range / Next 10 footer. Page state is
// clamped on every render so a live refetch that shrinks the list can't strand
// the user on an empty page.
function PagedList({ rows, kind }) {
  const [page, setPage] = useState(0)
  const pages = Math.max(1, Math.ceil(rows.length / PAGE))
  const p = Math.min(page, pages - 1)
  const slice = rows.slice(p * PAGE, p * PAGE + PAGE)
  return (
    <>
      <div className="mover-list">
        {slice.length
          ? slice.map((r) => <MoverRow key={r.ticker} r={r} kind={kind} />)
          : <div style={{ padding: 20, textAlign: 'center', color: 'var(--ink-3)', fontSize: 12 }}>No data.</div>}
      </div>
      {rows.length > PAGE && (
        <div className="mover-foot">
          <button className="pg-btn" disabled={p === 0} onClick={() => setPage(p - 1)}>← Prev</button>
          <button className="pg-btn" disabled={p >= pages - 1} onClick={() => setPage(p + 1)}>Next →</button>
        </div>
      )}
    </>
  )
}

function Panel({ title, pill, type }) {
  const { data } = useQuery({ queryKey: ['market-movers', type, FETCH], queryFn: () => getMovers(type, FETCH), refetchInterval: 3_000 })
  const rows = data?.movers || []
  return (
    <div className="mover-panel">
      <div className="mover-head"><h3>{title}</h3><span className="pill">{pill}</span></div>
      <PagedList rows={rows} kind="change" />
    </div>
  )
}

function HighLowPanel() {
  const [hl, setHl] = useState('high')
  const { data } = useQuery({ queryKey: ['market-highs-lows', hl, FETCH], queryFn: () => getHighsLows(hl, FETCH), refetchInterval: 3_000 })
  const rows = data?.movers || []
  return (
    <div className="mover-panel">
      <div className="mover-head val-head">
        <button className={'vt-chip' + (hl === 'high' ? ' active' : '')} onClick={() => setHl('high')}>52W High</button>
        <button className={'vt-chip' + (hl === 'low' ? ' active' : '')} onClick={() => setHl('low')}>52W Low</button>
      </div>
      {/* key resets paging back to page 1 when the High/Low toggle flips */}
      <PagedList key={hl} rows={rows} kind="change" />
    </div>
  )
}

function ValuationPanel() {
  const [dir, setDir] = useState('under')
  const { data } = useQuery({ queryKey: ['market-valuation', FETCH], queryFn: () => getValuation(FETCH), refetchInterval: 300_000 })
  const rows = (dir === 'under' ? data?.undervalued : data?.overvalued) || []
  return (
    <div className="mover-panel">
      <div className="mover-head val-head">
        <button className={'vt-chip' + (dir === 'under' ? ' active' : '')} onClick={() => setDir('under')}>Undervalued</button>
        <button className={'vt-chip' + (dir === 'over' ? ' active' : '')} onClick={() => setDir('over')}>Overvalued</button>
      </div>
      {/* key resets paging back to page 1 when the Under/Over toggle flips */}
      <PagedList key={dir} rows={rows} kind="upside" />
    </div>
  )
}

export default function Movers() {
  return (
    <div className="movers-block">
      <div className="section-h"><h2>Trending Stocks</h2><span className="sub">Live · US market</span></div>
      <div className="movers-grid">
        <Panel title="Most Active" pill="●" type="active" />
        <Panel title="Top Gainers" pill="▲" type="gainers" />
        <Panel title="Top Losers" pill="▼" type="losers" />
        <HighLowPanel />
        <ValuationPanel />
      </div>
    </div>
  )
}
