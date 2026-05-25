/* Market Trend tab — macro view: breadth/VIX stat row, index trend charts
 * (SPY/QQQ/DIA/IWM via getPriceHistory with a range selector), sector rotation.
 * Net-new (no mockup); styled to the redesign tokens. Breadth/VIX show today's
 * snapshot — trend-over-time lines await a history endpoint. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getBreadth, getIndices, getSectors, getPriceHistory } from '../../api'
import { num, pct, dirClass, spark } from '../fmt'

const RANGES = [['1M', 1], ['3M', 3], ['6M', 6], ['1Y', 12]]
const INDEXES = [['SPY', 'S&P 500'], ['QQQ', 'Nasdaq 100'], ['DIA', 'Dow Jones'], ['IWM', 'Russell 2000']]

function rangeBounds(months) {
  const end = new Date()
  const start = new Date()
  start.setMonth(start.getMonth() - months)
  return { start: start.toISOString().slice(0, 10), end: end.toISOString().slice(0, 10) }
}

function StatRow() {
  const { data: breadth } = useQuery({ queryKey: ['market-breadth'], queryFn: getBreadth, refetchInterval: 60_000 })
  const { data: idx } = useQuery({ queryKey: ['market-indices'], queryFn: getIndices, refetchInterval: 60_000 })
  const by = Object.fromEntries((idx?.indices || []).map(r => [r.ticker, r]))
  const spy = by.SPY, vix = by.VIX
  return (
    <div className="mt-stats">
      <div className="mt-stat"><div className="k">S&amp;P 500</div><div className={'v ' + (spy ? dirClass(spy.change_pct) : '')}>{spy ? pct(spy.change_pct) : '—'}</div></div>
      <div className="mt-stat"><div className="k">Breadth (A / D)</div><div className="v">{breadth ? `${breadth.advancers} / ${breadth.decliners}` : '—'}</div></div>
      <div className="mt-stat"><div className="k">52w highs</div><div className="v up">{breadth?.week52_highs ?? '—'}</div></div>
      <div className="mt-stat"><div className="k">52w lows</div><div className="v down">{breadth?.week52_lows ?? '—'}</div></div>
      <div className="mt-stat"><div className="k">VIX</div><div className={'v ' + (vix ? dirClass(vix.change_pct) : '')}>{vix ? (vix.close == null ? '—' : num(vix.close, { fd: 2 })) : '—'}</div></div>
    </div>
  )
}

function IndexCard({ ticker, name, range, bounds }) {
  const { data } = useQuery({ queryKey: ['idx-hist', ticker, range], queryFn: () => getPriceHistory(ticker, bounds), staleTime: 300_000 })
  const rows = Array.isArray(data) ? data : []
  const closes = rows.map(r => r.close).filter(v => v != null)
  const first = closes[0], last = closes[closes.length - 1]
  const chg = (first && last) ? (last - first) / first * 100 : null
  const color = chg == null ? '#6b7280' : chg >= 0 ? '#00d97e' : '#ff4d5c'
  const W = 400, H = 110
  const d = spark(closes, { width: W, height: H })
  const gid = 'mt_' + ticker
  return (
    <div className="mt-card">
      <div className="mt-card-head">
        <span className="name">{name}</span>
        <span><span className="px">{last == null ? '—' : num(last, { fd: 2 })}</span> <span className={'chg ' + dirClass(chg)}>{pct(chg)}</span></span>
      </div>
      <div className="mt-chart">
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
          <defs><linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.25" /><stop offset="100%" stopColor={color} stopOpacity="0" />
          </linearGradient></defs>
          <path d={d ? d + ` L${W},${H} L0,${H} Z` : ''} fill={`url(#${gid})`} />
          <path d={d} fill="none" stroke={color} strokeWidth="1.8" />
        </svg>
      </div>
    </div>
  )
}

function SectorRotation() {
  const { data } = useQuery({ queryKey: ['market-sectors'], queryFn: getSectors, refetchInterval: 300_000 })
  const sectors = (data?.sectors || []).filter(s => s.sector !== 'ETF')
  const sorted = sectors.slice().sort((a, b) => (b.change_pct ?? 0) - (a.change_pct ?? 0))
  const leaders = sorted.slice(0, 3)
  const laggards = sorted.slice(-3).reverse()
  const Row = (s) => (
    <div key={s.sector} className="mt-card-head" style={{ padding: '7px 0', borderBottom: '1px solid var(--line)' }}>
      <span style={{ fontSize: 12.5, color: 'var(--ink-2)' }}>{s.sector}</span>
      <span className={'chg ' + dirClass(s.change_pct)}>{pct(s.change_pct)}</span>
    </div>
  )
  return (
    <>
      <div className="section-h"><h2>Sector rotation</h2><span className="sub">Today · cap-weighted</span></div>
      <div className="mt-grid">
        <div className="mt-card"><div className="name" style={{ marginBottom: 8 }}>▲ Leaders</div>{leaders.map(Row)}</div>
        <div className="mt-card"><div className="name" style={{ marginBottom: 8 }}>▼ Laggards</div>{laggards.map(Row)}</div>
      </div>
    </>
  )
}

export default function MarketTrend() {
  const [range, setRange] = useState('6M')
  const months = RANGES.find(([l]) => l === range)?.[1] || 6
  const bounds = rangeBounds(months)
  return (
    <>
      <StatRow />
      <div className="mt-toolbar">
        <div className="section-h" style={{ margin: 0 }}><h2>Index trend</h2></div>
        <div className="mt-ranges">
          {RANGES.map(([l]) => (
            <button key={l} className={'mt-range' + (l === range ? ' active' : '')} onClick={() => setRange(l)}>{l}</button>
          ))}
        </div>
      </div>
      <div className="mt-grid">
        {INDEXES.map(([t, n]) => <IndexCard key={t} ticker={t} name={n} range={range} bounds={bounds} />)}
      </div>
      <SectorRotation />
    </>
  )
}
