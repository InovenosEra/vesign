/* Sector heatmap — cap/equal toggle, faded trendline behind each tile,
 * gainers/losers columns. Tile click → sector drill-down. Ported verbatim. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSectors } from '../../api'
import { pct } from '../fmt'

function SparkSVG({ values, gid }) {
  if (!Array.isArray(values) || values.length < 2) return null
  const n = values.length
  const min = Math.min(...values), max = Math.max(...values), span = (max - min) || 1
  const X = (i) => (i / (n - 1) * 100).toFixed(2)
  const Y = (v) => (45 + (1 - (v - min) / span) * 55).toFixed(2)   // lower 55%
  const pts = values.map((v, i) => `${X(i)},${Y(v)}`)
  const d = 'M ' + pts.join(' L ') + ' L 100,100 L 0,100 Z'
  return (
    <svg className="tile-spark" viewBox="0 0 100 100" preserveAspectRatio="none">
      <defs><linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor="#fff" stopOpacity="0.11" />
        <stop offset="100%" stopColor="#fff" stopOpacity="0" />
      </linearGradient></defs>
      <path d={d} fill={`url(#${gid})`} />
      <polyline points={pts.join(' ')} fill="none" stroke="rgba(255,255,255,0.22)" strokeWidth="1" vectorEffect="non-scaling-stroke" />
    </svg>
  )
}

function gclass(v) {
  const x = v == null ? 0 : v
  return x >= 1 ? 'g4' : x >= 0.5 ? 'g3' : x >= 0.25 ? 'g2' : x >= 0 ? 'g1' : x >= -0.25 ? 'r1' : x >= -0.5 ? 'r2' : 'r3'
}

function Col({ rows, kind, label }) {
  return (
    <div className={kind}>
      <div className="hd">{label}</div>
      {rows && rows.length
        ? rows.map((m, i) => <div className="mv" key={i}><span className="tk">{m.ticker}</span><span className="v">{pct(m.change_pct, { fd: 1 })}</span></div>)
        : <div className="mv none">—</div>}
    </div>
  )
}

export default function SectorHeatmap({ onOpenSector }) {
  const [mode, setMode] = useState('cap')
  const { data } = useQuery({ queryKey: ['market-sectors'], queryFn: getSectors, refetchInterval: 300_000 })
  const sectors = (data?.sectors || []).filter(s => s.sector !== 'ETF')
  const valOf = (s) => mode === 'cap' ? s.change_pct : s.change_pct_equal
  const sparkOf = (s) => mode === 'cap' ? s.sparkline : s.sparkline_equal
  const sorted = sectors.slice().sort((a, b) => (valOf(b) ?? 0) - (valOf(a) ?? 0))

  return (
    <div className="sector-block">
      <div className="section-h">
        <h2>Sectors</h2>
        <span className="sub">Today · {mode === 'cap' ? 'cap-weighted' : 'equal-weighted'} % change</span>
        <div className="wt-toggle">
          <button className={'wt-chip' + (mode === 'cap' ? ' active' : '')} onClick={() => setMode('cap')}>Cap-weighted</button>
          <button className={'wt-chip' + (mode === 'equal' ? ' active' : '')} onClick={() => setMode('equal')}>Equal-weight</button>
        </div>
      </div>
      <div className="heatmap">
        {sorted.map((s, i) => {
          const change = valOf(s)
          return (
            <div className={'tile ' + gclass(change)} key={s.sector} data-sector={s.sector}
              onClick={() => onOpenSector && onOpenSector(s.sector)}>
              <SparkSVG values={sparkOf(s)} gid={'spk' + i} />
              <div className="sec">{s.sector}</div>
              <div className="pct">{pct(change)}</div>
              <div className="movers2">
                <Col rows={s.gainers} kind="gainers" label="▲ Gainers" />
                <Col rows={s.losers} kind="losers" label="▼ Losers" />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
