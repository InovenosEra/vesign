/* Allocation donut — Sector / Ticker / Watchlist toggle, inline SVG segments.
 * Ported from portfolio-v1.html's donut render (extended with real toggle state). */
import { useState } from 'react'
import { num, pct } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'
import { concentration } from './derive'

const DONUT_COLORS = ['#3b82f6', '#22d3ee', '#00d97e', '#a855f7', '#f59e0b', '#ec4899', '#60a5fa', '#6b7280']
const C = 2 * Math.PI * 38 // circumference, r=38

function group(rows, mode) {
  const keyOf = (r) => mode === 'sector' ? (r.sector || 'Other')
    : mode === 'industry' ? (r.industry || 'Other')
    : r.ticker
  const map = new Map()
  rows.filter(r => r.value).forEach(r => {
    const k = keyOf(r)
    map.set(k, (map.get(k) || 0) + r.value)
  })
  let alloc = [...map.entries()].map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value)
  if (alloc.length > 7) {
    const top = alloc.slice(0, 7)
    const otherVal = alloc.slice(7).reduce((s, a) => s + a.value, 0)
    top.push({ name: 'Other', value: otherVal })
    alloc = top
  }
  return alloc
}

export default function AllocationDonut({ rows, totalValue, totalYld }) {
  const [mode, setMode] = useState('sector')
  const { symbol, rate } = useCurrency()
  const alloc = totalValue > 0 ? group(rows, mode) : []
  const conc = concentration(rows, totalValue)

  let offset = 0
  const segs = alloc.map((a, i) => {
    const frac = a.value / totalValue
    const len = frac * C
    const color = DONUT_COLORS[i % DONUT_COLORS.length]
    const seg = {
      key: i, color,
      dasharray: `${len.toFixed(2)} ${(C - len).toFixed(2)}`,
      dashoffset: (-offset).toFixed(2),
      frac, name: a.name,
    }
    offset += len
    return seg
  })

  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Allocation</h3>
        <div className="chips">
          <span className={'chip' + (mode === 'sector' ? ' active' : '')} onClick={() => setMode('sector')}>Sector</span>
          <span className={'chip' + (mode === 'industry' ? ' active' : '')} onClick={() => setMode('industry')}>Industry</span>
          <span className={'chip' + (mode === 'ticker' ? ' active' : '')} onClick={() => setMode('ticker')}>Ticker</span>
        </div>
      </div>
      <div className="alloc-body">
        <div className="alloc-donut-wrap">
          <svg viewBox="0 0 100 100">
            <circle cx="50" cy="50" r="38" stroke="rgba(255,255,255,0.04)" strokeWidth="14" fill="none" />
            <g>
              {segs.map(s => (
                <circle key={s.key} cx="50" cy="50" r="38" stroke={s.color} strokeWidth="14" fill="none"
                  strokeDasharray={s.dasharray} strokeDashoffset={s.dashoffset} transform="rotate(-90 50 50)" />
              ))}
            </g>
          </svg>
          <div className="alloc-center">
            <div className="lbl">Current value</div>
            <div className="big"><span className="s">{symbol}</span><span>{num(totalValue * rate, { fd: 0 })}</span></div>
            <div className={'delta ' + (totalYld >= 0 ? 'up' : 'down')}>{pct(totalYld)} lifetime</div>
          </div>
        </div>
        <div className="alloc-legend">
          {segs.map(s => (
            <div className="alloc-row" key={s.key}>
              <span className="sw" style={{ background: s.color }}></span>
              <span className="nm">{s.name}</span>
              <span className="pct">{(s.frac * 100).toFixed(1)}%</span>
            </div>
          ))}
        </div>
      </div>
      <div className="conc">
        <div className="conc-grid">
          <div className="conc-cell"><div className="cv">{conc.topPct.toFixed(0)}%</div><div className="cl">Top holding</div></div>
          <div className="conc-cell"><div className="cv">{conc.top5Pct.toFixed(0)}%</div><div className="cl">Top 5</div></div>
          <div className="conc-cell"><div className="cv">{conc.positions}</div><div className="cl">Positions</div></div>
          <div className="conc-cell"><div className="cv">{conc.topSectorPct.toFixed(0)}%</div><div className="cl">Top sector</div></div>
        </div>
        <span className={'conc-label ' + conc.label.toLowerCase()}>{conc.label}</span>
      </div>
    </div>
  )
}
