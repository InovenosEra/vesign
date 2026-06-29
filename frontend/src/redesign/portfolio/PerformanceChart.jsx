/* Performance chart — your portfolio vs the Vesign strategy vs SPY, inline SVG.
 * Range chips (1M…All) refetch the series at that horizon; a %/$ toggle switches
 * between normalized return % (all three series) and portfolio market value $
 * (single line, currency-formatted). Legend items toggle individual series.
 * Hover crosshair snaps to the nearest weekly point and tooltips the visible series. */
import { useState, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getPortfolioPerformance } from '../../api'
import { useCurrency } from '../../context/CurrencyContext'

const W = 800, H = 300
const RANGES = [['1M', 1], ['3M', 3], ['6M', 6], ['1Y', 12], ['2Y', 24], ['All', 60]]
const SERIES = [
  { key: 'portfolio', color: '#60a5fa', label: 'Holdings' },
  { key: 'vesign', color: '#00d97e', label: 'Vesign' },
  { key: 'spy', color: '#f59e0b', label: 'S&P 500' },
]
const VALUE_SERIES = { key: 'value', color: '#60a5fa', label: 'Holdings value' }

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`)
const fmtDate = (w) => new Date(w).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })

export default function PerformanceChart() {
  const [months, setMonths] = useState(12)
  const [mode, setMode] = useState('pct')          // 'pct' | 'value'
  const [hidden, setHidden] = useState(() => new Set())
  const { symbol, rate } = useCurrency()
  const { data: pts } = useQuery({
    queryKey: ['portfolio-performance', months],
    queryFn: () => getPortfolioPerformance('US', months),
  })
  const svgRef = useRef(null)
  const bodyRef = useRef(null)
  const [hover, setHover] = useState(null)

  const data = Array.isArray(pts) ? pts : []
  const N = data.length
  const isValue = mode === 'value'

  // Which series get drawn. In $ mode it's the single portfolio-value line.
  const seriesDefs = isValue ? [VALUE_SERIES] : SERIES.filter(s => !hidden.has(s.key))

  const fmtAxis = (v) => {
    if (!isValue) return `${v >= 0 ? '+' : ''}${v.toFixed(0)}%`
    const vv = v * rate, a = Math.abs(vv)
    return symbol + (a >= 1000 ? (vv / 1000).toFixed(a >= 10000 ? 0 : 1) + 'k' : vv.toFixed(0))
  }

  let lines = []
  let yAxis = ['—', '—', '—', '—', '—', '—']
  let xAxis = []
  let minV = 0, span = 1

  if (N >= 2) {
    const allVals = seriesDefs.flatMap(s => data.map(p => p[s.key])).filter(v => v != null)
    let maxV
    if (isValue) {
      maxV = Math.max(...allVals) * 1.05
      minV = Math.min(...allVals) * 0.96
    } else {
      maxV = Math.ceil((Math.max(...allVals, 0) * 1.05) / 10) * 10
      minV = Math.floor(Math.min(0, ...allVals) / 10) * 10
    }
    span = (maxV - minV) || 1
    const xFor = (i) => (i / (N - 1)) * W
    const yFor = (v) => H - ((v - minV) / span) * H

    lines = seriesDefs.map(s => {
      const toks = data.map((p, i) => p[s.key] == null ? null : `${xFor(i).toFixed(1)},${yFor(p[s.key]).toFixed(1)}`).filter(Boolean)
      let d = ''
      if (toks.length) {
        const firstX = toks[0].split(',')[0]
        const lastX = toks[toks.length - 1].split(',')[0]
        d = `M ${toks.join(' L ')} L ${lastX},${H} L ${firstX},${H} Z`
      }
      const last = toks.length ? toks[toks.length - 1].split(',') : null
      return { ...s, points: toks.join(' '), area: d, dot: last ? { cx: last[0], cy: last[1] } : null }
    })

    yAxis = [0, 1, 2, 3, 4, 5].map(i => fmtAxis(maxV - (i / 5) * span))
    xAxis = [0, 1, 2, 3, 4, 5, 6].map(i => {
      const dt = new Date(data[Math.round(i / 6 * (N - 1))].week)
      return dt.toLocaleDateString(undefined, { month: 'short' })
    })
  }

  const xForOuter = (i) => (i / (N - 1)) * W
  const yForOuter = (v) => H - ((v - minV) / span) * H

  function onMove(e) {
    if (N < 2 || !svgRef.current || !bodyRef.current) return
    const r = svgRef.current.getBoundingClientRect()
    const br = bodyRef.current.getBoundingClientRect()
    const frac = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width))
    const idx = Math.round(frac * (N - 1))
    const vbx = xForOuter(idx)
    const px = (r.left - br.left) + (vbx / W) * r.width
    setHover({ idx, vbx, px, frac })
  }
  const onLeave = () => setHover(null)

  const hp = hover ? data[hover.idx] : null
  const tipTransform = hover
    ? (hover.frac < 0.15 ? 'translateX(0)' : hover.frac > 0.85 ? 'translateX(-100%)' : 'translateX(-50%)')
    : ''

  const toggleSeries = (key) => setHidden(prev => {
    const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n
  })

  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Performance</h3>
        <div className="legend">
          {(isValue ? [VALUE_SERIES] : SERIES).map(s => (
            <span key={s.key}
              className={'lg-item' + (isValue ? '' : ' clickable') + (!isValue && hidden.has(s.key) ? ' off' : '')}
              onClick={isValue ? undefined : () => toggleSeries(s.key)}>
              <span className="sw" style={{ background: s.color }}></span> {s.label}
            </span>
          ))}
        </div>
        <div className="chips">
          <span className={'chip' + (mode === 'pct' ? ' active' : '')} onClick={() => setMode('pct')}>%</span>
          <span className={'chip' + (mode === 'value' ? ' active' : '')} onClick={() => setMode('value')}>$</span>
          <span className="chip-sep" />
          {RANGES.map(([label, m]) => (
            <span key={label} className={'chip' + (months === m ? ' active' : '')} onClick={() => setMonths(m)}>{label}</span>
          ))}
        </div>
      </div>
      <div className="chart-body" ref={bodyRef}>
        <div className="y-axis">{yAxis.map((v, i) => <span key={i}>{v}</span>)}</div>
        <svg ref={svgRef} viewBox="0 0 800 300" preserveAspectRatio="none" onMouseMove={onMove} onMouseLeave={onLeave}>
          {[50, 100, 150, 200, 250].map(y => (
            <line key={y} x1="0" x2="800" y1={y} y2={y} stroke="rgba(255,255,255,0.04)" />
          ))}
          {lines.map(s => (
            <g key={s.key}>
              <path d={s.area} fill={s.color} fillOpacity="0.10" />
              <polyline fill="none" stroke={s.color} strokeWidth={s.key === 'portfolio' || s.key === 'value' ? 2 : 1.8}
                strokeLinejoin="round" points={s.points} />
            </g>
          ))}
          {hover && (
            <g pointerEvents="none">
              <line x1={hover.vbx} x2={hover.vbx} y1="0" y2={H} stroke="rgba(255,255,255,0.22)" strokeWidth="1"
                strokeDasharray="3 3" vectorEffect="non-scaling-stroke" />
              {hp && seriesDefs.map(s => hp[s.key] != null && (
                <circle key={s.key} cx={hover.vbx} cy={yForOuter(hp[s.key])} r="4" fill={s.color} stroke="#0b0e11" strokeWidth="1.5" />
              ))}
            </g>
          )}
          {lines.map(s => s.dot && <circle key={s.key} cx={s.dot.cx} cy={s.dot.cy} r="3.5" fill={s.color} />)}
        </svg>
        {hp && (
          <div className="perf-tip" style={{ left: hover.px, transform: tipTransform }}>
            <div className="perf-tip-date">{fmtDate(hp.week)}</div>
            {seriesDefs.map(s => (
              <div className="perf-tip-row" key={s.key}>
                <span className="sw" style={{ background: s.color }}></span>
                <span className="perf-tip-lbl">{s.label}</span>
                <b>{isValue ? (hp.value == null ? '—' : symbol + (hp.value * rate).toLocaleString(undefined, { maximumFractionDigits: 0 })) : fmtPct(hp[s.key])}</b>
              </div>
            ))}
          </div>
        )}
      </div>
      <div className="x-axis">{xAxis.map((m, i) => <span key={i}>{m}</span>)}</div>
    </div>
  )
}
