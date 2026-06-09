/* Performance line chart — Vesign (green) vs your portfolio (blue), inline SVG.
 * Ported from portfolio-v1.html's PERFORMANCE CHART block, plus a hover
 * crosshair: moving over the chart snaps to the nearest weekly point and shows
 * both series' values in a tooltip. */
import { useState, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getPortfolioPerformance } from '../../api'

const W = 800, H = 300

function buildSeries(pts) {
  const ves = pts.map(p => p.vesign)
  const port = pts.map(p => p.portfolio)
  const all = ves.concat(port).filter(v => v != null)
  const maxV = Math.ceil((Math.max(...all) * 1.05) / 10) * 10
  const minV = Math.floor(Math.min(0, ...all) / 10) * 10
  const span = (maxV - minV) || 1
  const xFor = (i) => (i / (pts.length - 1)) * W
  const yFor = (v) => H - ((v - minV) / span) * H
  const ptsStr = (arr) =>
    arr.map((v, i) => (v == null ? null : `${xFor(i).toFixed(1)},${yFor(v).toFixed(1)}`))
       .filter(Boolean).join(' ')
  return { maxV, minV, span, ptsStr }
}

function paint(series, ptsStr) {
  const pstr = ptsStr(series)
  const arr = pstr.split(' ').filter(Boolean)
  if (!arr.length) return { points: '', d: '', dot: null }
  const firstX = arr[0].split(',')[0]
  const lastTok = arr[arr.length - 1]
  const lastX = lastTok.split(',')[0]
  const d = `M ${arr.join(' L ')} L ${lastX},${H} L ${firstX},${H} Z`
  const [dx, dy] = lastTok.split(',')
  return { points: pstr, d, dot: { cx: dx, cy: dy } }
}

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`)
const fmtDate = (w) =>
  new Date(w).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })

export default function PerformanceChart() {
  const { data: pts } = useQuery({ queryKey: ['portfolio-performance'], queryFn: () => getPortfolioPerformance('US') })
  const svgRef = useRef(null)
  const bodyRef = useRef(null)
  const [hover, setHover] = useState(null) // { idx, vbx, px, frac }

  const data = Array.isArray(pts) ? pts : []
  const N = data.length

  let ves = { points: '', d: '', dot: null }
  let port = { points: '', d: '', dot: null }
  let yAxis = ['—', '—', '—', '—', '—', '—']
  let xAxis = []
  let minV = 0, span = 1

  if (N >= 2) {
    const s = buildSeries(data)
    minV = s.minV; span = s.span
    ves = paint(data.map(p => p.vesign), s.ptsStr)
    port = paint(data.map(p => p.portfolio), s.ptsStr)
    yAxis = [0, 1, 2, 3, 4, 5].map(i => {
      const v = s.maxV - (i / 5) * span
      return `${v >= 0 ? '+' : ''}${v.toFixed(0)}%`
    })
    xAxis = [0, 1, 2, 3, 4, 5, 6].map(i => {
      const dt = new Date(data[Math.round(i / 6 * (N - 1))].week)
      return dt.toLocaleDateString(undefined, { month: 'short' })
    })
  }

  const xFor = (i) => (i / (N - 1)) * W
  const yFor = (v) => H - ((v - minV) / span) * H

  function onMove(e) {
    if (N < 2 || !svgRef.current || !bodyRef.current) return
    const r = svgRef.current.getBoundingClientRect()
    const br = bodyRef.current.getBoundingClientRect()
    const frac = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width))
    const idx = Math.round(frac * (N - 1))
    const vbx = xFor(idx)
    const px = (r.left - br.left) + (vbx / W) * r.width
    setHover({ idx, vbx, px, frac })
  }
  const onLeave = () => setHover(null)

  const hp = hover ? data[hover.idx] : null
  // Keep the tooltip inside the panel: left-anchor near the start, right-anchor
  // near the end, centred in the middle.
  const tipTransform = hover
    ? (hover.frac < 0.15 ? 'translateX(0)'
      : hover.frac > 0.85 ? 'translateX(-100%)'
      : 'translateX(-50%)')
    : ''

  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Performance</h3>
        <div className="legend">
          <span className="lg-item"><span className="sw" style={{ background: '#00d97e' }}></span> Vesign</span>
          <span className="lg-item"><span className="sw" style={{ background: '#60a5fa' }}></span> Your portfolio</span>
        </div>
        <div className="chips">
          <span className="chip active">1Y</span>
        </div>
      </div>
      <div className="chart-body" ref={bodyRef}>
        <div className="y-axis">
          {yAxis.map((v, i) => <span key={i}>{v}</span>)}
        </div>
        <svg ref={svgRef} viewBox="0 0 800 300" preserveAspectRatio="none"
          onMouseMove={onMove} onMouseLeave={onLeave}>
          <defs>
            <linearGradient id="vsg" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#00d97e" stopOpacity="0.22" />
              <stop offset="100%" stopColor="#00d97e" stopOpacity="0" />
            </linearGradient>
            <linearGradient id="psg" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#60a5fa" stopOpacity="0.18" />
              <stop offset="100%" stopColor="#60a5fa" stopOpacity="0" />
            </linearGradient>
            <filter id="endglow"><feGaussianBlur stdDeviation="4" /></filter>
          </defs>
          <line x1="0" x2="800" y1="50" y2="50" stroke="rgba(255,255,255,0.04)" />
          <line x1="0" x2="800" y1="100" y2="100" stroke="rgba(255,255,255,0.04)" />
          <line x1="0" x2="800" y1="150" y2="150" stroke="rgba(255,255,255,0.04)" />
          <line x1="0" x2="800" y1="200" y2="200" stroke="rgba(255,255,255,0.04)" />
          <line x1="0" x2="800" y1="250" y2="250" stroke="rgba(255,255,255,0.04)" />
          {/* Your portfolio (blue) */}
          <path d={port.d} fill="url(#psg)" />
          <polyline fill="none" stroke="#60a5fa" strokeWidth="1.8" strokeLinejoin="round" points={port.points} />
          {/* Vesign (green) */}
          <path d={ves.d} fill="url(#vsg)" />
          <polyline fill="none" stroke="#00d97e" strokeWidth="2" strokeLinejoin="round" points={ves.points} />
          {/* hover crosshair + snapped dots */}
          {hover && (
            <g pointerEvents="none">
              <line x1={hover.vbx} x2={hover.vbx} y1="0" y2={H}
                stroke="rgba(255,255,255,0.22)" strokeWidth="1"
                strokeDasharray="3 3" vectorEffect="non-scaling-stroke" />
              {hp && hp.portfolio != null && (
                <circle cx={hover.vbx} cy={yFor(hp.portfolio)} r="4"
                  fill="#60a5fa" stroke="#0b0e11" strokeWidth="1.5" />
              )}
              {hp && hp.vesign != null && (
                <circle cx={hover.vbx} cy={yFor(hp.vesign)} r="4"
                  fill="#00d97e" stroke="#0b0e11" strokeWidth="1.5" />
              )}
            </g>
          )}
          {/* end markers */}
          {ves.dot && <circle cx={ves.dot.cx} cy={ves.dot.cy} r="3.5" fill="#00d97e" />}
          {port.dot && <circle cx={port.dot.cx} cy={port.dot.cy} r="3" fill="#60a5fa" />}
        </svg>
        {hp && (
          <div className="perf-tip" style={{ left: hover.px, transform: tipTransform }}>
            <div className="perf-tip-date">{fmtDate(hp.week)}</div>
            <div className="perf-tip-row">
              <span className="sw" style={{ background: '#00d97e' }}></span>
              <span className="perf-tip-lbl">Vesign</span>
              <b>{fmtPct(hp.vesign)}</b>
            </div>
            <div className="perf-tip-row">
              <span className="sw" style={{ background: '#60a5fa' }}></span>
              <span className="perf-tip-lbl">Your portfolio</span>
              <b>{fmtPct(hp.portfolio)}</b>
            </div>
          </div>
        )}
      </div>
      <div className="x-axis">
        {xAxis.map((m, i) => <span key={i}>{m}</span>)}
      </div>
    </div>
  )
}
