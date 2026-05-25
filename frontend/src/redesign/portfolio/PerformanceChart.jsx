/* Performance line chart — Vesign (green) vs your portfolio (blue), inline SVG.
 * Ported verbatim from portfolio-v1.html's PERFORMANCE CHART block. */
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

export default function PerformanceChart() {
  const { data: pts } = useQuery({ queryKey: ['portfolio-performance'], queryFn: () => getPortfolioPerformance('US') })

  let ves = { points: '', d: '', dot: null }
  let port = { points: '', d: '', dot: null }
  let yAxis = ['—', '—', '—', '—', '—', '—']
  let xAxis = []

  if (Array.isArray(pts) && pts.length >= 2) {
    const { maxV, span, ptsStr } = buildSeries(pts)
    ves = paint(pts.map(p => p.vesign), ptsStr)
    port = paint(pts.map(p => p.portfolio), ptsStr)
    yAxis = [0, 1, 2, 3, 4, 5].map(i => {
      const v = maxV - (i / 5) * span
      return `${v >= 0 ? '+' : ''}${v.toFixed(0)}%`
    })
    const N = pts.length
    xAxis = [0, 1, 2, 3, 4, 5, 6].map(i => {
      const dt = new Date(pts[Math.round(i / 6 * (N - 1))].week)
      return dt.toLocaleDateString(undefined, { month: 'short' })
    })
  }

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
      <div className="chart-body">
        <div className="y-axis">
          {yAxis.map((v, i) => <span key={i}>{v}</span>)}
        </div>
        <svg viewBox="0 0 800 300" preserveAspectRatio="none">
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
          {/* end markers */}
          {ves.dot && <circle cx={ves.dot.cx} cy={ves.dot.cy} r="3.5" fill="#00d97e" />}
          {port.dot && <circle cx={port.dot.cx} cy={port.dot.cy} r="3" fill="#60a5fa" />}
        </svg>
      </div>
      <div className="x-axis">
        {xAxis.map((m, i) => <span key={i}>{m}</span>)}
      </div>
    </div>
  )
}
