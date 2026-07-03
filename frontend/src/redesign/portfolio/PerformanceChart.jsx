/* Performance chart — Holdings vs benchmarks (Vesign, S&P 500) and any tickers
 * the user adds, inline SVG. Range chips (1M…All) refetch at that horizon; a %/$
 * toggle switches between normalized return % (all lines) and Holdings market
 * value $ (single line). Holdings is always shown; up to 4 comparison lines can
 * be removed (×) or added (+ Add → ticker search). Hover snaps to the nearest
 * weekly point and tooltips the visible series. */
import { useState, useRef, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { getPortfolioPerformance, searchTickers } from '../../api'
import { useCurrency } from '../../context/CurrencyContext'
import { LOGO } from '../fmt'

const W = 800, H = 300
const RANGES = [['1M', 1], ['3M', 3], ['6M', 6], ['1Y', 12], ['2Y', 24], ['All', 60]]
const BUILTIN = {
  portfolio: { label: 'Holdings', color: '#60a5fa' },
  vesign: { label: 'Vesign', color: '#00d97e' },
  spy: { label: 'S&P 500', color: '#f59e0b' },
}
const EXTRA_COLORS = ['#a855f7', '#22d3ee', '#ec4899', '#eab308']
const VALUE_SERIES = { key: 'value', color: '#60a5fa', label: 'Holdings value' }
const MAX_COMPARISONS = 3   // Holdings + 3 = 4 lines max
// Market indexes live in index_prices, keyed by these symbols; map to friendly labels.
const INDEX_LABELS = { '^DJI': 'Dow Jones', '^NDX': 'Nasdaq 100', '^RUT': 'Russell 2000', '^GSPC': 'S&P 500' }
// One-click presets in the Add popover (re-add built-ins + the market indexes).
const PRESETS = [
  { key: 'vesign', label: 'Vesign' },
  { key: 'spy', label: 'S&P 500' },
  { key: '^DJI', label: 'Dow Jones' },
  { key: '^NDX', label: 'Nasdaq 100' },
  { key: '^RUT', label: 'Russell 2000' },
]

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`)
const fmtDate = (w) => new Date(w).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })

export default function PerformanceChart() {
  const [months, setMonths] = useState(12)
  const [mode, setMode] = useState('pct')                       // 'pct' | 'value'
  const [comparisons, setComparisons] = useState(['spy'])
  const [addOpen, setAddOpen] = useState(false)
  const [sq, setSq] = useState('')
  const { symbol, rate } = useCurrency()
  const svgRef = useRef(null)
  const bodyRef = useRef(null)
  const addRef = useRef(null)
  const [hover, setHover] = useState(null)

  const extraParam = comparisons.filter(k => !BUILTIN[k]).join(',')
  const { data: pts } = useQuery({
    queryKey: ['portfolio-performance', months, extraParam],
    queryFn: () => getPortfolioPerformance('US', months, extraParam),
    placeholderData: keepPreviousData,
  })
  const { data: searchRes } = useQuery({
    queryKey: ['perf-search', sq],
    queryFn: () => searchTickers(sq, 8),
    enabled: addOpen && sq.trim().length >= 1,
  })

  // Close the add-popover on outside click.
  useEffect(() => {
    if (!addOpen) return
    const onDoc = (e) => { if (addRef.current && !addRef.current.contains(e.target)) setAddOpen(false) }
    document.addEventListener('mousedown', onDoc)
    return () => document.removeEventListener('mousedown', onDoc)
  }, [addOpen])

  const data = Array.isArray(pts) ? pts : []
  const N = data.length
  const isValue = mode === 'value'

  // Resolve each comparison to a {key,label,color}; tickers get palette colors.
  let tIdx = 0
  const compDefs = comparisons.map(k => BUILTIN[k]
    ? { key: k, ...BUILTIN[k] }
    : { key: k, label: INDEX_LABELS[k] || k, color: EXTRA_COLORS[(tIdx++) % EXTRA_COLORS.length] })
  const seriesDefs = isValue
    ? [VALUE_SERIES]
    : [{ key: 'portfolio', ...BUILTIN.portfolio }, ...compDefs]

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
      return `${dt.toLocaleDateString(undefined, { month: 'short' })}'${String(dt.getFullYear()).slice(-2)}`
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

  const removeComp = (key) => setComparisons(prev => prev.filter(k => k !== key))
  const addComp = (ticker) => setComparisons(prev =>
    (prev.length >= MAX_COMPARISONS || prev.includes(ticker)) ? prev : [...prev, ticker])
  const canAdd = comparisons.length < MAX_COMPARISONS
  const addable = (searchRes || []).filter(r => r.ticker && !comparisons.includes(r.ticker) && r.ticker !== 'SPY')
  const availPresets = PRESETS.filter(p => !comparisons.includes(p.key))

  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Performance</h3>
        <div className="legend">
          {isValue ? (
            <span className="lg-item"><span className="sw" style={{ background: VALUE_SERIES.color }}></span> {VALUE_SERIES.label}</span>
          ) : (
            <>
              <span className="lg-item"><span className="sw" style={{ background: BUILTIN.portfolio.color }}></span> Holdings</span>
              {compDefs.map(s => (
                <span key={s.key} className="lg-item lg-removable">
                  <span className="sw" style={{ background: s.color }}></span> {s.label}
                  <button className="lg-x" onClick={() => removeComp(s.key)} title={`Remove ${s.label}`}>×</button>
                </span>
              ))}
              {canAdd && (
                <span className="lg-add-wrap" ref={addRef}>
                  <button className="lg-add" onClick={() => setAddOpen(o => !o)}>+ Add</button>
                  {addOpen && (
                    <div className="lg-add-pop">
                      {availPresets.length > 0 && (
                        <div className="lg-presets">
                          {availPresets.map(p => (
                            <button key={p.key} className="lg-preset"
                              onClick={() => { addComp(p.key); setAddOpen(false); setSq('') }}>{p.label}</button>
                          ))}
                        </div>
                      )}
                      <input autoFocus placeholder="Add ticker…" value={sq} onChange={e => setSq(e.target.value)} />
                      <div className="lg-add-list">
                        {addable.length === 0
                          ? <div className="lg-add-empty">{sq.trim() ? 'No matches' : 'Type a ticker or company'}</div>
                          : addable.map(r => (
                            <button key={r.ticker} className="lg-add-item"
                              onClick={() => { addComp(r.ticker); setAddOpen(false); setSq('') }}>
                              <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
                              <b>{r.ticker}</b><span className="co">{r.company}</span>
                            </button>
                          ))}
                      </div>
                    </div>
                  )}
                </span>
              )}
            </>
          )}
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
          {/* gridlines aligned to the 6 y-axis label rows (0…H in 5 steps) */}
          {[0, 1, 2, 3, 4, 5].map(i => (
            <line key={i} x1="0" x2="800" y1={i * H / 5} y2={i * H / 5} stroke="rgba(255,255,255,0.05)" />
          ))}
          {lines.map(s => (
            <g key={s.key}>
              <path d={s.area} fill={s.color} fillOpacity="0.07" />
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
