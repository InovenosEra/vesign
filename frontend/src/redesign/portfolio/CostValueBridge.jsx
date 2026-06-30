/* Cost → Value bridge — a waterfall that attributes every dollar between what you
 * invested and what you hold today to the holding that earned (or lost) it. Start
 * bar = total cost, one step per holding (gainers first, then losers), end bar =
 * market value. No y-axis: each bar carries its own value. Pure data from
 * buildBridge(); money labels respect the selected currency.
 *
 * Sits below the holdings table in a 2/3 + 1/3 row: the bridge on the left, a
 * vertical "Vesign's read" insights rail on the right (signal mix, health, ML,
 * biggest upside, watch-out, concentration, top driver). */
import { useState } from 'react'
import { useCurrency } from '../../context/CurrencyContext'
import { LOGO } from '../fmt'
import { buildBridge, signalMix, avgHealthWeighted, topUpside, weakestHealth, concentration } from './derive'

const VB_W = 1040, VB_H = 312
const PAD_L = 24, PAD_R = 24, PAD_TOP = 40, PAD_BOT = 76
const GRAD = { tot: 'url(#brg-tot)', gain: 'url(#brg-gain)', loss: 'url(#brg-loss)' }

const HEALTH_COLOR = { 1: '#ff4d5c', 2: '#c2660c', 3: '#ff9500', 4: '#00d97e', 5: '#0a8f54' }
function HealthDots({ score }) {
  const n = score == null ? 0 : Math.max(0, Math.min(5, Math.round(score)))
  const c = HEALTH_COLOR[n] || '#6b7280'
  return (
    <span className="vr-dots">
      {[0, 1, 2, 3, 4].map(i => <span key={i} className={'vd' + (i < n ? '' : ' off')} style={i < n ? { background: c } : undefined} />)}
    </span>
  )
}

export default function CostValueBridge({ rows, totals }) {
  const { symbol, rate, fmtPrice } = useCurrency()
  const [hover, setHover] = useState(null)   // { bar, x, y }

  const { cost, value, segs, peak } = buildBridge(rows, totals)
  if (!segs.length) return null

  // Full, decimal-free currency for the bridge: $58,643 / +$11,626 (currency-converted).
  const money = (v) => symbol + Math.round(Math.abs(v) * rate).toLocaleString()
  const signed = (v) => (v >= 0 ? '+' : '−') + money(v)

  const plotH = VB_H - PAD_TOP - PAD_BOT
  const n = segs.length + 2
  const gap = 30
  const bw = (VB_W - PAD_L - PAD_R - (n - 1) * gap) / n
  const yMax = (Math.max(value, peak) * 1.07) || 1
  const y = (v) => PAD_TOP + plotH * (1 - v / yMax)
  const baseY = y(0)

  // Bars left→right: Cost total, one per holding, Value total.
  const bars = []
  let x = PAD_L
  bars.push({ kind: 'tot', label: 'Cost', sub: 'invested', total: cost, level: cost, x, top: y(cost), bot: baseY })
  x += bw + gap
  segs.forEach(s => {
    bars.push({ kind: s.kind, seg: s, level: s.to, x, top: y(Math.max(s.from, s.to)), bot: y(Math.min(s.from, s.to)) })
    x += bw + gap
  })
  bars.push({ kind: 'tot', label: 'Value', sub: 'today', total: value, level: value, x, top: y(value), bot: baseY })

  const gainSum = segs.filter(s => s.pnl > 0).reduce((a, s) => a + s.pnl, 0)
  const lossSum = segs.filter(s => s.pnl < 0).reduce((a, s) => a + s.pnl, 0)

  const onEnter = (bar, e) => setHover({ bar, x: e.clientX, y: e.clientY })
  const onMove = (e) => setHover(h => (h ? { ...h, x: e.clientX, y: e.clientY } : h))
  const onLeave = () => setHover(null)

  // ---- right rail: "Vesign's read" + bridge-tied extras ----
  const mix = signalMix(rows)
  const rated = mix.BUY + mix.HOLD + mix.SELL
  const avgH = avgHealthWeighted(rows)
  const mlVals = rows.map(r => r.prediction_score).filter(v => v != null)
  const avgML = mlVals.length ? mlVals.reduce((s, v) => s + v, 0) / mlVals.length * 100 : null
  const up = topUpside(rows)
  const weak = weakestHealth(rows)
  const conc = concentration(rows, totals.totalValue)
  const driver = segs[0]   // biggest contributor (segs sorted by pnl desc)
  const posture = mix.SELL > 0
    ? { word: 'Cautious', cls: 'cau' }
    : (avgH != null && avgH >= 4 && up && up.upside > 0)
      ? { word: 'Constructive', cls: 'con' }
      : { word: 'Neutral', cls: 'neu' }

  return (
    <>
      <div className="bridge-row">
        {/* LEFT — bridge (66%) */}
        <div className="panel">
          <div className="panel-head">
            <h3>P&amp;L</h3>
            <div className="legend">
              <span className="lg-item"><span className="sw" style={{ background: 'var(--blue-2)' }} /> Cost / Value</span>
              <span className="lg-item"><span className="sw" style={{ background: 'var(--green)' }} /> Gain</span>
              <span className="lg-item"><span className="sw" style={{ background: 'var(--red)' }} /> Loss</span>
            </div>
          </div>

          <div className="bridge-body">
            <svg viewBox={`0 0 ${VB_W} ${VB_H}`} preserveAspectRatio="xMidYMid meet">
              <defs>
                <linearGradient id="brg-tot" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0" stopColor="#60a5fa" /><stop offset="1" stopColor="#2f6fb0" />
                </linearGradient>
                <linearGradient id="brg-gain" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0" stopColor="#00d97e" /><stop offset="1" stopColor="#0a8f54" />
                </linearGradient>
                <linearGradient id="brg-loss" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0" stopColor="#ff5d6c" /><stop offset="1" stopColor="#b8313e" />
                </linearGradient>
              </defs>

              <line x1={PAD_L} y1={baseY} x2={VB_W - PAD_R} y2={baseY} stroke="var(--line-2)" />

              {bars.slice(0, -1).map((b, i) => {
                const lvl = y(b.level)
                return <line key={i} x1={b.x} y1={lvl} x2={bars[i + 1].x + bw} y2={lvl}
                  stroke="var(--ink-3)" strokeWidth="1" strokeDasharray="2 3" opacity="0.55" />
              })}

              {bars.map((b, i) => {
                const cx = b.x + bw / 2
                const h = Math.max(b.bot - b.top, 2)
                return (
                  <g key={i} style={{ cursor: 'pointer' }}
                    onMouseEnter={(e) => onEnter(b, e)} onMouseMove={onMove} onMouseLeave={onLeave}>
                    <rect x={b.x} y={b.top} width={bw} height={h} rx="5" fill={GRAD[b.kind]}
                      style={hover?.bar === b ? { filter: 'brightness(1.16)' } : undefined} />
                    {b.kind === 'tot'
                      ? <text x={cx} y={b.top - 10} textAnchor="middle" className="brg-tot-lbl">{money(b.total)}</text>
                      : <text x={cx} y={b.top - 8} textAnchor="middle" className="brg-step-lbl"
                          fill={b.seg.pnl >= 0 ? 'var(--green)' : 'var(--red)'}>{signed(b.seg.pnl)}</text>}
                    {b.kind === 'tot' ? (
                      <text x={cx} y={baseY + 26} textAnchor="middle" className="brg-axis" fill="var(--blue-2)">{b.label}</text>
                    ) : (
                      <>
                        <image href={LOGO(b.seg.ticker)} x={cx - 11} y={baseY + 11} width="22" height="22"
                          clipPath="inset(0 round 5px)" />
                        <text x={cx} y={baseY + 49} textAnchor="middle" className="brg-axis" fill="var(--ink)">{b.seg.ticker}</text>
                        <text x={cx} y={baseY + 63} textAnchor="middle" className="brg-axis-sub"
                          fill={b.seg.pnl >= 0 ? 'var(--green)' : 'var(--red)'}>
                          {(b.seg.yld >= 0 ? '+' : '') + (b.seg.yld == null ? '' : b.seg.yld.toFixed(0)) + '%'}
                        </text>
                      </>
                    )}
                  </g>
                )
              })}
            </svg>
          </div>

          <div className="bridge-cap">
            {gainSum > 0 && <>Gains <b style={{ color: 'var(--green)' }}>{signed(gainSum)}</b></>}
            {gainSum > 0 && lossSum < 0 && <>{' · '}</>}
            {lossSum < 0 && <>Losses <b style={{ color: 'var(--red)' }}>{signed(lossSum)}</b></>}
          </div>
        </div>

        {/* RIGHT — Vesign's read rail (33%) */}
        <aside className="panel insights-rail">
          <div className="ir-head">
            <div>
              <div className="ir-title">Vesign's read</div>
              <div className="ir-subtitle">the model's view of your positions</div>
            </div>
            <span className={'ir-posture ' + posture.cls}>{posture.word}</span>
          </div>

          <div className="ir-row">
            <span className="ir-k">Signal mix</span>
            <span className="ir-v">
              {mix.BUY > 0 && <span className="vr-chip buy">{mix.BUY} BUY</span>}
              {mix.HOLD > 0 && <span className="vr-chip hold">{mix.HOLD} HOLD</span>}
              {mix.SELL > 0 && <span className="vr-chip sell">{mix.SELL} SELL</span>}
              {rated === 0 && <span className="ir-muted">No signals</span>}
            </span>
          </div>

          <div className="ir-row">
            <span className="ir-k">Avg health</span>
            <span className="ir-v">{avgH == null ? '—' : <>{avgH.toFixed(1)}<span className="ir-of">/5</span> <HealthDots score={avgH} /></>}</span>
          </div>

          <div className="ir-row">
            <span className="ir-k">Avg ML quality</span>
            <span className="ir-v">{avgML == null ? '—' : `${avgML.toFixed(0)}%`}</span>
          </div>

          <div className="ir-row">
            <span className="ir-k">Biggest upside</span>
            <span className="ir-v">
              {up == null ? '—' : <><img className="ir-logo" src={LOGO(up.ticker)} alt="" />{up.ticker}
                <span style={{ color: 'var(--green)' }}>+{up.upside.toFixed(0)}%</span></>}
            </span>
          </div>

          <div className="ir-row">
            <span className="ir-k">Watch-out</span>
            <span className="ir-v">
              {weak == null ? '—' : <><img className="ir-logo" src={LOGO(weak.ticker)} alt="" />{weak.ticker}
                <span className="ir-muted">health {weak.health_score}/5</span></>}
            </span>
          </div>

          <div className="ir-row">
            <span className="ir-k">Concentration</span>
            <span className="ir-v">{conc.label}<span className="ir-muted">top-5 {conc.top5Pct.toFixed(0)}%</span></span>
          </div>

          <div className="ir-row">
            <span className="ir-k">Top driver</span>
            <span className="ir-v">
              <img className="ir-logo" src={LOGO(driver.ticker)} alt="" />{driver.ticker}
              <span style={{ color: driver.pnl >= 0 ? 'var(--green)' : 'var(--red)' }}>{signed(driver.pnl)}</span>
            </span>
          </div>
        </aside>
      </div>

      {hover && (
        <div className="bridge-tip" style={{
          left: Math.min(hover.x + 14, (typeof window !== 'undefined' ? window.innerWidth : 9999) - 200),
          top: hover.y + 14,
        }}>
          {hover.bar.kind === 'tot' ? (
            <>
              <div className="bt-head">{hover.bar.label} total</div>
              <div className="bt-row"><span>{hover.bar.label === 'Cost' ? 'Invested' : 'Market value'}</span><b>{money(hover.bar.total)}</b></div>
            </>
          ) : (
            <>
              <div className="bt-head"><img src={LOGO(hover.bar.seg.ticker)} alt="" />{hover.bar.seg.ticker} · {hover.bar.seg.company || ''}</div>
              <div className="bt-row"><span>{hover.bar.seg.qty} sh @ {fmtPrice(hover.bar.seg.avg)}</span><b>{money(hover.bar.seg.cost)}</b></div>
              <div className="bt-row"><span>now @ {fmtPrice(hover.bar.seg.last)}</span><b>{money(hover.bar.seg.value)}</b></div>
              <div className="bt-row"><span>contribution</span>
                <b style={{ color: hover.bar.seg.pnl >= 0 ? 'var(--green)' : 'var(--red)' }}>
                  {signed(hover.bar.seg.pnl)} ({hover.bar.seg.yld >= 0 ? '+' : ''}{hover.bar.seg.yld?.toFixed(1)}%)
                </b></div>
            </>
          )}
        </div>
      )}
    </>
  )
}
