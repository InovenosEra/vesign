/* VeSign public landing page — standalone marketing page for logged-out visitors.
 * Renders OUTSIDE AppShell (own nav + footer), inside a `.rd` wrapper so the
 * scoped redesign tokens apply. Real data (win rate / trade count / etc.) comes
 * from the public /api/stats endpoint; the "Platform" section below reuses the
 * ACTUAL Signals/Market/Portfolio component CSS (signals.css, portfolio.css) so
 * those mockups are visually identical to the real product — only the ticker
 * data in them is a fixed illustrative example, not a live fetch. */
import { useEffect, useRef, useState } from 'react'
import { SCROLL_DURATION_MS, nextIndex, nearestIndex, scrollYAt } from './landingScroll'
import { Link } from 'react-router-dom'
import './redesign.css'
import './signals/signals.css'
import './portfolio/portfolio.css'
import './landing.css'

/* ── Real public stats (/api/stats — unauthenticated) ─────────────────────── */
function useStats() {
  const [stats, setStats] = useState(null)
  useEffect(() => {
    let alive = true
    fetch('/api/stats')
      .then(r => (r.ok ? r.json() : null))
      .then(d => { if (alive) setStats(d) })
      .catch(() => {})
    return () => { alive = false }
  }, [])
  return stats
}

const fmtInt = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'))
const fmtPct = (n) => (n == null ? '—' : `${n}%`)
const fmtSignedPct = (n) => (n == null ? '—' : `${n >= 0 ? '+' : ''}${n}%`)

function proofStats(stats) {
  return [
    { k: 'Win rate, closed trades', v: fmtPct(stats?.win_rate) },
    { k: 'Closed trades', v: fmtInt(stats?.closed_trades) },
    { k: 'Avg return per closed trade', v: fmtSignedPct(stats?.avg_yield) },
    { k: 'Avg holding period', v: stats?.avg_hold_days == null ? '—' : `${stats.avg_hold_days} days` },
  ]
}

/* Free/Pro/Max tier copy is shared with the sign-up plan explainer — single
 * source in ./tiers (placeholder data, see TODO there). */
import { TIERS as PRICING } from './tiers'

const FAQS = [
  {
    q: 'Is this investment advice?',
    a: 'No. VeSign is a research and information platform. Signals are the output of a systematic model and are published for research purposes only — they are not personalized investment advice, and we are not a broker or advisor. Always do your own diligence.',
  },
  {
    q: 'How are signals generated?',
    a: 'A daily systematic process screens 1,800+ US-listed stocks and combines technical indicators, a machine-learning price model, analyst consensus, and company financial health into a single decision. When the evidence lines up, a BUY or SELL is published — with the reasoning written out in plain language.',
  },
  {
    q: 'Where do the performance numbers come from?',
    a: 'They are the results of running the model’s strategy on historical US market data. The full list of trades the model generated — entries, exits, winners and losers — is published on the performance page, alongside an equity curve against the S&P 500. They are not live-traded or real-money returns, and past performance does not guarantee future results.',
  },
  {
    q: 'Where does the data come from?',
    a: 'Prices, fundamentals, and analyst estimates come from institutional-grade market data providers and are refreshed daily after the US close. Some displayed prices may be delayed or indicative.',
  },
  {
    q: 'Is there a free plan?',
    a: 'Yes. The Free plan includes the market overview, a sample of the daily signals, and the full public track record — no credit card required.',
  },
]

/* Same V-mark as AppShell, with its own gradient id so the defs never collide. */
function LogoMark({ className }) {
  return (
    <svg className={className} viewBox="0 0 100 100" fill="none" aria-label="VeSign">
      <defs>
        <linearGradient id="rd-grad-landing" x1="50" y1="0" x2="50" y2="100" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#a5f3fc" />
          <stop offset="35%" stopColor="#22d3ee" />
          <stop offset="75%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#1d4ed8" />
        </linearGradient>
      </defs>
      <path d="M4 22 L50 96 L96 22 L78 32 L50 70 L22 32 Z" fill="url(#rd-grad-landing)" />
      <path d="M40 32 Q50 22 60 32" stroke="url(#rd-grad-landing)" strokeWidth="2.2" fill="none" />
      <path d="M34 28 Q50 14 66 28" stroke="url(#rd-grad-landing)" strokeWidth="2" fill="none" opacity="0.85" />
      <path d="M28 24 Q50 6 72 24" stroke="url(#rd-grad-landing)" strokeWidth="1.8" fill="none" opacity="0.7" />
    </svg>
  )
}

/* ── Nav ───────────────────────────────────────────────────────────────────── */
export function LandingNav() {
  return (
    <header className="ld-nav">
      <a href="#top" className="ld-nav-logo">
        <LogoMark className="ld-logo-mark" />
        <span className="ld-logo-text">VeSign</span>
      </a>
      <nav className="ld-nav-links">
        <a href="/#how">How it works</a>
        <a href="/#platform">Platform</a>
        <a href="/#pricing">Pricing</a>
        <a href="/#faq">FAQ</a>
      </nav>
      <div className="ld-nav-ctas">
        <Link to="/sign-in" className="ld-btn ghost">Log in</Link>
        <Link to="/sign-up" className="ld-btn primary">Sign up free</Link>
      </div>
    </header>
  )
}

/* ── Decorative ticker (illustrative — same convention as the old hero's mock
 * signal card: aria-hidden, clearly not a live feed). Wiring this to the real
 * live tape needs its own QueryClientProvider on the logged-out route, which
 * doesn't exist today — TODO if that's worth adding. */
const TAPE_ITEMS = [
  ['NVDA', '+2.41%', 'up'], ['AAPL', '+0.84%', 'up'], ['GOOGL', '+1.12%', 'up'],
  ['TSLA', '-1.94%', 'down'], ['MSFT', '+0.63%', 'up'], ['AVGO', '+1.87%', 'up'],
  ['XOM', '-0.42%', 'down'], ['BAC', '+0.91%', 'up'],
]
function DecorativeTape() {
  const items = [...TAPE_ITEMS, ...TAPE_ITEMS]
  return (
    <div className="ld-tape" aria-hidden="true">
      <div className="ld-tape-track">
        {items.map(([tk, ch, dir], i) => (
          <span key={i} className={'ld-tape-item ' + dir}>{tk} {ch}</span>
        ))}
      </div>
    </div>
  )
}

/* ── Hero: full-bleed canvas particle field behind a centered thesis ──────── */
function HeroCanvas() {
  const ref = useRef(null)
  useEffect(() => {
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    const canvas = ref.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    let W, H, DPR, raf, flareTimer
    function resize() {
      DPR = Math.min(2, window.devicePixelRatio || 1)
      W = canvas.clientWidth; H = canvas.clientHeight
      canvas.width = W * DPR; canvas.height = H * DPR
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0)
    }
    window.addEventListener('resize', resize)
    resize()

    const N = 110
    const pts = []
    for (let i = 0; i < N; i++) {
      pts.push({ x: Math.random() * W, y: Math.random() * H, vx: (Math.random() - 0.5) * 0.18, vy: (Math.random() - 0.5) * 0.18, r: 1 + Math.random() * 1.6, flare: 0, fc: null })
    }
    function pickFlare() { const p = pts[Math.floor(Math.random() * pts.length)]; p.flare = 1; p.fc = Math.random() < 0.72 ? '0,217,126' : '255,77,92' }
    function drawStatic() {
      ctx.clearRect(0, 0, W, H)
      pts.forEach(p => { ctx.beginPath(); ctx.fillStyle = 'rgba(148,163,184,0.3)'; ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2); ctx.fill() })
    }
    function frame() {
      ctx.clearRect(0, 0, W, H)
      for (let i = 0; i < pts.length; i++) {
        for (let j = i + 1; j < pts.length; j++) {
          const a = pts[i], b = pts[j]
          const dx = a.x - b.x, dy = a.y - b.y, d2 = dx * dx + dy * dy
          if (d2 < 125 * 125) {
            const o = (1 - d2 / (125 * 125)) * 0.09
            ctx.strokeStyle = `rgba(96,165,250,${o})`; ctx.lineWidth = 1
            ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke()
          }
        }
      }
      pts.forEach(p => {
        p.x += p.vx; p.y += p.vy
        if (p.x < 0 || p.x > W) p.vx *= -1
        if (p.y < 0 || p.y > H) p.vy *= -1
        let color, radius, alpha
        if (p.flare > 0) {
          color = p.fc; radius = p.r + p.flare * 3.6; alpha = 0.25 + p.flare * 0.7
          p.flare -= 0.018; if (p.flare < 0) p.flare = 0
        } else { color = '148,163,184'; radius = p.r; alpha = 0.32 }
        ctx.beginPath(); ctx.fillStyle = `rgba(${color},${alpha})`; ctx.arc(p.x, p.y, radius, 0, Math.PI * 2); ctx.fill()
        if (p.flare > 0) { ctx.beginPath(); ctx.strokeStyle = `rgba(${color},${p.flare * 0.4})`; ctx.arc(p.x, p.y, radius + 6, 0, Math.PI * 2); ctx.stroke() }
      })
      raf = requestAnimationFrame(frame)
    }
    if (reduce) { drawStatic() } else { flareTimer = setInterval(pickFlare, 480); raf = requestAnimationFrame(frame) }
    return () => {
      window.removeEventListener('resize', resize)
      if (raf) cancelAnimationFrame(raf)
      if (flareTimer) clearInterval(flareTimer)
    }
  }, [])
  return <canvas className="ld-hero-canvas" ref={ref} aria-hidden="true" />
}

function Hero() {
  return (
    <header className="ld-hero" id="top">
      <HeroCanvas />
      <div className="ld-hero-inner">
        <div className="ld-eyebrow">US equities · Research platform</div>
        <h1 className="ld-thesis">Every signal comes with its <span className="g">receipts.</span></h1>
        <p className="ld-hero-sub">
          A daily systematic read on the U.S. market — technicals, machine learning,
          and analyst consensus, boiled down to plain English. Every trade published,
          wins and losses alike.
        </p>
        <div className="ld-hero-ctas">
          <Link to="/sign-up" className="ld-btn primary lg">Sign up free</Link>
          <a href="/#pricing" className="ld-btn ghost lg">See how it's priced</a>
        </div>
      </div>
      <DecorativeTape />
      <div className="ld-scrollcue">Watch it reason<div className="chev" /></div>
    </header>
  )
}

/* ── Engine scene: data feeds in, a layered neural net "thinks", signals out ─
 * TODO: everything below is a fixed illustrative example (aria-hidden), not a
 * live model call. Kept static/CSS-driven deliberately: no per-frame JS, and
 * no `filter`/`backdrop-filter` on anything that animates infinitely — this
 * codebase has hit real Chrome GPU-compositing "ghost smear" bugs from that
 * exact combination before (see project history on the Signals page + shared
 * header). Glow here is radial-gradient/box-shadow only. */
const ENG_VB_W = 1400, ENG_VB_H = 620
const dist = (x1, y1, x2, y2) => Math.hypot(x2 - x1, y2 - y1)

/* Net geometry: 3 columns (6 → 7 → 5 nodes) laid out around the panel's
 * center. Input column has one node per feed below so each dashed line lands
 * on its own neuron; the output column doesn't need a 1:1 slot per signal
 * since those already cycle in and out rather than all showing at once. */
const NET_CX = 700, NET_CY = 310, NET_ROW_GAP = 65
const NET_LAYER_X = { in: 550, hid: 700, out: 850 }
const netColumn = (x, count, r) =>
  Array.from({ length: count }, (_, i) => ({ x, y: NET_CY + (i - (count - 1) / 2) * NET_ROW_GAP, r }))
const NET_IN_NODES = netColumn(NET_LAYER_X.in, 6, 5)
const NET_HID_NODES = netColumn(NET_LAYER_X.hid, 7, 6.5)
const NET_OUT_NODES = netColumn(NET_LAYER_X.out, 5, 5)
const NET_ALL_NODES = [...NET_IN_NODES, ...NET_HID_NODES, ...NET_OUT_NODES]
const NET_EDGES = [
  ...NET_IN_NODES.flatMap((a, ai) => NET_HID_NODES.map((b, bi) => ({ id: `i${ai}h${bi}`, a, b }))),
  ...NET_HID_NODES.flatMap((a, ai) => NET_OUT_NODES.map((b, bi) => ({ id: `h${ai}o${bi}`, a, b }))),
]
/* A curated subset of edges carries a traveling pulse (rest stay dim/static)
 * — animating all 77 would read as noise instead of "thinking". */
const NET_PULSE_META = {
  i0h0: { dur: 2.6, delay: -0.3 }, i0h3: { dur: 3.1, delay: -1.4 },
  i1h1: { dur: 2.8, delay: -0.8 }, i1h5: { dur: 3.4, delay: -2.1 },
  i2h2: { dur: 2.5, delay: -1.9 }, i2h6: { dur: 3.0, delay: -0.5 },
  i3h0: { dur: 2.9, delay: -2.6 }, i3h4: { dur: 3.3, delay: -1.1 },
  i4h1: { dur: 2.7, delay: -0.2 }, i4h6: { dur: 3.2, delay: -1.7 },
  i5h3: { dur: 2.6, delay: -2.3 },
  h0o0: { dur: 2.4, delay: -0.6 }, h1o2: { dur: 2.8, delay: -1.5 },
  h2o4: { dur: 3.1, delay: -0.9 }, h3o1: { dur: 2.5, delay: -2.0 },
  h4o3: { dur: 2.9, delay: -1.2 }, h5o0: { dur: 3.0, delay: -0.4 },
  h6o2: { dur: 2.7, delay: -1.8 },
}

const bezierIn = (y, tx, ty) => {
  const x0 = 40
  const cx1 = x0 + (tx - x0) * 0.55, cx2 = x0 + (tx - x0) * 0.85
  return `M ${x0} ${y} C ${cx1.toFixed(0)} ${y}, ${cx2.toFixed(0)} ${ty.toFixed(0)}, ${tx.toFixed(1)} ${ty.toFixed(1)}`
}
const bezierOut = (ox, oy, y) => {
  const x0 = 1350
  const cx1 = ox + (x0 - ox) * 0.18, cx2 = ox + (x0 - ox) * 0.5
  return `M ${ox.toFixed(1)} ${oy.toFixed(1)} C ${cx1.toFixed(0)} ${oy.toFixed(0)}, ${cx2.toFixed(0)} ${y}, ${x0} ${y}`
}
/* Feed lines fade to transparent right as they reach their input neuron (via
 * a per-feed gradient) rather than stopping dead at a marker — the node's own
 * glow takes over from there. */
const FEED_COLORS = {
  price: 'var(--blue-2)',
  fund: 'var(--green)',
  fin: '#c084fc',
  an: 'var(--gold)',
  news: 'var(--ink-3)',
  macro: '#22d3ee',
}
/* Slow, slightly-offset durations per feed — reads as steady/constant rather
 * than one mechanical pulse (all six moving in lockstep would look nervous,
 * not stable). Each entry's y/target is its dedicated input-layer node, so
 * the fan-in never crosses another line and always lands on its own neuron. */
const ENGINE_INPUTS = [
  { label: 'PRICE', cls: 'price', dur: 12 },
  { label: 'FUNDAMENTALS', cls: 'fund', dur: 15 },
  { label: 'FINANCIALS', cls: 'fin', dur: 13.5 },
  { label: 'ANALYST', cls: 'an', dur: 11 },
  { label: 'NEWS', cls: 'news', dur: 16 },
  { label: 'MACRO', cls: 'macro', dur: 14 },
].map((f, i) => ({ ...f, y: NET_IN_NODES[i].y, tx: NET_IN_NODES[i].x, ty: NET_IN_NODES[i].y }))
/* dur/delay: each output's full appear → hold → erase cycle. Shared verbatim
 * between the line and its card below so a signal's line can never linger
 * (or vanish) out of sync with the card it's delivering. Seven feeds with a
 * wide hold window (see engCardCycle/engLineDraw) and irregular durations so
 * several are reliably overlapping at any moment — signals never "queue up"
 * one at a time. `node` picks which output-layer neuron the line leaves from. */
const ENGINE_OUTPUTS = [
  { cardY: 94, node: 0, cls: 'sigOut1', verdict: 'buy', ticker: 'NVDA', dur: 6, delay: -0.5 },
  { cardY: 171, node: 1, cls: 'sigOut2', verdict: 'hold', ticker: 'MSFT', dur: 7, delay: -2.2 },
  { cardY: 253, node: 2, cls: 'sigOut3', verdict: 'sell', ticker: 'XOM', dur: 5.5, delay: -1.0 },
  { cardY: 335, node: 3, cls: 'sigOut4', verdict: 'buy', ticker: 'AAPL', dur: 8, delay: -4.5 },
  { cardY: 420, node: 4, cls: 'sigOut5', verdict: 'hold', ticker: 'GOOGL', dur: 6.5, delay: -3.0 },
  { cardY: 498, node: 0, cls: 'sigOut6', verdict: 'sell', ticker: 'TSLA', dur: 7.5, delay: -5.8 },
  { cardY: 571, node: 2, cls: 'sigOut7', verdict: 'buy', ticker: 'AVGO', dur: 6.2, delay: -1.8 },
]

/* the four steps below feed off the same "how it works" story as the engine
 * scene above, so they render inside it rather than as a separate section. */
const STEPS = [
  {
    n: '01', t: 'Screen', tag: '1,800+ stocks, daily',
    d: '1,800+ US-listed stocks re-scored every trading day after the close.',
  },
  {
    n: '02', t: 'Score', tag: '3 independent reads',
    d: 'Three independent reads per stock — technicals, an ML model, analyst consensus.',
  },
  {
    n: '03', t: 'Signal', tag: 'BUY / SELL, live',
    d: 'A BUY or SELL goes out only when the evidence lines up, with the “why” in plain language.',
  },
  {
    n: '04', t: 'Track',
    d: 'Every position is stop-managed; every closed trade — win or lose — is published.',
  },
]

function EngineScene() {
  return (
    <section className="ld-scene" id="how">
      <div className="ld-scene-head">
        <div className="tag">How it works</div>
        <h2>From signals to a <span className="g">signal.</span></h2>
        <p>Every signal is the result of many independent data feeds converging into one continuously-running model — not a bare score.</p>
      </div>
      <div className="ld-engine" aria-hidden="true">
        <div className="eng-cap left">
          <span className="n"><b>{STEPS[0].n}</b>{STEPS[0].t}</span>
          <span className="tag">{STEPS[0].tag}</span>
        </div>
        <div className="eng-cap center">
          <span className="n"><b>{STEPS[1].n}</b>{STEPS[1].t}</span>
          <span className="tag">{STEPS[1].tag}</span>
        </div>
        <div className="eng-cap right">
          <span className="n"><b>{STEPS[2].n}</b>{STEPS[2].t}</span>
          <span className="tag">{STEPS[2].tag}</span>
        </div>

        <div className="eng-glow" />

        <svg className="eng-svg" viewBox={`0 0 ${ENG_VB_W} ${ENG_VB_H}`} preserveAspectRatio="xMidYMid meet">
          <defs>
            {ENGINE_INPUTS.map((f) => (
              <linearGradient key={'grad-' + f.cls} id={'feedFade-' + f.cls} gradientUnits="userSpaceOnUse" x1={40} y1={f.y} x2={f.tx} y2={f.ty}>
                <stop offset="0%" style={{ stopColor: FEED_COLORS[f.cls], stopOpacity: 1 }} />
                <stop offset="55%" style={{ stopColor: FEED_COLORS[f.cls], stopOpacity: 1 }} />
                <stop offset="100%" style={{ stopColor: FEED_COLORS[f.cls], stopOpacity: 0 }} />
              </linearGradient>
            ))}
            <radialGradient id="netNodeGlow" cx="35%" cy="30%" r="75%">
              <stop offset="0%" style={{ stopColor: '#eafcff', stopOpacity: 1 }} />
              <stop offset="45%" style={{ stopColor: 'var(--blue-2)', stopOpacity: 0.95 }} />
              <stop offset="100%" style={{ stopColor: 'var(--blue-2)', stopOpacity: 0.35 }} />
            </radialGradient>
          </defs>

          {ENGINE_INPUTS.map((f) => (
            <g key={f.cls}>
              <path
                className={'eng-feed ' + f.cls}
                d={bezierIn(f.y, f.tx, f.ty)}
                fill="none"
                style={{ stroke: `url(#feedFade-${f.cls})`, animationDuration: `${f.dur}s` }}
              />
              <text className={'eng-lbl ' + f.cls} x="40" y={f.y - 10}>{f.label}</text>
            </g>
          ))}

          {/* the net: dim static wiring between every adjacent-layer pair, a
           * sparse subset lit with a traveling pulse, neurons on top glowing
           * via a shared radial gradient (no filter/blur). */}
          <g className="net">
            {NET_EDGES.map((e) => (
              <line key={e.id} className="net-edge" x1={e.a.x} y1={e.a.y} x2={e.b.x} y2={e.b.y} />
            ))}
            {NET_EDGES.filter((e) => NET_PULSE_META[e.id]).map((e) => {
              const meta = NET_PULSE_META[e.id]
              const len = dist(e.a.x, e.a.y, e.b.x, e.b.y)
              return (
                <line
                  key={'p-' + e.id}
                  className="net-edge pulse"
                  x1={e.a.x} y1={e.a.y} x2={e.b.x} y2={e.b.y}
                  style={{ '--gap': len.toFixed(0), '--off': (-(len + 16)).toFixed(0), animationDuration: `${meta.dur}s`, animationDelay: `${meta.delay}s` }}
                />
              )
            })}
            {NET_ALL_NODES.map((n, i) => (
              <circle key={'n' + i} className="net-node" cx={n.x} cy={n.y} r={n.r} style={{ animationDelay: `-${(i * 0.31).toFixed(2)}s` }} />
            ))}
          </g>

          {ENGINE_OUTPUTS.map((s) => {
            const { x: ox, y: oy } = NET_OUT_NODES[s.node]
            const len = Math.round(dist(ox, oy, 1350, s.cardY) * 1.15)
            return (
              <path
                key={s.cls}
                className={'eng-signal ' + s.verdict}
                d={bezierOut(ox, oy, s.cardY)}
                fill="none"
                style={{ '--len': len, animationDuration: `${s.dur}s`, animationDelay: `${s.delay}s` }}
              />
            )
          })}
        </svg>

        {ENGINE_OUTPUTS.map((s) => (
          <div
            key={s.cls}
            className={`eng-card ${s.verdict}`}
            style={{ top: `${(s.cardY / ENG_VB_H) * 100}%`, animationDuration: `${s.dur}s`, animationDelay: `${s.delay}s` }}
          >
            <img className="eng-card-logo" src={`/logos/${s.ticker}.png`} alt="" />
            <span className="tk">{s.ticker}</span>
            <span className="pill">{s.verdict.toUpperCase()}</span>
          </div>
        ))}
      </div>

      <div className="ld-flow">
        {STEPS.map(s => (
          <div className="ld-flow-item" key={s.n}>
            <div className="n">{s.n}</div>
            <h4>{s.t}</h4>
            <p>{s.d}</p>
          </div>
        ))}
      </div>
      <div className="ld-track">
        <span className="n">{STEPS[3].n}</span>
        <h4>{STEPS[3].t}</h4>
        <p>{STEPS[3].d}</p>
      </div>
    </section>
  )
}

/* ── Proof / model results (real /api/stats data, bigger number treatment) ── */
function Proof({ stats }) {
  return (
    <section className="ld-section ld-proof" id="proof">
      <div className="ld-section-head">
        <div className="tag">The proof</div>
        <h2>Every trade, accounted for.</h2>
        <p>
          These are the results our model produced running its strategy on historical
          US market data — every trade it generated, winners and losers, nothing
          cherry-picked. The full list and an equity curve against the S&amp;P 500
          are public.
        </p>
      </div>
      <div className="ld-proof-grid">
        {proofStats(stats).map(s => (
          <div className="ld-proof-cell" key={s.k}>
            <div className="v">{s.v}</div>
            <div className="k">{s.k}</div>
          </div>
        ))}
      </div>
      <div className="ld-proof-foot">
        <span className="note">
          Results from running the model's strategy on historical data — not
          live-traded or real-money returns. Past performance does not
          guarantee future results.
        </span>
        <Link to="/performance" className="ld-btn ghost">
          Browse all {stats?.closed_trades ? fmtInt(stats.closed_trades) : ''} trades →
        </Link>
      </div>
    </section>
  )
}

/* ── Platform: the real Signals/Market/Portfolio component CSS, illustrative
 * fixed data (not a live fetch — see file header). ──────────────────────── */
function PlatformShots() {
  return (
    <section className="ld-section" id="platform">
      <div className="ld-section-head">
        <div className="tag">The platform</div>
        <h2>A full workspace, not a mailing list</h2>
        <p>Market, Signals, Portfolio and Research — one consistent home for the signal once it's published. These are the actual product components, not illustrations.</p>
      </div>
      <div className="ld-shots">
        <div className="ld-shots-grid">
          <figure className="ld-device tall">
            <div className="ld-device-bar"><i /><i /><i /><span className="u">ve-sign.com/signals</span></div>
            <div className="ld-device-body">
              <div className="sigcard buy">
                <div className="sc-head">
                  <img className="sc-logo" src="/logos/NVDA.png" alt="NVDA" />
                  <div className="sc-id"><div className="trow"><span className="tk">NVDA</span></div><div className="co">NVIDIA Corp</div></div>
                  <div className="sig-why"><div className="sig-why-head">Price reclaimed the 50-day average on rising volume. The 5-day model projects +6.2%, analyst consensus sits 18% above market.</div></div>
                </div>
                <div className="sc-cockpit">
                  <div className="cell"><div className="l">Current Price</div><div className="v num">$194.83</div><div className="sub2 num up">+2.41%</div></div>
                  <div className="cell"><div className="l">Price Target</div><div className="v num">$229.90</div><div className="sub2 num up">+18.0%</div></div>
                  <div className="cell"><div className="l">5D ML</div><div className="v num">$206.90</div><div className="sub2 num up">+6.2%</div></div>
                  <div className="cell"><div className="l">Health</div><span className="health"><span className="d" /><span className="d" /><span className="d" /><span className="d" /><span className="d off" /></span></div>
                </div>
              </div>
              <div className="sigcard buy">
                <div className="sc-head">
                  <img className="sc-logo" src="/logos/AVGO.png" alt="AVGO" />
                  <div className="sc-id"><div className="trow"><span className="tk">AVGO</span></div><div className="co">Broadcom Inc</div></div>
                  <div className="sig-why"><div className="sig-why-head">Breakout above consolidation range on above-average volume, financial health in the top decile of the universe.</div></div>
                </div>
                <div className="sc-cockpit">
                  <div className="cell"><div className="l">Current Price</div><div className="v num">$360.45</div><div className="sub2 num up">+1.87%</div></div>
                  <div className="cell"><div className="l">Price Target</div><div className="v num">$400.10</div><div className="sub2 num up">+11.0%</div></div>
                  <div className="cell"><div className="l">5D ML</div><div className="v num">$374.15</div><div className="sub2 num up">+3.8%</div></div>
                  <div className="cell"><div className="l">Health</div><span className="health"><span className="d" /><span className="d" /><span className="d" /><span className="d" /><span className="d" /></span></div>
                </div>
              </div>
            </div>
            <figcaption><b>Signals cockpit</b>Today's BUYs and SELLs, reasoning expanded inline.</figcaption>
          </figure>

          <div className="ld-shots-side">
            <figure className="ld-device">
              <div className="ld-device-bar"><i /><i /><i /><span className="u">ve-sign.com/market</span></div>
              <div className="ld-device-body">
                <div className="ld-indices-mini">
                  <div className="idx-card"><div className="name">S&amp;P 500</div><div className="price">7,483.20</div><div className="change up"><span className="pct">+0.34%</span></div></div>
                  <div className="idx-card"><div className="name">Nasdaq 100</div><div className="price">29,329</div><div className="change down"><span className="pct">-1.61%</span></div></div>
                  <div className="idx-card"><div className="name">VIX</div><div className="price">16.15</div><div className="change down"><span className="pct">-2.62%</span></div></div>
                </div>
                <div className="mover-panel">
                  <div className="mover-head"><h3>Most active</h3></div>
                  <div className="mover-list">
                    <div className="mover-row"><img className="logo-mini" src="/logos/NVDA.png" alt="" /><div className="mname"><div className="tk">NVDA</div><div className="co">NVIDIA Corp</div></div><div className="px">194.83</div><div className="ch up">+2.41%</div></div>
                    <div className="mover-row"><img className="logo-mini" src="/logos/AAPL.png" alt="" /><div className="mname"><div className="tk">AAPL</div><div className="co">Apple Inc</div></div><div className="px">308.63</div><div className="ch up">+4.84%</div></div>
                  </div>
                </div>
              </div>
              <figcaption><b>Market overview</b>Indices, movers and breadth at a glance.</figcaption>
            </figure>

            <figure className="ld-device">
              <div className="ld-device-bar"><i /><i /><i /><span className="u">ve-sign.com/portfolio</span></div>
              <div className="ld-device-body">
                <div className="nw-hero ld-nw-mini">
                  <div className="nw-main">
                    <div className="nw-label">Current value</div>
                    <div className="nw-value"><span className="s">$</span>142,318.60</div>
                    <div className="nw-sub"><span className="nw-today up">▲ $612.40 (+0.43%) today</span></div>
                  </div>
                  <div className="nw-chips">
                    <div className="nw-chip"><div className="lbl">Invested</div><div className="val">$103,378</div></div>
                    <div className="nw-chip"><div className="lbl">vs Vesign (1Y)</div><div className="val down">-3.8%</div></div>
                  </div>
                </div>
              </div>
              <figcaption><b>Portfolio</b>Net worth, benchmarked against the model.</figcaption>
            </figure>
          </div>
        </div>
      </div>
    </section>
  )
}

/* ── Pricing ───────────────────────────────────────────────────────────────── */
function Pricing() {
  return (
    <section className="ld-section" id="pricing">
      <div className="ld-section-head">
        <div className="tag">Pricing</div>
        <h2>Start free. Upgrade when the signals earn it.</h2>
        <p><span className="ld-placeholder-tag">Placeholder pricing — final tiers TBD</span></p>
      </div>
      <div className="ld-pricing">
        {PRICING.map(p => (
          <div className={'ld-plan' + (p.featured ? ' featured' : '')} key={p.name}>
            {p.featured && <div className="ld-plan-flag">Most popular</div>}
            <div className="ld-plan-name">{p.name}</div>
            <div className="ld-plan-price">{p.price}<span>{p.period}</span></div>
            <div className="ld-plan-blurb">{p.blurb}</div>
            <ul>
              {p.features.map(f => <li key={f}>{f}</li>)}
            </ul>
            <Link to="/sign-up" className={'ld-btn ' + (p.featured ? 'primary' : 'ghost')}>{p.cta}</Link>
          </div>
        ))}
      </div>
    </section>
  )
}

/* ── FAQ ───────────────────────────────────────────────────────────────────── */
function Faq() {
  return (
    <section className="ld-section narrow" id="faq">
      <div className="ld-section-head">
        <div className="tag">Questions</div>
        <h2>Answered straight</h2>
      </div>
      <div className="ld-faq">
        {FAQS.map(f => (
          <details className="ld-faq-item" key={f.q}>
            <summary>{f.q}<span className="ld-faq-caret">▾</span></summary>
            <p>{f.a}</p>
          </details>
        ))}
      </div>
    </section>
  )
}

/* ── Final CTA + footer ────────────────────────────────────────────────────── */
function FinalCta() {
  return (
    <section className="ld-final">
      <h2>Read tomorrow's signals<br />with the reasoning attached.</h2>
      <p>Free plan, no credit card. The model’s full results are public either way.</p>
      <Link to="/sign-up" className="ld-btn primary lg">Sign up free</Link>
    </section>
  )
}

export function LandingFooter() {
  return (
    <footer className="ld-footer">
      <div className="ld-footer-top">
        <div className="ld-footer-brand">
          <LogoMark className="ld-logo-mark sm" />
          <span className="ld-logo-text">VeSign</span>
        </div>
        <nav className="ld-footer-links">
          <a href="/#how">How it works</a>
          <Link to="/performance">Performance</Link>
          <a href="/#pricing">Pricing</a>
          <a href="/#faq">FAQ</a>
          <Link to="/contact">Contact</Link>
          <Link to="/sign-in">Log in</Link>
        </nav>
      </div>
      <div className="ld-footer-legal">
        <p className="ld-disclaimer">
          Research and information only — not investment advice. VeSign is not a
          broker-dealer or investment advisor. Past performance does not
          guarantee future results. Prices may be delayed or indicative.
        </p>
        <span className="ld-copy">© {new Date().getFullYear()} VeSign. All rights reserved.</span>
      </div>
    </footer>
  )
}

/* Takes over section-to-section navigation from the browser's native CSS
 * scroll-snap (which offers no control over its settle animation) so the
 * transition glides on every input device instead of jumping. Only mounted
 * when the user hasn't asked for reduced motion — see LandingPage's effect
 * below, where CSS's own `ld-snap` mandatory-snap stays as the fallback
 * otherwise. Pure math lives in landingScroll.js; this is just the
 * wheel/keyboard/rAF glue, so it isn't unit-tested — verify manually. */
function mountEasedSectionScroll(html, main) {
  html.style.scrollSnapType = 'none'
  let rafId = null
  let animating = false

  // Mirrors the native `scroll-snap-align: start` behavior it replaces: that
  // respects each element's `scroll-margin-top`, which landing.css sets to
  // 64px on the hero specifically to cancel out the sticky nav's flow height
  // (the hero's real offsetTop is 64, but its intended resting scrollY is 0 —
  // see landing.css's own comment on `.rd .ld-hero`). Reading raw offsetTop
  // here would snap back to scrollY=64 and reintroduce the "hero mis-snapped,
  // next section peeking through" bug the mount effect elsewhere works around.
  const sectionTops = () => Array.from(main.children).map((el) => {
    const marginTop = parseFloat(getComputedStyle(el).scrollMarginTop) || 0
    return el.offsetTop - marginTop
  })

  const animateTo = (targetTop) => {
    const startY = window.scrollY
    const startTime = performance.now()
    animating = true
    const step = (now) => {
      const elapsed = now - startTime
      window.scrollTo(0, scrollYAt(startY, targetTop, elapsed))
      if (elapsed < SCROLL_DURATION_MS) {
        rafId = requestAnimationFrame(step)
      } else {
        animating = false
      }
    }
    rafId = requestAnimationFrame(step)
  }

  const go = (direction) => {
    if (animating) return
    const tops = sectionTops()
    const current = nearestIndex(tops, window.scrollY)
    animateTo(tops[nextIndex(current, direction, tops.length)])
  }

  const isTypingTarget = (el) =>
    !!el && (['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName) || el.isContentEditable)

  const onWheel = (e) => {
    if (e.ctrlKey) return // pinch-zoom gesture — leave it to the browser
    e.preventDefault()
    go(e.deltaY > 0 ? 1 : -1)
  }
  const onKeyDown = (e) => {
    if (isTypingTarget(document.activeElement)) return
    if (e.key === 'PageDown' || e.key === 'ArrowDown') { e.preventDefault(); go(1) }
    else if (e.key === 'PageUp' || e.key === 'ArrowUp') { e.preventDefault(); go(-1) }
  }

  window.addEventListener('wheel', onWheel, { passive: false })
  window.addEventListener('keydown', onKeyDown)

  return () => {
    window.removeEventListener('wheel', onWheel)
    window.removeEventListener('keydown', onKeyDown)
    if (rafId) cancelAnimationFrame(rafId)
    html.style.scrollSnapType = ''
  }
}

/* ── Page ──────────────────────────────────────────────────────────────────── */
export default function LandingPage() {
  // Match the document canvas to the redesign --bg while mounted (same pattern
  // as AppShell) so overscroll never flashes the legacy background.
  useEffect(() => {
    const html = document.documentElement
    const prev = html.style.background
    html.style.background = '#0a0e15'
    html.classList.add('ld-snap')
    // The browser can restore a stale non-zero scroll position across
    // reloads; with the sticky nav that leaves the hero mis-snapped (its
    // bottom short of the viewport, next section peeking through). Force a
    // clean start every time the landing page mounts.
    const prevRestoration = window.history.scrollRestoration
    window.history.scrollRestoration = 'manual'
    window.scrollTo(0, 0)

    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    const main = html.querySelector('.rd.ld main')
    const cleanupScroll = (!reduceMotion && main) ? mountEasedSectionScroll(html, main) : null

    return () => {
      html.style.background = prev
      html.classList.remove('ld-snap')
      window.history.scrollRestoration = prevRestoration
      if (cleanupScroll) cleanupScroll()
    }
  }, [])

  const stats = useStats()
  return (
    <div className="rd ld">
      <LandingNav />
      <main>
        <Hero />
        <EngineScene />
        <Proof stats={stats} />
        <PlatformShots />
        <Pricing />
        <Faq />
        <FinalCta />
      </main>
      <LandingFooter />
    </div>
  )
}
