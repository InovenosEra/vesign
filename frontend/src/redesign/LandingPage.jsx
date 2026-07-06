/* VeSign public landing page — standalone marketing page for logged-out visitors.
 * Renders OUTSIDE AppShell (own nav + footer), inside a `.rd` wrapper so the
 * scoped redesign tokens apply. Real data (win rate / trade count / etc.) comes
 * from the public /api/stats endpoint; the "Platform" section below reuses the
 * ACTUAL Signals/Market/Portfolio component CSS (signals.css, portfolio.css) so
 * those mockups are visually identical to the real product — only the ticker
 * data in them is a fixed illustrative example, not a live fetch. */
import { useEffect, useRef, useState } from 'react'
import { SCROLL_DURATION_MS, nextIndex, scrollYAt } from './landingScroll'
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

/* ── Engine scene: 4 bordered, chevron-connected panels illustrating each
 * step of "how it works" — rebuilt 2026-07-06 to match a reference
 * infographic. Static throughout except Panel 2's neural net (pulsing
 * nodes/edges) — the one place continuous motion earns its keep ("the
 * model is thinking"). No filter/backdrop-filter on anything that
 * animates infinitely (see this codebase's Chrome ghost-smear history);
 * no CSS transform on SVG shape elements. Everything here is aria-hidden,
 * fixed illustrative content — not a live model call. */

function PanelArrow() {
  return <div className="eng-panel-arrow" aria-hidden="true">→</div>
}

function FunnelIcon() {
  return (
    <svg className="eng-chips-funnel" viewBox="0 0 24 24" fill="none" stroke="var(--ink-3)" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 4h18l-7 9v6l-4 2v-8z" />
    </svg>
  )
}

const SCREEN_ROWS = [
  { label: 'Technicals', desc: 'RSI, moving averages, volatility', color: 'var(--blue-2)',
    icon: <><polyline points="2,15 6,9 10,12 15,4" /><circle cx="15" cy="4" r="1.4" /></> },
  { label: 'Price action', desc: 'Trend, volume', color: 'var(--green)',
    icon: <><rect x="2" y="9" width="3" height="6" /><rect x="7" y="5" width="3" height="10" /><rect x="12" y="2" width="3" height="13" /></> },
  { label: 'Fundamentals', desc: 'P/E, growth, debt', color: '#c084fc',
    icon: <><rect x="2" y="2" width="13" height="13" rx="1.2" /><line x1="5" y1="6" x2="12" y2="6" /><line x1="5" y1="9" x2="12" y2="9" /><line x1="5" y1="12" x2="9.5" y2="12" /></> },
  { label: 'Macro data', desc: 'GDP, interest rates', color: '#22d3ee',
    icon: <><circle cx="8.5" cy="8.5" r="6.5" /><ellipse cx="8.5" cy="8.5" rx="2.8" ry="6.5" /><line x1="2" y1="8.5" x2="15" y2="8.5" /></> },
  { label: 'News & sentiment', desc: 'AI analysis of headlines', color: 'var(--gold)',
    icon: <><path d="M2 3 h11 v9 h-6 l-3 3 v-3 h-2 z" /></> },
]

/* Scattered "pile of tickers" cluster — a handful of sharp, vivid chips
 * (depth "fore") mixed with dimmer, blurred ones (depth "back") sitting
 * behind/around them, evoking "there are 1,800+ of these, we're only
 * showing a few clearly." Static (no animation), so the blur here doesn't
 * combine with anything infinitely-animating — safe per this file's
 * ghost-smear rule. */
const SCREEN_CHIPS = [
  { t: 'MSFT', bg: '#e8eaf0', fg: '#111', x: 26, y: -8, r: -4, depth: 'back' },
  { t: 'NVDA', bg: 'var(--green)', fg: '#04150d', x: 54, y: 30, r: 3, depth: 'fore' },
  { t: 'WMT', bg: 'var(--blue-2)', fg: '#04121f', x: 56, y: 70, r: -3, depth: 'fore' },
  { t: 'AMZN', bg: '#e8eaf0', fg: '#111', x: 16, y: 108, r: 2, depth: 'fore' },
  { t: 'GOOGL', bg: '#c084fc', fg: '#1c0b2e', x: 60, y: 146, r: -2, depth: 'back' },
  { t: 'TSLA', bg: 'var(--red)', fg: '#fff', x: 6, y: 182, r: 4, depth: 'back' },
  { t: 'AMX', bg: 'var(--gold)', fg: '#241a02', x: 38, y: 208, r: -3, depth: 'fore' },
  { t: 'NVDA', bg: 'var(--green)', fg: '#04150d', x: 60, y: 240, r: 3, depth: 'back' },
  { t: 'WMT', bg: 'var(--blue-2)', fg: '#04121f', x: 58, y: 268, r: -2, depth: 'back' },
  { t: 'MSFT', bg: '#e8eaf0', fg: '#111', x: 18, y: 294, r: 2, depth: 'fore' },
]

function ChipsZone() {
  return (
    <div className="eng-chips-zone">
      <div className="eng-chips-cluster-wrap">
        <div className="eng-chips-cluster">
          {SCREEN_CHIPS.map((c, i) => (
            <span
              key={i}
              className={`eng-scr-chip ${c.depth}`}
              style={{ '--x': `${c.x}px`, '--y': `${c.y}px`, '--r': `${c.r}deg`, background: c.bg, color: c.fg }}
            >
              {c.t}
            </span>
          ))}
          <FunnelIcon />
        </div>
      </div>
      <div className="eng-panel-foot">Daily stock universe (1,800+)</div>
    </div>
  )
}

function ScreenPanel() {
  return (
    <div className="eng-panel">
      <div className="eng-panel-pillrow">
        <div className="eng-panel-head"><span className="n">01</span><span className="t">Screen</span></div>
      </div>
      <div className="eng-panel-card">
        <div className="eng-panel-body">
          <div className="eng-panel-sub">Daily stock universe (1,800+)</div>
          <div className="eng-scr-rows">
            {SCREEN_ROWS.map((r) => (
              <div className="eng-scr-row" key={r.label}>
                <span className="eng-scr-icon-badge" style={{ '--c': r.color }}>
                  <svg className="eng-scr-icon" viewBox="0 0 17 17" fill="none" stroke={r.color} strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
                    {r.icon}
                  </svg>
                </span>
                <div className="eng-scr-row-text">
                  <div className="eng-scr-row-label">{r.label}</div>
                  <div className="eng-scr-row-desc">{r.desc}</div>
                </div>
                <span className="eng-scr-row-stub" style={{ '--c': r.color }} />
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="eng-panel-foot">Criteria-based filtering</div>
      <PanelArrow />
    </div>
  )
}

const dist = (x1, y1, x2, y2) => Math.hypot(x2 - x1, y2 - y1)

/* Net geometry, scoped to just this panel's own small viewBox (not the
 * whole scene like before). Same node counts and all-to-all wiring as the
 * original diagram — only the coordinates shrank to fit one panel. */
const NET_VB_W = 460, NET_VB_H = 420
const NET_CY = 210, NET_ROW_GAP = 46
const NET_LAYER_X = { in: 90, hid: 230, out: 370 }
const netColumn = (x, count, r) =>
  Array.from({ length: count }, (_, i) => ({ x, y: NET_CY + (i - (count - 1) / 2) * NET_ROW_GAP, r }))
const NET_IN_NODES = netColumn(NET_LAYER_X.in, 6, 4.5)
const NET_HID_NODES = netColumn(NET_LAYER_X.hid, 7, 5.5)
const NET_OUT_NODES = netColumn(NET_LAYER_X.out, 5, 4.5)
const NET_ALL_NODES = [...NET_IN_NODES, ...NET_HID_NODES, ...NET_OUT_NODES]
const NET_EDGES = [
  ...NET_IN_NODES.flatMap((a, ai) => NET_HID_NODES.map((b, bi) => ({ id: `i${ai}h${bi}`, a, b }))),
  ...NET_HID_NODES.flatMap((a, ai) => NET_OUT_NODES.map((b, bi) => ({ id: `h${ai}o${bi}`, a, b }))),
]
/* Same curated pulse subset as the original diagram — edge IDs are
 * coordinate-independent, so this list carries over unchanged. */
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

/* Stub connectors at the net's own left/right edges, colored to echo the
 * neighboring panels' own colors (Screen's row colors on the left, Signal's
 * verdict colors on the right) — a contained, self-drawn approximation of
 * the reference's cross-panel flowing lines, without needing this panel to
 * know the other panels' actual DOM positions. */
const NET_IN_STUB_COLORS = ['var(--blue-2)', 'var(--green)', '#c084fc', '#22d3ee', 'var(--gold)', 'var(--blue-2)']
const NET_OUT_STUB_COLORS = ['var(--green)', 'var(--gold)', 'var(--red)', 'var(--green)', 'var(--gold)']

function ScorePanel() {
  return (
    <div className="eng-panel">
      <div className="eng-panel-pillrow">
        <div className="eng-panel-head"><span className="n">02</span><span className="t">Score</span></div>
      </div>
      <div className="eng-panel-card">
        <div className="eng-panel-body">
          <div className="eng-panel-sub">Deep learning model</div>
          <div className="eng-net-labels">
            <span>Feature engineering</span>
            <span>Pattern recognition</span>
          </div>
          <svg className="eng-net-svg" viewBox={`0 0 ${NET_VB_W} ${NET_VB_H}`} preserveAspectRatio="xMidYMid meet">
            <defs>
              <radialGradient id="netNodeGlow" cx="35%" cy="30%" r="75%">
                <stop offset="0%" style={{ stopColor: '#eafcff', stopOpacity: 1 }} />
                <stop offset="45%" style={{ stopColor: 'var(--blue-2)', stopOpacity: 0.95 }} />
                <stop offset="100%" style={{ stopColor: 'var(--blue-2)', stopOpacity: 0.35 }} />
              </radialGradient>
            </defs>
            {NET_IN_NODES.map((n, i) => (
              <line key={'stub-in-' + i} className="eng-net-stub" x1={0} y1={n.y} x2={n.x} y2={n.y} stroke={NET_IN_STUB_COLORS[i]} />
            ))}
            {NET_OUT_NODES.map((n, i) => (
              <line key={'stub-out-' + i} className="eng-net-stub" x1={n.x} y1={n.y} x2={NET_VB_W} y2={n.y} stroke={NET_OUT_STUB_COLORS[i]} />
            ))}
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
          </svg>
          <div className="eng-net-labels eng-net-labels-sub">
            <span>Feature extraction</span>
            <span>Multi-factor attribution</span>
          </div>
        </div>
      </div>
      <div className="eng-panel-foot">Data fusion &amp; advanced modeling</div>
      <PanelArrow />
    </div>
  )
}

const SIGNAL_ROWS = [
  { ticker: 'AAPL', verdict: 'buy' },
  { ticker: 'GOOGL', verdict: 'hold' },
  { ticker: 'TSLA', verdict: 'sell' },
  { ticker: 'NVDA', verdict: 'buy' },
  { ticker: 'AMZN', verdict: 'hold' },
]

function SignalPanel() {
  return (
    <div className="eng-panel">
      <div className="eng-panel-pillrow">
        <div className="eng-panel-head"><span className="n">03</span><span className="t">Signal</span></div>
      </div>
      <div className="eng-panel-card">
        <div className="eng-panel-body">
          <div className="eng-panel-sub">Daily BUY/HOLD/SELL scores</div>
          <div className="eng-sig-rows">
            {SIGNAL_ROWS.map((s) => (
              <div className={`eng-sig-row ${s.verdict}`} key={s.ticker}>
                <img className="eng-sig-logo" src={`/logos/${s.ticker}.png`} alt="" />
                <span className="eng-sig-tk">{s.ticker}</span>
                <span className="eng-sig-pill">{s.verdict.toUpperCase()}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="eng-panel-foot">Daily signal decision (BUY/HOLD/SELL)</div>
      <PanelArrow />
    </div>
  )
}

const TRACK_ALPHA = '0,110 30,100 60,105 90,80 120,85 150,60 180,65 210,40 240,45 270,20 300,25'
const TRACK_BENCH = '0,110 30,108 60,112 90,105 120,108 150,100 180,103 210,98 240,100 270,95 300,92'
const TRACK_MARKERS = [
  { x: 90, y: 80, kind: 'buy' },
  { x: 150, y: 60, kind: 'sell' },
  { x: 270, y: 20, kind: 'buy' },
]
const TRACK_STATS = [
  { k: 'Win Rate', v: '64%' },
  { k: 'Alpha Generation', v: '+11% vs. S&P' },
  { k: 'Sharpe Ratio', v: '1.7' },
]

function TrackPanel() {
  return (
    <div className="eng-panel">
      <div className="eng-panel-pillrow">
        <div className="eng-panel-head"><span className="n">04</span><span className="t">Track</span></div>
      </div>
      <div className="eng-panel-card">
        <div className="eng-panel-body">
          <div className="eng-panel-sub">Historical signal performance</div>
          <svg className="eng-trk-chart" viewBox="0 0 300 120" preserveAspectRatio="none">
            <defs>
              <linearGradient id="trkAlphaFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" style={{ stopColor: 'var(--green)', stopOpacity: 0.32 }} />
                <stop offset="100%" style={{ stopColor: 'var(--green)', stopOpacity: 0 }} />
              </linearGradient>
            </defs>
            <polygon points={`${TRACK_ALPHA} 300,120 0,120`} fill="url(#trkAlphaFill)" stroke="none" />
            <polyline points={TRACK_BENCH} fill="none" stroke="var(--ink-3)" strokeWidth="1.5" />
            <polyline points={TRACK_ALPHA} fill="none" stroke="var(--green)" strokeWidth="2" />
            {TRACK_MARKERS.map((m, i) => (
              <circle key={i} cx={m.x} cy={m.y} r="3.2" fill={m.kind === 'buy' ? 'var(--green)' : 'var(--red)'} />
            ))}
          </svg>
          <div className="eng-trk-legend">
            <span className="dot buy" /> Alpha &nbsp; <span className="dot bench" /> Benchmark
          </div>
          <div className="eng-trk-stats">
            {TRACK_STATS.map((s) => (
              <div className="eng-trk-stat" key={s.k}>
                <span className="k">{s.k}</span>
                <span className="v">{s.v}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="eng-panel-foot">Long-term accuracy &amp; alpha tracking</div>
    </div>
  )
}

function EngineScene() {
  return (
    <section className="ld-scene" id="how">
      <div className="ld-scene-head">
        <div className="tag">How it works</div>
        <h2>From signals to a <span className="g">signal.</span></h2>
        <p>Every signal is the result of many independent data feeds converging into one continuously-running model — not a bare score.</p>
      </div>
      <div className="eng-panels" aria-hidden="true">
        <ChipsZone />
        <ScreenPanel />
        <ScorePanel />
        <SignalPanel />
        <TrackPanel />
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

  // Which section the viewport's top edge currently sits inside — the
  // largest index whose own top has been reached. Deliberately NOT
  // landingScroll's nearestIndex (nearest-by-distance-to-any-top): once
  // scrolled more than halfway through an oversized section's overflow
  // (Proof/Platform/Pricing/FAQ/FinalCta have no min-height, so an expanded
  // FAQ answer can push one well past one viewport), that section's midpoint
  // can be closer to the NEXT section's top than to its own, mis-identifying
  // "current" as the section ahead. Harmless for a forward gesture (nextIndex
  // just clamps back to the same, already-last index), but for a backward
  // gesture it resolves to a real, different index — the true current
  // section's own top — triggering an erroneous hijack-jump that skips
  // exactly the unscrolled content this fix exists to protect. Containment
  // by top boundary has no such failure mode at any section height.
  const currentSectionIndex = (tops, scrollY) => {
    let idx = 0
    for (let i = 1; i < tops.length; i++) {
      if (tops[i] <= scrollY + 1) idx = i
      else break
    }
    return idx
  }

  // Only worth hijacking a gesture into an animated section-jump when BOTH:
  // (a) there's an actual next/previous section to jump to (not already at
  // the first/last section in that direction), AND (b) the viewport is
  // already at the edge of the CURRENT section's own content in that
  // direction. Otherwise return null so the caller lets the native
  // wheel/key scroll proceed instead — which reveals more of an oversized
  // section rather than skipping over its lower content, and lets a forward
  // gesture at the last section (FinalCta) fall through into <main>'s
  // sibling <LandingFooter>, which sectionTops() can't see since it only
  // reads main.children.
  const decideTarget = (direction) => {
    const tops = sectionTops()
    const sections = Array.from(main.children)
    const current = currentSectionIndex(tops, window.scrollY)
    const target = nextIndex(current, direction, tops.length)
    if (target === current) return null // first/last section, nothing further to jump to
    const bottom = tops[current] + sections[current].offsetHeight
    const atEdge = direction > 0
      ? (window.scrollY + window.innerHeight) >= bottom - 1
      : window.scrollY <= tops[current] + 1
    return atEdge ? tops[target] : null
  }

  // Returns whether the gesture was consumed (either it started an animated
  // jump, or an animation is already mid-flight and native scroll shouldn't
  // fight it) — callers use this to decide whether to preventDefault, so
  // wheel and keyboard share one hijack/pass-through decision.
  const go = (direction) => {
    if (animating) return true
    const targetTop = decideTarget(direction)
    if (targetTop == null) return false
    animateTo(targetTop)
    return true
  }

  const isTypingTarget = (el) =>
    !!el && (['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName) || el.isContentEditable)

  const onWheel = (e) => {
    if (e.ctrlKey) return // pinch-zoom gesture — leave it to the browser
    if (e.deltaY === 0) return // horizontal-only trackpad swipe — not a section gesture
    if (go(e.deltaY > 0 ? 1 : -1)) e.preventDefault()
  }
  const onKeyDown = (e) => {
    if (isTypingTarget(document.activeElement)) return
    if (e.key === 'PageDown' || e.key === 'ArrowDown') { if (go(1)) e.preventDefault() }
    else if (e.key === 'PageUp' || e.key === 'ArrowUp') { if (go(-1)) e.preventDefault() }
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
