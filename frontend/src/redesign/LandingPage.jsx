/* VeSign public landing page — standalone marketing page for logged-out visitors.
 * Renders OUTSIDE AppShell (own nav + footer), inside a `.rd` wrapper so the
 * scoped redesign tokens apply. Real data (win rate / trade count / etc.) comes
 * from the public /api/stats endpoint; the "Platform" section below reuses the
 * ACTUAL Signals/Market/Portfolio component CSS (signals.css, portfolio.css) so
 * those mockups are visually identical to the real product — only the ticker
 * data in them is a fixed illustrative example, not a live fetch. */
import { useEffect, useRef, useState } from 'react'
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

/* ── "Watch it think" reasoning scene ─────────────────────────────────────── */
/* TODO: this is a fixed illustrative example (aria-hidden), not a live model
 * call — wiring it to a real cached explanation needs getSignalExplanation()
 * confirmed reachable without auth first. */
const REASONING_SCRIPT = [
  {
    tk: 'NVDA',
    lines: [
      'Screening 1,842 US stocks…',
      'Technical — price reclaimed 50-day average, volume +38% vs. avg',
      'ML 5-day model — projects +6.2%',
      'Analyst consensus — target sits 18% above market',
      'Financial health — strong (4/5)',
    ],
    verdict: 'buy', verdictText: 'BUY · NVDA',
  },
  {
    tk: 'XOM',
    lines: [
      'Screening 1,842 US stocks…',
      'Technical — broke below the 20-day average on rising volume',
      'ML 5-day model — projects -3.1%',
      'Analyst consensus — 3 downgrades in the last 30 days',
      'Financial health — moderate (3/5)',
    ],
    verdict: 'sell', verdictText: 'SELL · XOM',
  },
]

function ReasoningTerminal() {
  const bodyRef = useRef(null)
  useEffect(() => {
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    const body = bodyRef.current
    if (!body || reduce) return
    let idx = 0, cancelled = false, timers = []
    const setT = (fn, ms) => { const id = setTimeout(fn, ms); timers.push(id); return id }

    function playEntry(entry) {
      if (cancelled) return
      body.innerHTML = ''
      const p = document.createElement('div')
      p.className = 'line prompt'
      p.innerHTML = `$ read <span class="tk">${entry.tk}</span><span class="cursor"></span>`
      body.appendChild(p)
      let li = 0
      function next() {
        if (cancelled) return
        if (li >= entry.lines.length) {
          setT(() => {
            if (cancelled) return
            const v = document.createElement('div')
            v.className = 'verdict ' + entry.verdict
            v.textContent = entry.verdictText
            body.appendChild(v)
            requestAnimationFrame(() => v.classList.add('show'))
            setT(() => { idx = (idx + 1) % REASONING_SCRIPT.length; playEntry(REASONING_SCRIPT[idx]) }, 3400)
          }, 250)
          return
        }
        const text = entry.lines[li]
        const div = document.createElement('div')
        div.className = 'line'
        div.innerHTML = text + '<span class="cursor"></span>'
        body.appendChild(div)
        setT(() => { li++; next() }, Math.min(1300, 480 + text.length * 6))
      }
      setT(next, 500)
    }

    let started = false
    let io
    if ('IntersectionObserver' in window) {
      io = new IntersectionObserver((entries) => {
        entries.forEach(e => { if (e.isIntersecting && !started) { started = true; playEntry(REASONING_SCRIPT[0]) } })
      }, { threshold: 0.4 })
      io.observe(body.closest('.ld-term'))
    } else {
      playEntry(REASONING_SCRIPT[0])
    }
    return () => { cancelled = true; timers.forEach(clearTimeout); io?.disconnect() }
  }, [])

  return (
    <div className="ld-term">
      <div className="ld-term-bar"><i /><i /><i /><span className="lbl">vesign — example read</span></div>
      <div className="ld-term-body" ref={bodyRef}>
        <div className="line prompt">$ read <span className="tk">NVDA</span></div>
        <div className="line k">Screening 1,842 US stocks…</div>
        <div className="line">Technical — price reclaimed 50-day average, volume +38% vs. avg</div>
        <div className="line">ML 5-day model — projects +6.2%</div>
        <div className="line">Analyst consensus — target sits 18% above market</div>
        <div className="line">Financial health — strong (4/5)</div>
        <div className="verdict show buy">BUY · NVDA</div>
      </div>
      <div className="ld-term-foot"><span>Example read — the real thing runs on every signal</span></div>
    </div>
  )
}

function ReasoningScene() {
  return (
    <section className="ld-scene">
      <div className="ld-scene-head">
        <div className="tag">Not a black box</div>
        <h2>Watch it think.</h2>
        <p>Every signal on the platform is published with reasoning like this attached — not a bare score.</p>
      </div>
      <ReasoningTerminal />
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

/* ── How it works — open flow, not cards ──────────────────────────────────── */
const STEPS = [
  {
    n: '01', t: 'Screen',
    d: 'Every trading day, 1,800+ US-listed stocks are re-scored after the close — prices, volumes, fundamentals, and analyst estimates refreshed.',
  },
  {
    n: '02', t: 'Score',
    d: 'Three independent reads per stock: technical indicators, a machine-learning price model, and analyst consensus — cross-checked against company financial health.',
  },
  {
    n: '03', t: 'Signal',
    d: 'Only when the evidence lines up does a BUY or SELL go out — with the “why” written in plain language, not a black-box score.',
  },
  {
    n: '04', t: 'Track',
    d: 'Every open position is stop-managed, and every closed trade — winner or loser — lands in the published results.',
  },
]

function HowItWorks() {
  return (
    <section className="ld-section" id="how">
      <div className="ld-section-head">
        <div className="tag">How a signal is made</div>
        <h2>Systematic. No discretion.</h2>
        <p>The same four steps run on every stock, every trading day — nothing hand-picked.</p>
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

/* ── Page ──────────────────────────────────────────────────────────────────── */
export default function LandingPage() {
  // Match the document canvas to the redesign --bg while mounted (same pattern
  // as AppShell) so overscroll never flashes the legacy background.
  useEffect(() => {
    const html = document.documentElement
    const prev = html.style.background
    html.style.background = '#0a0e15'
    return () => { html.style.background = prev }
  }, [])

  const stats = useStats()
  return (
    <div className="rd ld">
      <LandingNav />
      <main>
        <Hero />
        <ReasoningScene />
        <Proof stats={stats} />
        <HowItWorks />
        <PlatformShots />
        <Pricing />
        <Faq />
        <FinalCta />
      </main>
      <LandingFooter />
    </div>
  )
}
