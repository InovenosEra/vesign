/* VeSign public landing page — standalone marketing page for logged-out visitors.
 * Renders OUTSIDE AppShell (own nav + footer), inside a `.rd` wrapper so the
 * scoped redesign tokens apply. ALL stats / prices / screenshots are
 * PLACEHOLDERS, clearly marked with TODOs — nothing here calls the backend.
 */
import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import './redesign.css'
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
        <Link to="/performance">Performance</Link>
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

/* ── Hero ──────────────────────────────────────────────────────────────────── */
/* The mock signal card IS the pitch: a signal you can actually read.
   TODO: replace mock content with a real (anonymized) signal screenshot. */
function HeroSignalCard() {
  return (
    <div className="ld-hero-visual" aria-hidden="true">
      <div className="ld-mock-card back">
        <div className="ld-mock-head">
          <span className="ld-mock-logo">▦</span>
          <div className="ld-mock-id"><span className="tk">— — —</span><span className="co">Unlocked with Pro</span></div>
          <span className="ld-mock-pill sell">SELL</span>
        </div>
      </div>
      <div className="ld-mock-card">
        <div className="ld-mock-head">
          <span className="ld-mock-logo">NV</span>
          <div className="ld-mock-id"><span className="tk">NVDA</span><span className="co">NVIDIA Corporation</span></div>
          <span className="ld-mock-pill buy">BUY</span>
        </div>
        <p className="ld-mock-why">
          Price reclaimed the 50-day average on rising volume. The 5-day model
          projects +6.2%, analyst consensus sits 18% above the market, and
          financial health is strong. Three independent reads, one direction.
        </p>
        <div className="ld-mock-metrics">
          <div className="m"><span className="k">5D ML</span><span className="v up">+6.2%</span></div>
          <div className="m"><span className="k">Analyst upside</span><span className="v up">+18%</span></div>
          <div className="m"><span className="k">Health</span><span className="v dots"><i className="on" /><i className="on" /><i className="on" /><i className="on" /><i /></span></div>
        </div>
      </div>
    </div>
  )
}

function Hero({ stats }) {
  return (
    <section className="ld-hero" id="top">
      <div className="ld-hero-copy">
        <div className="ld-eyebrow">US equities · Research platform</div>
        <h1>Signals you can<br />actually read.</h1>
        <p className="ld-sub">
          VeSign screens 1,800+ US stocks every day and publishes BUY and SELL
          signals with the reasoning attached — technicals, machine learning,
          and analyst consensus, written in plain language. A full research
          platform built around them.
        </p>
        <div className="ld-hero-ctas">
          <Link to="/sign-up" className="ld-btn primary lg">Sign up free</Link>
          <Link to="/performance" className="ld-btn ghost lg">See the results</Link>
        </div>
        <div className="ld-hero-proof">
          {proofStats(stats).slice(0, 3).map(s => (
            <div className="ld-proof-stat" key={s.k}>
              <span className="v">{s.v}</span>
              <span className="k">{s.k}</span>
            </div>
          ))}
        </div>
        <p className="ld-verify-line">
          Results from running the model's strategy on historical data —
          every trade, winners and losers, is published.{' '}
          <Link to="/performance">See the full results →</Link>
        </p>
      </div>
      <HeroSignalCard />
    </section>
  )
}

/* ── Proof / model results ─────────────────────────────────────────────────── */
function Proof({ stats }) {
  return (
    <section className="ld-section" id="proof">
      <div className="ld-section-head">
        <h2>How the model has performed</h2>
        <p>
          These are the results our model produced on historical US market
          data — every trade it generated, winners and losers, with nothing
          cherry-picked. The full list and an equity curve against the
          S&amp;P 500 are public.
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
          Browse all {stats?.closed_trades ? fmtInt(stats.closed_trades) : ''} trades
        </Link>
      </div>
    </section>
  )
}

/* ── How it works ──────────────────────────────────────────────────────────── */
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
        <h2>How a signal is made</h2>
        <p>A systematic daily process. No discretion, no cherry-picking.</p>
      </div>
      <div className="ld-steps">
        {STEPS.map(s => (
          <div className="ld-step" key={s.n}>
            <div className="n">{s.n}</div>
            <div className="t">{s.t}</div>
            <p className="d">{s.d}</p>
          </div>
        ))}
      </div>
    </section>
  )
}

/* ── Screenshots ───────────────────────────────────────────────────────────── */
/* TODO: replace placeholder frames with real product screenshots. */
const SHOTS = [
  { t: 'Market overview', c: 'Indices, sectors, movers, breadth and news — the day at a glance.' },
  { t: 'Signals cockpit', c: 'Today’s BUYs and SELLs with the reasoning expanded inline.' },
  { t: 'Research & portfolio', c: 'Fundamentals, analyst targets and your holdings in one place.' },
]

function Screenshots() {
  return (
    <section className="ld-section" id="platform">
      <div className="ld-section-head">
        <h2>A full research platform around the signals</h2>
        <p>Market, Signals, Portfolio and Research — one consistent workspace.</p>
      </div>
      <div className="ld-shots">
        {SHOTS.map(s => (
          <figure className="ld-shot" key={s.t}>
            <div className="ld-shot-frame">
              <div className="ld-shot-bar"><i /><i /><i /></div>
              <div className="ld-shot-ph">Screenshot coming soon</div>
            </div>
            <figcaption>
              <span className="t">{s.t}</span>
              <span className="c">{s.c}</span>
            </figcaption>
          </figure>
        ))}
      </div>
    </section>
  )
}

/* ── Pricing ───────────────────────────────────────────────────────────────── */
function Pricing() {
  return (
    <section className="ld-section" id="pricing">
      <div className="ld-section-head">
        <h2>Pricing</h2>
        <p>Start free. Upgrade when the signals earn it.{' '}
          <span className="ld-placeholder-tag">Placeholder pricing — final tiers TBD</span>
        </p>
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
        <h2>Questions, answered straight</h2>
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
      <h2>Read tomorrow’s signals<br />with the reasoning attached.</h2>
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
        <Hero stats={stats} />
        <Proof stats={stats} />
        <HowItWorks />
        <Screenshots />
        <Pricing />
        <Faq />
        <FinalCta />
      </main>
      <LandingFooter />
    </div>
  )
}
