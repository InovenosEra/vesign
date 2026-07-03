/* VeSign public Performance page — the model's HISTORICAL results: headline
 * stats (/api/stats), equity curve vs SPY (/api/performance/equity-curve),
 * and the full closed-trade ledger (/api/performance/ledger). All three
 * endpoints are public/unauthenticated. Framing throughout: results produced
 * by running the model's strategy on historical data — never presented as
 * live-traded or real-money returns. */
import { useEffect, useMemo, useRef, useState } from 'react'
import { LandingNav, LandingFooter } from './LandingPage'
import './redesign.css'
import './landing.css'
import './performance.css'

const fmtInt = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'))
const fmtPct = (n, dp = 1) => (n == null ? '—' : `${n >= 0 ? '+' : ''}${Number(n).toFixed(dp)}%`)
const fmtPrice = (n) => (n == null ? '—' : `$${Number(n).toFixed(2)}`)
const fmtDay = (s) => (s ? String(s).slice(0, 10) : '—')

function useJson(url) {
  const [data, setData] = useState(null)
  useEffect(() => {
    let alive = true
    fetch(url)
      .then(r => (r.ok ? r.json() : null))
      .then(d => { if (alive) setData(d) })
      .catch(() => {})
    return () => { alive = false }
  }, [url])
  return data
}

/* ── Headline stats ─────────────────────────────────────────────────────── */
function StatsStrip({ stats }) {
  const cells = [
    { k: 'Closed trades', v: fmtInt(stats?.closed_trades) },
    { k: 'Win rate', v: stats?.win_rate == null ? '—' : `${stats.win_rate}%` },
    { k: 'Avg return per trade', v: stats?.avg_yield == null ? '—' : fmtPct(stats.avg_yield) },
    { k: 'Avg holding period', v: stats?.avg_hold_days == null ? '—' : `${stats.avg_hold_days} days` },
    { k: 'US stocks tracked', v: fmtInt(stats?.tickers_tracked) },
  ]
  return (
    <div className="pf-stats">
      {cells.map(c => (
        <div className="pf-stat" key={c.k}>
          <div className="v">{c.v}</div>
          <div className="k">{c.k}</div>
        </div>
      ))}
    </div>
  )
}

/* ── Equity curve (model vs SPY), inline SVG in the redesign chart style ──── */
const W = 920, H = 320

function EquityCurve({ curve }) {
  const svgRef = useRef(null)
  const [hover, setHover] = useState(null)
  const pts = curve?.points ?? []
  const N = pts.length

  const geom = useMemo(() => {
    if (N < 2) return null
    const vals = pts.flatMap(p => [p.model, p.spy]).filter(v => v != null)
    const maxV = Math.ceil((Math.max(...vals) * 1.06) / 25) * 25
    const minV = Math.floor(Math.min(0, ...vals) / 25) * 25
    const span = (maxV - minV) || 1
    const xFor = (i) => (i / (N - 1)) * W
    const yFor = (v) => H - ((v - minV) / span) * H
    const line = (key) => pts
      .map((p, i) => (p[key] == null ? null : `${xFor(i).toFixed(1)},${yFor(p[key]).toFixed(1)}`))
      .filter(Boolean).join(' ')
    const model = line('model')
    const area = model
      ? `M ${model.split(' ').join(' L ')} L ${W},${H} L 0,${H} Z`
      : ''
    // x labels: one per calendar year boundary
    const years = []
    let prev = ''
    pts.forEach((p, i) => {
      const y = p.date.slice(0, 4)
      if (y !== prev) { years.push({ x: xFor(i), label: y }); prev = y }
    })
    return { maxV, minV, span, xFor, yFor, model, spy: line('spy'), area, years }
  }, [pts, N])

  const onMove = (e) => {
    if (!geom || !svgRef.current) return
    const rect = svgRef.current.getBoundingClientRect()
    const frac = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width))
    setHover(Math.round(frac * (N - 1)))
  }

  if (!geom) return <div className="pf-chart-empty">Loading curve…</div>
  const hp = hover != null ? pts[hover] : null
  const hx = hover != null ? geom.xFor(hover) : 0
  const yTicks = [0, 0.25, 0.5, 0.75, 1].map(f => geom.minV + f * geom.span)

  return (
    <div className="pf-chart">
      <div className="pf-chart-head">
        <div className="pf-legend">
          <span className="pf-leg model"><i />Model strategy (historical)</span>
          <span className="pf-leg spy"><i />SPY buy &amp; hold</span>
        </div>
        {hp && (
          <div className="pf-hover-vals">
            <span className="d">{fmtDay(hp.date)}</span>
            <span className="m">{fmtPct(hp.model)}</span>
            <span className="s">{fmtPct(hp.spy)}</span>
          </div>
        )}
      </div>
      <div className="pf-chart-body">
        <div className="pf-yaxis">
          {[...yTicks].reverse().map(v => <span key={v}>{Math.round(v)}%</span>)}
        </div>
        <svg
          ref={svgRef}
          viewBox={`0 0 ${W} ${H}`}
          preserveAspectRatio="none"
          onMouseMove={onMove}
          onMouseLeave={() => setHover(null)}
        >
          {yTicks.map(v => (
            <line key={v} x1="0" x2={W} y1={geom.yFor(v)} y2={geom.yFor(v)}
              stroke="rgba(255,255,255,0.06)" strokeWidth="1" />
          ))}
          <line x1="0" x2={W} y1={geom.yFor(0)} y2={geom.yFor(0)}
            stroke="rgba(255,255,255,0.16)" strokeWidth="1" strokeDasharray="4 4" />
          <path d={geom.area} fill="rgba(0,217,126,0.07)" />
          <polyline points={geom.spy} fill="none" stroke="#60a5fa" strokeWidth="1.6" />
          <polyline points={geom.model} fill="none" stroke="#00d97e" strokeWidth="2" />
          {hp && (
            <g>
              <line x1={hx} x2={hx} y1="0" y2={H} stroke="rgba(255,255,255,0.25)" strokeWidth="1" />
              {hp.model != null && <circle cx={hx} cy={geom.yFor(hp.model)} r="3.5" fill="#00d97e" />}
              {hp.spy != null && <circle cx={hx} cy={geom.yFor(hp.spy)} r="3.5" fill="#60a5fa" />}
            </g>
          )}
        </svg>
      </div>
      <div className="pf-xaxis">
        {geom.years.map(y => (
          <span key={y.label} style={{ left: `${(y.x / W) * 100}%` }}>{y.label}</span>
        ))}
      </div>
      <p className="pf-method">
        Model series: $1,000 allocated to each BUY signal the model generated,
        with sale proceeds recycled into later signals; each point is the
        running yield — portfolio equity (cash plus market value of open
        positions) relative to total capital deployed up to that date.
        Benchmark: SPY buy-and-hold over the same period, normalized to the
        same start. Results come from running the model's strategy on
        historical data; they are not live-traded or real-money returns.
      </p>
    </div>
  )
}

/* ── Full ledger table — sortable, paginated client-side ──────────────────── */
const COLS = [
  { key: 'ticker', label: 'Ticker' },
  { key: 'buy_date', label: 'Entry date' },
  { key: 'sell_date', label: 'Exit date' },
  { key: 'buy_price', label: 'Entry price', num: true },
  { key: 'sell_price', label: 'Exit price', num: true },
  { key: 'return_pct', label: 'Return', num: true },
]
const PAGE = 100

function Ledger({ ledger }) {
  const [sort, setSort] = useState({ key: 'sell_date', dir: 'desc' })
  const [page, setPage] = useState(0)
  const trades = ledger?.trades ?? []

  const sorted = useMemo(() => {
    const arr = [...trades]
    const { key, dir } = sort
    const mul = dir === 'asc' ? 1 : -1
    arr.sort((a, b) => {
      const av = a[key], bv = b[key]
      if (av == null) return 1
      if (bv == null) return -1
      return (av < bv ? -1 : av > bv ? 1 : 0) * mul
    })
    return arr
  }, [trades, sort])

  const pages = Math.max(1, Math.ceil(sorted.length / PAGE))
  const view = sorted.slice(page * PAGE, (page + 1) * PAGE)

  const clickSort = (key) => {
    setPage(0)
    setSort(s => (s.key === key ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'desc' }))
  }

  return (
    <div className="pf-ledger">
      <table>
        <thead>
          <tr>
            {COLS.map(c => (
              <th key={c.key} className={c.num ? 'num' : ''} onClick={() => clickSort(c.key)}>
                {c.label}
                {sort.key === c.key && <span className="arrow">{sort.dir === 'asc' ? '▲' : '▼'}</span>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {view.map((t, i) => (
            <tr key={`${t.ticker}-${t.buy_date}-${t.sell_date}-${i}`}>
              <td className="tk">
                <img className="logo-mini" src={`/logos/${t.ticker}.png`} alt=""
                  onError={e => { e.currentTarget.style.visibility = 'hidden' }} />
                {t.ticker}
              </td>
              <td>{fmtDay(t.buy_date)}</td>
              <td>{fmtDay(t.sell_date)}</td>
              <td className="num">{fmtPrice(t.buy_price)}</td>
              <td className="num">{fmtPrice(t.sell_price)}</td>
              <td className={'num ret ' + (t.return_pct >= 0 ? 'up' : 'down')}>
                {t.return_pct == null ? '—' : fmtPct(t.return_pct * 100)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="pf-pager">
        <span className="count">
          {fmtInt(sorted.length)} trades · page {page + 1} of {pages}
        </span>
        <div className="btns">
          <button className="ld-btn ghost" disabled={page === 0} onClick={() => setPage(p => p - 1)}>← Prev</button>
          <button className="ld-btn ghost" disabled={page >= pages - 1} onClick={() => setPage(p => p + 1)}>Next →</button>
        </div>
      </div>
    </div>
  )
}

/* ── Page ──────────────────────────────────────────────────────────────────── */
export default function PerformancePage() {
  const stats = useJson('/api/stats')
  const curve = useJson('/api/performance/equity-curve')
  const ledger = useJson('/api/performance/ledger?limit=5000')

  useEffect(() => {
    const html = document.documentElement
    const prev = html.style.background
    html.style.background = '#000000'
    return () => { html.style.background = prev }
  }, [])

  return (
    <div className="rd ld pf">
      <LandingNav />
      <main>
        <section className="ld-section pf-head-section">
          <div className="ld-section-head">
            <h2>Model performance</h2>
            <p>
              How the model's strategy performed on historical US market data
              {curve?.start ? ` from ${curve.start.slice(0, 4)} to today` : ''} —
              the headline numbers, the equity curve against the S&amp;P 500,
              and every individual trade the model generated, winners and
              losers alike. Past performance does not guarantee future results.
            </p>
          </div>
          <StatsStrip stats={stats} />
        </section>

        <section className="ld-section">
          <div className="ld-section-head">
            <h2>Equity curve vs the S&amp;P 500</h2>
          </div>
          <EquityCurve curve={curve} />
        </section>

        <section className="ld-section">
          <div className="ld-section-head">
            <h2>Every trade the model generated</h2>
            <p>
              The complete list of closed trades from the model's historical
              run — nothing filtered, losers included. Click a column to sort.
            </p>
          </div>
          <Ledger ledger={ledger} />
        </section>
      </main>
      <LandingFooter />
    </div>
  )
}
