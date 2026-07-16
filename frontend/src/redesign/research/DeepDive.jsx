/* Deep-dive tab — per-ticker detail. Ported from research-v1.html's
 * loadDeepDive()/paintPriceChart() inline JS.
 *
 * Sections: search bar (+ recent pills, live suggestions via searchTickers),
 * hero, price chart (inline SVG, same path math as the mockup) + verdict,
 * fundamentals grid, analyst targets + ML predictions two-up, signal history,
 * recent news.
 *
 * Data: getResearch, getPriceHistory, getEarnings, getNews, getSignalMarkers,
 * searchTickers. */
import { useState, useEffect, useMemo, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getResearch, getPriceHistory, getEarnings, getNews, getSignalMarkers, searchTickers,
  getWatchlists, getWatchlistTickers, addTicker, removeTicker, generateAIReport, WHITE_BG_LOGOS } from '../../api'
import { num, pct, dateFmt, ago, LOGO } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'
import { useMe } from '../../context/MeContext'

const LockGlyph = ({ size = 11 }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4"
    strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <rect x="4.5" y="11" width="15" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" />
  </svg>
)

/* Points outward from the analyst-range bar toward an off-range current price. */
const OffRangeArrow = ({ dir, size = 12 }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.6"
    strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d={dir === 'left' ? 'M15 6l-6 6 6 6' : 'M9 6l6 6-6 6'} />
  </svg>
)

const sigCls = (s) => ({ BUY: 'buy', SELL: 'sell' }[s] || 'hold')
const dirCls = (v) => v == null ? '' : v > 0 ? 'up' : v < 0 ? 'down' : 'muted'
const capB = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'
// margins/roe/growth come back as raw fractions
const pctFrac = (f, fd = 1) => f == null ? '—' : (f * 100).toFixed(fd) + '%'
const RANGES = [['1M', 1], ['3M', 3], ['6M', 6], ['1Y', 12], ['5Y', 60], ['ALL', 600]]
const RECENT = ['NVDA', 'META', 'MTD', 'MU', 'AAPL']
const HEALTH_WORD = { 1: 'weak', 2: 'weak', 3: 'fair', 4: 'strong', 5: 'strong' }

/* Small directional glyph for the verdict band's signal tag — same inline-SVG,
 * stroke-based style as LockGlyph, no new icon set. */
const SignalGlyph = ({ signal, size = 12 }) => {
  const common = { width: size, height: size, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2.4, strokeLinecap: 'round', strokeLinejoin: 'round', 'aria-hidden': true }
  if (signal === 'BUY') return <svg {...common}><path d="M12 19V5M5 12l7-7 7 7" /></svg>
  if (signal === 'SELL') return <svg {...common}><path d="M12 5v14M5 12l7 7 7-7" /></svg>
  return <svg {...common}><path d="M5 12h14" /></svg>
}

/* One-line, no-LLM synthesis for the verdict band — assembled from fields
 * already fetched via getResearch, so it's free and instant (unlike the AI
 * Report below, which is a real Anthropic call and stays a deliberate click).
 * Each fragment is independently omitted when its data is null, so the
 * sentence degrades gracefully instead of showing a broken half-thought.
 * Numbers render as their own colored/mono spans — same tabular-nums
 * treatment as every other stat on this page — instead of one flat run of text. */
function verdictFragments({ up, mean, fmtPrice, health, ml, modelLocked }) {
  const out = []
  if (up != null && mean != null) {
    out.push(
      <span key="up">Analysts see <b className={'vb-num ' + dirCls(up)}>{pct(up)}</b> {up >= 0 ? 'upside' : 'downside'} to the <b className="vb-num">{fmtPrice(mean)}</b> average target.</span>,
    )
  }
  if (!modelLocked && health != null) {
    const n = Math.max(1, Math.min(5, health))
    const cls = n >= 4 ? 'up' : n <= 2 ? 'down' : ''
    out.push(<span key="health">Health <b className={'vb-num ' + cls}>{HEALTH_WORD[n]} ({health}/5)</b>.</span>)
  }
  if (!modelLocked && ml != null) {
    const lean = ml > 0 ? 'bullish' : ml < 0 ? 'bearish' : 'flat'
    out.push(<span key="ml">ML leans <b className={'vb-num ' + dirCls(ml)}>{lean} ({pct(ml)} 5-day)</b>.</span>)
  }
  return out
}

const _day = (d) => String(d || '').slice(0, 10)

/* The AI report backend returns one prose blob with literal **bold** markers
 * around its 3 section headings (Current Situation / Key Risks /
 * Recommendation), a redundant leading "# TICKER — Company | Price | Signal"
 * title (already shown in the hero above), "---" dividers, and — per
 * backend/main.py:3704's prompt template — a closing line the model echoes
 * back verbatim: "Vesign Score: N/100 | win rate ... | avg return ...". That
 * raw 0-100 composite must never reach the UI (feedback_vqs_internal_only —
 * enforced everywhere else in the redesign), so it's stripped here rather
 * than rendered. The legacy page shows this text completely raw (pre-wrap,
 * asterisks/dashes/score and all); this renders it properly instead. */
function renderReport(text) {
  if (!text) return null
  return text.split(/\n{2,}/)
    .map(b => b.trim())
    .filter(b => b && !/^-{3,}$/.test(b) && !/^#\s/.test(b) && !/Vesign Score:/i.test(b))
    .map((block, i) => (
      <p className="dd-ai-p" key={i}>
        {block.split(/(\*\*[^*]+\*\*)/g).filter(Boolean).map((part, j) =>
          part.startsWith('**') && part.endsWith('**')
            ? <strong key={j}>{part.slice(2, -2)}</strong>
            : part)}
      </p>
    ))
}

/* Inline-SVG price line — identical path math to the mockup's paintPriceChart.
 * BUY/SELL signal markers are plotted on the line (the legend promises them).
 * Hover crosshair + tooltip added on top — the chart previously had zero
 * interactivity beyond the range chips, unlike every other price chart in
 * the app (Holdings' performance chart already does hover+tooltip). */
function PriceChart({ history, markers, fmtPrice }) {
  const W = 800, H = 340
  const svgRef = useRef(null)
  const [hoverIdx, setHoverIdx] = useState(null)
  const out = useMemo(() => {
    if (!Array.isArray(history) || history.length < 2) return null
    const closes = history.map(p => p.close)
    const min = Math.min(...closes), max = Math.max(...closes), span = (max - min) || 1
    const xFor = (i) => i / (history.length - 1) * W
    const yFor = (v) => H - ((v - min) / span) * H
    const ptsArr = history.map((p, i) => `${xFor(i).toFixed(1)},${yFor(p.close).toFixed(1)}`)
    const pts = ptsArr.join(' ')
    const fill = `M ${ptsArr.join(' L ')} L ${W},${H} L 0,${H} Z`
    const [dx, dy] = ptsArr[ptsArr.length - 1].split(',')
    // Convert through the selected currency — every other price on this page
    // (hero, analyst targets, EPS) does too; the axis was the one holdout
    // still hardcoding '$' + raw USD, so it went stale/wrong under ILS/EUR.
    const yLabels = [0, 1, 2, 3, 4].map(i => fmtPrice(max - i / 4 * span, 0))
    const N = history.length
    const xLabels = [0, 1, 2, 3, 4, 5, 6].map(i => {
      const dt = new Date(history[Math.round(i / 6 * (N - 1))].date)
      return dt.toLocaleDateString(undefined, { month: 'short' })
    })
    // Map each marker to the nearest in-range price point.
    const lo = _day(history[0].date), hiD = _day(history[N - 1].date)
    const marks = (Array.isArray(markers) ? markers : []).map(m => {
      const md = _day(m.date)
      if (md < lo || md > hiD) return null
      let idx = history.findIndex(p => _day(p.date) === md)
      if (idx < 0) {
        const mt = new Date(md).getTime()
        let best = -1, bestD = Infinity
        history.forEach((p, i) => { const dd = Math.abs(new Date(_day(p.date)).getTime() - mt); if (dd < bestD) { bestD = dd; best = i } })
        idx = best
      }
      if (idx < 0) return null
      return { x: xFor(idx), y: yFor(history[idx].close), buy: (m.signal || '').toUpperCase() === 'BUY' }
    }).filter(Boolean)
    return { pts, fill, dx, dy, yLabels, xLabels, marks, min, span, n: N }
  }, [history, markers, fmtPrice])

  const hover = (out && hoverIdx != null && history[hoverIdx]) ? (() => {
    const p = history[hoverIdx]
    const x = hoverIdx / (out.n - 1) * W
    const y = H - ((p.close - out.min) / out.span) * H
    return { x, y, date: p.date, close: p.close }
  })() : null

  const handleMove = (e) => {
    if (!out || !svgRef.current) return
    const rect = svgRef.current.getBoundingClientRect()
    const frac = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width))
    setHoverIdx(Math.round(frac * (out.n - 1)))
  }

  return (
    <>
      <div className="dd-chart-body" onMouseLeave={() => setHoverIdx(null)}>
        <div className="dd-y-axis" style={{ left: 18, right: 'auto' }}>
          {(out?.yLabels || ['—', '—', '—', '—', '—']).map((l, i) => <span key={i}>{l}</span>)}
        </div>
        <svg ref={svgRef} viewBox="0 0 800 340" preserveAspectRatio="none"
          onMouseMove={handleMove} style={{ cursor: out ? 'crosshair' : 'default' }}>
          <defs>
            <linearGradient id="dd-nvda-grad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#60a5fa" stopOpacity="0.20" />
              <stop offset="100%" stopColor="#60a5fa" stopOpacity="0" />
            </linearGradient>
            <filter id="dd-glow"><feGaussianBlur stdDeviation="3" /></filter>
          </defs>
          <line x1="0" x2="800" y1="68" y2="68" stroke="rgba(255,255,255,0.04)" />
          <line x1="0" x2="800" y1="136" y2="136" stroke="rgba(255,255,255,0.04)" />
          <line x1="0" x2="800" y1="204" y2="204" stroke="rgba(255,255,255,0.04)" />
          <line x1="0" x2="800" y1="272" y2="272" stroke="rgba(255,255,255,0.04)" />
          <path d={out?.fill || ''} fill="url(#dd-nvda-grad)" />
          <polyline fill="none" stroke="#60a5fa" strokeWidth="2" strokeLinejoin="round" points={out?.pts || ''} />
          {(out?.marks || []).map((m, i) => (
            <polygon key={i}
              points={m.buy
                ? `${m.x},${m.y + 13} ${m.x - 5},${m.y + 22} ${m.x + 5},${m.y + 22}`
                : `${m.x},${m.y - 13} ${m.x - 5},${m.y - 22} ${m.x + 5},${m.y - 22}`}
              fill={m.buy ? '#00d97e' : '#ff4d5c'} stroke="#0b0e14" strokeWidth="1" />
          ))}
          {out && <circle cx={out.dx} cy={out.dy} r="7" fill="#60a5fa" opacity="0.35" filter="url(#dd-glow)" />}
          {out && <circle cx={out.dx} cy={out.dy} r="4" fill="#60a5fa" />}
          {hover && <line x1={hover.x} x2={hover.x} y1="0" y2={H} stroke="rgba(255,255,255,0.18)" strokeDasharray="3,4" />}
          {hover && <circle cx={hover.x} cy={hover.y} r="5" fill="#0b0e14" stroke="#60a5fa" strokeWidth="2" />}
        </svg>
        {hover && (
          <div className="dd-chart-tooltip" style={{ left: Math.max(8, Math.min(92, hover.x / W * 100)) + '%' }}>
            <div className="d">{dateFmt(hover.date)}</div>
            <div className="p">{fmtPrice(hover.close)}</div>
          </div>
        )}
      </div>
      <div className="dd-x-axis">
        {(out?.xLabels || []).map((l, i) => <span key={i}>{l}</span>)}
      </div>
    </>
  )
}

export default function DeepDive({ ticker, setTicker }) {
  const { fmtPrice, symbol } = useCurrency()
  const me = useMe()
  const navigate = useNavigate()
  // Vesign-model fields (signal/health/ML) are Pro+; server nulls them for Free.
  const modelLocked = me.plan !== 'pro' && me.plan !== 'max'
  const [range, setRange] = useState('1Y')   // active chart-range chip label
  const months = RANGES.find(([l]) => l === range)?.[1] || 12
  const [input, setInput] = useState(ticker)
  const [sugQ, setSugQ] = useState('')
  // DeepDive stays mounted across ticker changes (route is /research/:ticker?,
  // never remounted, so tab-switching doesn't reset scroll/state) — external
  // navigation (e.g. SignalModalRd's "Open full research", NewsReader) changes
  // the `ticker` prop directly without going through this component's own
  // search box, so the box needs an explicit resync or it shows the old ticker
  // and can navigate back to it if the user hits Enter.
  useEffect(() => { setInput(ticker) }, [ticker])

  // AI Research Report — on-demand (not cached like the Signals explainer),
  // Pro+/Max gated same as the rest of the Vesign-model sections on this page.
  const [aiReport, setAiReport] = useState(null)
  const [aiError, setAiError] = useState(null)
  const [aiLoading, setAiLoading] = useState(false)
  const [entryPrice, setEntryPrice] = useState('')
  useEffect(() => { setAiReport(null); setAiError(null); setEntryPrice('') }, [ticker])
  const generateReport = async () => {
    setAiLoading(true); setAiError(null); setAiReport(null)
    try {
      const data = await generateAIReport(ticker, entryPrice ? parseFloat(entryPrice) : null)
      setAiReport(data.report)
    } catch (e) {
      setAiError(e.message || 'Failed to generate report')
    } finally {
      setAiLoading(false)
    }
  }

  const { data: r } = useQuery({ queryKey: ['research', ticker], queryFn: () => getResearch(ticker), enabled: !!ticker, refetchInterval: 20_000 })
  const { data: history } = useQuery({
    queryKey: ['price-history', ticker, months],
    queryFn: () => getPriceHistory(ticker, rangeBounds(months)),
    enabled: !!ticker,
  })
  const { data: earnings } = useQuery({ queryKey: ['earnings', ticker], queryFn: () => getEarnings(ticker), enabled: !!ticker })
  const { data: news } = useQuery({ queryKey: ['news', ticker], queryFn: () => getNews(ticker), enabled: !!ticker })
  const { data: markers } = useQuery({ queryKey: ['markers', ticker], queryFn: () => getSignalMarkers(ticker, 60), enabled: !!ticker })

  // Live search suggestions for the deep-dive input.
  const { data: suggestions } = useQuery({
    queryKey: ['dd-search', sugQ], queryFn: () => searchTickers(sugQ, 6), enabled: sugQ.trim().length >= 1,
  })

  // Watchlist membership — powers the "In your watchlists" cell + the Watchlist button.
  const qc = useQueryClient()
  const { data: watchlists } = useQuery({ queryKey: ['dd-watchlists'], queryFn: getWatchlists })
  const wlIds = (watchlists || []).map(w => w.id).join(',')
  const { data: membership } = useQuery({
    queryKey: ['dd-membership', ticker, wlIds],
    enabled: !!ticker && Array.isArray(watchlists) && watchlists.length > 0,
    queryFn: async () => Promise.all((watchlists || []).map(async w => {
      const ts = await getWatchlistTickers(w.id).catch(() => [])
      const has = Array.isArray(ts) && ts.some(x => (x.ticker || x) === ticker)
      return { id: w.id, name: w.name, has }
    })),
  })
  const memberOf = (membership || []).filter(m => m.has)
  const firstList = (watchlists || [])[0]
  const inFirst = !!(membership || []).find(m => m.id === firstList?.id)?.has
  const toggleWatch = useMutation({
    mutationFn: async () => {
      if (!firstList) return
      if (inFirst) await removeTicker(firstList.id, ticker)
      else await addTicker(firstList.id, ticker)
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ['dd-membership'] }),
  })

  const submit = (t) => {
    const v = (t || '').trim().toUpperCase()
    if (!v) return
    setTicker(v); setInput(v); setSugQ('')
  }

  const close = r?.close
  const dayChange = r?.day_change_pct ?? null
  const ml = r?.prediction_score == null ? null : r.prediction_score * 100
  const up = r?.fair_value_upside == null ? null : r.fair_value_upside * 100
  const lo = r?.target_low_price, mean = r?.target_mean_price, hi = r?.target_high_price
  const posFn = (lo != null && hi != null && hi > lo) ? (v) => Math.max(0, Math.min(100, (v - lo) / (hi - lo) * 100)) : null
  // Current price isn't guaranteed to fall inside the analyst Low–High range —
  // a stock can trade below every analyst's target or above all of them.
  // Clamping it onto the bar (the earlier approach) drew the marker exactly on
  // top of Low/High, silently claiming "current == low" when it's actually
  // lower — a real misrepresentation, not just a layout bug. rawCurPos is
  // unclamped so we can detect that and render it honestly instead: an
  // off-range badge beside the bar rather than a marker glued to an edge it
  // doesn't actually sit at.
  const rawCurPos = (lo != null && hi != null && hi > lo && close != null) ? (close - lo) / (hi - lo) * 100 : null
  const curBelowRange = rawCurPos != null && rawCurPos < 0
  const curAboveRange = rawCurPos != null && rawCurPos > 100
  const curPos = (rawCurPos != null && !curBelowRange && !curAboveRange) ? rawCurPos : null
  const meanPos = (posFn && mean != null) ? posFn(mean) : null
  // Current/Mean labels sit directly under their own marker's real position on
  // the range (not evenly spaced in a row) — a value near the low end had its
  // marker rendered on the left while its label sat in a fixed middle slot,
  // so the two had no visible connection. Stagger the labels vertically when
  // the two markers land close enough together that the labels would collide.
  // (Only applies when Current is actually on the bar — the off-range badge
  // lives beside the bar entirely, so it can't collide with Mean's label.)
  const labelsClose = curPos != null && meanPos != null && Math.abs(curPos - meanPos) < 14
  // The marker itself is fine sitting exactly at 0%/100% — it's anchored by
  // its left edge, not centered, so it never overflows. Its label IS centered
  // on that same x (translateX(-50%) to sit under the line), so right at the
  // edge, half the label's text would fall outside the card and get clipped.
  // Clamp label position only, independently of where the marker itself sits.
  const clampLabelPos = (p) => p == null ? null : Math.max(9, Math.min(91, p))
  // Upside to the high target — the analyst range's own bullish edge, not
  // just the average. Same live `close` baseline as `up` (fair_value_upside).
  const bestCase = (close != null && hi != null && close > 0) ? (hi - close) / close * 100 : null
  const nextEarn = Array.isArray(earnings) && earnings.length ? earnings[0] : null
  const vbFragments = useMemo(() => verdictFragments({ up, mean, fmtPrice, health: r?.health_score, ml, modelLocked }),
    [up, mean, fmtPrice, r?.health_score, ml, modelLocked])

  // Signal history from markers (BUY/SELL events). Mockup showed static rows;
  // we render real markers when available, newest first.
  const histRows = Array.isArray(markers)
    ? markers.slice().sort((a, b) => (a.date < b.date ? 1 : -1)).slice(0, 8)
    : []

  return (
    <div className="dd-body">
      {/* SEARCH BAR + recent */}
      <div className="dd-search-bar">
        <div style={{ flex: 1, position: 'relative' }}>
          {/* contain:paint on this wrapper only (not the dropdown below) — same fix as
              .wl-toolbar, see project_focus_triggered_color_glitch memory. */}
          <div style={{ contain: 'paint' }}>
            <input className="ticker-input" type="text" placeholder="Type a ticker — e.g. AAPL, MSFT, GOOGL…"
              value={input}
              onChange={(e) => { setInput(e.target.value); setSugQ(e.target.value) }}
              onKeyDown={(e) => { if (e.key === 'Enter') submit(input) }}
              style={{ width: '100%' }} />
          </div>
          {sugQ.trim() && Array.isArray(suggestions) && suggestions.length > 0 && (
            <div style={{
              position: 'absolute', top: 40, left: 0, right: 0, zIndex: 20,
              background: 'var(--bg-3)', border: '1px solid var(--line-2)', borderRadius: 6,
              overflow: 'hidden', maxHeight: 260, overflowY: 'auto',
            }}>
              {suggestions.map(s => (
                <div key={s.ticker} onClick={() => submit(s.ticker)}
                  style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px', cursor: 'pointer', fontSize: 12.5 }}
                  onMouseDown={(e) => e.preventDefault()}>
                  <img src={LOGO(s.ticker)} alt="" style={{ width: 18, height: 18, borderRadius: 4, objectFit: 'contain' }} />
                  <span style={{ fontFamily: 'var(--mono)', fontWeight: 600, color: 'var(--ink)' }}>{s.ticker}</span>
                  <span style={{ color: 'var(--ink-3)' }}>{s.company || ''}</span>
                </div>
              ))}
            </div>
          )}
        </div>
        <div className="recent">
          {RECENT.map(t => (
            <span key={t} className={'r-pill' + (t === ticker ? ' active' : '')} onClick={() => submit(t)}>
              <img src={LOGO(t)} alt="" /> {t}
            </span>
          ))}
        </div>
      </div>

      {/* HERO */}
      <div className="dd-hero">
        <div className="dd-hero-left">
          <img className={'dd-hero-logo' + (WHITE_BG_LOGOS.has(ticker) ? ' white-bg' : '')} src={LOGO(ticker)} alt={ticker} />
          <div className="dd-hero-id">
            <div className="tk">{r?.ticker || ticker}</div>
            <div className="co">{r?.company || '—'}</div>
            <div className="meta">
              <span className="pill">{[r?.sector, r?.industry].filter(Boolean).join(' · ') || '—'}</span>
              {r?.domain && <a href={'https://' + r.domain} target="_blank" rel="noreferrer">{r.domain} ↗</a>}
            </div>
          </div>
        </div>
        <div />
        <div className="dd-hero-price">
          <div className="px"><span className="s">{symbol}</span><span>{close == null ? '—' : fmtPrice(close).replace(symbol, '')}</span></div>
          {/* Today's actual price move, not the ML 5-day prediction (which already
              has its own slot in the verdict panel below) — day_change_pct isn't a
              model field, so it's never redacted and shouldn't be locked here. */}
          <div className={'delta ' + dirCls(dayChange)}>{dayChange == null ? '—' : pct(dayChange)}</div>
          <div className="dd-hero-actions">
            <div className={'btn sm' + (inFirst ? ' primary' : '')}
              onClick={() => !toggleWatch.isPending && firstList && toggleWatch.mutate()}
              style={{ cursor: firstList ? 'pointer' : 'default', opacity: toggleWatch.isPending ? 0.6 : 1 }}>
              <svg width="13" height="13" viewBox="0 0 24 24" fill={inFirst ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="2"><path d="M19 21l-7-5-7 5V5a2 2 0 012-2h10a2 2 0 012 2z" /></svg>
              {inFirst ? `In ${firstList?.name || 'watchlist'}` : 'Watchlist'}
            </div>
            {memberOf.length > 0 && (
              <div className="dd-hero-wl-chips">
                {memberOf.map(m => <span key={m.id} className="sector-pill">{m.name}</span>)}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* VERDICT BAND — synthesized from data already on hand, no LLM call.
          The full AI Report further down is the deliberate, written-out version. */}
      <div className={'dd-verdict-band ' + (modelLocked ? 'locked' : sigCls(r?.signal))}>
        <div className="vb-head">
          {modelLocked
            ? <span className="sig-tag rd-lock-pill" title="Vesign signal — Upgrade to Pro"><LockGlyph /></span>
            : <span className={'sig-tag ' + sigCls(r?.signal)}><SignalGlyph signal={r?.signal} />{r?.signal || '—'}</span>}
          {r?.trade_count
            ? <span className="vb-since">{r.trade_count} historical trade{r.trade_count === 1 ? '' : 's'} · WR {r.win_rate != null ? r.win_rate.toFixed(0) : '—'}%</span>
            : null}
        </div>
        <div className="vb-sentence">
          {vbFragments.length
            ? vbFragments
            : <span className="muted">Not enough data yet for a read on {ticker}.</span>}
          {modelLocked && (
            <span className="vb-locked">
              <LockGlyph size={10} /> Vesign model locked — <a onClick={() => navigate('/account')}>Upgrade to Pro</a>
            </span>
          )}
        </div>
        <div className="vb-tail">
          <div className="vb-earnings"><span className="l">Next earnings</span><span className="v">{nextEarn?.date ? dateFmt(nextEarn.date) : '—'}</span></div>
          <button className="btn sm primary" onClick={() => document.getElementById('dd-ai-report')?.scrollIntoView({ behavior: 'smooth' })}>
            Get the full AI written take →
          </button>
        </div>
      </div>

      {/* CHART */}
      <div className="dd-chart-panel">
        <div className="dd-chart-head">
          <h3>{modelLocked ? 'Price' : 'Price · Vesign signals overlay'}</h3>
          <div className="legend">
            <span className="lg-item"><span className="sw" style={{ background: '#60a5fa' }} /> {ticker} price</span>
            {!modelLocked && <>
              <span className="lg-item"><span className="sw tri" style={{ background: '#00d97e' }} /> BUY signal</span>
              <span className="lg-item"><span className="sw tri" style={{ background: '#ff4d5c' }} /> SELL signal</span>
            </>}
          </div>
          <div className="chips">
            {RANGES.map(([lbl]) => (
              <span key={lbl} className={'chip' + (lbl === range ? ' active' : '')}
                onClick={() => setRange(lbl)}>{lbl}</span>
            ))}
          </div>
        </div>
        <PriceChart history={history} markers={markers} fmtPrice={fmtPrice} />
      </div>

      {/* FUNDAMENTALS */}
      <div>
        <div className="section-h" style={{ padding: '0 4px' }}>
          <h2>Fundamentals</h2>
          <span className="sub">TTM · last reported</span>
        </div>
        <div className="dd-fund-grid">
          <div className="dd-fund-group-label">Valuation</div>
          <div className="dd-fund-cell"><div className="l">Market Cap</div><div className="v">{capB(r?.market_cap)}</div></div>
          <div className="dd-fund-cell"><div className="l">P/E (TTM)</div><div className="v">{r?.pe_ttm == null ? '—' : num(r.pe_ttm, { fd: 1 })}</div></div>
          <div className="dd-fund-cell"><div className="l">EPS (TTM)</div><div className="v">{r?.eps_ttm == null ? '—' : fmtPrice(r.eps_ttm)}</div></div>
          <div className="dd-fund-cell"><div className="l">Revenue (TTM)</div><div className="v">{r?.revenue_ttm == null ? '—' : capB(r.revenue_ttm)}</div></div>

          <div className="dd-fund-group-label">Growth & profitability</div>
          <div className="dd-fund-cell"><div className="l">Revenue growth</div><div className={'v ' + dirCls(r?.revenue_growth)}>{r?.revenue_growth == null ? '—' : (r.revenue_growth >= 0 ? '+' : '') + (r.revenue_growth * 100).toFixed(0) + '%'}<small>YoY</small></div></div>
          <div className="dd-fund-cell"><div className="l">Gross margin</div><div className="v">{pctFrac(r?.gross_margin)}</div></div>
          <div className="dd-fund-cell"><div className="l">Op. margin</div><div className="v">{pctFrac(r?.op_margin)}</div></div>
          <div className="dd-fund-cell"><div className="l">Net margin</div><div className="v">{pctFrac(r?.net_margin)}</div></div>
          <div className="dd-fund-cell"><div className="l">ROE</div><div className="v">{pctFrac(r?.roe)}</div></div>
          <div className="dd-fund-cell"><div className="l">Debt / Equity</div><div className="v">{r?.de_ratio == null ? '—' : num(r.de_ratio, { fd: 2 })}</div></div>

          <div className="dd-fund-group-label">Technicals</div>
          <div className="dd-fund-cell"><div className="l">RSI</div><div className="v">{r?.rsi == null ? '—' : num(r.rsi, { fd: 1 })}</div></div>
          <div className="dd-fund-cell"><div className="l">BB %B</div><div className="v">{r?.bb_pct_b == null ? '—' : num(r.bb_pct_b, { fd: 2 })}</div></div>
        </div>
      </div>

      {/* ANALYST + ML TWO-UP */}
      <div className="dd-grid-2">
        {/* ANALYST TARGETS */}
        <div className="dd-panel">
          <div className="dd-panel-head">
            <h3>Analyst targets</h3>
            <span className="meta">{r?.number_of_analysts ? `${Math.round(r.number_of_analysts)} analysts · I/B/E/S` : '—'}</span>
          </div>
          <div className="dd-analyst-body">
            {/* Low/High replace the old "Range: $180 → $500 / Mean: $302" text
                header — that duplicated the exact same two numbers the bar
                already shows below, just in prose. Same value+caption style as
                the Current/Mean callouts under the bar, so all four numbers on
                this card read as one consistent language instead of two. */}
            <div className="dd-an-edge-header">
              <div className="dd-an-point-label static">
                <span className="pv">{lo != null ? fmtPrice(lo, 0) : '—'}</span><span className="pl">Low</span>
              </div>
              <div className="dd-an-point-label static">
                <span className="pv">{hi != null ? fmtPrice(hi, 0) : '—'}</span><span className="pl">High</span>
              </div>
            </div>
            <div className="dd-an-track-row">
              {curBelowRange && (
                <div className="dd-an-offrange">
                  <OffRangeArrow dir="left" />
                  <div className="dd-an-point-label static cur">
                    <span className="pv">{fmtPrice(close)}</span><span className="pl">Current</span>
                  </div>
                </div>
              )}
              <div className="dd-an-target-bar" style={{ marginBottom: labelsClose ? 78 : 46 }}>
                <div className="dd-an-range" style={{ left: '0%', right: '0%' }} />
                {curPos != null && <div className="dd-an-mark cur" style={{ left: curPos + '%' }} />}
                {meanPos != null && <div className="dd-an-mark mean" style={{ left: meanPos + '%' }} />}
                {curPos != null && (
                  <div className="dd-an-point-label cur" style={{ left: clampLabelPos(curPos) + '%' }}>
                    <span className="pv">{fmtPrice(close)}</span><span className="pl">Current</span>
                  </div>
                )}
                {meanPos != null && (
                  <div className={'dd-an-point-label mean' + (labelsClose ? ' stagger' : '')} style={{ left: clampLabelPos(meanPos) + '%' }}>
                    <span className="pv">{fmtPrice(mean, 0)}</span><span className="pl">Mean</span>
                  </div>
                )}
              </div>
              {curAboveRange && (
                <div className="dd-an-offrange">
                  <div className="dd-an-point-label static cur">
                    <span className="pv">{fmtPrice(close)}</span><span className="pl">Current</span>
                  </div>
                  <OffRangeArrow dir="right" />
                </div>
              )}
            </div>
            {(curBelowRange || curAboveRange) && (
              <div className="dd-an-offrange-note">
                Trading {curBelowRange ? 'below every analyst\'s low target' : 'above every analyst\'s high target'} — {fmtPrice(close)} is {curBelowRange ? 'below' : 'above'} the {curBelowRange ? fmtPrice(lo, 0) : fmtPrice(hi, 0)} {curBelowRange ? 'low' : 'high'}.
              </div>
            )}
            {/* Analyst count already lives in the panel head above ("58 analysts ·
                I/B/E/S", which also carries the data source) — a second "Analysts:
                58" cell here was the same number twice, so it's gone. In its place:
                upside to the high target — the most bullish analyst's number,
                not just the average. */}
            <div className="dd-an-rec">
              <div className="cell"><div className="l">Upside to mean</div><div className={'v ' + dirCls(up)}>{up == null ? '—' : pct(up)}</div></div>
              <div className="cell"><div className="l">Upside to high</div><div className={'v ' + dirCls(bestCase)}>{bestCase == null ? '—' : pct(bestCase)}</div></div>
            </div>
          </div>
        </div>

        {/* ML PREDICTIONS */}
        <div className={'dd-panel' + (modelLocked ? ' rd-lock-wrap' : '')}>
          {modelLocked && (
            <div className="rd-lock-overlay">
              <span className="rd-lock-ico"><LockGlyph size={20} /></span>
              <div className="rd-lock-title">Vesign model</div>
              <button className="rd-lock-cta" onClick={() => navigate('/account')}>Upgrade to Pro</button>
            </div>
          )}
          <div className={modelLocked ? 'rd-blur' : ''}>
          <div className="dd-panel-head">
            <h3>ML predictions</h3>
            <span className="meta">Walk-forward · quarterly retrain</span>
          </div>
          <div className="dd-ml-body">
            <div className="dd-ml-row">
              <div className="top">
                <span className="lbl">5-day return (ML)</span>
                <span className={'pred ' + dirCls(ml)}>{ml == null ? '—' : pct(ml)}</span>
              </div>
              <div className="bar"><span className={'fill' + (ml < 0 ? ' down' : '')} style={{ width: Math.max(2, Math.min(100, Math.abs(ml || 0) * 5)).toFixed(0) + '%' }} /></div>
              <div className="conf">
                <span>Prediction score</span>
                <span>{r?.prediction_score == null ? '—' : r.prediction_score.toFixed(4)}</span>
              </div>
            </div>
          </div>
          </div>
        </div>
      </div>

      {/* SIGNAL HISTORY */}
      <div>
        <div className="section-h" style={{ padding: '0 4px' }}>
          <h2>Vesign signal history</h2>
          <span className="sub">
            {r?.trade_count
              ? `${r.trade_count} closed trade${r.trade_count === 1 ? '' : 's'} on ${ticker}` + (r.win_rate != null ? ` · WR ${r.win_rate.toFixed(0)}%` : '')
              : `No closed Vesign trades on ${ticker} yet`}
          </span>
        </div>
        <div className={'dd-panel' + (modelLocked ? ' rd-lock-wrap' : '')}>
          {modelLocked && (
            <div className="rd-lock-overlay">
              <span className="rd-lock-ico"><LockGlyph size={20} /></span>
              <div className="rd-lock-title">Vesign model</div>
              <button className="rd-lock-cta" onClick={() => navigate('/account')}>Upgrade to Pro</button>
            </div>
          )}
          <div className={'dd-history-body' + (modelLocked ? ' rd-blur' : '')}>
            {histRows.length ? histRows.map((m, i) => {
              const s = (m.signal || '').toUpperCase()
              // Closed trades carry a real realized return_pct from trade_log
              // (backend/main.py's /api/signals/markers). The still-open position
              // (closed=false, BUY only — see the endpoint's merge logic) has no
              // trade_log row yet, so its return is computed here from the live
              // price vs this lot's own entry price.
              let retText = '—', retCls = 'open', stillOpen = false
              if (m.closed && m.return_pct != null) {
                retText = pct(m.return_pct * 100); retCls = m.return_pct >= 0 ? 'up' : 'down'
              } else if (!m.closed && s === 'BUY' && close != null && m.close) {
                const unrealized = (close - m.close) / m.close * 100
                retText = pct(unrealized); retCls = unrealized >= 0 ? 'up' : 'down'; stillOpen = true
              }
              return (
                <div className={'dd-hrow' + (stillOpen ? ' still-open' : '')} key={i}>
                  <div className={'sig ' + sigCls(s)}>{s}</div>
                  <div className="date">{dateFmt(m.date)}</div>
                  <div className="note">
                    {m.fair_value_upside != null && `Pred upside ${pct(m.fair_value_upside * 100)} · `}
                    {m.health_score != null ? `Health ${m.health_score}/5` : ''}
                  </div>
                  <div className="px"><span className="l">{s === 'SELL' ? 'Exit' : 'Entry'}</span>{m.close == null ? '—' : fmtPrice(m.close)}</div>
                  <div className={'ret ' + retCls}>{retText}</div>
                  <div className="days">{stillOpen ? <span className="open-badge">● open</span> : ''}</div>
                </div>
              )
            }) : (
              <div className="dd-hrow"><div className="note" style={{ color: 'var(--ink-3)', gridColumn: '1 / -1' }}>No Vesign signal history for {ticker}.</div></div>
            )}
          </div>
        </div>
      </div>

      {/* AI RESEARCH REPORT */}
      <div id="dd-ai-report">
        <div className="section-h" style={{ padding: '0 4px' }}>
          <h2>AI research report</h2>
          <span className="sub">Generated by Vesign AI</span>
        </div>
        <div className={'dd-panel' + (modelLocked ? ' rd-lock-wrap' : '')}>
          {modelLocked && (
            <div className="rd-lock-overlay">
              <span className="rd-lock-ico"><LockGlyph size={20} /></span>
              <div className="rd-lock-title">Vesign model</div>
              <button className="rd-lock-cta" onClick={() => navigate('/account')}>Upgrade to Pro</button>
            </div>
          )}
          <div className={'dd-ai-body' + (modelLocked ? ' rd-blur' : '')}>
            <div className="dd-ai-controls">
              <input type="number" className="dd-ai-entry" placeholder="Entry price (optional)"
                value={entryPrice} onChange={(e) => setEntryPrice(e.target.value)} />
              <button className="btn sm primary" onClick={generateReport} disabled={aiLoading}>
                {aiLoading ? 'Generating…' : 'Generate AI report ▶'}
              </button>
            </div>
            {aiLoading && <div className="dd-ai-status">Analyzing {ticker}…</div>}
            {aiError && !aiLoading && <div className="dd-ai-status err">{aiError}</div>}
            {aiReport && !aiLoading && <div className="dd-ai-text">{renderReport(aiReport)}</div>}
            {!aiReport && !aiLoading && !aiError && (
              <div className="dd-ai-status muted">Get an AI-written read on {ticker}'s current situation, key risks, and Vesign's recommendation.</div>
            )}
          </div>
        </div>
      </div>

      {/* RECENT NEWS */}
      <div>
        <div className="section-h" style={{ padding: '0 4px' }}>
          <h2>Recent news</h2>
          <span className="sub">Latest</span>
        </div>
        <div className="dd-panel">
          <div className="dd-news-body">
            {Array.isArray(news) && news.length ? news.map((n, i) => (
              <div className="dd-news-row" key={i} onClick={() => { if (n.url) window.open(n.url, '_blank') }}>
                <div>
                  <div className="headline">{n.title || ''}</div>
                  <div className="meta"><span className="src">{n.source || ''}</span></div>
                </div>
                <div className="time">{ago(n.date)}</div>
              </div>
            )) : (
              <div className="dd-news-row"><div className="headline" style={{ color: 'var(--ink-3)' }}>No recent news.</div></div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

/* Convert a months window to {start,end} so getPriceHistory uses the explicit
 * range path (its rolling-months param isn't exposed in api.js). */
function rangeBounds(months) {
  const end = new Date()
  const start = new Date()
  start.setMonth(start.getMonth() - months)
  const iso = (d) => d.toISOString().slice(0, 10)
  return { start: iso(start), end: iso(end) }
}
