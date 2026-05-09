import { useState, useEffect, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ReferenceLine, ResponsiveContainer,
} from 'recharts'
import {
  searchTickers, getResearch, generateAIReport, getNews, getAnalystChanges,
  getPriceHistory, getSignalMarkers, WHITE_BG_LOGOS,
} from '../api'
import SignalModal from '../components/SignalModal'

// ─── Helpers ──────────────────────────────────────────────────────────────────
function fmt(n, d = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d })
    : '—'
}
function fmtMktCap(v) {
  if (!v) return '—'
  const b = v / 1e9
  return b >= 1 ? `$${b.toFixed(1)}B` : `$${(v / 1e6).toFixed(0)}M`
}
function scoreColor(s) {
  if (s == null) return '#8b90a0'
  if (s >= 86) return '#2ecc71'
  if (s >= 71) return '#3fb0de'
  if (s >= 51) return '#e67e22'
  if (s >= 31) return '#f39c12'
  return '#8b90a0'
}
function scoreLabel(s, t) {
  if (s == null) return '—'
  if (s >= 86) return t('research.score.signalActive', 'Signal active')
  if (s >= 71) return t('research.score.approaching', 'Approaching signal')
  if (s >= 51) return t('research.score.watching', 'Watching closely')
  if (s >= 31) return t('research.score.earlyWatch', 'Early watch')
  return t('research.score.notOnRadar', 'Not on radar')
}
function sigStyle(signal) {
  const map = {
    BUY:  { background: 'rgba(46,204,113,0.15)', color: '#2ecc71', border: '1px solid #2ecc71' },
    SELL: { background: 'rgba(231,76,60,0.15)',  color: '#e74c3c', border: '1px solid #e74c3c' },
    HOLD: { background: 'rgba(99,110,130,0.15)', color: '#a0aec0', border: '1px solid #636e82' },
  }
  return map[signal] || map.HOLD
}
function subDate(months) {
  const d = new Date()
  d.setMonth(d.getMonth() - months)
  return d.toISOString().slice(0, 10)
}

const PERIODS = [
  { label: '1M', months: 1 },
  { label: '3M', months: 3 },
  { label: '6M', months: 6 },
  { label: '1Y', months: 12 },
  { label: '2Y', months: 24 },
]

// ─── TickerSearch ─────────────────────────────────────────────────────────────
function TickerSearch({ onSelect }) {
  const { t } = useTranslation()
  const [q, setQ] = useState('')
  const [results, setResults] = useState([])
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    if (!q.trim()) { setResults([]); return }
    const id = setTimeout(async () => {
      try { const data = await searchTickers(q, 8); setResults(data || []); setOpen(true) }
      catch (_) {}
    }, 250)
    return () => clearTimeout(id)
  }, [q])

  useEffect(() => {
    function onMD(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', onMD)
    return () => document.removeEventListener('mousedown', onMD)
  }, [])

  function select(item) { setQ(''); setResults([]); setOpen(false); onSelect(item) }

  return (
    <div ref={ref} style={{ position: 'relative', maxWidth: 500 }}>
      <input
        value={q} onChange={e => setQ(e.target.value)}
        placeholder={t('table.searchGlobal', 'Search ticker or company…')}
        style={{
          width: '100%', padding: '11px 16px', borderRadius: 12,
          border: '1px solid var(--border)', background: 'var(--surface)',
          color: 'var(--text)', fontSize: 15, boxSizing: 'border-box', outline: 'none',
        }}
        autoFocus
      />
      {open && results.length > 0 && (
        <div style={{
          position: 'absolute', top: 'calc(100% + 6px)', left: 0, right: 0,
          background: 'var(--surface)', border: '1px solid var(--border)',
          borderRadius: 12, zIndex: 300, overflow: 'hidden',
          boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
        }}>
          {results.map(r => (
            <button key={r.ticker} onClick={() => select(r)} style={{
              display: 'flex', alignItems: 'center', gap: 10, width: '100%',
              padding: '9px 14px', background: 'transparent', border: 'none',
              cursor: 'pointer', color: 'var(--text)', textAlign: 'left',
            }}
              onMouseEnter={e => e.currentTarget.style.background = 'rgba(255,255,255,0.06)'}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
            >
              {r.logo_url
                ? <img src={r.logo_url} alt="" style={{ width: 24, height: 24, objectFit: 'contain', borderRadius: 4 }} onError={e => e.target.style.display = 'none'} />
                : <div style={{ width: 24, height: 24, borderRadius: 4, background: 'var(--border)' }} />
              }
              <span style={{ fontWeight: 700, fontSize: 13 }}>{r.ticker}</span>
              <span style={{ fontSize: 12, color: 'var(--muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.company}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ─── SVG Semi-circle Score Gauge ──────────────────────────────────────────────
function ScoreGauge({ score }) {
  const r = 65, cx = 80, cy = 85
  const halfCirc = Math.PI * r       // ≈204.2
  const fullCirc = 2 * Math.PI * r   // ≈408.4
  const filled = score != null ? (score / 100) * halfCirc : 0
  const color = scoreColor(score)

  return (
    <svg viewBox="0 0 160 100" style={{ width: '100%', display: 'block' }}>
      {/* Track */}
      <circle cx={cx} cy={cy} r={r} fill="none"
        stroke="rgba(255,255,255,0.07)" strokeWidth={13} strokeLinecap="round"
        strokeDasharray={`${halfCirc} ${fullCirc}`}
        transform={`rotate(-180 ${cx} ${cy})`}
      />
      {/* Filled arc */}
      <circle cx={cx} cy={cy} r={r} fill="none"
        stroke={color} strokeWidth={13} strokeLinecap="round"
        strokeDasharray={`${filled} ${fullCirc}`}
        transform={`rotate(-180 ${cx} ${cy})`}
        style={{ transition: 'stroke-dasharray 0.8s ease' }}
      />
      {/* Score number */}
      <text x={cx} y={cy - 14} textAnchor="middle" dominantBaseline="middle"
        fill={color} fontSize="30" fontWeight="800" fontFamily="inherit">
        {score ?? '—'}
      </text>
      <text x={cx} y={cy + 5} textAnchor="middle" dominantBaseline="middle"
        fill="#8b90a0" fontSize="11" fontFamily="inherit">
        / 100
      </text>
    </svg>
  )
}

// ─── Analyst Price Range Bar ──────────────────────────────────────────────────
function AnalystRangeBar({ low, mean, high, current }) {
  const { t } = useTranslation()
  if (!low || !high) return (
    <div style={{ fontSize: 12, color: 'var(--muted)', padding: '8px 0' }}>{t('research.analyst.noTargets', 'No analyst targets')}</div>
  )
  const minV = Math.min(low, current ?? low) * 0.96
  const maxV = Math.max(high, current ?? high) * 1.04
  const span = maxV - minV
  const pct = v => `${((v - minV) / span * 100).toFixed(2)}%`

  return (
    <div style={{ padding: '4px 0 8px' }}>
      <div style={{ position: 'relative', height: 6, background: 'var(--border)', borderRadius: 99, margin: '22px 2px 30px' }}>
        {/* Low→High range fill */}
        <div style={{
          position: 'absolute', top: 0, height: '100%', borderRadius: 99,
          left: pct(low),
          width: `${((high - low) / span * 100).toFixed(2)}%`,
          background: 'linear-gradient(90deg, #f39c12 0%, #2ecc71 100%)',
        }} />
        {/* Current price pin */}
        {current && (
          <div style={{ position: 'absolute', left: pct(current), transform: 'translateX(-50%)', top: -7 }}>
            <div style={{ width: 4, height: 20, background: 'var(--accent)', borderRadius: 2 }} />
            <div style={{
              position: 'absolute', top: 22, left: '50%', transform: 'translateX(-50%)',
              fontSize: 10, color: 'var(--accent)', fontWeight: 700, whiteSpace: 'nowrap',
            }}>
              ${fmt(current, 0)}
            </div>
          </div>
        )}
        {/* Mean target pin */}
        {mean && (
          <div style={{ position: 'absolute', left: pct(mean), transform: 'translateX(-50%)', top: -5 }}>
            <div style={{ width: 2, height: 16, background: '#f39c12', borderRadius: 2 }} />
            <div style={{
              position: 'absolute', top: 18, left: '50%', transform: 'translateX(-50%)',
              fontSize: 10, color: '#f39c12', fontWeight: 700, whiteSpace: 'nowrap',
            }}>
              ${fmt(mean, 0)}
            </div>
          </div>
        )}
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: 'var(--muted)', marginTop: 6 }}>
        <span>{t('research.analyst.low', 'Low')} ${fmt(low, 0)}</span>
        <span>{t('research.analyst.high', 'High')} ${fmt(high, 0)}</span>
      </div>
    </div>
  )
}

// ─── Marker Label (signal name + price, two SVG text lines) ──────────────────
function MarkerLabel({ viewBox, signal, price }) {
  const { x, y } = viewBox
  const color = signal === 'BUY' ? '#2ecc71' : '#e74c3c'
  return (
    <g>
      <text x={x} y={y - 14} textAnchor="middle" fill={color} fontSize={9} fontWeight={700} fontFamily="inherit">{signal}</text>
      <text x={x} y={y - 4}  textAnchor="middle" fill={color} fontSize={8}  fontFamily="inherit" opacity={0.85}>${Math.round(price)}</text>
    </g>
  )
}

// ─── Price Chart ──────────────────────────────────────────────────────────────
function PriceChart({ ticker }) {
  const { t } = useTranslation()
  const [periodIdx, setPeriodIdx] = useState(2)
  const [prices, setPrices] = useState([])
  const [markers, setMarkers] = useState([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    const { months } = PERIODS[periodIdx]
    const start = subDate(months)
    const end = new Date().toISOString().slice(0, 10)
    setLoading(true)
    Promise.all([
      getPriceHistory(ticker, { start, end }),
      getSignalMarkers(ticker, months + 1),
    ]).then(([priceData, sigData]) => {
      const sorted = (priceData || [])
        .filter(p => p.date >= start)
        .sort((a, b) => a.date.localeCompare(b.date))
      setPrices(sorted)
      // Only show SELL markers that follow a BUY (actual trade exits)
      const filtered = (sigData || []).filter(s => s.date >= start).sort((a, b) => a.date.localeCompare(b.date))
      const paired = []
      let hasBuy = false
      for (const s of filtered) {
        if (s.signal === 'BUY') { hasBuy = true; paired.push(s) }
        else if (s.signal === 'SELL' && hasBuy) { hasBuy = false; paired.push(s) }
      }
      setMarkers(paired)
    }).catch(() => {}).finally(() => setLoading(false))
  }, [ticker, periodIdx])

  const { months } = PERIODS[periodIdx]
  function fmtTick(d) {
    const [y, m, day] = d.split('-')
    const names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    return months <= 3 ? `${m}/${day}` : `${names[+m - 1]} '${y.slice(2)}`
  }

  const minY = prices.length ? Math.min(...prices.map(p => p.close)) * 0.98 : 0
  const maxY = prices.length ? Math.max(...prices.map(p => p.close)) * 1.02 : 100

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
          {t('research.priceChart.title', 'Price Chart')}
        </span>
        <div style={{ display: 'flex', gap: 5 }}>
          {PERIODS.map((p, i) => (
            <button key={p.label} onClick={() => setPeriodIdx(i)} style={{
              padding: '4px 12px', borderRadius: 20, fontSize: 11, fontWeight: 600,
              border: 'none', cursor: 'pointer', transition: 'all 0.15s',
              background: periodIdx === i ? 'var(--accent)' : 'rgba(255,255,255,0.06)',
              color: periodIdx === i ? '#0b0e18' : 'var(--muted)',
            }}>
              {p.label}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div style={{ height: 220, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--muted)', fontSize: 13 }}>
          {t('research.priceChart.loading', 'Loading chart…')}
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={prices} margin={{ top: 32, right: 8, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="researchAreaGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#3fb0de" stopOpacity={0.35} />
                <stop offset="95%" stopColor="#3fb0de" stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis dataKey="date" tickFormatter={fmtTick}
              tick={{ fill: '#8b90a0', fontSize: 10 }} tickLine={false} axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis domain={[minY, maxY]}
              tick={{ fill: '#8b90a0', fontSize: 10 }} tickLine={false} axisLine={false}
              tickFormatter={v => `$${v >= 1000 ? (v / 1000).toFixed(1) + 'k' : v.toFixed(0)}`}
              width={52}
            />
            <Tooltip
              contentStyle={{ background: '#1a1f2e', border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8, fontSize: 12 }}
              formatter={v => [`$${fmt(v)}`, t('research.priceChart.close', 'Close')]}
            />
            {markers.map(m => (
              <ReferenceLine key={m.date + m.signal} x={m.date}
                stroke={m.signal === 'BUY' ? '#2ecc71' : '#e74c3c'}
                strokeDasharray="3 3" strokeWidth={1.5}
                label={props => <MarkerLabel {...props} signal={m.signal} price={m.close} />}
              />
            ))}
            <Area type="monotone" dataKey="close" stroke="#3fb0de" strokeWidth={2}
              fill="url(#researchAreaGrad)" dot={false}
              activeDot={{ r: 4, fill: '#3fb0de', strokeWidth: 0 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}

// ─── Metric Card ─────────────────────────────────────────────────────────────
function MetricCard({ label, value, sub, color }) {
  return (
    <div style={{
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 12, padding: '16px 12px', textAlign: 'center',
    }}>
      {/* dir=ltr forces numbers like "+28%" to render as "+28%" even in RTL pages */}
      <div dir="ltr" style={{ fontSize: 22, fontWeight: 800, color: color || 'var(--text)', lineHeight: 1.1 }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 3 }}>{sub}</div>}
      <div style={{ fontSize: 10, color: 'var(--muted)', marginTop: 8, textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</div>
    </div>
  )
}

// ─── Main Page ────────────────────────────────────────────────────────────────
export default function ResearchPage() {
  const { t, i18n } = useTranslation()
  const { ticker: urlTicker } = useParams()
  const navigate = useNavigate()

  const [research, setResearch] = useState(null)
  const [loadingR, setLoadingR] = useState(false)
  const [errR, setErrR] = useState(null)

  const [report, setReport] = useState(null)
  const [loadingReport, setLoadingReport] = useState(false)
  const [entryPrice, setEntryPrice] = useState('')

  const [activeTab, setActiveTab] = useState('news')
  const [news, setNews] = useState(null)
  const [analystChanges, setAnalystChanges] = useState(null)
  const [loadingTab, setLoadingTab] = useState(false)

  const [modalRow, setModalRow] = useState(null)

  useEffect(() => { if (urlTicker) loadResearch(urlTicker) }, [urlTicker])

  async function loadResearch(ticker) {
    setLoadingR(true); setErrR(null); setResearch(null)
    setReport(null); setNews(null); setAnalystChanges(null)
    try {
      const data = await getResearch(ticker)
      setResearch(data)
      navigate(`/research/${ticker}`, { replace: true })
      loadTab('news', ticker)
    } catch (e) {
      setErrR(e.message || t('research.loadingFailed', 'Failed to load research data'))
    } finally {
      setLoadingR(false)
    }
  }

  async function loadTab(tab, tickerOverride) {
    const t = tickerOverride || research?.ticker
    if (!t) return
    setActiveTab(tab); setLoadingTab(true)
    try {
      if (tab === 'news') setNews(await getNews(t, 10))
      else setAnalystChanges(await getAnalystChanges(t, 10))
    } catch (_) {}
    setLoadingTab(false)
  }

  async function handleGenerateReport() {
    if (!research) return
    setLoadingReport(true); setReport(null)
    try {
      const ep = entryPrice ? parseFloat(entryPrice) : null
      const data = await generateAIReport(research.ticker, ep, i18n.language)
      setReport(data.report)
    } catch (e) {
      setReport(`${t('research.aiReport.errorPrefix', 'Error')}: ${e.message || t('research.aiReport.failed', 'Failed to generate report')}`)
    } finally {
      setLoadingReport(false)
    }
  }

  function openModal() {
    if (!research) return
    setModalRow({
      ticker: research.ticker, company: research.company, logo_url: research.logo_url,
      industry: research.industry, signal: research.signal, close: research.close,
      rsi: research.rsi, fair_value_upside: research.fair_value_upside,
      target_mean_price: research.target_mean_price, target_low_price: research.target_low_price,
      target_high_price: research.target_high_price, health_score: research.health_score,
      health_reason: research.health_reason, market_cap: research.market_cap,
      prediction_score: research.prediction_score,
    })
  }

  // Metric card computed values
  const rsi = research?.rsi
  const rsiColor = rsi == null ? '#8b90a0' : rsi < 30 ? '#2ecc71' : rsi < 50 ? '#f39c12' : '#8b90a0'
  const rsiSub = rsi == null ? null
    : rsi < 30 ? t('research.metric.rsiOversold', 'Oversold')
    : rsi < 50 ? t('research.metric.rsiNeutral', 'Neutral')
    : t('research.metric.rsiOverbought', 'Overbought')

  const upside = research?.fair_value_upside
  const upPct = upside != null ? `${upside >= 0 ? '+' : ''}${(upside * 100).toFixed(0)}%` : '—'
  const upColor = upside == null ? '#8b90a0' : upside >= 0.30 ? '#2ecc71' : upside >= 0 ? '#f39c12' : '#e74c3c'

  const winRate = research?.win_rate
  const wrColor = winRate == null ? '#8b90a0' : winRate >= 60 ? '#2ecc71' : winRate >= 40 ? '#f39c12' : '#e74c3c'

  const avgRet = research?.avg_return
  const arColor = avgRet == null ? '#8b90a0' : avgRet > 0 ? '#2ecc71' : '#e74c3c'

  const panel = {
    background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 14, padding: '18px 20px',
  }

  return (
    <div style={{ padding: '24px 24px 80px' }}>

      {/* Search */}
      <div style={{ marginBottom: 24 }}>
        <TickerSearch onSelect={item => loadResearch(item.ticker)} />
      </div>

      {loadingR && (
        <div style={{ textAlign: 'center', color: 'var(--muted)', padding: 60, fontSize: 15 }}>
          {t('research.loading', 'Loading research…')}
        </div>
      )}
      {errR && (
        <div style={{ color: '#e74c3c', padding: '12px 16px', borderRadius: 8, marginBottom: 20, background: 'rgba(231,76,60,0.1)', border: '1px solid rgba(231,76,60,0.3)' }}>
          {errR}
        </div>
      )}
      {!research && !loadingR && !errR && (
        <div style={{ textAlign: 'center', color: 'var(--muted)', padding: '80px 0', fontSize: 15 }}>
          {t('research.searchPrompt', 'Search for any stock to start your research.')}
        </div>
      )}

      {research && (
        <>
          {/* ── Hero ─────────────────────────────────────────────────────── */}
          <div style={{
            ...panel, marginBottom: 20, padding: '20px 24px',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            flexWrap: 'wrap', gap: 16,
            background: 'linear-gradient(135deg, var(--surface) 0%, rgba(63,176,222,0.05) 100%)',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
              {research.logo_url && (
                <img src={research.logo_url} alt=""
                  className={WHITE_BG_LOGOS.has(research.ticker) ? 'logo logo-white-bg' : 'logo'}
                  style={{ width: 60, height: 60, objectFit: 'contain', borderRadius: 14, flexShrink: 0 }}
                  onError={e => e.target.style.display = 'none'}
                />
              )}
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <span style={{ fontSize: 28, fontWeight: 800, letterSpacing: '-0.02em' }}>{research.ticker}</span>
                  <span style={{ ...sigStyle(research.signal), padding: '3px 12px', borderRadius: 20, fontSize: 12, fontWeight: 700 }}>
                    {research.signal || 'HOLD'}
                  </span>
                </div>
                <div style={{ fontSize: 15, marginTop: 2 }}>{research.company}</div>
                <div style={{ fontSize: 12, marginTop: 3, color: 'var(--muted)' }}>
                  {[research.industry, fmtMktCap(research.market_cap)].filter(Boolean).join(' · ')}
                </div>
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
              <div style={{ textAlign: 'right' }}>
                <div style={{ fontSize: 34, fontWeight: 800, lineHeight: 1 }}>${fmt(research.close)}</div>
                <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 4 }}>{t('research.lastClose', 'Last close')}</div>
              </div>
              <button onClick={openModal} style={{
                padding: '9px 18px', borderRadius: 10, cursor: 'pointer',
                background: 'transparent', border: '1px solid var(--border)',
                color: 'var(--accent)', fontSize: 13, fontWeight: 600, whiteSpace: 'nowrap',
              }}>
                {t('research.viewChart', 'View Chart')} ↗
              </button>
            </div>
          </div>

          {/* ── Two-column grid ───────────────────────────────────────────── */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: 20, alignItems: 'start' }}>

            {/* ── Left column ──────────────────────────────────────────────── */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

              {/* Price chart */}
              <div style={panel}>
                <PriceChart ticker={research.ticker} />
              </div>

              {/* 4 Metric cards */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
                <MetricCard
                  label={t('research.metric.rsi', 'RSI')}
                  value={rsi != null ? rsi.toFixed(1) : '—'}
                  sub={rsiSub}
                  color={rsiColor}
                />
                <MetricCard
                  label={t('research.metric.upside', 'Analyst Upside')}
                  value={upPct}
                  sub={upside != null && research.number_of_analysts ? t('research.metric.analystsCount', '{{count}} analysts', { count: research.number_of_analysts }) : null}
                  color={upColor}
                />
                <MetricCard
                  label={t('research.metric.winRate', 'Win Rate')}
                  value={research.trade_count > 0 ? `${winRate}%` : '—'}
                  sub={research.trade_count > 0 ? t('research.metric.tradesCount', '{{count}} trades', { count: research.trade_count }) : t('research.metric.noTrades', 'No trades')}
                  color={wrColor}
                />
                <MetricCard
                  label={t('research.metric.avgReturn', 'Avg Return')}
                  value={research.trade_count > 0 && avgRet != null
                    ? `${avgRet > 0 ? '+' : ''}${avgRet}%` : '—'}
                  color={arColor}
                />
              </div>

              {/* AI Report */}
              <div style={panel}>
                <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 14 }}>{t('research.aiReport.title', 'AI Research Report')}</div>
                <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
                  <input
                    type="number" placeholder={t('research.aiReport.entryPlaceholder', 'Entry price (optional)')} value={entryPrice}
                    onChange={e => setEntryPrice(e.target.value)}
                    style={{
                      padding: '8px 12px', borderRadius: 8, width: 200,
                      border: '1px solid var(--border)', background: 'var(--bg)',
                      color: 'var(--text)', fontSize: 13,
                    }}
                  />
                  <button onClick={handleGenerateReport} disabled={loadingReport} style={{
                    padding: '8px 22px', borderRadius: 8,
                    cursor: loadingReport ? 'not-allowed' : 'pointer',
                    background: loadingReport ? 'var(--border)' : 'var(--accent)',
                    border: 'none', color: '#0b0e18', fontSize: 13, fontWeight: 700,
                    opacity: loadingReport ? 0.7 : 1,
                  }}>
                    {loadingReport ? t('research.aiReport.generating', 'Generating…') : `${t('research.aiReport.generate', 'Generate AI Report')} ▶`}
                  </button>
                </div>
                {loadingReport && (
                  <div style={{ marginTop: 16, color: 'var(--muted)', fontSize: 13 }}>
                    {t('research.aiReport.analyzing', 'Analyzing {{ticker}}…', { ticker: research.ticker })}
                  </div>
                )}
                {report && !loadingReport && (
                  <div style={{
                    marginTop: 18, whiteSpace: 'pre-wrap', lineHeight: 1.8,
                    fontSize: 14, borderTop: '1px solid var(--border)', paddingTop: 16,
                  }}>
                    {report}
                  </div>
                )}
              </div>
            </div>

            {/* ── Right column ─────────────────────────────────────────────── */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

              {/* Vesign Score gauge */}
              <div style={{ ...panel, textAlign: 'center', paddingBottom: 14 }}>
                <div style={{ fontSize: 11, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>
                  {t('research.score.title', 'Vesign Score')}
                </div>
                <ScoreGauge score={research.vesign_score} />
                <div style={{ fontSize: 13, fontWeight: 700, color: scoreColor(research.vesign_score), marginTop: 6 }}>
                  {scoreLabel(research.vesign_score, t)}
                </div>
              </div>

              {/* Analyst consensus + range bar */}
              <div style={panel}>
                <div style={{ fontSize: 11, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 10 }}>
                  {t('research.analyst.consensus', 'Analyst Consensus')}
                </div>
                {research.target_mean_price ? (
                  <>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                      <span dir="ltr" style={{ fontSize: 22, fontWeight: 800 }}>${fmt(research.target_mean_price, 0)}</span>
                      {upside != null && (
                        <span dir="ltr" style={{ fontSize: 14, fontWeight: 700, color: upColor }}>
                          {upside >= 0 ? '+' : ''}{(upside * 100).toFixed(0)}%
                        </span>
                      )}
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 2 }}>
                      {research.number_of_analysts
                        ? t('research.analyst.meanTargetWith', 'Mean target · {{count}} analysts', { count: research.number_of_analysts })
                        : t('research.analyst.meanTarget', 'Mean target')}
                    </div>
                    <AnalystRangeBar
                      low={research.target_low_price}
                      mean={research.target_mean_price}
                      high={research.target_high_price}
                      current={research.close}
                    />
                  </>
                ) : (
                  <div style={{ fontSize: 13, color: 'var(--muted)' }}>{t('research.analyst.noTargets', 'No analyst targets')}</div>
                )}
              </div>

              {/* Company health */}
              <div style={panel}>
                <div style={{ fontSize: 11, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 10 }}>
                  {t('research.health.title', 'Company Health')}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 8 }}>
                  {[1,2,3,4,5].map(i => (
                    <div key={i} style={{
                      flex: 1, height: 8, borderRadius: 4,
                      background: research.health_score != null && i <= research.health_score
                        ? '#3fb0de' : 'var(--border)',
                      transition: 'background 0.3s',
                    }} />
                  ))}
                  <span style={{ fontSize: 13, fontWeight: 700, marginLeft: 6, flexShrink: 0 }}>
                    {research.health_score != null ? `${research.health_score}/5` : '—'}
                  </span>
                </div>
                {research.health_reason && (
                  <div style={{ fontSize: 12, color: 'var(--muted)', lineHeight: 1.5 }}>
                    {research.health_reason}
                  </div>
                )}
              </div>

              {/* News + Analyst changes tabs */}
              <div style={panel}>
                <div style={{ display: 'flex', marginBottom: 14, borderBottom: '1px solid var(--border)' }}>
                  {['news', 'analyst'].map(tab => (
                    <button key={tab} onClick={() => { if (tab !== activeTab) loadTab(tab) }} style={{
                      padding: '7px 14px', background: 'transparent', border: 'none',
                      cursor: 'pointer', fontSize: 12, fontWeight: 600, marginBottom: -1,
                      color: activeTab === tab ? 'var(--accent)' : 'var(--muted)',
                      borderBottom: activeTab === tab ? '2px solid var(--accent)' : '2px solid transparent',
                    }}>
                      {tab === 'news' ? t('research.tab.news', 'News') : t('research.tab.analystChanges', 'Analyst Changes')}
                    </button>
                  ))}
                </div>

                {loadingTab && (
                  <div style={{ color: 'var(--muted)', fontSize: 12, padding: '8px 0' }}>{t('table.loading', 'Loading…')}</div>
                )}

                {!loadingTab && activeTab === 'news' && news && (
                  news.length === 0
                    ? <div style={{ color: 'var(--muted)', fontSize: 12 }}>{t('research.news.empty', 'No recent news.')}</div>
                    : news.slice(0, 8).map((item, i) => (
                      <div key={i} style={{
                        paddingBottom: 12, marginBottom: 12,
                        borderBottom: i < Math.min(news.length, 8) - 1 ? '1px solid var(--border)' : 'none',
                      }}>
                        <a href={item.url} target="_blank" rel="noopener noreferrer"
                          style={{ color: 'var(--text)', fontWeight: 600, fontSize: 12, textDecoration: 'none', lineHeight: 1.4, display: 'block' }}>
                          {item.title}
                        </a>
                        <div style={{ fontSize: 10, color: 'var(--muted)', marginTop: 3 }}>
                          {item.site} · {item.publishedDate?.slice(0, 10)}
                        </div>
                      </div>
                    ))
                )}

                {!loadingTab && activeTab === 'analyst' && analystChanges && (
                  analystChanges.length === 0
                    ? <div style={{ color: 'var(--muted)', fontSize: 12 }}>{t('research.analyst.noChanges', 'No recent analyst changes.')}</div>
                    : analystChanges.slice(0, 8).map((item, i) => (
                      <div key={i} style={{
                        paddingBottom: 10, marginBottom: 10, fontSize: 12,
                        borderBottom: i < Math.min(analystChanges.length, 8) - 1 ? '1px solid var(--border)' : 'none',
                      }}>
                        <span style={{ fontWeight: 700 }}>{item.analystCompany || item.analyst}</span>
                        {' · '}
                        <span style={{ color: item.action === 'upgrade' ? '#2ecc71' : item.action === 'downgrade' ? '#e74c3c' : 'var(--muted)' }}>
                          {item.action || item.rating}
                        </span>
                        {item.priceTarget && <span style={{ color: 'var(--muted)' }}> · ${item.priceTarget}</span>}
                        <div style={{ fontSize: 10, color: 'var(--muted)', marginTop: 2 }}>{item.date?.slice(0, 10)}</div>
                      </div>
                    ))
                )}
              </div>
            </div>
          </div>
        </>
      )}

      {modalRow && <SignalModal row={modalRow} onClose={() => setModalRow(null)} />}
    </div>
  )
}
