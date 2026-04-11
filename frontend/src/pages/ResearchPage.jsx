import { useState, useEffect, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { searchTickers, getResearch, generateAIReport, getNews, getAnalystChanges, WHITE_BG_LOGOS } from '../api'
import SignalModal from '../components/SignalModal'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
}

function fmtPct(n, decimals = 1) {
  return n != null ? `${n > 0 ? '+' : ''}${Number(n * 100).toFixed(decimals)}%` : '—'
}

function fmtMktCap(v) {
  if (!v) return '—'
  const b = v / 1e9
  return b >= 1 ? `$${b.toFixed(1)}B` : `$${(v / 1e6).toFixed(0)}M`
}

function scoreColor(s) {
  if (s >= 86) return '#2ecc71'
  if (s >= 71) return '#3fb0de'
  if (s >= 51) return '#e67e22'
  if (s >= 31) return '#f39c12'
  return '#8b90a0'
}

function scoreLabel(s) {
  if (s >= 86) return 'Signal active'
  if (s >= 71) return 'Approaching signal'
  if (s >= 51) return 'Watching closely'
  if (s >= 31) return 'Early watch'
  return 'Not on radar'
}

function signalBadgeStyle(signal) {
  const map = {
    BUY:  { background: 'rgba(46,204,113,0.15)', color: '#2ecc71', border: '1px solid #2ecc71' },
    SELL: { background: 'rgba(231,76,60,0.15)',  color: '#e74c3c', border: '1px solid #e74c3c' },
    HOLD: { background: 'rgba(99,110,130,0.15)', color: '#a0aec0', border: '1px solid #636e82' },
  }
  return map[signal] || map.HOLD
}

// ---------------------------------------------------------------------------
// Ticker search dropdown (reuses existing searchTickers API)
// ---------------------------------------------------------------------------
function TickerSearch({ onSelect }) {
  const [q, setQ] = useState('')
  const [results, setResults] = useState([])
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    if (!q.trim()) { setResults([]); return }
    const id = setTimeout(async () => {
      try {
        const data = await searchTickers(q, 8)
        setResults(data || [])
        setOpen(true)
      } catch (_) {}
    }, 250)
    return () => clearTimeout(id)
  }, [q])

  useEffect(() => {
    function onMouseDown(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  function select(item) {
    setQ('')
    setResults([])
    setOpen(false)
    onSelect(item)
  }

  return (
    <div ref={ref} style={{ position: 'relative', width: '100%', maxWidth: 480 }}>
      <input
        value={q}
        onChange={e => setQ(e.target.value)}
        placeholder="Search ticker or company name..."
        style={{
          width: '100%', padding: '10px 16px', borderRadius: 10,
          border: '1px solid var(--border)', background: 'var(--surface)',
          color: 'var(--text)', fontSize: 15, boxSizing: 'border-box',
          outline: 'none',
        }}
        autoFocus
      />
      {open && results.length > 0 && (
        <div style={{
          position: 'absolute', top: 'calc(100% + 6px)', left: 0, right: 0,
          background: 'var(--surface)', border: '1px solid var(--border)',
          borderRadius: 10, zIndex: 300, overflow: 'hidden',
          boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
        }}>
          {results.map(r => (
            <button
              key={r.ticker}
              onClick={() => select(r)}
              style={{
                display: 'flex', alignItems: 'center', gap: 10,
                width: '100%', padding: '9px 14px',
                background: 'transparent', border: 'none',
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

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------
export default function ResearchPage() {
  const { ticker: urlTicker } = useParams()
  const navigate = useNavigate()

  const [research, setResearch] = useState(null)
  const [loadingResearch, setLoadingResearch] = useState(false)
  const [researchError, setResearchError] = useState(null)

  const [report, setReport] = useState(null)
  const [loadingReport, setLoadingReport] = useState(false)
  const [entryPrice, setEntryPrice] = useState('')

  const [activeTab, setActiveTab] = useState('news')
  const [news, setNews] = useState(null)
  const [analystChanges, setAnalystChanges] = useState(null)
  const [loadingTab, setLoadingTab] = useState(false)

  const [modalRow, setModalRow] = useState(null)

  // Load ticker from URL param on mount
  useEffect(() => {
    if (urlTicker) loadResearch(urlTicker)
  }, [urlTicker])

  async function loadResearch(ticker) {
    setLoadingResearch(true)
    setResearchError(null)
    setResearch(null)
    setReport(null)
    setNews(null)
    setAnalystChanges(null)
    try {
      const data = await getResearch(ticker)
      setResearch(data)
      navigate(`/research/${ticker}`, { replace: true })
      // Load news by default
      loadTab('news', ticker)
    } catch (e) {
      setResearchError(e.message || 'Failed to load research data')
    } finally {
      setLoadingResearch(false)
    }
  }

  async function loadTab(tab, tickerOverride) {
    const t = tickerOverride || research?.ticker
    if (!t) return
    setActiveTab(tab)
    setLoadingTab(true)
    try {
      if (tab === 'news') {
        const data = await getNews(t, 10)
        setNews(data || [])
      } else {
        const data = await getAnalystChanges(t, 10)
        setAnalystChanges(data || [])
      }
    } catch (_) {}
    setLoadingTab(false)
  }

  async function handleGenerateReport() {
    if (!research) return
    setLoadingReport(true)
    setReport(null)
    try {
      const ep = entryPrice ? parseFloat(entryPrice) : null
      const data = await generateAIReport(research.ticker, ep)
      setReport(data.report)
    } catch (e) {
      setReport(`Error: ${e.message || 'Failed to generate report'}`)
    } finally {
      setLoadingReport(false)
    }
  }

  function openChart() {
    if (!research) return
    setModalRow({
      ticker:            research.ticker,
      company:           research.company,
      logo_url:          research.logo_url,
      industry:          research.industry,
      description:       research.description,
      description_short: research.description_short,
      signal:            research.signal,
      close:             research.close,
      rsi:               research.rsi,
      fair_value_upside: research.fair_value_upside,
      target_mean_price: research.target_mean_price,
      target_low_price:  research.target_low_price,
      target_high_price: research.target_high_price,
      health_score:      research.health_score,
      health_reason:     research.health_reason,
      market_cap:        research.market_cap,
      prediction_score:  research.prediction_score,
    })
  }

  const panelStyle = {
    background: 'var(--surface)', border: '1px solid var(--border)',
    borderRadius: 12, padding: '18px 20px',
  }

  return (
    <div style={{ maxWidth: 900, margin: '0 auto', padding: '28px 16px 60px' }}>
      <h2 style={{ marginTop: 0, marginBottom: 20, fontSize: 22, fontWeight: 700 }}>
        Stock Research
      </h2>

      {/* Search bar */}
      <div style={{ marginBottom: 28 }}>
        <TickerSearch onSelect={item => loadResearch(item.ticker)} />
      </div>

      {loadingResearch && (
        <div style={{ textAlign: 'center', color: 'var(--muted)', padding: 48 }}>
          Loading research data…
        </div>
      )}

      {researchError && (
        <div style={{ color: '#e74c3c', padding: '12px 16px', borderRadius: 8, background: 'rgba(231,76,60,0.1)', border: '1px solid rgba(231,76,60,0.3)', marginBottom: 20 }}>
          {researchError}
        </div>
      )}

      {research && (
        <>
          {/* Header row */}
          <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', flexWrap: 'wrap', gap: 16, marginBottom: 24 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
              {research.logo_url && (
                <img
                  src={research.logo_url}
                  alt=""
                  className={WHITE_BG_LOGOS.has(research.ticker) ? 'logo logo-white-bg' : 'logo'}
                  style={{ width: 52, height: 52, objectFit: 'contain', borderRadius: 10 }}
                  onError={e => e.target.style.display = 'none'}
                />
              )}
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <span style={{ fontSize: 22, fontWeight: 800 }}>{research.ticker}</span>
                  <span
                    style={{
                      ...signalBadgeStyle(research.signal),
                      padding: '2px 10px', borderRadius: 20, fontSize: 12, fontWeight: 700,
                    }}
                  >
                    {research.signal || 'HOLD'}
                  </span>
                </div>
                <div style={{ color: 'var(--muted)', fontSize: 13, marginTop: 2 }}>
                  {research.company}
                </div>
                <div style={{ color: 'var(--muted)', fontSize: 12, marginTop: 2 }}>
                  {[research.industry, fmtMktCap(research.market_cap)].filter(Boolean).join(' · ')}
                </div>
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ textAlign: 'right' }}>
                <div style={{ fontSize: 22, fontWeight: 700 }}>${fmt(research.close)}</div>
                <div style={{ fontSize: 12, color: 'var(--muted)' }}>Last price</div>
              </div>
              <button
                onClick={openChart}
                style={{
                  padding: '8px 16px', borderRadius: 8, cursor: 'pointer',
                  background: 'transparent', border: '1px solid var(--border)',
                  color: 'var(--accent)', fontSize: 13, fontWeight: 600,
                  whiteSpace: 'nowrap',
                }}
              >
                View Chart ↗
              </button>
            </div>
          </div>

          {/* Vesign Score bar */}
          <div style={{ ...panelStyle, marginBottom: 20 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
              <span style={{ fontWeight: 700, fontSize: 14 }}>Vesign Score</span>
              <span style={{ fontSize: 20, fontWeight: 800, color: scoreColor(research.vesign_score) }}>
                {research.vesign_score ?? '—'} <span style={{ fontSize: 14, color: 'var(--muted)', fontWeight: 400 }}>/ 100</span>
              </span>
            </div>
            <div style={{ height: 12, background: 'var(--border)', borderRadius: 99, overflow: 'hidden', marginBottom: 8 }}>
              <div style={{
                height: '100%',
                width: `${research.vesign_score ?? 0}%`,
                background: scoreColor(research.vesign_score ?? 0),
                borderRadius: 99,
                transition: 'width 0.6s ease',
              }} />
            </div>
            <div style={{ fontSize: 12, color: scoreColor(research.vesign_score ?? 0), fontWeight: 600 }}>
              {scoreLabel(research.vesign_score ?? 0)}
            </div>
          </div>

          {/* Health + Analyst panels */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 20 }}>
            {/* Health */}
            <div style={panelStyle}>
              <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 10, color: 'var(--muted)' }}>COMPANY HEALTH</div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                {[1,2,3,4,5].map(i => (
                  <div key={i} style={{
                    width: 18, height: 18, borderRadius: 4,
                    background: research.health_score != null && i <= research.health_score
                      ? '#3fb0de' : 'var(--border)',
                  }} />
                ))}
                <span style={{ fontSize: 14, fontWeight: 700, marginLeft: 4 }}>
                  {research.health_score != null ? `${research.health_score}/5` : '—'}
                </span>
              </div>
              {research.health_reason && (
                <div style={{ fontSize: 12, color: 'var(--muted)', lineHeight: 1.5 }}>
                  {research.health_reason}
                </div>
              )}
            </div>

            {/* Analyst consensus */}
            <div style={panelStyle}>
              <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 10, color: 'var(--muted)' }}>ANALYST CONSENSUS</div>
              {research.target_mean_price ? (
                <>
                  <div style={{ fontSize: 18, fontWeight: 700 }}>
                    ${fmt(research.target_mean_price)}
                    {research.fair_value_upside != null && (
                      <span style={{ fontSize: 13, marginLeft: 8, color: research.fair_value_upside >= 0 ? '#2ecc71' : '#e74c3c', fontWeight: 600 }}>
                        {fmtPct(research.fair_value_upside)}
                      </span>
                    )}
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 4 }}>
                    Range: ${fmt(research.target_low_price)} – ${fmt(research.target_high_price)}
                  </div>
                  {research.number_of_analysts && (
                    <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>
                      {research.number_of_analysts} analysts
                    </div>
                  )}
                </>
              ) : (
                <div style={{ fontSize: 13, color: 'var(--muted)' }}>No analyst data</div>
              )}
            </div>
          </div>

          {/* Trade history summary */}
          {research.trade_count > 0 && (
            <div style={{ ...panelStyle, marginBottom: 20 }}>
              <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 10, color: 'var(--muted)' }}>VESIGN TRACK RECORD</div>
              <div style={{ display: 'flex', gap: 32 }}>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700 }}>{research.trade_count}</div>
                  <div style={{ fontSize: 12, color: 'var(--muted)' }}>Trades</div>
                </div>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: research.win_rate >= 50 ? '#2ecc71' : '#e74c3c' }}>
                    {research.win_rate != null ? `${research.win_rate}%` : '—'}
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--muted)' }}>Win rate</div>
                </div>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: research.avg_return >= 0 ? '#2ecc71' : '#e74c3c' }}>
                    {research.avg_return != null ? `${research.avg_return > 0 ? '+' : ''}${research.avg_return}%` : '—'}
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--muted)' }}>Avg return</div>
                </div>
              </div>
            </div>
          )}

          {/* AI Report section */}
          <div style={{ ...panelStyle, marginBottom: 20 }}>
            <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 14 }}>AI Research Report</div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
              <input
                type="number"
                placeholder="Entry price (optional)"
                value={entryPrice}
                onChange={e => setEntryPrice(e.target.value)}
                style={{
                  padding: '8px 12px', borderRadius: 8, width: 200,
                  border: '1px solid var(--border)', background: 'var(--surface)',
                  color: 'var(--text)', fontSize: 13,
                }}
              />
              <button
                onClick={handleGenerateReport}
                disabled={loadingReport}
                style={{
                  padding: '8px 20px', borderRadius: 8, cursor: loadingReport ? 'not-allowed' : 'pointer',
                  background: loadingReport ? 'var(--border)' : 'var(--accent)',
                  border: 'none', color: '#0b0e18', fontSize: 13, fontWeight: 700,
                  opacity: loadingReport ? 0.6 : 1,
                }}
              >
                {loadingReport ? 'Generating…' : 'Generate AI Report ▶'}
              </button>
            </div>

            {loadingReport && (
              <div style={{ marginTop: 20, color: 'var(--muted)', fontSize: 13 }}>
                Analyzing {research.ticker}…
              </div>
            )}

            {report && !loadingReport && (
              <div style={{
                marginTop: 20, whiteSpace: 'pre-wrap', lineHeight: 1.75,
                fontSize: 14, color: 'var(--text)',
                borderTop: '1px solid var(--border)', paddingTop: 16,
              }}>
                {report}
              </div>
            )}
          </div>

          {/* News / Analyst Changes tabs */}
          <div style={panelStyle}>
            <div style={{ display: 'flex', gap: 0, marginBottom: 16, borderBottom: '1px solid var(--border)' }}>
              {['news', 'analyst'].map(tab => (
                <button
                  key={tab}
                  onClick={() => { if (tab !== activeTab) loadTab(tab) }}
                  style={{
                    padding: '8px 20px', background: 'transparent', border: 'none',
                    cursor: 'pointer', fontSize: 13, fontWeight: 600,
                    color: activeTab === tab ? 'var(--accent)' : 'var(--muted)',
                    borderBottom: activeTab === tab ? '2px solid var(--accent)' : '2px solid transparent',
                    marginBottom: -1,
                  }}
                >
                  {tab === 'news' ? 'News' : 'Analyst Changes'}
                </button>
              ))}
            </div>

            {loadingTab && <div style={{ color: 'var(--muted)', fontSize: 13 }}>Loading…</div>}

            {!loadingTab && activeTab === 'news' && news && (
              news.length === 0
                ? <div style={{ color: 'var(--muted)', fontSize: 13 }}>No recent news.</div>
                : news.map((item, i) => (
                  <div key={i} style={{ paddingBottom: 14, marginBottom: 14, borderBottom: i < news.length - 1 ? '1px solid var(--border)' : 'none' }}>
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{ color: 'var(--text)', fontWeight: 600, fontSize: 13, textDecoration: 'none' }}
                    >
                      {item.title}
                    </a>
                    <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 3 }}>
                      {item.site} · {item.publishedDate ? item.publishedDate.slice(0, 10) : ''}
                    </div>
                  </div>
                ))
            )}

            {!loadingTab && activeTab === 'analyst' && analystChanges && (
              analystChanges.length === 0
                ? <div style={{ color: 'var(--muted)', fontSize: 13 }}>No recent analyst changes.</div>
                : analystChanges.map((item, i) => (
                  <div key={i} style={{ paddingBottom: 12, marginBottom: 12, borderBottom: i < analystChanges.length - 1 ? '1px solid var(--border)' : 'none', fontSize: 13 }}>
                    <span style={{ fontWeight: 700 }}>{item.analystCompany || item.analyst}</span>
                    {' · '}
                    <span style={{ color: item.action === 'upgrade' ? '#2ecc71' : item.action === 'downgrade' ? '#e74c3c' : 'var(--muted)' }}>
                      {item.action || item.rating}
                    </span>
                    {item.priceTarget && <span style={{ color: 'var(--muted)' }}> · ${item.priceTarget}</span>}
                    <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 2 }}>{item.date?.slice(0, 10)}</div>
                  </div>
                ))
            )}
          </div>
        </>
      )}

      {!research && !loadingResearch && !researchError && (
        <div style={{ textAlign: 'center', color: 'var(--muted)', padding: '64px 0', fontSize: 15 }}>
          Search for a stock to get started.
        </div>
      )}

      {modalRow && (
        <SignalModal row={modalRow} onClose={() => setModalRow(null)} />
      )}
    </div>
  )
}
