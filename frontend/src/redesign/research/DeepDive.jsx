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
import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getResearch, getPriceHistory, getEarnings, getNews, getSignalMarkers, searchTickers } from '../../api'
import { num, pct, dateFmt, ago, LOGO } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'

const sigCls = (s) => ({ BUY: 'buy', SELL: 'sell' }[s] || 'hold')
const dirCls = (v) => v == null ? '' : v > 0 ? 'up' : v < 0 ? 'down' : 'muted'
const capB = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'
const healthDots = (n) => [0, 1, 2, 3, 4].map(i => <span key={i} className={'s' + (i < (n || 0) ? '' : ' off')} />)
// margins/roe/growth come back as raw fractions
const pctFrac = (f, fd = 1) => f == null ? '—' : (f * 100).toFixed(fd) + '%'
const RANGES = [['1M', 1], ['3M', 3], ['6M', 6], ['1Y', 12], ['5Y', 60], ['ALL', 60]]
const RECENT = ['NVDA', 'META', 'MTD', 'MU', 'AAPL']

/* Inline-SVG price line — identical path math to the mockup's paintPriceChart. */
function PriceChart({ history }) {
  const W = 800, H = 340
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
    const yLabels = [0, 1, 2, 3, 4].map(i => '$' + num(max - i / 4 * span, { fd: 0 }))
    const N = history.length
    const xLabels = [0, 1, 2, 3, 4, 5, 6].map(i => {
      const dt = new Date(history[Math.round(i / 6 * (N - 1))].date)
      return dt.toLocaleDateString(undefined, { month: 'short' })
    })
    return { pts, fill, dx, dy, yLabels, xLabels }
  }, [history])

  return (
    <>
      <div className="dd-chart-body">
        <div className="dd-y-axis" style={{ left: 18, right: 'auto' }}>
          {(out?.yLabels || ['—', '—', '—', '—', '—']).map((l, i) => <span key={i}>{l}</span>)}
        </div>
        <svg viewBox="0 0 800 340" preserveAspectRatio="none">
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
          {out && <circle cx={out.dx} cy={out.dy} r="4" fill="#60a5fa" />}
        </svg>
      </div>
      <div className="dd-x-axis">
        {(out?.xLabels || []).map((l, i) => <span key={i}>{l}</span>)}
      </div>
    </>
  )
}

export default function DeepDive({ ticker, setTicker }) {
  const { fmtPrice, symbol } = useCurrency()
  const [range, setRange] = useState('1Y')   // active chart-range chip label
  const months = RANGES.find(([l]) => l === range)?.[1] || 12
  const [input, setInput] = useState(ticker)
  const [sugQ, setSugQ] = useState('')

  const { data: r } = useQuery({ queryKey: ['research', ticker], queryFn: () => getResearch(ticker), enabled: !!ticker })
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

  const submit = (t) => {
    const v = (t || '').trim().toUpperCase()
    if (!v) return
    setTicker(v); setInput(v); setSugQ('')
  }

  const close = r?.close
  const ml = r?.prediction_score == null ? null : r.prediction_score * 100
  const up = r?.fair_value_upside == null ? null : r.fair_value_upside * 100
  const lo = r?.target_low_price, mean = r?.target_mean_price, hi = r?.target_high_price
  const posFn = (lo != null && hi != null && hi > lo) ? (v) => Math.max(0, Math.min(100, (v - lo) / (hi - lo) * 100)) : null
  const nextEarn = Array.isArray(earnings) && earnings.length ? earnings[0] : null

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
          <input className="ticker-input" type="text" placeholder="Type a ticker — e.g. AAPL, MSFT, GOOGL…"
            value={input}
            onChange={(e) => { setInput(e.target.value); setSugQ(e.target.value) }}
            onKeyDown={(e) => { if (e.key === 'Enter') submit(input) }}
            style={{ width: '100%' }} />
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
          <img className="dd-hero-logo" src={LOGO(ticker)} alt={ticker} />
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
          <div className={'delta ' + dirCls(ml)}>{ml == null ? '—' : pct(ml)}</div>
          <div className="dd-hero-actions">
            <div className="btn">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M19 21l-7-5-7 5V5a2 2 0 012-2h10a2 2 0 012 2z" /></svg>
              Watchlist
            </div>
            <div className="btn">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 3v18h18 M7 14l4-4 4 4 5-5" /></svg>
              Compare
            </div>
            <div className="btn primary">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 5v14m-7-7h14" /></svg>
              New trade
            </div>
          </div>
        </div>
      </div>

      {/* CHART + VERDICT */}
      <div className="dd-chart-row">
        <div className="dd-chart-panel">
          <div className="dd-chart-head">
            <h3>Price · Vesign signals overlay</h3>
            <div className="legend">
              <span className="lg-item"><span className="sw" style={{ background: '#60a5fa' }} /> {ticker} price</span>
              <span className="lg-item"><span className="sw tri" style={{ background: '#00d97e' }} /> BUY signal</span>
              <span className="lg-item"><span className="sw tri" style={{ background: '#ff4d5c' }} /> SELL signal</span>
            </div>
            <div className="chips">
              {RANGES.map(([lbl]) => (
                <span key={lbl} className={'chip' + (lbl === range ? ' active' : '')}
                  onClick={() => setRange(lbl)}>{lbl}</span>
              ))}
            </div>
          </div>
          <PriceChart history={history} />
        </div>

        {/* VERDICT */}
        <div className="dd-verdict">
          <div className="dd-verdict-head">
            <span className={'sig-tag ' + sigCls(r?.signal)}>{r?.signal || '—'}</span>
            <span className="since">{r?.trade_count ? `${r.trade_count} historical trade${r.trade_count === 1 ? '' : 's'}` : ''}</span>
          </div>
          <div className="dd-verdict-body">
            <div className="dd-vstat">
              <div className="lbl">VQS score <span className="desc">Vesign quality score (1–10)</span></div>
              <div className="val purple"><span>{r?.vesign_score ?? '—'}</span> <small style={{ color: 'var(--ink-3)', fontSize: 11 }}>/10</small></div>
            </div>
            <div className="dd-vstat">
              <div className="lbl">Predicted upside <span className="desc">to analyst mean target</span></div>
              <div className={'val ' + dirCls(up)}>{up == null ? '—' : pct(up)}</div>
            </div>
            <div className="dd-vstat">
              <div className="lbl">Health <span className="desc">balance sheet · profitability</span></div>
              <div className="val"><span className="health">{healthDots(r?.health_score)}</span></div>
            </div>
            <div className="dd-vstat">
              <div className="lbl">ML 5-day</div>
              <div className={'val ' + dirCls(ml)}>{ml == null ? '—' : pct(ml)}</div>
            </div>
            <div className="dd-vstat">
              <div className="lbl">Next earnings</div>
              <div className="val" style={{ fontSize: 14 }}>{nextEarn?.date ? dateFmt(nextEarn.date) : '—'}</div>
            </div>
            <div className="dd-vstat">
              <div className="lbl">In your watchlists</div>
              <div className="val" style={{ fontSize: 12 }}>
                <span className="sector-pill" style={{ display: 'inline-block', padding: '2px 8px', borderRadius: 3, fontFamily: 'var(--mono)', fontSize: 10.5, background: 'var(--bg-3)', color: 'var(--ink-2)', marginLeft: 4 }}>Core Tech</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* FUNDAMENTALS */}
      <div>
        <div className="section-h" style={{ padding: '0 4px' }}>
          <h2>Fundamentals</h2>
          <span className="sub">TTM · last reported</span>
          <a className="right" href="#">View full financials →</a>
        </div>
        <div className="dd-fund-grid">
          <div className="dd-fund-cell"><div className="l">Market Cap</div><div className="v">{capB(r?.market_cap)}</div></div>
          <div className="dd-fund-cell"><div className="l">P/E (TTM)</div><div className="v">{r?.pe_ttm == null ? '—' : num(r.pe_ttm, { fd: 1 })}</div></div>
          <div className="dd-fund-cell"><div className="l">EPS (TTM)</div><div className="v">{r?.eps_ttm == null ? '—' : fmtPrice(r.eps_ttm)}</div></div>
          <div className="dd-fund-cell"><div className="l">Revenue (TTM)</div><div className="v">{r?.revenue_ttm == null ? '—' : capB(r.revenue_ttm)}</div></div>
          <div className="dd-fund-cell"><div className="l">Revenue growth</div><div className={'v ' + dirCls(r?.revenue_growth)}>{r?.revenue_growth == null ? '—' : (r.revenue_growth >= 0 ? '+' : '') + (r.revenue_growth * 100).toFixed(0) + '%'}<small>YoY</small></div></div>
          <div className="dd-fund-cell"><div className="l">Gross margin</div><div className="v">{pctFrac(r?.gross_margin)}</div></div>
          <div className="dd-fund-cell"><div className="l">Op. margin</div><div className="v">{pctFrac(r?.op_margin)}</div></div>
          <div className="dd-fund-cell"><div className="l">Net margin</div><div className="v">{pctFrac(r?.net_margin)}</div></div>
          <div className="dd-fund-cell"><div className="l">ROE</div><div className="v">{pctFrac(r?.roe)}</div></div>
          <div className="dd-fund-cell"><div className="l">Debt / Equity</div><div className="v">{r?.de_ratio == null ? '—' : num(r.de_ratio, { fd: 2 })}</div></div>
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
            <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between' }}>
              <div style={{ fontSize: 12.5, color: 'var(--ink-2)' }}>{(lo != null && hi != null) ? `Range: ${fmtPrice(lo, 0)} → ${fmtPrice(hi, 0)}` : 'Range: —'}</div>
              <div style={{ fontFamily: 'var(--mono)', fontSize: 13, color: 'var(--ink)', fontWeight: 600 }}>{mean != null ? `Mean: ${fmtPrice(mean, 0)}` : 'Mean: —'}</div>
            </div>
            <div className="dd-an-target-bar">
              <div className="dd-an-range" style={{ left: '0%', right: '0%' }} />
              {posFn && close != null && <div className="dd-an-tooltip" style={{ left: posFn(close) + '%' }}>Current {fmtPrice(close)}</div>}
              {posFn && close != null && <div className="dd-an-mark cur" style={{ left: posFn(close) + '%' }} />}
              {posFn && mean != null && <div className="dd-an-mark mean" style={{ left: posFn(mean) + '%' }} />}
            </div>
            <div className="dd-an-labels">
              <span>{lo != null ? fmtPrice(lo, 0) : '—'}<br /><span className="v">Low</span></span>
              <span>{close != null ? fmtPrice(close) : '—'}<br /><span className="v" style={{ color: 'var(--gold)' }}>Current</span></span>
              <span>{mean != null ? fmtPrice(mean, 0) : '—'}<br /><span className="v">Mean</span></span>
              <span>{hi != null ? fmtPrice(hi, 0) : '—'}<br /><span className="v">High</span></span>
            </div>
            <div className="dd-an-rec">
              <div className="cell"><div className="l">Upside to mean</div><div className={'v ' + dirCls(up)}>{up == null ? '—' : pct(up)}</div></div>
              <div className="cell"><div className="l">Analysts</div><div className="v">{r?.number_of_analysts ? Math.round(r.number_of_analysts) : '—'}</div></div>
              <div className="cell"><div className="l">VQS</div><div className="v">{r?.vesign_score ?? '—'}<span className="sub">/10</span></div></div>
            </div>
          </div>
        </div>

        {/* ML PREDICTIONS */}
        <div className="dd-panel">
          <div className="dd-panel-head">
            <h3>ML predictions</h3>
            <span className="meta">Walk-forward model · v2.4</span>
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
            <div className="dd-ml-row">
              <div className="top">
                <span className="lbl">20-day return</span>
                <span className="pred up">+8.2%</span>
              </div>
              <div className="bar"><span className="fill" style={{ width: '41%' }} /></div>
              <div className="conf">
                <span>Confidence <span className="v">Medium</span></span>
                <span>Range +4.4% — +12.8%</span>
              </div>
            </div>
            <div className="dd-ml-row">
              <div className="top">
                <span className="lbl">Direction probability (5d)</span>
                <span className="pred up">78% UP</span>
              </div>
              <div className="bar"><span className="fill" style={{ width: '28%' }} /></div>
              <div className="conf">
                <span>Up <span className="v">78%</span></span>
                <span>Down <span className="v">22%</span></span>
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
          <a className="right" href="#">View all signals →</a>
        </div>
        <div className="dd-panel">
          <div className="dd-history-body">
            {histRows.length ? histRows.map((m, i) => {
              const s = (m.signal || '').toUpperCase()
              return (
                <div className="dd-hrow" key={i}>
                  <div className={'sig ' + sigCls(s)}>{s}</div>
                  <div className="date">{dateFmt(m.date)}</div>
                  <div className="note">
                    {m.vqs != null && <>VQS <span className="vqs">{m.vqs}</span> · </>}
                    {m.fair_value_upside != null && `Pred upside ${pct(m.fair_value_upside * 100)} · `}
                    {m.health_score != null ? `Health ${m.health_score}/5` : ''}
                  </div>
                  <div className="px"><span className="l">{s === 'SELL' ? 'Exit' : 'Entry'}</span>{m.close == null ? '—' : fmtPrice(m.close)}</div>
                  <div className="ret open">—</div>
                  <div className="days" />
                </div>
              )
            }) : (
              <div className="dd-hrow"><div className="note" style={{ color: 'var(--ink-3)', gridColumn: '1 / -1' }}>No Vesign signal history for {ticker}.</div></div>
            )}
          </div>
        </div>
      </div>

      {/* RECENT NEWS */}
      <div>
        <div className="section-h" style={{ padding: '0 4px' }}>
          <h2>Recent news</h2>
          <span className="sub">Latest</span>
          <a className="right" href="#">All {ticker} news →</a>
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
