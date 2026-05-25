/* Redesign Signal Modal — React port of the mockup at
 * assets/signal-modal.js, scoped CSS at ./signal-modal.css.
 *
 * Drop-in replacement for components/SignalModal.jsx: props { row, onClose }.
 * The fake tier/wallet/paywall layer from the mockup is intentionally omitted —
 * the real app gates access via auth, so the full content always renders.
 *
 * Data mirrors research/DeepDive.jsx: getResearch (primary), getPriceHistory
 * (chart), getSignalMarkers (Signal history tab), getNews (News tab). */
import { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getResearch, getPriceHistory, getSignalMarkers, getNews, WHITE_BG_LOGOS } from '../api'
import { num, pct, dateFmt, ago, dirClass, LOGO } from './fmt'
import { useCurrency } from '../context/CurrencyContext'
import { useLivePrices } from '../hooks/useLivePrices'
import './signal-modal.css'

const sigCls = (s) => ({ BUY: 'buy', SELL: 'sell' }[s] || 'hold')
const capB = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'
const pctFrac = (f, fd = 1) => f == null ? '—' : (f * 100).toFixed(fd) + '%'
const healthDots = (n) => [0, 1, 2, 3, 4].map(i => <span key={i} className={'s' + (i < (n || 0) ? '' : ' off')} />)

const CHART_RANGES = [['1M', 1], ['3M', 3], ['1Y', 12], ['5Y', 60]]
const TABS = [
  { id: 'm-overview', label: 'Overview' },
  { id: 'm-signals', label: 'Signal history' },
  { id: 'm-news', label: 'News' },
  { id: 'm-financials', label: 'Financials' },
]

/* months window → {start,end} ISO for getPriceHistory's explicit-range path. */
function rangeBounds(months) {
  const end = new Date()
  const start = new Date()
  start.setMonth(start.getMonth() - months)
  const iso = (d) => d.toISOString().slice(0, 10)
  return { start: iso(start), end: iso(end) }
}

/* Inline-SVG price line — same path math as the mockup's renderMiniChart
 * (viewBox 600x200, top pad 18, bottom pad 16). Demo BUY/SELL markers are
 * dropped: they can't be placed without real per-signal x-positions. */
function MiniChart({ history }) {
  const out = useMemo(() => {
    if (!Array.isArray(history) || history.length < 2) return null
    const W = 600, H = 200, padT = 18, padB = 16
    const ys = history.map(d => d.close).filter(v => v != null)
    if (ys.length < 2) return null
    const min = Math.min(...ys), max = Math.max(...ys), span = (max - min) || 1
    const xFor = (i) => (i / (history.length - 1)) * W
    const yFor = (v) => padT + (1 - (v - min) / span) * (H - padT - padB)
    const ptsArr = history.map((d, i) => `${xFor(i).toFixed(1)},${yFor(d.close).toFixed(1)}`)
    const pts = ptsArr.join(' ')
    const fill = `M ${ptsArr.join(' L ')} L ${W},${H} L 0,${H} Z`
    const [ex, ey] = ptsArr[ptsArr.length - 1].split(',')
    return { pts, fill, ex, ey }
  }, [history])

  return (
    <svg viewBox="0 0 600 200" preserveAspectRatio="none">
      <defs>
        <linearGradient id="m-grad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#60a5fa" stopOpacity="0.22" />
          <stop offset="100%" stopColor="#60a5fa" stopOpacity="0" />
        </linearGradient>
        <filter id="m-glow"><feGaussianBlur stdDeviation="2.5" /></filter>
      </defs>
      <line x1="0" x2="600" y1="40" y2="40" stroke="rgba(255,255,255,0.04)" />
      <line x1="0" x2="600" y1="80" y2="80" stroke="rgba(255,255,255,0.04)" />
      <line x1="0" x2="600" y1="120" y2="120" stroke="rgba(255,255,255,0.04)" />
      <line x1="0" x2="600" y1="160" y2="160" stroke="rgba(255,255,255,0.04)" />
      <path d={out?.fill || ''} fill="url(#m-grad)" />
      <polyline fill="none" stroke="#60a5fa" strokeWidth="1.8" strokeLinejoin="round" points={out?.pts || ''} />
      {out && <circle cx={out.ex} cy={out.ey} r="8" fill="#60a5fa" opacity="0.45" filter="url(#m-glow)" />}
      {out && <circle cx={out.ex} cy={out.ey} r="3" fill="#60a5fa" />}
    </svg>
  )
}

export default function SignalModalRd({ row, onClose }) {
  const { fmtPrice, symbol } = useCurrency()
  const ticker = row?.ticker
  const [tab, setTab] = useState('m-overview')
  const [range, setRange] = useState('1Y')
  const months = CHART_RANGES.find(([l]) => l === range)?.[1] || 12

  // Escape to close — document listener (no focusable overlay → no focus ring).
  useEffect(() => {
    const h = (e) => { if (e.key === 'Escape') onClose?.() }
    document.addEventListener('keydown', h)
    return () => document.removeEventListener('keydown', h)
  }, [onClose])

  // Overview/Financials/strip — primary data source.
  const { data: r } = useQuery({
    queryKey: ['research', ticker], queryFn: () => getResearch(ticker), enabled: !!ticker,
  })
  // Price chart (refetches per active range chip).
  const { data: history } = useQuery({
    queryKey: ['price-history', ticker, months],
    queryFn: () => getPriceHistory(ticker, rangeBounds(months)),
    enabled: !!ticker,
  })
  // Lazy: only fetch markers/news when their tab is shown.
  const { data: markers } = useQuery({
    queryKey: ['markers', ticker], queryFn: () => getSignalMarkers(ticker, 60),
    enabled: !!ticker && tab === 'm-signals',
  })
  const { data: news } = useQuery({
    queryKey: ['news', ticker], queryFn: () => getNews(ticker, 8),
    enabled: !!ticker && tab === 'm-news',
  })

  const close = r?.close ?? row?.price ?? null
  // Live current price (overlaid intraday); entry/target prices stay as stored.
  const { prices: livePx } = useLivePrices(ticker ? [ticker] : [])
  const liveClose = livePx[ticker] ?? close
  const up = r?.fair_value_upside == null ? null : r.fair_value_upside * 100
  const ml = r?.prediction_score == null ? null : r.prediction_score * 100
  const lo = r?.target_low_price, mean = r?.target_mean_price, hi = r?.target_high_price
  const posFn = (lo != null && hi != null && hi > lo)
    ? (v) => Math.max(0, Math.min(100, (v - lo) / (hi - lo) * 100)) : null

  const histRows = Array.isArray(markers)
    ? markers.slice().sort((a, b) => (a.date < b.date ? 1 : -1)).slice(0, 8) : []

  const company = r?.company || row?.company || '—'
  const whiteBg = ticker && WHITE_BG_LOGOS.has(ticker)
  const sector = [r?.sector, r?.industry].filter(Boolean).join(' · ') || '—'

  return (
    <div className="modal-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose?.() }}>
      <div className="modal" role="dialog" aria-modal="true">

        {/* HEAD */}
        <div className="m-head">
          <div className="m-head-left">
            <img className={'m-logo' + (whiteBg ? '' : ' transparent')} src={LOGO(ticker)} alt={ticker || ''} />
            <div className="m-id">
              <div className="tk">{r?.ticker || ticker || '—'}</div>
              <div className="co">{company}</div>
              <div className="meta">
                {r?.exchange && <span className="pill">{r.exchange}</span>}
                <span className="pill">{sector}</span>
                {r?.domain && <a href={'https://' + r.domain} target="_blank" rel="noreferrer">{r.domain} ↗</a>}
              </div>
            </div>
          </div>
          <div />
          <div className="m-price">
            <div className="px"><span className="s">{symbol}</span>{liveClose == null ? '—' : fmtPrice(liveClose).replace(symbol, '')}</div>
            <div className={'delta ' + dirClass(ml)}>{ml == null ? '—' : pct(ml)} <span className="abs">ML 5-day</span></div>
          </div>
          <div className="m-close" aria-label="Close" onClick={() => onClose?.()}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round"><path d="M6 6l12 12M18 6L6 18" /></svg>
          </div>
        </div>

        {/* SIGNAL STRIP */}
        <div className="m-signal-strip">
          <div className="strip-default" style={{ display: 'contents' }}>
            <span className={'m-sig-tag ' + sigCls(r?.signal)}>{r?.signal || '—'}</span>
            <div className="m-sig-note">
              VQS <span className="vqs">{r?.vesign_score ?? '—'}</span> · Predicted upside <span className="hl" style={{ color: 'var(--green, #00d97e)' }}>{up == null ? '—' : pct(up)}</span> · Health <span className="hl">{r?.health_score ?? '—'} / 5</span>
            </div>
            <div className="m-sig-since">{r?.trade_count ? <>{r.trade_count} historical trade{r.trade_count === 1 ? '' : 's'}</> : ''}</div>
          </div>
        </div>

        {/* TABS */}
        <div className="m-tabs">
          {TABS.map(t => (
            <a key={t.id} className={'m-tab' + (tab === t.id ? ' active' : '')}
              onClick={(e) => { e.preventDefault(); setTab(t.id) }}>
              {t.label}
              {t.id === 'm-signals' && histRows.length > 0 && <span className="count">{histRows.length}</span>}
              {t.id === 'm-news' && Array.isArray(news) && news.length > 0 && <span className="count">{news.length}</span>}
            </a>
          ))}
        </div>

        {/* BODY */}
        <div className="m-body">

          {/* OVERVIEW */}
          <div className={'m-pane' + (tab === 'm-overview' ? ' active' : '')} id="m-overview">
            <div className="m-overview-top">
              <div className="m-chart">
                <div className="m-chart-head">
                  <h4>Price · Vesign signals</h4>
                  <div className="chips">
                    {CHART_RANGES.map(([lbl]) => (
                      <span key={lbl} className={'chip' + (lbl === range ? ' active' : '')}
                        onClick={() => setRange(lbl)}>{lbl}</span>
                    ))}
                  </div>
                </div>
                <div className="m-chart-body">
                  <MiniChart history={history} />
                </div>
              </div>

              <div className="m-verdict">
                <div className="m-vrow"><div className="lbl">VQS score<small>Vesign quality (1–10)</small></div><div className="val purple">{r?.vesign_score ?? '—'} / 10</div></div>
                <div className="m-vrow"><div className="lbl">Predicted upside<small>to analyst mean</small></div><div className={'val ' + dirClass(up)}>{up == null ? '—' : pct(up)}</div></div>
                <div className="m-vrow"><div className="lbl">Health<small>5-point score</small></div><div className="val"><span className="health">{healthDots(r?.health_score)}</span></div></div>
                <div className="m-vrow"><div className="lbl">ML 5-day<small>walk-forward v2.4</small></div><div className={'val ' + dirClass(ml)}>{ml == null ? '—' : pct(ml)}</div></div>
                <div className="m-vrow"><div className="lbl">Entry price<small>Vesign target</small></div><div className="val">{close == null ? '—' : fmtPrice(close)}</div></div>
              </div>
            </div>

            <h4 className="m-section-h">Fundamentals</h4>
            <div className="m-fund-mini">
              <div className="m-fund-cell"><div className="l">Market Cap</div><div className="v">{capB(r?.market_cap)}</div></div>
              <div className="m-fund-cell"><div className="l">P/E (TTM)</div><div className="v">{r?.pe_ttm == null ? '—' : num(r.pe_ttm, { fd: 1 })}</div></div>
              <div className="m-fund-cell"><div className="l">EPS (TTM)</div><div className="v">{r?.eps_ttm == null ? '—' : fmtPrice(r.eps_ttm)}</div></div>
              <div className="m-fund-cell"><div className="l">Revenue (TTM)</div><div className="v">{capB(r?.revenue_ttm)}</div></div>
              <div className="m-fund-cell"><div className="l">Revenue growth</div><div className={'v ' + dirClass(r?.revenue_growth)}>{r?.revenue_growth == null ? '—' : (r.revenue_growth >= 0 ? '+' : '') + (r.revenue_growth * 100).toFixed(0) + '%'}<small>YoY</small></div></div>
              <div className="m-fund-cell"><div className="l">Net margin</div><div className="v">{pctFrac(r?.net_margin)}</div></div>
              <div className="m-fund-cell"><div className="l">ROE</div><div className="v">{pctFrac(r?.roe)}</div></div>
              <div className="m-fund-cell"><div className="l">D/E</div><div className="v">{r?.de_ratio == null ? '—' : num(r.de_ratio, { fd: 2 })}</div></div>
            </div>

            <h4 className="m-section-h with-meta">Analyst targets <span className="meta">{r?.number_of_analysts ? `${Math.round(r.number_of_analysts)} analysts · I/B/E/S` : '—'}</span></h4>
            <div className="m-analyst">
              <div className="top">
                <span className="ttl">{(lo != null && hi != null) ? `Range ${fmtPrice(lo, 0)} → ${fmtPrice(hi, 0)}` : 'Range —'}</span>
                <span className="mean">{mean != null ? `Mean target ${fmtPrice(mean, 0)}` + (up != null ? ` · ${pct(up)}` : '') : 'Mean —'}</span>
              </div>
              <div className="bar">
                <div className="range" style={{ left: '0%', right: '0%' }} />
                {posFn && liveClose != null && <div className="mark cur" style={{ left: posFn(liveClose) + '%' }} />}
                {posFn && mean != null && <div className="mark mean" style={{ left: posFn(mean) + '%' }} />}
              </div>
              <div className="labels">
                <div className="col"><span className="l">Low</span><span className="v">{lo != null ? fmtPrice(lo, 0) : '—'}</span></div>
                <div className="col c"><span className="l">Current</span><span className="v gold">{liveClose != null ? fmtPrice(liveClose) : '—'}</span></div>
                <div className="col c"><span className="l">Mean</span><span className="v">{mean != null ? fmtPrice(mean, 0) : '—'}</span></div>
                <div className="col r"><span className="l">High</span><span className="v">{hi != null ? fmtPrice(hi, 0) : '—'}</span></div>
              </div>
            </div>
          </div>

          {/* SIGNAL HISTORY */}
          <div className={'m-pane' + (tab === 'm-signals' ? ' active' : '')} id="m-signals">
            <h4 className="m-section-h with-meta">Vesign signal history <span className="meta">
              {r?.trade_count
                ? `${r.trade_count} closed trade${r.trade_count === 1 ? '' : 's'}` + (r.win_rate != null ? ` · Win rate ${r.win_rate.toFixed(0)}%` : '')
                : `No closed trades on ${ticker || ''} yet`}
            </span></h4>
            <div className="m-history">
              {histRows.length ? histRows.map((m, i) => {
                const s = (m.signal || '').toUpperCase()
                const mu = m.fair_value_upside == null ? null : m.fair_value_upside * 100
                return (
                  <div className="m-h-row" key={i}>
                    <div className={'sig ' + sigCls(s)}>{s}</div>
                    <div className="date">{dateFmt(m.date)}</div>
                    <div className="note">
                      {m.vqs != null && <>VQS <span className="vqs">{m.vqs}</span> · </>}
                      {mu != null && `Pred upside ${pct(mu)} · `}
                      {m.health_score != null ? `Health ${m.health_score}/5` : ''}
                    </div>
                    <div className="px"><span className="l">{s === 'SELL' ? 'Exit' : 'Entry'}</span>{m.close == null ? '—' : fmtPrice(m.close)}</div>
                    <div className="ret open">—</div>
                  </div>
                )
              }) : (
                <div className="m-h-row"><div className="note" style={{ gridColumn: '1 / -1', color: 'var(--ink-3)' }}>No Vesign signal history for {ticker || 'this ticker'}.</div></div>
              )}
            </div>
          </div>

          {/* NEWS */}
          <div className={'m-pane' + (tab === 'm-news' ? ' active' : '')} id="m-news">
            <h4 className="m-section-h with-meta">Recent news <span className="meta">Latest</span></h4>
            <div className="m-news">
              {Array.isArray(news) && news.length ? news.map((n, i) => (
                <div className="m-n-row" key={i} onClick={() => { if (n.url) window.open(n.url, '_blank') }}>
                  <div>
                    <div className="headline">{n.title || ''}</div>
                    <div className="meta"><span className="src">{n.source || ''}</span></div>
                  </div>
                  <div className="time">{ago(n.date)}</div>
                </div>
              )) : (
                <div className="m-n-row"><div className="headline" style={{ color: 'var(--ink-3)' }}>No recent news.</div></div>
              )}
            </div>
          </div>

          {/* FINANCIALS */}
          <div className={'m-pane' + (tab === 'm-financials' ? ' active' : '')} id="m-financials">
            <h4 className="m-section-h with-meta">Full fundamentals <span className="meta">TTM · last reported</span></h4>
            <div className="m-fund-mini" style={{ gridTemplateColumns: 'repeat(3, 1fr)' }}>
              <div className="m-fund-cell"><div className="l">Market Cap</div><div className="v">{capB(r?.market_cap)}</div></div>
              <div className="m-fund-cell"><div className="l">P/E (TTM)</div><div className="v">{r?.pe_ttm == null ? '—' : num(r.pe_ttm, { fd: 1 })}</div></div>
              <div className="m-fund-cell"><div className="l">EPS (TTM)</div><div className="v">{r?.eps_ttm == null ? '—' : fmtPrice(r.eps_ttm)}</div></div>
              <div className="m-fund-cell"><div className="l">Revenue (TTM)</div><div className="v">{capB(r?.revenue_ttm)}</div></div>
              <div className="m-fund-cell"><div className="l">Revenue growth</div><div className={'v ' + dirClass(r?.revenue_growth)}>{r?.revenue_growth == null ? '—' : (r.revenue_growth >= 0 ? '+' : '') + (r.revenue_growth * 100).toFixed(0) + '%'}</div></div>
              <div className="m-fund-cell"><div className="l">Gross margin</div><div className="v">{pctFrac(r?.gross_margin)}</div></div>
              <div className="m-fund-cell"><div className="l">Op margin</div><div className="v">{pctFrac(r?.op_margin)}</div></div>
              <div className="m-fund-cell"><div className="l">Net margin</div><div className="v">{pctFrac(r?.net_margin)}</div></div>
              <div className="m-fund-cell"><div className="l">ROE</div><div className="v">{pctFrac(r?.roe)}</div></div>
              <div className="m-fund-cell"><div className="l">D/E</div><div className="v">{r?.de_ratio == null ? '—' : num(r.de_ratio, { fd: 2 })}</div></div>
              <div className="m-fund-cell"><div className="l">RSI</div><div className="v">{r?.rsi == null ? '—' : num(r.rsi, { fd: 1 })}</div></div>
              <div className="m-fund-cell"><div className="l">BB %B</div><div className="v">{r?.bb_pct_b == null ? '—' : num(r.bb_pct_b, { fd: 2 })}</div></div>
            </div>
          </div>

        </div>

        {/* FOOT */}
        <div className="m-foot">
          <div className="actions">
            <div className="btn">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M19 21l-7-5-7 5V5a2 2 0 012-2h10a2 2 0 012 2z" /></svg>
              Add to watchlist
            </div>
            <div className="btn">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 3v18h18 M7 14l4-4 4 4 5-5" /></svg>
              Compare
            </div>
          </div>
          <div className="spacer" />
          <a className="link">Open full research →</a>
          <div className="btn primary">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 5v14m-7-7h14" /></svg>
            New trade
          </div>
        </div>

      </div>
    </div>
  )
}
