import { useState, useRef, useEffect, useLayoutEffect, useContext } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { getPriceHistory, getSignalMarkers, getAnalystHistory, getNews, getSignalsByTickers, WHITE_BG_LOGOS } from '../api'
import { MarketContext } from '../context/MarketContext'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'

function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
}

const CHART_PERIODS = [1, 3, 6, 12, 24]

function monthsAgo(n) {
  const d = new Date(); d.setMonth(d.getMonth() - n); return d.toISOString().slice(0, 10)
}

export default function SignalModal({ row: rowProp, onClose }) {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)

  // Fetch full signal data if the caller passed a partial row (e.g. WatchlistPage, GlobalSearch)
  const needsSupplement = !rowProp.description_short && !rowProp.description && !rowProp.health_reason
  const { data: tickerInfo } = useQuery({
    queryKey: ['ticker-info', rowProp.ticker],
    queryFn: () => getSignalsByTickers([rowProp.ticker]).then(rows => rows?.[0] ?? null),
    enabled: needsSupplement && !!rowProp.ticker,
    staleTime: 600_000,
  })
  // Supplemental data fills missing fields; rowProp's own fields take priority
  const row = needsSupplement && tickerInfo ? { ...tickerInfo, ...rowProp } : rowProp

  const isIL      = row?.ticker?.endsWith('.TA') ?? market === 'IL'
  const currency  = isIL ? '₪' : '$'
  const priceScale = isIL ? 100 : 1  // IL prices stored in agorot, display in ₪
  const today     = new Date().toISOString().slice(0, 10)

  const [activePeriod, setActivePeriod] = useState(12)
  const [chartStart, setChartStart]     = useState(() => monthsAgo(12))
  const [chartEnd,   setChartEnd]       = useState(today)
  const [descTab, setDescTab]           = useState('info')

  function selectPeriod(months) {
    setActivePeriod(months)
    setChartStart(monthsAgo(months))
    setChartEnd(today)
  }

  // Fetch a 7-day-earlier start so we have a base price for yield even if market was closed
  const fetchStart = (() => { const d = new Date(chartStart); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()

  const { data: history = [], isLoading } = useQuery({
    queryKey: ['price-history-signal', row.ticker, fetchStart, chartEnd],
    queryFn: () => getPriceHistory(row.ticker, { start: fetchStart, end: chartEnd }),
    staleTime: 300_000,
  })

  const { data: markers = [] } = useQuery({
    queryKey: ['signal-markers', row.ticker],
    queryFn: () => getSignalMarkers(row.ticker, 13),
    staleTime: 300_000,
  })

  const { data: analystHistory = [] } = useQuery({
    queryKey: ['analyst-history', row.ticker, fetchStart, chartEnd],
    queryFn: () => getAnalystHistory(row.ticker, { start: fetchStart, end: chartEnd }),
    staleTime: 300_000,
  })

  const { data: newsData = [], isLoading: newsLoading } = useQuery({
    queryKey: ['news', row.ticker],
    queryFn: () => getNews(row.ticker, 5),
    enabled: descTab === 'news',
    staleTime: 300_000,
  })


  // Scale chart data for IL (agorot → ₪)
  const chartHistory = history.map(d => ({ ...d, close: d.close / priceScale }))

  // Merge analyst targets into chart data (forward-fill)
  const analystMap = Object.fromEntries(analystHistory.map(a => [a.date, a]))
  let lastAnalyst = null
  const chartData = chartHistory.map(d => {
    if (analystMap[d.date]) lastAnalyst = analystMap[d.date]
    if (!lastAnalyst) return d
    return {
      ...d,
      target_low:  lastAnalyst.target_low_price  != null ? lastAnalyst.target_low_price  / priceScale : undefined,
      target_mean: lastAnalyst.target_mean_price != null ? lastAnalyst.target_mean_price / priceScale : undefined,
      target_high: lastAnalyst.target_high_price != null ? lastAnalyst.target_high_price / priceScale : undefined,
    }
  })

  const basePeriod  = chartHistory.filter(d => d.date <= chartStart).at(-1) ?? chartHistory[0]
  const yieldPeriod = basePeriod && chartHistory.length > 0
    ? ((chartHistory.at(-1).close - basePeriod.close) / basePeriod.close) * 100
    : null

  const hasTargets = chartData.some(d => d.target_high != null)
  const allPrices  = [
    ...chartHistory.map(d => d.close),
    ...(hasTargets ? chartData.filter(d => d.target_low  != null).map(d => d.target_low)  : []),
    ...(hasTargets ? chartData.filter(d => d.target_high != null).map(d => d.target_high) : []),
  ]
  const minPrice = allPrices.length ? Math.min(...allPrices) * 0.97 : 0
  const maxPrice = allPrices.length ? Math.max(...allPrices) * 1.03 : 0

  useEffect(() => {
    const handler = e => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [onClose])

  const generalColRef = useRef(null)
  const [generalColH, setGeneralColH] = useState(null)
  useLayoutEffect(() => {
    if (generalColRef.current) setGeneralColH(generalColRef.current.offsetHeight)
  })

  const wrapperRef = useRef(null)
  const [wrapperWidth, setWrapperWidth] = useState(0)
  useEffect(() => {
    if (!wrapperRef.current) return
    const obs = new ResizeObserver(entries => setWrapperWidth(entries[0].contentRect.width))
    obs.observe(wrapperRef.current)
    return () => obs.disconnect()
  }, [isLoading])

  function dateToX(dateStr) {
    const idx = chartData.findIndex(d => d.date === dateStr)
    if (idx < 0 || chartData.length <= 1) return null
    const plotLeft  = 8 + 48
    const plotWidth = wrapperWidth - plotLeft - 16
    return plotLeft + (idx / (chartData.length - 1)) * plotWidth
  }

  const pairs = []
  let pendingBuy = null
  for (const m of markers) {
    if (m.signal === 'BUY') {
      if (!pendingBuy) pendingBuy = m   // keep first BUY of streak
    } else if (m.signal === 'SELL' && pendingBuy) {
      pairs.push({ buy: pendingBuy, sell: m })
      pendingBuy = null
    }
  }
  const openBuy = pendingBuy

  const PLOT_TOP    = 70
  const PLOT_BOTTOM = 332

  function priceBox(cx, value, color, byOverride) {
    const px = 8, fs = 11
    const bw = value.length * 6.8 + px * 2
    const bh = fs + 10
    const by = byOverride !== undefined ? byOverride : PLOT_TOP - bh - 4
    return (
      <g>
        <rect x={cx - bw / 2} y={by} width={bw} height={bh} rx={4} fill="var(--surface)" stroke={color} strokeWidth={1.5} />
        <text x={cx} y={by + bh / 2} textAnchor="middle" dominantBaseline="central"
          fontSize={fs} style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
          {value}
        </text>
      </g>
    )
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header" style={{ alignItems: 'flex-start', marginBottom: 8 }}>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6, flexShrink: 0 }}>
            {row.logo_url
              ? <img src={row.logo_url} alt="" className="modal-logo" style={{ width: 96, height: 96, borderRadius: 10, objectFit: 'contain', flexShrink: 0, ...(WHITE_BG_LOGOS.has(row.ticker) ? { background: '#fff', padding: 6 } : {}) }} onError={e => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex'; }} />
              : null}
            <div className="modal-logo-placeholder" style={{
              width: 96, height: 96, flexShrink: 0, borderRadius: 10,
              background: 'var(--surface)', border: '1px solid var(--border)',
              display: row.logo_url ? 'none' : 'flex',
              alignItems: 'center', justifyContent: 'center',
              fontSize: 13, fontWeight: 'bold', color: 'var(--text)',
            }}>
              {row.ticker?.replace(/\.TA$/, '')}
            </div>
            {/* Tab bar below logo */}
            <div style={{ display: 'flex', gap: 4 }}>
              {['info', 'news'].map(tab => (
                <button key={tab}
                  className={`period-chip${descTab === tab ? ' active' : ''}`}
                  onClick={() => setDescTab(tab)}
                  style={{ fontSize: 11, padding: '2px 10px' }}>
                  {tab === 'info' ? t('modal.tabInfo') : t('modal.tabNews')}
                </button>
              ))}
            </div>
          </div>
          <div ref={generalColRef} className="modal-general-col" style={{ display: 'flex', flexDirection: 'column', gap: 4, flexShrink: 0, width: 300 }}>
            <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.general')}</div>
            <div style={{ padding: '0', border: '1px solid var(--border)', borderRadius: 8 }}>
              <table style={{ fontSize: 12, borderCollapse: 'collapse', width: '100%', margin: 0, tableLayout: 'fixed' }}>
                <tbody>
                  {[
                    [t('modal.ticker'),        <strong>{row.ticker?.replace(/\.TA$/, '') ?? '—'}</strong>],
                    [t('modal.company'),       row.company ?? '—'],
                    [t('modal.industry'),      row.industry ?? '—'],
                    [t('modal.marketCap'),     row.market_cap != null ? (row.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'],
                    [t('col.signal'),          row.signal ? <span className={`badge badge-${row.signal}`}>{row.signal}</span> : '—'],
                    [t('modal.price'),         row.close != null ? fmt(row.close / priceScale) : '—'],
                    [t('modal.rsi'),           row.rsi != null ? row.rsi.toFixed(1) : '—'],
                    [t('modal.analystTarget'), (() => { const base = row.target_mean_price; const close = row.close; if (!base || !close) return '—'; const pct = ((base - close) / close) * 100; return <span className={pct >= 0 ? 'up' : 'down'}>{pct >= 0 ? '+' : ''}{pct.toFixed(1)}%</span> })()],
                    [t('modal.mlScore'),       (() => { const s = row.prediction_score; if (s == null) return '—'; const pct = s * 100; return <span className={pct >= 0 ? 'up' : 'down'}>{pct >= 0 ? '▲' : '▼'} {Math.abs(pct).toFixed(1)}%</span> })()],
                    [activePeriod ? t('modal.yieldPeriod', { months: activePeriod }) : t('modal.yieldCustom'), yieldPeriod != null ? <span className={yieldPeriod >= 0 ? 'up' : 'down'}>{yieldPeriod >= 0 ? '+' : ''}{fmt(yieldPeriod)}%</span> : '—'],
                  ].map(([label, value]) => (
                    <tr key={label}>
                      <td style={{ color: 'var(--muted)', padding: '6px 8px 6px 12px', verticalAlign: 'middle', whiteSpace: 'nowrap', width: 108 }}>{label}</td>
                      <td style={{ padding: '6px 12px 6px 0', verticalAlign: 'middle', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          {(row.description_short || row.description || row.health_score || true) && (
            <div className="modal-desc-col" style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4, overflow: 'hidden', ...(generalColH ? { height: generalColH } : {}) }}>
              {/* Info tab */}
              {descTab === 'info' && (<>
                {(row.description_short || row.description) && (<>
                  <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.description')}</div>
                  <div style={{ fontSize: 12, lineHeight: 1.6, overflowY: 'auto', flex: 1, minHeight: 0, padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8 }}>
                    {row.description_short || row.description}
                  </div>
                </>)}
                {row.health_score && (() => {
                  const labels = ['', t('health.weak'), t('health.fair'), t('health.good'), t('health.great'), t('health.excellent')]
                  const colors = ['', '#e74c3c', '#e67e22', '#f1c40f', '#2ecc71', '#1a9e55']
                  const score  = row.health_score
                  return (<>
                    <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.companyHealth')}</div>
                    <div style={{ padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflowY: 'auto' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                        {[1,2,3,4,5].map(i => (
                          <div key={i} style={{ width: 20, height: 8, borderRadius: 3, background: i <= score ? colors[score] : 'var(--border)' }} />
                        ))}
                        <span style={{ fontSize: 12, fontWeight: 'bold', color: colors[score], marginLeft: 4 }}>{labels[score]}</span>
                      </div>
                      {row.health_reason && <div style={{ fontSize: 12, lineHeight: 1.6 }}>{row.health_reason}</div>}
                    </div>
                  </>)
                })()}
              </>)}

              {/* News tab */}
              {descTab === 'news' && (<>
                <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.tabNews')}</div>
                <div style={{ fontSize: 12, lineHeight: 1.6, overflowY: 'auto', flex: 1, minHeight: 0, padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8 }}>
                  {newsLoading
                    ? <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('table.loading')}</div>
                    : newsData.length === 0
                      ? <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('modal.noNews')}</div>
                      : newsData.map((n, i) => (
                        <div key={i} style={{ paddingBottom: 8, marginBottom: 8, borderBottom: '1px solid var(--border)' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 2 }}>
                            <span style={{ fontSize: 10, color: 'var(--muted)' }}>{(n.date || '').slice(0, 10)}</span>
                            {n.source && <span style={{ fontSize: 10, background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 4, padding: '0 5px' }}>{n.source}</span>}
                          </div>
                          {n.url
                            ? <a href={n.url} target="_blank" rel="noopener noreferrer" style={{ fontSize: 12, fontWeight: 600, color: 'var(--text)', textDecoration: 'none', display: 'block', marginBottom: 2 }}>{n.title}</a>
                            : <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 2 }}>{n.title}</div>
                          }
                          {n.summary && <div style={{ fontSize: 11, color: 'var(--muted)', lineHeight: 1.4, overflow: 'hidden', display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' }}>{n.summary}</div>}
                        </div>
                      ))
                  }
                </div>
              </>)}
            </div>
          )}
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Chart */}
        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>{t('modal.loadingChart')}</p>
        ) : (
          <div ref={wrapperRef} style={{ position: 'relative', overflow: 'hidden', outline: 'none' }}>

            {/* Period selector — overlaid at top of chart, aligned with plot area */}
            <div style={{ position: 'absolute', top: 36, left: 56, right: 16, display: 'flex', alignItems: 'center', gap: 6, zIndex: 20, flexWrap: 'wrap' }}>
              {CHART_PERIODS.map(m => (
                <button key={m} className={`period-chip${activePeriod === m ? ' active' : ''}`}
                  onClick={() => selectPeriod(m)}
                >{m}M</button>
              ))}
              <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
                <input type="date" value={chartStart} max={chartEnd}
                  style={{ fontSize: 12, padding: '4px 8px', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--surface)', color: 'var(--text)', cursor: 'pointer' }}
                  onChange={e => { setChartStart(e.target.value); setActivePeriod(null) }} />
                <span style={{ fontSize: 12, color: 'var(--muted)' }}>→</span>
                <input type="date" value={chartEnd} min={chartStart} max={today}
                  style={{ fontSize: 12, padding: '4px 8px', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--surface)', color: 'var(--text)', cursor: 'pointer' }}
                  onChange={e => { setChartEnd(e.target.value); setActivePeriod(null) }} />
              </span>
            </div>
            <ResponsiveContainer width="100%" height={340}>
              <LineChart data={chartData} margin={{ top: 70, right: 16, bottom: 8, left: 8 }}>
                <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
                <XAxis
                  dataKey="date"
                  tick={{ fill: 'var(--muted)', fontSize: 11 }}
                  tickFormatter={d => { const [, m, day] = d.split('-'); return `${day}/${m}` }}
                  interval="preserveStartEnd"
                  minTickGap={50}
                />
                <YAxis
                  domain={[minPrice, maxPrice]}
                  tick={{ fill: 'var(--muted)', fontSize: 11 }}
                  tickFormatter={v => v.toFixed(0)}
                  width={48}
                />
                <Tooltip
                  contentStyle={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12 }}
                  labelStyle={{ color: 'var(--muted)' }}
                  itemStyle={{ color: 'var(--text)' }}
                  wrapperStyle={{ zIndex: 50 }}
                  labelFormatter={d => { const [y, m, day] = d.split('-'); return `${day}/${m}/${y.slice(2)}` }}
                  formatter={(v, name) => {
                    const labels = {
                      close: t('modal.chartClose'),
                      target_low: t('modal.chartTargetLow'),
                      target_mean: t('modal.chartTargetBase'),
                      target_high: t('modal.chartTargetHigh'),
                    }
                    return [v != null ? v.toFixed(2) : '—', labels[name] || name]
                  }}
                />
                <Line type="monotone" dataKey="close" stroke="var(--accent)" dot={false} strokeWidth={2} name="close" />
                {hasTargets && <>
                  <Line type="stepAfter" dataKey="target_low"  stroke="#e74c3c" dot={false} strokeWidth={1.5} strokeDasharray="5 3" name="target_low"  connectNulls={false} />
                  <Line type="stepAfter" dataKey="target_mean" stroke="#f39c12" dot={false} strokeWidth={1.5} strokeDasharray="5 3" name="target_mean" connectNulls={false} />
                  <Line type="stepAfter" dataKey="target_high" stroke="#2ecc71" dot={false} strokeWidth={1.5} strokeDasharray="5 3" name="target_high" connectNulls={false} />
                </>}
              </LineChart>
            </ResponsiveContainer>

            {wrapperWidth > 0 && chartData.length > 1 && (
              <svg style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: 340, pointerEvents: 'none', overflow: 'visible', zIndex: 10 }}>

                {pairs.map((p, i) => {
                  const buyX  = dateToX(p.buy.date)
                  const sellX = dateToX(p.sell.date)
                  const pct   = p.buy.close && p.sell.close
                    ? ((p.sell.close - p.buy.close) / p.buy.close) * 100
                    : null
                  const color = pct != null && pct >= 0 ? 'var(--green)' : 'var(--red)'
                  const lineY = PLOT_TOP + 18
                  return (
                    <g key={i}>
                      {buyX  != null && <>
                        <line x1={buyX}  y1={PLOT_TOP} x2={buyX}  y2={PLOT_BOTTOM} style={{ stroke: 'var(--green)', strokeWidth: 2 }} />
                        {p.buy.close  != null && priceBox(buyX,  fmt(p.buy.close  / priceScale, 1), 'var(--green)')}
                      </>}
                      {sellX != null && <>
                        <line x1={sellX} y1={PLOT_TOP} x2={sellX} y2={PLOT_BOTTOM} style={{ stroke: 'var(--red)',   strokeWidth: 2 }} />
                        {p.sell.close != null && priceBox(sellX, fmt(p.sell.close / priceScale, 1), 'var(--red)')}
                      </>}
                      {buyX != null && sellX != null && pct != null && (() => {
                        const label = `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`
                        const bw = label.length * 6.8 + 16
                        const plotRight = wrapperWidth - 24
                        const cx = Math.min(Math.max((buyX + sellX) / 2, 56 + bw / 2), plotRight - bw / 2)
                        const bh = 18
                        return <>
                          <line x1={buyX} y1={lineY} x2={sellX} y2={lineY}
                            style={{ stroke: color, strokeWidth: 1.5, strokeDasharray: '4 3' }} />
                          <rect x={cx - bw / 2} y={lineY + 4} width={bw} height={bh} rx={3}
                            fill="var(--surface)" stroke={color} strokeWidth={1.5} />
                          <text x={cx} y={lineY + 4 + bh / 2} textAnchor="middle" dominantBaseline="central" fontSize={10}
                            style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                            {label}
                          </text>
                        </>
                      })()}
                    </g>
                  )
                })}

                {openBuy && (() => {
                  const buyX  = dateToX(openBuy.date)
                  const lastX = dateToX(chartData.at(-1).date)
                  if (!buyX) return null
                  const currentPrice = chartData.at(-1).close
                  const pct       = openBuy.close ? ((currentPrice - openBuy.close / priceScale) / (openBuy.close / priceScale)) * 100 : null
                  const gainColor = pct != null && pct >= 0 ? 'var(--green)' : 'var(--red)'
                  const priceText = openBuy.close != null ? fmt(openBuy.close / priceScale, 1) : ''
                  const yieldText = pct != null ? `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%` : ''

                  const price_bh = 21, gain_bh = 18, gap = 4
                  const bw_price = priceText.length * 6.8 + 16
                  const bw_gain  = yieldText.length * 6.8 + 16

                  const pairBoxes = []
                  for (const p of pairs) {
                    const bx = dateToX(p.buy.date)
                    const sx = dateToX(p.sell.date)
                    if (bx != null && p.buy.close != null)
                      pairBoxes.push({ x: bx, w: fmt(p.buy.close / priceScale, 1).length * 6.8 + 16 })
                    if (sx != null && p.sell.close != null)
                      pairBoxes.push({ x: sx, w: fmt(p.sell.close / priceScale, 1).length * 6.8 + 16 })
                  }
                  const hasCollision = pairBoxes.some(b => Math.abs(buyX - b.x) < (bw_price + b.w) / 2)

                  const price_by = hasCollision
                    ? PLOT_TOP - price_bh - 4 - price_bh - 6
                    : PLOT_TOP - price_bh - 4
                  const gain_by  = price_by - gain_bh - gap

                  return (
                    <g>
                      <line x1={buyX} y1={PLOT_TOP} x2={buyX} y2={PLOT_BOTTOM} style={{ stroke: 'var(--green)', strokeWidth: 2 }} />
                      {openBuy.close != null && priceBox(buyX, priceText, 'var(--green)', price_by)}
                      {yieldText && <>
                        <rect x={buyX - bw_gain / 2} y={gain_by} width={bw_gain} height={gain_bh} rx={4}
                          fill="var(--surface)" stroke={gainColor} strokeWidth={1.5} />
                        <text x={buyX} y={gain_by + gain_bh / 2} textAnchor="middle" dominantBaseline="central"
                          fontSize={11} style={{ fill: gainColor, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                          {yieldText}
                        </text>
                      </>}
                      {(() => {
                        const midY = (PLOT_TOP + PLOT_BOTTOM) / 2
                        const tw = 32, th = 16
                        return (
                          <g transform={`rotate(-90, ${buyX}, ${midY})`}>
                            <rect x={buyX - tw / 2} y={midY - th / 2} width={tw} height={th} rx={3}
                              fill="var(--surface)" opacity={0.85} />
                            <text x={buyX} y={midY} textAnchor="middle" dominantBaseline="central"
                              fontSize={10} style={{ fill: 'var(--green)', fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                              {t('modal.open')}
                            </text>
                          </g>
                        )
                      })()}
                      {lastX != null && (
                        <line x1={buyX} y1={PLOT_TOP + 18} x2={lastX} y2={PLOT_TOP + 18}
                          style={{ stroke: gainColor, strokeWidth: 1.5, strokeDasharray: '4 3' }} />
                      )}
                    </g>
                  )
                })()}

              </svg>
            )}

            {/* Legend */}
            <div style={{ display: 'flex', gap: 16, justifyContent: 'center', flexWrap: 'wrap', padding: '6px 0 2px', fontSize: 11, color: 'var(--muted)' }}>
              {[
                { color: 'var(--accent)', dash: false, label: t('modal.chartClose') },
                ...(pairs.length > 0 || openBuy ? [
                  { color: 'var(--green)', dash: false, vertical: true, label: t('modal.legendBuy') },
                  { color: 'var(--red)',   dash: false, vertical: true, label: t('modal.legendSell') },
                ] : []),
                ...(hasTargets ? [
                  { color: '#2ecc71', dash: true, label: t('modal.chartTargetHigh') },
                  { color: '#f39c12', dash: true, label: t('modal.chartTargetBase') },
                  { color: '#e74c3c', dash: true, label: t('modal.chartTargetLow') },
                ] : []),
              ].map(({ color, dash, vertical, label }) => (
                <span key={label} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                  {vertical
                    ? <span style={{ width: 2, height: 12, background: color, borderRadius: 1, display: 'inline-block' }} />
                    : <svg width="24" height="10" style={{ display: 'inline-block', verticalAlign: 'middle' }}>
                        <line x1="0" y1="5" x2="24" y2="5"
                          stroke={color} strokeWidth="2"
                          strokeDasharray={dash ? '5 3' : undefined} />
                      </svg>
                  }
                  {label}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
