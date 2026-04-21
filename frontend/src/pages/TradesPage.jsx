import { useState, useRef, useEffect, useLayoutEffect, useContext } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { getTrades, getOpenTrades, getPriceHistory, getAnalystHistory, getNews, WHITE_BG_LOGOS } from '../api'
import { useSort } from '../hooks/useSort'
import { useLivePrices } from '../hooks/useLivePrices'
import { MarketContext } from '../context/MarketContext'

function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
}

function fmtDate(str) {
  if (!str) return '—'
  const d = new Date(str)
  const dd = String(d.getDate()).padStart(2, '0')
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const yy = String(d.getFullYear()).slice(2)
  return `${dd}/${mm}/${yy}`
}

function isoMonthsAgo(n) {
  const d = new Date()
  d.setMonth(d.getMonth() - n)
  return d.toISOString().slice(0, 10)
}

function countTradingDays(startStr, endStr) {
  let count = 0
  const d = new Date(startStr)
  const end = new Date(endStr)
  while (d <= end) {
    const day = d.getDay()
    if (day !== 0 && day !== 6) count++
    d.setDate(d.getDate() + 1)
  }
  return count
}

function Pagination({ page, pages, onChange }) {
  if (pages <= 1) return null
  return (
    <div className="pagination">
      <button disabled={page === 1} onClick={() => onChange(1)}>«</button>
      <button disabled={page === 1} onClick={() => onChange(page - 1)}>‹ Prev</button>
      <span>{page} / {pages}</span>
      <button disabled={page === pages} onClick={() => onChange(page + 1)}>Next ›</button>
      <button disabled={page === pages} onClick={() => onChange(pages)}>»</button>
    </div>
  )
}

function Th({ label, col, sort, onSort, className }) {
  const active = sort.key === col
  return (
    <th onClick={() => onSort(col)} style={{ cursor: 'pointer' }} className={className}>
      {label}{active ? <span style={{ marginLeft: 3 }}>{sort.dir === 'asc' ? '▲' : '▼'}</span> : null}
    </th>
  )
}

// ---------------------------------------------------------------------------
// Custom price label for chart reference lines
// ---------------------------------------------------------------------------

function PriceBoxLabel({ viewBox, value, color }) {
  if (!value || !viewBox) return null
  const { x, y } = viewBox
  const px = 8, fontSize = 11
  const boxW = value.length * 6.8 + px * 2
  const boxH = fontSize + 10
  const boxY = y - boxH - 4
  return (
    <g>
      <rect
        x={x - boxW / 2}
        y={boxY}
        width={boxW}
        height={boxH}
        rx={4}
        fill="var(--surface)"
        stroke={color}
        strokeWidth={1.5}
      />
      <text
        x={x}
        y={boxY + boxH / 2}
        textAnchor="middle"
        dominantBaseline="central"
        fill={color}
        fontSize={fontSize}
        fontWeight="700"
        fontFamily="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
      >
        {value}
      </text>
    </g>
  )
}

// ---------------------------------------------------------------------------
// Trade chart modal — supports multiple BUY/SELL pairs
// ---------------------------------------------------------------------------

function TradeModal({ row, start, end, onClose }) {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const isIL      = row.ticker?.endsWith('.TA') ?? market === 'IL'
  const currency  = isIL ? '₪' : '$'
  const priceScale = isIL ? 100 : 1

  const [descTab, setDescTab] = useState('info')

  const today = new Date().toISOString().slice(0, 10)
  const [activePeriod, setActivePeriod] = useState(12)
  const [chartStart, setChartStart]     = useState(() => { const d = new Date(); d.setMonth(d.getMonth() - 12); return d.toISOString().slice(0, 10) })
  const [chartEnd,   setChartEnd]       = useState(today)

  function selectPeriod(months) {
    setActivePeriod(months)
    const d = new Date(); d.setMonth(d.getMonth() - months)
    setChartStart(d.toISOString().slice(0, 10))
    setChartEnd(today)
  }

  // Fetch full 5Y history once; period switches slice client-side (no refetch)
  const fullStart5y = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 5); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()
  const { data: fullHistory = [], isLoading } = useQuery({
    queryKey: ['price-history', row.ticker],
    queryFn: () => getPriceHistory(row.ticker, { start: fullStart5y, end: today }),
    staleTime: 300_000,
  })
  const history = fullHistory.filter(p => {
    const d = p.date?.slice(0, 10) || p.date
    return d >= chartStart && d <= chartEnd
  })

  const end12m    = new Date().toISOString().slice(0, 10)
  const target12m = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); return d.toISOString().slice(0, 10) })()
  const start12m  = (() => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()
  const { data: history12m = [] } = useQuery({
    queryKey: ['price-history-12m', row.ticker],
    queryFn: () => getPriceHistory(row.ticker, { start: start12m, end: end12m }),
    staleTime: 300_000,
  })
  const base12m  = history12m.filter(d => d.date <= target12m).at(-1)
  const yield12m = base12m && history12m.length > 0
    ? ((history12m.at(-1).close - base12m.close) / base12m.close) * 100
    : null

  const { data: newsData = [], isLoading: newsLoading } = useQuery({
    queryKey: ['news', row.ticker],
    queryFn: () => getNews(row.ticker, 5),
    enabled: descTab === 'news',
    staleTime: 300_000,
  })

  // Full 5Y analyst history (sliced client-side to current chart window)
  const { data: fullAnalystHistory = [] } = useQuery({
    queryKey: ['analyst-history', row.ticker],
    queryFn: () => getAnalystHistory(row.ticker, { start: fullStart5y, end: today }),
    staleTime: 300_000,
  })
  const analystHistory = fullAnalystHistory.filter(a => {
    const d = a.date?.slice(0, 10) || a.date
    return d >= chartStart && d <= chartEnd
  })

  // Scale prices for IL (agorot → ₪)
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
  const hasTargets = chartData.some(d => d.target_low != null || d.target_mean != null || d.target_high != null)

  const basePeriod  = chartData.filter(d => d.date <= chartStart).at(-1) ?? chartData[0]
  const yieldPeriod = basePeriod && chartData.length > 0
    ? ((chartData.at(-1).close - basePeriod.close) / basePeriod.close) * 100
    : null

  const minPrice = chartData.length ? Math.min(...chartData.map(d => d.close)) * 0.97 : 0
  const maxPrice = chartData.length ? Math.max(...chartData.map(d => d.close)) * 1.03 : 0

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
    const plotWidth = wrapperWidth - plotLeft - 24
    return plotLeft + (idx / (chartData.length - 1)) * plotWidth
  }

  const PLOT_TOP    = 100
  const PLOT_BOTTOM = 332

  function priceBox(cx, value, color) {
    const px = 8, fs = 11
    const bw = value.length * 6.8 + px * 2
    const bh = fs + 10
    const by = PLOT_TOP - bh - 4
    return (
      <g>
        <rect x={cx - bw / 2} y={by} width={bw} height={bh} rx={4} style={{ fill: 'var(--surface)' }} />
        <text x={cx} y={by + bh / 2} textAnchor="middle" dominantBaseline="central"
          fontSize={fs} style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
          {value}
        </text>
      </g>
    )
  }

  const healthLabels = ['', t('health.weak'), t('health.fair'), t('health.good'), t('health.great'), t('health.excellent')]
  const healthColors = ['', '#e74c3c', '#e67e22', '#f1c40f', '#2ecc71', '#1a9e55']

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div className="modal-header" style={{ alignItems: 'flex-start' }}>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6, flexShrink: 0 }}>
            {row.logo_url
              ? <img src={row.logo_url} alt="" className="modal-logo" style={{ width: 96, height: 96, borderRadius: 10, objectFit: 'contain', flexShrink: 0, ...(WHITE_BG_LOGOS.has(row.ticker) ? { background: '#fff', padding: 6 } : {}) }} onError={e => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }} />
              : null}
            <div className="modal-logo-placeholder" style={{ width: 96, height: 96, flexShrink: 0, borderRadius: 10, background: 'var(--surface)', border: '1px solid var(--border)', display: row.logo_url ? 'none' : 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 13, fontWeight: 'bold', color: 'var(--text)' }}>
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

          {/* General column */}
          <div ref={generalColRef} className="modal-general-col" style={{ display: 'flex', flexDirection: 'column', gap: 4, flexShrink: 0, width: 300 }}>
            <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.general')}</div>
            <div style={{ padding: '8px 0px', border: '1px solid var(--border)', borderRadius: 8 }}>
              <table style={{ fontSize: 12, borderCollapse: 'collapse', width: '100%', margin: 0, tableLayout: 'fixed' }}>
                <tbody>
                  {[
                    [t('modal.ticker'),        <strong>{row.ticker?.replace(/\.TA$/, '') ?? '—'}</strong>],
                    [t('modal.company'),       row.company ?? '—'],
                    [t('modal.industry'),      row.industry ?? '—'],
                    [t('modal.marketCap'),     row.market_cap != null ? (row.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'],
                    [t('modal.currentSignal'), row.current_signal ? <span className={`badge badge-${row.current_signal}`}>{row.current_signal}</span> : '—'],
                    [t('modal.currentPrice'),  history12m.length > 0 ? fmt(history12m.at(-1).close / (isIL ? 100 : 1)) : '—'],
                    [activePeriod ? `${t('modal.yieldPeriod', { label: ({3:'3M',6:'6M',12:'1Y',24:'2Y',36:'3Y',60:'5Y'}[activePeriod] || `${activePeriod}M`) })} (organic)` : `${t('modal.yieldCustom')} (organic)`, yieldPeriod != null ? <span className={yieldPeriod >= 0 ? 'up' : 'down'}>{yieldPeriod >= 0 ? '+' : ''}{fmt(yieldPeriod)}%</span> : '—'],
                    ...(row.unrealized_pct == null ? [[activePeriod ? `${t('modal.yieldPeriod', { label: ({3:'3M',6:'6M',12:'1Y',24:'2Y',36:'3Y',60:'5Y'}[activePeriod] || `${activePeriod}M`) })} (Vesign)` : `${t('modal.yieldCustom')} (Vesign)`, row.avg_return != null ? <span className={row.avg_return >= 0 ? 'up' : 'down'}>{row.avg_return >= 0 ? '+' : ''}{fmt(row.avg_return)}%</span> : '—']] : []),
                    ...(row.unrealized_pct != null ? [[t('modal.yieldSinceBuy'), <span className={row.unrealized_pct >= 0 ? 'up' : 'down'}>{row.unrealized_pct >= 0 ? '+' : ''}{fmt(row.unrealized_pct)}%</span>]] : []),
                  ].map(([label, value]) => (
                    <tr key={label} style={{ height: 22 }}>
                      <td style={{ color: 'var(--muted)', paddingRight: 8, verticalAlign: 'middle', whiteSpace: 'nowrap', width: 120 }}>{label}</td>
                      <td style={{ verticalAlign: 'middle', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Description + Health column (tabbed) */}
          <div className="modal-desc-col" style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4, overflow: 'hidden', ...(generalColH ? { height: generalColH } : {}) }}>
            {/* Info tab */}
            {descTab === 'info' && (<>
              {(row.description_short || row.description) && (<>
                <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.description')}</div>
                <div style={{ fontSize: 12, lineHeight: 1.6, overflowY: 'auto', flex: 1, minHeight: 0, padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8 }}>
                  {row.description_short || row.description}
                </div>
              </>)}
              {row.health_score && (<>
                <div style={{ fontSize: 14, color: 'var(--muted)', paddingLeft: 13, fontWeight: 'bold' }}>{t('modal.companyHealth')}</div>
                <div style={{ padding: '8px 12px', border: '1px solid var(--border)', borderRadius: 8, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflowY: 'auto' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                    {[1,2,3,4,5].map(i => (
                      <div key={i} style={{ width: 20, height: 8, borderRadius: 3, background: i <= row.health_score ? healthColors[row.health_score] : 'var(--border)' }} />
                    ))}
                    <span style={{ fontSize: 12, fontWeight: 'bold', color: healthColors[row.health_score], marginLeft: 4 }}>{healthLabels[row.health_score]}</span>
                  </div>
                  {row.health_reason && <div style={{ fontSize: 12, lineHeight: 1.6 }}>{row.health_reason}</div>}
                </div>
              </>)}
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

          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Chart */}
        {isLoading ? (
          <p className="loading" style={{ padding: 40 }}>{t('modal.loadingChart')}</p>
        ) : (
          <div ref={wrapperRef} style={{ position: 'relative', overflow: 'hidden' }}>

            {/* Period selector — overlaid at top of chart */}
            <div style={{ position: 'absolute', top: 8, left: 56, right: 24, display: 'flex', alignItems: 'center', gap: 6, zIndex: 20, flexWrap: 'wrap' }}>
              {[[3, '3M'], [6, '6M'], [12, '1Y'], [24, '2Y'], [36, '3Y'], [60, '5Y']].map(([m, label]) => (
                <button key={m} className={`period-chip${activePeriod === m ? ' active' : ''}`}
                  onClick={() => selectPeriod(m)}
                >{label}</button>
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
              <LineChart data={chartData} margin={{ top: 100, right: 24, bottom: 8, left: 8 }}>
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
                  wrapperStyle={{ zIndex: 50 }}
                  content={({ active, payload, label }) => {
                    if (!active || !payload || !payload.length) return null
                    const [y, m, day] = (label || '').split('-')
                    const dateStr = y ? `${day}/${m}/${y.slice(2)}` : label
                    const labels = {
                      close: t('modal.chartClose'),
                      target_low: t('modal.chartTargetLow'),
                      target_mean: t('modal.chartTargetBase'),
                      target_high: t('modal.chartTargetHigh'),
                    }
                    const order = { close: 0, target_high: 1, target_mean: 2, target_low: 3 }
                    const sorted = [...payload].sort((a, b) =>
                      (order[a.dataKey] ?? 99) - (order[b.dataKey] ?? 99)
                    )
                    return (
                      <div style={{
                        background: 'var(--surface)', border: '1px solid var(--border)',
                        borderRadius: 6, fontSize: 12, padding: '8px 12px',
                        color: 'var(--text)',
                      }}>
                        <div style={{ color: 'var(--muted)', marginBottom: 4 }}>{dateStr}</div>
                        {sorted.map(p => (
                          <div key={p.dataKey}
                               style={{ fontWeight: p.dataKey === 'close' ? 700 : 400 }}>
                            {labels[p.dataKey] || p.dataKey} : {p.value != null ? p.value.toFixed(2) : '—'}
                          </div>
                        ))}
                      </div>
                    )
                  }}
                />
                <Line type="monotone" dataKey="close" stroke="var(--accent)" dot={false} strokeWidth={2} name="close" />
                {hasTargets && <>
                  <Line type="stepAfter" dataKey="target_low"  stroke="#e74c3c" strokeOpacity={0.45} dot={false} strokeWidth={1} strokeDasharray="5 3" name="target_low"  connectNulls={false} />
                  <Line type="stepAfter" dataKey="target_mean" stroke="#f39c12" strokeOpacity={0.45} dot={false} strokeWidth={1} strokeDasharray="5 3" name="target_mean" connectNulls={false} />
                  <Line type="stepAfter" dataKey="target_high" stroke="#2ecc71" strokeOpacity={0.45} dot={false} strokeWidth={1} strokeDasharray="5 3" name="target_high" connectNulls={false} />
                </>}
              </LineChart>
            </ResponsiveContainer>

            {wrapperWidth > 0 && chartData.length > 1 && (
              <svg style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: 340, pointerEvents: 'none', overflow: 'visible' }}>
                {row.trades.map((trade, i) => {
                  const buyX  = trade.buy_date  ? dateToX(trade.buy_date.slice(0, 10))  : null
                  const sellX = trade.sell_date ? dateToX(trade.sell_date.slice(0, 10)) : null
                  return (
                    <g key={i}>
                      {buyX != null && <>
                        <line x1={buyX} y1={PLOT_TOP} x2={buyX} y2={PLOT_BOTTOM} style={{ stroke: 'var(--green)', strokeWidth: 2 }} />
                        {trade.buy_price != null && priceBox(buyX, currency + fmt(trade.buy_price / priceScale, 1), 'var(--green)')}
                      </>}
                      {sellX != null && <>
                        <line x1={sellX} y1={PLOT_TOP} x2={sellX} y2={PLOT_BOTTOM} style={{ stroke: 'var(--red)', strokeWidth: 2 }} />
                        {trade.sell_price != null && priceBox(sellX, currency + fmt(trade.sell_price / priceScale, 1), 'var(--red)')}
                      </>}
                      {buyX != null && sellX != null && trade.buy_price != null && trade.sell_price != null && (() => {
                        const pct   = ((trade.sell_price - trade.buy_price) / trade.buy_price) * 100
                        const color = pct >= 0 ? 'var(--green)' : 'var(--red)'
                        const lineY = PLOT_TOP + 18
                        return <>
                          <line x1={buyX} y1={lineY} x2={sellX} y2={lineY} style={{ stroke: color, strokeWidth: 1.5, strokeDasharray: '4 3' }} />
                          <text x={(buyX + sellX) / 2} y={lineY - 6} textAnchor="middle" fontSize={10}
                            style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                            {pct >= 0 ? '+' : ''}{pct.toFixed(1)}%
                          </text>
                        </>
                      })()}
                      {buyX != null && trade.result === 'Open' && (() => {
                        const lastX = dateToX(chartData.at(-1).date)
                        const currentPrice = chartData.at(-1).close
                        const pct = trade.buy_price != null && currentPrice != null
                          ? ((currentPrice - trade.buy_price / priceScale) / (trade.buy_price / priceScale)) * 100
                          : null
                        const color = pct != null && pct >= 0 ? 'var(--green)' : 'var(--red)'
                        const lineY = PLOT_TOP + 18
                        return <>
                          <text x={buyX + 6} y={PLOT_TOP + 30} fontSize={10}
                            style={{ fill: 'var(--green)', fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                            {t('modal.open').toUpperCase()}
                          </text>
                          {lastX != null && pct != null && <>
                            <line x1={buyX} y1={lineY} x2={lastX} y2={lineY} style={{ stroke: color, strokeWidth: 1.5, strokeDasharray: '4 3' }} />
                            <text x={(buyX + lastX) / 2} y={lineY - 6} textAnchor="middle" fontSize={10}
                              style={{ fill: color, fontWeight: 700, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
                              {pct >= 0 ? '+' : ''}{pct.toFixed(1)}%
                            </text>
                          </>}
                        </>
                      })()}
                    </g>
                  )
                })}
              </svg>
            )}

            {/* Legend */}
            <div style={{ display: 'flex', gap: 16, justifyContent: 'center', flexWrap: 'wrap', padding: '6px 0 2px', fontSize: 11, color: 'var(--muted)' }}>
              {[
                { color: 'var(--accent)', dash: false, label: t('modal.chartClose') },
                { color: 'var(--green)', dash: false, vertical: true, label: t('modal.legendBuy') },
                { color: 'var(--red)',   dash: false, vertical: true, label: t('modal.legendSell') },
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

// ---------------------------------------------------------------------------
// Open Trades table
// ---------------------------------------------------------------------------

function OpenTradesTable({ data, search, page, pageSize, setPage, onSelect }) {
  const { t } = useTranslation()
  const { sorted, sort, toggle } = useSort(data, 'buy_date', 'desc')

  const th = (label, col, className) => <Th label={label} col={col} sort={sort} onSort={toggle} className={className} />

  const filtered  = search ? sorted.filter(t =>
    t.ticker?.toLowerCase().includes(search.toLowerCase()) ||
    t.company?.toLowerCase().includes(search.toLowerCase())
  ) : sorted
  const pages     = Math.max(1, Math.ceil(filtered.length / pageSize))
  const paginated = filtered.slice((page - 1) * pageSize, page * pageSize)

  const tickers = data.map(t => t.ticker)
  const { prices, marketOpen } = useLivePrices(tickers)

  return (
    <>
      <div className="data-table-wrap">
        <table>
          <thead>
            <tr>
              <th></th>
              {th(t('col.ticker'),       'ticker')}
              {th(t('col.company'),      'company')}
              {th(t('col.marketCap'),    'market_cap',    'col-hide-sm')}
              {th(t('col.buyDate'),      'buy_date')}
              {th(t('col.buyPrice'),     'buy_price')}
              {th(t('col.lastDayPrice'), 'current_price', 'col-hide-sm')}
              {th(t('col.daysHeld'),     'days_held')}
              <th>{t('col.livePrice')}</th>
              <th>{t('col.yield')}</th>
            </tr>
          </thead>
          <tbody>
            {paginated.length === 0
              ? <tr><td colSpan={10} className="empty" style={{ textAlign: 'center' }}>{t('trades.noOpen')}</td></tr>
              : paginated.map((trade, i) => {
                const isIL = trade.ticker?.endsWith('.TA')
                const isOpen = isIL ? (marketOpen !== false) : marketOpen
                const live = prices[trade.ticker]
                const closePrice = trade.current_price
                const displayLive  = live != null ? (isIL ? live / 100 : live) : null
                const displayClose = closePrice != null ? (isIL ? closePrice / 100 : closePrice) : null
                const diff = displayLive != null && displayClose != null ? displayLive - displayClose : null
                const pct  = diff != null && displayClose ? (diff / displayClose) * 100 : null
                const cls  = diff != null && diff >= 0 ? 'up' : 'down'
                const arrow = diff != null && diff >= 0 ? '▲' : '▼'

                return (
                  <tr key={i} className="clickable-row" onClick={() => onSelect(trade)}>
                    <td>{trade.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(trade.ticker) ? ' logo-white-bg' : ''}`} src={trade.logo_url} alt="" /> : null}</td>
                    <td><strong>{trade.ticker}</strong></td>
                    <td>{trade.company ?? '—'}</td>
                    <td className="col-hide-sm">{trade.market_cap != null ? (trade.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'}</td>
                    <td>{fmtDate(trade.buy_date)}</td>
                    <td>{fmt(trade.buy_price)}</td>
                    <td className="col-hide-sm">{fmt(trade.current_price)}</td>
                    <td>{trade.days_held ?? '—'}</td>
                    <td>
                      {!isOpen
                        ? <span style={{ color: 'var(--muted)', fontSize: 12 }}>{t('market.closedShort')}</span>
                        : displayLive == null
                          ? <span style={{ color: 'var(--muted)' }}>—</span>
                          : <div>
                              <div>{displayLive.toFixed(2)}</div>
                              {diff != null && <div className={cls} style={{ fontSize: 11 }}>{arrow} {Math.abs(diff).toFixed(2)} ({Math.abs(pct).toFixed(2)}%)</div>}
                            </div>
                      }
                    </td>
                    {(() => {
                      const priceForYield = (isOpen && displayLive != null) ? displayLive : displayClose
                      const buyPrice = trade.buy_price != null ? (isIL ? trade.buy_price / 100 : trade.buy_price) : null
                      const yieldPct = priceForYield != null && buyPrice ? ((priceForYield - buyPrice) / buyPrice) * 100 : null
                      return (
                        <td className={yieldPct != null ? (yieldPct >= 0 ? 'up' : 'down') : ''}>
                          {yieldPct != null ? `${yieldPct >= 0 ? '+' : ''}${fmt(yieldPct)}%` : '—'}
                        </td>
                      )
                    })()}
                  </tr>
                )
              })
            }
          </tbody>
        </table>
      </div>
      <Pagination page={page} pages={pages} onChange={setPage} />
    </>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function TradesPage() {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const oneYearAgo = new Date()
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1)

  const [start, setStart]           = useState(oneYearAgo.toISOString().slice(0, 10))
  const [end,   setEnd]             = useState(new Date().toISOString().slice(0, 10))
  const [openSearch, setOpenSearch]     = useState('')
  const [openPage, setOpenPage]         = useState(1)
  const [openPageSize, setOpenPageSize] = useState(10)
  const [selected, setSelected]         = useState(null)
  const [selectedOpen, setSelectedOpen] = useState(null)
  const [search, setSearch]         = useState('')
  const [page, setPage]             = useState(1)
  const [pageSize, setPageSize]     = useState(10)

  const { data: trades, isLoading, isError } = useQuery({
    queryKey: ['trades', start, end, market],
    queryFn: () => getTrades({ start, end, market }),
    staleTime: 300_000,
    placeholderData: (prev) => prev,  // keep old data visible during refetch
  })

  // Prefetch all period chips on mount so switching is instant
  const qc = useQueryClient()
  useEffect(() => {
    const today = new Date().toISOString().slice(0, 10)
    const opts = { staleTime: 300_000 }
    for (const months of [3, 6, 12, 24, 36, 60]) {
      const d = new Date(); d.setMonth(d.getMonth() - months)
      const s = d.toISOString().slice(0, 10)
      qc.prefetchQuery({
        queryKey: ['trades', s, today, market],
        queryFn: () => getTrades({ start: s, end: today, market }),
        ...opts,
      })
    }
  }, [market, qc])

  const { data: openTrades = [], isLoading: loadingOpen, isError: errorOpen, error: openError } = useQuery({
    queryKey: ['trades-open', market],
    queryFn: () => getOpenTrades(market),
  })

  const { sorted, sort, toggle } = useSort(trades, 'last_sell_date', 'desc')

  const total     = trades ? trades.length : 0
  const totalPairs = trades ? trades.reduce((s, t) => s + t.trade_count, 0) : 0
  const wins      = trades ? trades.reduce((s, t) => s + t.win_count, 0) : 0
  const avgReturn = total > 0
    ? trades.reduce((s, t) => s + t.avg_return * t.trade_count, 0) / totalPairs
    : null
  const avgDays   = total > 0
    ? trades.reduce((s, t) => s + t.avg_days * t.trade_count, 0) / totalPairs
    : null

  const beatMarket = trades
    ? trades.reduce((n, t) =>
        n + (t.organic_yield != null
          ? t.trades.filter(p => p.return_pct > t.organic_yield).length
          : 0), 0)
    : null

  const th = (label, col, className) => <Th label={label} col={col} sort={sort} onSort={toggle} className={className} />

  return (
    <div>
      <p className="page-title">{t('trades.title')}</p>

      <div className="controls">
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('trades.from')}</label>
        <input type="date" value={start} onChange={e => setStart(e.target.value)} />
        <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('trades.to')}</label>
        <input type="date" value={end} onChange={e => setEnd(e.target.value)} />
        <span style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {[[3, '3M'], [6, '6M'], [12, '1Y'], [24, '2Y'], [36, '3Y'], [60, '5Y']].map(([m, label]) => (
            <button
              key={m}
              className="period-chip"
              onClick={() => { setStart(isoMonthsAgo(m)); setEnd(new Date().toISOString().slice(0, 10)) }}
            >{label}</button>
          ))}
        </span>
      </div>

      {isLoading && <p className="loading">{t('table.loading')}</p>}
      {isError   && <p className="error">{t('trades.failedLoad')}</p>}

      {trades && total > 0 && (
        <div className="metrics">
          <div className="metric-card">
            <div className="label">{t('trades.totalTrades')}</div>
            <div className="value">{totalPairs}</div>
          </div>
          <div className="metric-card">
            <div className="label">{t('trades.winRate')}</div>
            <div className="value">{totalPairs > 0 ? ((wins / totalPairs) * 100).toFixed(1) : '—'}%</div>
          </div>
          <div className="metric-card">
            <div className="label">{t('trades.avgYield')}</div>
            <div className={`value ${avgReturn >= 0 ? 'up' : 'down'}`}>
              {avgReturn >= 0 ? '+' : ''}{fmt(avgReturn)}%
            </div>
          </div>
          <div className="metric-card">
            <div className="label">{t('trades.avgDays')}</div>
            <div className="value">{avgDays != null ? Math.round(avgDays) : '—'}</div>
          </div>
        </div>
      )}

      {trades && total === 0 && (
        <p className="empty">{t('trades.noCompleted')}</p>
      )}

      {trades && total > 0 && (() => {
        const filtered = search
          ? sorted.filter(t =>
              t.ticker?.toLowerCase().includes(search.toLowerCase()) ||
              t.company?.toLowerCase().includes(search.toLowerCase())
            )
          : sorted
        const pages     = Math.max(1, Math.ceil(filtered.length / pageSize))
        const paginated = filtered.slice((page - 1) * pageSize, page * pageSize)
        return (
        <>
        <div className="controls" style={{ marginTop: 0 }}>
          <input
            placeholder={`🔍 ${t('table.search')}`}
            value={search}
            onChange={e => { setSearch(e.target.value); setPage(1) }}
            style={{ width: 240 }}
          />
          {search && <button onClick={() => { setSearch(''); setPage(1) }}>{t('table.clear')}</button>}
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('table.rows')}</label>
            <select value={pageSize} onChange={e => { setPageSize(Number(e.target.value)); setPage(1) }}>
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </span>
        </div>
        <div className="data-table-wrap">
          <table>
            <thead>
              <tr>
                <th></th>
                {th(t('col.ticker'),    'ticker')}
                {th(t('col.company'),   'company')}
                {th(t('col.marketCap'), 'market_cap', 'col-hide-sm')}
                {th(t('col.trades'),    'trade_count')}
                {th(t('col.buyDate'),   'first_buy_date')}
                {th(t('col.sellDate'),  'last_sell_date')}
                {th(t('col.avgDays'),   'avg_days', 'col-hide-sm')}
                {th(t('col.winRate'),   'win_count')}
                {th(t('col.avgYield'),  'avg_return')}
              </tr>
            </thead>
            <tbody>
              {paginated.length === 0
                ? <tr><td colSpan={10} className="empty" style={{ textAlign: 'center' }}>{t('trades.noMatches')}</td></tr>
                : paginated.map((trade, i) => {
                const winRate = trade.trade_count > 0 ? (trade.win_count / trade.trade_count) * 100 : 0
                return (
                  <tr key={i} className="clickable-row" onClick={() => setSelected(trade)}>
                    <td>{trade.logo_url ? <img className={`logo${WHITE_BG_LOGOS.has(trade.ticker) ? ' logo-white-bg' : ''}`} src={trade.logo_url} alt="" /> : null}</td>
                    <td><strong>{trade.ticker}</strong></td>
                    <td>{trade.company ?? '—'}</td>
                    <td className="col-hide-sm">{trade.market_cap != null ? (trade.market_cap / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '—'}</td>
                    <td>{trade.trade_count}</td>
                    <td>{fmtDate(trade.first_buy_date)}</td>
                    <td>{fmtDate(trade.last_sell_date)}</td>
                    <td className="col-hide-sm">{Math.round(trade.avg_days)}</td>
                    <td className={winRate >= 50 ? 'up' : 'down'}>{winRate.toFixed(0)}%</td>
                    <td className={trade.avg_return >= 0 ? 'up' : 'down'}>
                      {trade.avg_return >= 0 ? '+' : ''}{fmt(trade.avg_return)}%
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        <Pagination page={page} pages={pages} onChange={setPage} />
        </>
        )
      })()}

      {selected && <TradeModal row={selected} start={start} end={end} onClose={() => setSelected(null)} />}
      {selectedOpen && (
        <TradeModal
          row={{ ...selectedOpen, trades: [{ buy_date: selectedOpen.buy_date, sell_date: null, buy_price: selectedOpen.buy_price, sell_price: null, result: 'Open' }] }}
          start={isoMonthsAgo(12)}
          end={new Date().toISOString().slice(0, 10)}
          onClose={() => setSelectedOpen(null)}
        />
      )}

      {/* Open Trades */}
      <p className="page-title" style={{ marginTop: 40 }}>{t('trades.openTitle')}</p>

      {loadingOpen && <p className="loading">{t('table.loading')}</p>}
      {errorOpen && <p className="error">Error: {openError?.message}</p>}
      {!loadingOpen && !errorOpen && (() => {
        const filtered = openTrades
        const openCount  = filtered.length
        const winCount   = filtered.filter(t => (t.unrealized_pct ?? 0) > 0).length
        const winRate    = openCount > 0 ? (winCount / openCount) * 100 : null
        const avgYield   = openCount > 0 ? filtered.reduce((s, t) => s + (t.unrealized_pct ?? 0), 0) / openCount : null
        const avgDaysOpen = openCount > 0 ? filtered.reduce((s, t) => s + (t.days_held ?? 0), 0) / openCount : null
        return (
          <>
            {openCount > 0 && (
              <div className="metrics">
                <div className="metric-card">
                  <div className="label">{t('trades.openTrades')}</div>
                  <div className="value">{openCount}</div>
                </div>
                <div className="metric-card">
                  <div className="label">{t('trades.winRate')}</div>
                  <div className="value">{winRate != null ? `${winRate.toFixed(1)}%` : '—'}</div>
                </div>
                <div className="metric-card">
                  <div className="label">{t('trades.avgYield')}</div>
                  <div className={`value ${avgYield >= 0 ? 'up' : 'down'}`}>
                    {avgYield != null ? `${avgYield >= 0 ? '+' : ''}${fmt(avgYield)}%` : '—'}
                  </div>
                </div>
                <div className="metric-card">
                  <div className="label">{t('trades.avgDaysOpen')}</div>
                  <div className="value">{avgDaysOpen != null ? Math.round(avgDaysOpen) : '—'}</div>
                </div>
              </div>
            )}
            <div className="controls" style={{ marginTop: 0 }}>
              <input
                placeholder={`🔍 ${t('table.search')}`}
                value={openSearch}
                onChange={e => { setOpenSearch(e.target.value); setOpenPage(1) }}
                style={{ width: 240 }}
              />
              {openSearch && <button onClick={() => { setOpenSearch(''); setOpenPage(1) }}>{t('table.clear')}</button>}
              <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
                <label style={{ color: 'var(--muted)', fontSize: 13 }}>{t('table.rows')}</label>
                <select value={openPageSize} onChange={e => { setOpenPageSize(Number(e.target.value)); setOpenPage(1) }}>
                  <option value={10}>10</option>
                  <option value={25}>25</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                </select>
              </span>
            </div>
            <OpenTradesTable data={filtered} search={openSearch} page={openPage} pageSize={openPageSize} setPage={setOpenPage} onSelect={setSelectedOpen} />
          </>
        )
      })()}
    </div>
  )
}
