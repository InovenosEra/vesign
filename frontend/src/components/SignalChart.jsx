import { useState, useRef, useEffect, useContext } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from 'recharts'
import { getPriceHistory, getSignalMarkers, getAnalystHistory } from '../api'
import { MarketContext } from '../context/MarketContext'
import { useCurrency } from '../context/CurrencyContext'

const CHART_PERIODS = [[3, '3M'], [6, '6M'], [12, '1Y'], [24, '2Y'], [36, '3Y'], [60, '5Y']]
const PERIOD_LABEL = Object.fromEntries(CHART_PERIODS)

function monthsAgo(n) {
  const d = new Date(); d.setMonth(d.getMonth() - n); return d.toISOString().slice(0, 10)
}

export { CHART_PERIODS, PERIOD_LABEL }

// Shared modal chart used by SignalModal and TradesPage's TradeModal so the
// line-chart visuals are identical everywhere. Data sources (price history,
// signal markers, analyst history) are fetched internally; the parent only
// supplies the ticker. The optional onPeriodChange callback lets the parent
// keep its own info table in sync with the chart's selected window.
export default function SignalChart({ ticker, onPeriodChange }) {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const { fmtPrice } = useCurrency()

  const isIL       = ticker?.endsWith('.TA') ?? market === 'IL'
  const priceScale = isIL ? 100 : 1
  const today      = new Date().toISOString().slice(0, 10)

  const [activePeriod, setActivePeriod] = useState(12)
  const [chartStart,   setChartStart]   = useState(() => monthsAgo(12))
  const [chartEnd,     setChartEnd]     = useState(today)

  function selectPeriod(months) {
    setActivePeriod(months)
    setChartStart(monthsAgo(months))
    setChartEnd(today)
  }

  // Fetch full history once per ticker; period switches filter client-side.
  // 120 months = 10 years comfortably covers the 2020-01-01 DB cutoff so a
  // user-typed start date back to Jan 2020 always has data behind it.
  const fullStart  = monthsAgo(120)
  const fetchStart = (() => { const d = new Date(fullStart); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10) })()

  const { data: fullHistory = [], isLoading } = useQuery({
    queryKey: ['price-history-signal', ticker],
    queryFn: () => getPriceHistory(ticker, { start: fetchStart, end: today }),
    staleTime: 300_000,
    placeholderData: keepPreviousData,
  })

  const history = fullHistory.filter(p => {
    const d = p.date?.slice(0, 10) || p.date
    return d >= chartStart && d <= chartEnd
  })

  const { data: markers = [] } = useQuery({
    queryKey: ['signal-markers', ticker],
    queryFn: () => getSignalMarkers(ticker, 120),
    staleTime: 300_000,
  })

  const { data: fullAnalystHistory = [] } = useQuery({
    queryKey: ['analyst-history', ticker],
    queryFn: () => getAnalystHistory(ticker, { start: fetchStart, end: today }),
    staleTime: 300_000,
    placeholderData: keepPreviousData,
  })

  const analystHistory = fullAnalystHistory.filter(a => {
    const d = a.date?.slice(0, 10) || a.date
    return d >= chartStart && d <= chartEnd
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

  // Forward analyst projection: from the latest available row, extend 12mo
  // of analyst targets to the right. Ghost rows widen the x-axis so the
  // projection cone occupies ~12% of plot width.
  const lastActualRow = chartData.length > 0 ? chartData[chartData.length - 1] : null
  const hasProjection = !!(lastActualRow && (
    lastActualRow.target_high != null ||
    lastActualRow.target_mean != null ||
    lastActualRow.target_low  != null
  ))
  const projectionEnd = lastActualRow ? (() => {
    const d = new Date(lastActualRow.date + 'T00:00:00Z')
    d.setUTCMonth(d.getUTCMonth() + 12)
    return d.toISOString().slice(0, 10)
  })() : null
  const projectionRows = []
  if (hasProjection && projectionEnd && lastActualRow) {
    const N = Math.max(10, Math.floor(chartData.length * 0.125))
    const startMs = new Date(lastActualRow.date + 'T00:00:00Z').getTime()
    const endMs   = new Date(projectionEnd     + 'T00:00:00Z').getTime()
    for (let i = 1; i <= N; i++) {
      const ms = startMs + ((endMs - startMs) * i) / N
      projectionRows.push({ date: new Date(ms).toISOString().slice(0, 10), close: null })
    }
  }
  const chartDataDisplay = projectionRows.length > 0 ? [...chartData, ...projectionRows] : chartData

  // X-axis ticks: only history dates, not projection ghost rows
  const xTicks = (() => {
    if (chartData.length === 0) return undefined
    if (chartData.length <= 8) return chartData.map(d => d.date)
    const n = 8
    return Array.from({ length: n }, (_, i) =>
      chartData[Math.floor(i * (chartData.length - 1) / (n - 1))].date
    )
  })()

  // Collapse to single Base line when single-analyst coverage gives 3 equal targets
  const targetsCollapse = !!(
    lastActualRow &&
    lastActualRow.target_high != null &&
    lastActualRow.target_mean != null &&
    lastActualRow.target_low  != null &&
    lastActualRow.target_high === lastActualRow.target_mean &&
    lastActualRow.target_mean === lastActualRow.target_low
  )

  const basePeriod  = chartHistory.filter(d => d.date <= chartStart).at(-1) ?? chartHistory[0]
  const yieldPeriod = basePeriod && chartHistory.length > 0
    ? ((chartHistory.at(-1).close - basePeriod.close) / basePeriod.close) * 100
    : null

  // Push period+yield info to parent so its info table stays in sync.
  useEffect(() => {
    if (!onPeriodChange) return
    onPeriodChange({ activePeriod, chartStart, chartEnd, yieldPeriod })
  }, [activePeriod, chartStart, chartEnd, yieldPeriod, onPeriodChange])

  const allPrices  = [
    ...chartHistory.map(d => d.close),
    ...(hasProjection ? [
      lastActualRow.target_low,
      lastActualRow.target_mean,
      lastActualRow.target_high,
    ].filter(x => x != null) : []),
  ]
  const minPrice = allPrices.length ? Math.min(...allPrices) * 0.97 : 0
  const maxPrice = allPrices.length ? Math.max(...allPrices) * 1.03 : 0

  // Path B: collect every BUY lot per trade so add-on lots render as well.
  const pairs = []
  let openLots = []
  for (const m of markers) {
    if (m.signal === 'BUY') {
      openLots.push(m)
    } else if (m.signal === 'SELL' && openLots.length > 0) {
      const avgCost = openLots.reduce((s, l) => s + (l.close ?? 0), 0) / openLots.length
      pairs.push({ buy: openLots[0], sell: m, lots: openLots, avgCost })
      openLots = []
    }
  }
  const openBuy = openLots.length > 0 ? openLots[0] : null
  const openLotsAll = openLots
  const openAvgCost = openLots.length > 0
    ? openLots.reduce((s, l) => s + (l.close ?? 0), 0) / openLots.length
    : null

  const wrapperRef = useRef(null)
  const [wrapperWidth, setWrapperWidth] = useState(0)
  useEffect(() => {
    if (!wrapperRef.current) return
    const obs = new ResizeObserver(entries => setWrapperWidth(entries[0].contentRect.width))
    obs.observe(wrapperRef.current)
    return () => obs.disconnect()
  }, [isLoading])

  const PLOT_RIGHT_MARGIN = 60
  const PLOT_LEFT = 8 + 56  // chart margin.left + YAxis width
  function dateToX(dateStr) {
    const idx = chartDataDisplay.findIndex(d => d.date === dateStr)
    if (idx < 0 || chartDataDisplay.length <= 1) return null
    const plotWidth = wrapperWidth - PLOT_LEFT - PLOT_RIGHT_MARGIN
    return PLOT_LEFT + (idx / (chartDataDisplay.length - 1)) * plotWidth
  }

  const PLOT_TOP        = 100
  const PLOT_BOTTOM     = 332  // full-height BUY/SELL vertical lines
  const PLOT_BOTTOM_PX  = 302  // actual Recharts plot bottom (340 - 8 - 30)

  function priceToY(price) {
    if (price == null || maxPrice <= minPrice) return null
    const ratio = (maxPrice - price) / (maxPrice - minPrice)
    return PLOT_TOP + ratio * (PLOT_BOTTOM_PX - PLOT_TOP)
  }

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

  if (isLoading) {
    return <p className="loading" style={{ padding: 40 }}>{t('modal.loadingChart')}</p>
  }

  return (
    <div ref={wrapperRef} style={{ position: 'relative', overflow: 'hidden', outline: 'none', margin: '0 -24px' }}>

      {/* Period selector — overlaid at top of chart, aligned with plot area */}
      <div style={{ position: 'absolute', top: 8, left: 56, right: 16, display: 'flex', alignItems: 'center', gap: 6, zIndex: 20, flexWrap: 'wrap' }}>
        {CHART_PERIODS.map(([m, label]) => (
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
        <LineChart data={chartDataDisplay} margin={{ top: 100, right: PLOT_RIGHT_MARGIN, bottom: 8, left: 8 }}>
          <XAxis
            dataKey="date"
            tick={{ fill: 'var(--muted)', fontSize: 11 }}
            tickFormatter={d => { const [, m, day] = d.split('-'); return `${day}/${m}` }}
            ticks={xTicks}
            axisLine={false}
          />
          <YAxis
            domain={[minPrice, maxPrice]}
            tick={{ fill: 'var(--muted)', fontSize: 11 }}
            tickFormatter={v => fmtPrice(v, 0)}
            width={56}
          />
          <Tooltip
            wrapperStyle={{ zIndex: 50 }}
            content={({ active, payload, label }) => {
              if (!active || !payload || !payload.length) return null
              if (lastActualRow && label > lastActualRow.date) return null
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
                      {labels[p.dataKey] || p.dataKey} : {p.value != null ? fmtPrice(p.value) : '—'}
                    </div>
                  ))}
                </div>
              )
            }}
          />
          <Line type="monotone" dataKey="close" stroke="var(--accent)" dot={false} strokeWidth={2} name="close" />
          {/* DCA: horizontal avg-cost line for any multi-lot closed pair whose
              window touches the visible range — otherwise Recharts stretches it. */}
          {pairs.map((p, i) => {
            if (!(p.lots.length > 1 && p.avgCost && p.buy.date && p.sell.date)) return null
            const inWindow = (p.buy.date <= chartEnd && p.sell.date >= chartStart)
            if (!inWindow) return null
            const fromX = p.buy.date  < chartStart ? chartStart : p.buy.date
            const toX   = p.sell.date > chartEnd   ? chartEnd   : p.sell.date
            return (
              <ReferenceLine key={`dca-avg-closed-${i}`}
                segment={[
                  { x: fromX, y: p.avgCost / priceScale },
                  { x: toX,   y: p.avgCost / priceScale },
                ]}
                stroke="#dc2626" strokeWidth={1.5} strokeDasharray="6 4"
                ifOverflow="visible"
                label={{ value: `avg $${(p.avgCost / priceScale).toFixed(2)}`, position: 'insideTopRight', fill: '#dc2626', fontSize: 10, fontWeight: 700 }}
              />
            )
          })}
          {openLotsAll.length > 1 && openAvgCost && openBuy?.date && chartData.length > 0 && (
            <ReferenceLine
              segment={[
                { x: openBuy.date, y: openAvgCost / priceScale },
                { x: chartData[chartData.length - 1].date, y: openAvgCost / priceScale },
              ]}
              stroke="#dc2626" strokeWidth={1.5} strokeDasharray="6 4"
              ifOverflow="visible"
              label={{ value: `avg $${(openAvgCost / priceScale).toFixed(2)}`, position: 'insideTopRight', fill: '#dc2626', fontSize: 10, fontWeight: 700 }}
            />
          )}
        </LineChart>
      </ResponsiveContainer>

      {wrapperWidth > 0 && chartData.length > 1 && (
        <svg style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: 340, pointerEvents: 'none', overflow: 'visible', zIndex: 10 }}>

          {pairs.map((p, i) => {
            const buyX  = dateToX(p.buy.date)
            const sellX = dateToX(p.sell.date)
            const pct   = p.avgCost && p.sell.close
              ? ((p.sell.close - p.avgCost) / p.avgCost) * 100
              : null
            const color = pct != null && pct >= 0 ? 'var(--green)' : 'var(--red)'
            const lineY = PLOT_TOP + 18
            return (
              <g key={i}>
                {buyX  != null && <>
                  <line x1={buyX}  y1={PLOT_TOP} x2={buyX}  y2={PLOT_BOTTOM} style={{ stroke: 'var(--green)', strokeWidth: 2 }} />
                  {p.buy.close  != null && priceBox(buyX,  fmtPrice(p.buy.close  / priceScale, 1), 'var(--green)')}
                </>}
                {p.lots.slice(1).map((lot, k) => {
                  const lx = dateToX(lot.date)
                  if (lx == null) return null
                  const seq = k + 2
                  return (
                    <g key={`addon-${i}-${seq}`}>
                      <line x1={lx} y1={PLOT_TOP + 30} x2={lx} y2={PLOT_BOTTOM}
                        style={{ stroke: 'var(--green)', strokeWidth: 1.5, strokeOpacity: 0.6, strokeDasharray: '3 3' }} />
                      <text x={lx} y={PLOT_TOP + 26} fontSize={10} fontWeight={700}
                        fill="var(--green)" textAnchor="middle">×{seq}</text>
                    </g>
                  )
                })}
                {sellX != null && <>
                  <line x1={sellX} y1={PLOT_TOP} x2={sellX} y2={PLOT_BOTTOM} style={{ stroke: 'var(--red)',   strokeWidth: 2 }} />
                  {p.sell.close != null && priceBox(sellX, fmtPrice(p.sell.close / priceScale, 1), 'var(--red)')}
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

          {/* Custom X-axis line truncated at the last actual data date */}
          {lastActualRow && (() => {
            const endX = dateToX(lastActualRow.date)
            if (endX == null) return null
            return (
              <line
                x1={PLOT_LEFT - 8} y1={PLOT_BOTTOM_PX}
                x2={endX}          y2={PLOT_BOTTOM_PX}
                stroke="var(--border)" strokeWidth={1}
              />
            )
          })()}

          {/* Forward analyst projection cone (animated) */}
          {hasProjection && lastActualRow && (() => {
            const apexX = dateToX(lastActualRow.date)
            const endX  = dateToX(projectionEnd)
            const apexY = priceToY(lastActualRow.close)
            const highY = priceToY(lastActualRow.target_high)
            const meanY = priceToY(lastActualRow.target_mean)
            const lowY  = priceToY(lastActualRow.target_low)
            if (apexX == null || endX == null || apexY == null) return null
            const lineLen = (x1, y1, x2, y2) => Math.hypot(x2 - x1, y2 - y1)
            const lenHigh = highY != null ? lineLen(apexX, apexY, endX, highY) : 0
            const lenMean = meanY != null ? lineLen(apexX, apexY, endX, meanY) : 0
            const lenLow  = lowY  != null ? lineLen(apexX, apexY, endX, lowY)  : 0
            const fillAnim  = { opacity: 0, animation: 'fadeInProj 0.6s ease-out 2.2s forwards' }
            const labelAnim = { opacity: 0, animation: 'fadeInProj 0.5s ease-out 2.5s forwards' }
            const lineAnim = (len) => ({
              strokeDasharray: len,
              strokeDashoffset: len,
              animation: 'drawProjLine 1.2s ease-out 1.5s forwards',
            })
            return (
              <g>
                {!targetsCollapse && highY != null && meanY != null && (
                  <polygon points={`${apexX},${apexY} ${endX},${highY} ${endX},${meanY}`}
                    fill="#16a34a" fillOpacity={0.08} stroke="none" style={fillAnim} />
                )}
                {!targetsCollapse && meanY != null && lowY != null && (
                  <polygon points={`${apexX},${apexY} ${endX},${meanY} ${endX},${lowY}`}
                    fill="#dc2626" fillOpacity={0.08} stroke="none" style={fillAnim} />
                )}
                {!targetsCollapse && highY != null && (
                  <line x1={apexX} y1={apexY} x2={endX} y2={highY} stroke="#16a34a" strokeWidth={1.5} style={lineAnim(lenHigh)} />
                )}
                {meanY != null && (
                  <line x1={apexX} y1={apexY} x2={endX} y2={meanY} stroke="#64748b" strokeWidth={1.5} style={lineAnim(lenMean)} />
                )}
                {!targetsCollapse && lowY != null && (
                  <line x1={apexX} y1={apexY} x2={endX} y2={lowY} stroke="#dc2626" strokeWidth={1.5} style={lineAnim(lenLow)} />
                )}
                {!targetsCollapse && highY != null && (<>
                  <text x={endX + 6} y={highY - 4} fontSize={10} fontWeight={700} fill="#16a34a" style={labelAnim}>High</text>
                  <text x={endX + 6} y={highY + 10} fontSize={11} fontWeight={700} fill="#16a34a" style={labelAnim}>{fmtPrice(lastActualRow.target_high, 1)}</text>
                </>)}
                {meanY != null && (<>
                  <text x={endX + 6} y={meanY - 4} fontSize={10} fontWeight={700} fill="#64748b" style={labelAnim}>Base</text>
                  <text x={endX + 6} y={meanY + 10} fontSize={11} fontWeight={700} fill="#64748b" style={labelAnim}>{fmtPrice(lastActualRow.target_mean, 1)}</text>
                </>)}
                {!targetsCollapse && lowY != null && (<>
                  <text x={endX + 6} y={lowY - 4} fontSize={10} fontWeight={700} fill="#dc2626" style={labelAnim}>Low</text>
                  <text x={endX + 6} y={lowY + 10} fontSize={11} fontWeight={700} fill="#dc2626" style={labelAnim}>{fmtPrice(lastActualRow.target_low, 1)}</text>
                </>)}
              </g>
            )
          })()}

          {/* Current open trade marker */}
          {openBuy && (() => {
            const buyX  = dateToX(openBuy.date)
            const lastX = dateToX(chartData.at(-1).date)
            if (!buyX) return null
            const currentPrice = chartData.at(-1).close
            const avgCostScaled = openAvgCost != null ? openAvgCost / priceScale : null
            const pct       = avgCostScaled ? ((currentPrice - avgCostScaled) / avgCostScaled) * 100 : null
            const gainColor = pct != null && pct >= 0 ? 'var(--green)' : 'var(--red)'
            const priceText = openBuy.close != null ? fmtPrice(openBuy.close / priceScale, 1) : ''
            const yieldText = pct != null ? `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%` : ''

            const price_bh = 21, gain_bh = 18, gap = 4
            const bw_price = priceText.length * 6.8 + 16
            const bw_gain  = yieldText.length * 6.8 + 16

            const pairBoxes = []
            for (const p of pairs) {
              const bx = dateToX(p.buy.date)
              const sx = dateToX(p.sell.date)
              if (bx != null && p.buy.close != null)
                pairBoxes.push({ x: bx, w: fmtPrice(p.buy.close / priceScale, 1).length * 6.8 + 16 })
              if (sx != null && p.sell.close != null)
                pairBoxes.push({ x: sx, w: fmtPrice(p.sell.close / priceScale, 1).length * 6.8 + 16 })
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
                {openLotsAll.slice(1).map((lot, k) => {
                  const lx = dateToX(lot.date)
                  if (lx == null) return null
                  const seq = k + 2
                  return (
                    <g key={`open-addon-${seq}`}>
                      <line x1={lx} y1={PLOT_TOP + 30} x2={lx} y2={PLOT_BOTTOM}
                        style={{ stroke: 'var(--green)', strokeWidth: 1.5, strokeOpacity: 0.6, strokeDasharray: '3 3' }} />
                      <text x={lx} y={PLOT_TOP + 26} fontSize={10} fontWeight={700}
                        fill="var(--green)" textAnchor="middle">×{seq}</text>
                    </g>
                  )
                })}
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
  )
}
