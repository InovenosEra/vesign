import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts'
import { getPortfolioHoldings, getSuccessRate, WHITE_BG_LOGOS } from '../api'
import { useLivePrices } from '../hooks/useLivePrices'
import SignalModal from '../components/SignalModal'

const PIE_COLORS = [
  '#00d2ff', '#3498db', '#2ecc71', '#f39c12', '#e74c3c', '#9b59b6',
  '#1abc9c', '#e67e22', '#34495e', '#16a085', '#8e44ad', '#d35400',
]

function fmt(n, dec = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec })
    : '—'
}

function fmtMktCap(n) {
  if (n == null) return '—'
  return (n / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 })
}

function SummaryCard({ label, value, sub, color }) {
  return (
    <div style={{
      background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10,
      padding: '14px 18px', minWidth: 140, flex: '1 1 140px',
    }}>
      <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 700, color: color || 'var(--text)' }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

export default function PortfolioPage() {
  const { t } = useTranslation()
  const [allocMode, setAllocMode] = useState('ticker')
  const [selectedRow, setSelectedRow] = useState(null)

  const { data: holdings = [], isLoading } = useQuery({
    queryKey: ['portfolio-holdings'],
    queryFn: getPortfolioHoldings,
    staleTime: 60_000,
  })

  const { data: successRateData = [] } = useQuery({
    queryKey: ['success-rate', 12],
    queryFn: () => getSuccessRate(12),
    staleTime: 300_000,
  })

  const tickers = useMemo(() => holdings.map(h => h.ticker), [holdings])
  const { prices } = useLivePrices(tickers)

  // Enrich holdings with live prices
  const enriched = useMemo(() => holdings.map(h => {
    const livePrice = prices[h.ticker]
    const currentPrice = livePrice ?? (h.avg_price)
    const currentValue = currentPrice != null ? currentPrice * h.total_qty : null
    const pnlAbs = currentValue != null ? currentValue - h.total_cost : null
    const pnlPct = pnlAbs != null && h.total_cost ? (pnlAbs / h.total_cost) * 100 : null
    return { ...h, livePrice, currentValue, pnlAbs, pnlPct }
  }), [holdings, prices])

  // Summary totals
  const totalInvested = enriched.reduce((s, h) => s + (h.total_cost || 0), 0)
  const totalValue = enriched.reduce((s, h) => s + (h.currentValue ?? h.total_cost ?? 0), 0)
  const totalPnlAbs = totalValue - totalInvested
  const totalPnlPct = totalInvested > 0 ? (totalPnlAbs / totalInvested) * 100 : null

  // Allocation chart data
  const pieData = useMemo(() => {
    if (allocMode === 'sector') {
      const byIndustry = {}
      enriched.forEach(h => {
        const key = h.industry || 'Other'
        byIndustry[key] = (byIndustry[key] || 0) + (h.currentValue ?? h.total_cost ?? 0)
      })
      return Object.entries(byIndustry)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => b.value - a.value)
    }
    return enriched
      .map(h => ({ name: h.ticker, value: h.currentValue ?? h.total_cost ?? 0 }))
      .filter(d => d.value > 0)
      .sort((a, b) => b.value - a.value)
  }, [enriched, allocMode])

  const totalPie = pieData.reduce((s, d) => s + d.value, 0)

  // What-if aggregates
  const { totalTrades, wins, totalReturn } = useMemo(() => {
    let t = 0, w = 0, r = 0
    successRateData.forEach(row => {
      t += row.total_trades || 0
      w += row.wins || 0
      r += (row.avg_return_pct || 0) * (row.total_trades || 0)
    })
    return { totalTrades: t, wins: w, totalReturn: t > 0 ? r / t : 0 }
  }, [successRateData])
  const vesignWinRate = totalTrades > 0 ? (wins / totalTrades) * 100 : null
  const diff = totalPnlPct != null && vesignWinRate != null ? vesignWinRate - totalPnlPct : null

  if (isLoading) {
    return <div style={{ padding: 40, color: 'var(--muted)' }}>{t('table.loading')}</div>
  }

  if (holdings.length === 0) {
    return (
      <div style={{ padding: 40, color: 'var(--muted)', textAlign: 'center', fontSize: 14 }}>
        {t('portfolio.noHoldings')}
      </div>
    )
  }

  return (
    <div style={{ padding: '0 24px 40px' }}>
      <h2 style={{ fontSize: 18, fontWeight: 700, margin: '24px 0 16px', color: 'var(--text)' }}>
        {t('portfolio.title')}
      </h2>

      {/* Summary cards */}
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 24 }}>
        <SummaryCard label={t('watchlist.totalInvested')} value={`$${fmt(totalInvested)}`} />
        <SummaryCard label={t('watchlist.currentValue')} value={`$${fmt(totalValue)}`} />
        <SummaryCard
          label={t('watchlist.totalPnlAbs')}
          value={`${totalPnlAbs >= 0 ? '+' : ''}$${fmt(Math.abs(totalPnlAbs))}`}
          color={totalPnlAbs >= 0 ? 'var(--green)' : 'var(--red)'}
        />
        <SummaryCard
          label={t('watchlist.totalPnlPct')}
          value={totalPnlPct != null ? `${totalPnlPct >= 0 ? '+' : ''}${fmt(totalPnlPct)}%` : '—'}
          color={totalPnlPct != null ? (totalPnlPct >= 0 ? 'var(--green)' : 'var(--red)') : undefined}
        />
      </div>

      {/* Allocation section */}
      <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10, padding: '16px 18px', marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
          <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>{t('portfolio.allocation')}</span>
          <div style={{ display: 'flex', gap: 4 }}>
            {[['ticker', t('portfolio.byTicker')], ['sector', t('portfolio.bySector')]].map(([mode, label]) => (
              <button key={mode}
                className={`period-chip${allocMode === mode ? ' active' : ''}`}
                onClick={() => setAllocMode(mode)}
                style={{ fontSize: 11, padding: '2px 10px' }}>
                {label}
              </button>
            ))}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 24, alignItems: 'center', flexWrap: 'wrap' }}>
          <div style={{ width: 200, height: 200, flexShrink: 0 }}>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={pieData} dataKey="value" cx="50%" cy="50%" innerRadius={55} outerRadius={90} strokeWidth={1}>
                  {pieData.map((_, i) => (
                    <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12 }}
                  formatter={(v) => [`$${fmt(v)}`, '']}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <table style={{ fontSize: 12, borderCollapse: 'collapse', width: '100%' }}>
              <tbody>
                {pieData.map((d, i) => {
                  const pct = totalPie > 0 ? (d.value / totalPie) * 100 : 0
                  const holding = enriched.find(h => h.ticker === d.name)
                  return (
                    <tr key={i}>
                      <td style={{ paddingBottom: 4, paddingRight: 8 }}>
                        <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: 2, background: PIE_COLORS[i % PIE_COLORS.length], marginRight: 6 }} />
                        {d.name}
                      </td>
                      <td style={{ paddingBottom: 4, paddingRight: 12, textAlign: 'right' }}>${fmt(d.value, 0)}</td>
                      <td style={{ paddingBottom: 4, paddingRight: 12, textAlign: 'right', color: 'var(--muted)' }}>{pct.toFixed(1)}%</td>
                      {holding && (
                        <td style={{ paddingBottom: 4, textAlign: 'right', color: holding.pnlPct >= 0 ? 'var(--green)' : 'var(--red)' }}>
                          {holding.pnlPct != null ? `${holding.pnlPct >= 0 ? '+' : ''}${holding.pnlPct.toFixed(1)}%` : '—'}
                        </td>
                      )}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Holdings table */}
      <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10, marginBottom: 24, overflow: 'hidden' }}>
        <table className="signals-table" style={{ width: '100%' }}>
          <thead>
            <tr>
              <th style={{ width: 36 }}></th>
              <th>{t('col.ticker')}</th>
              <th>{t('col.company')}</th>
              <th style={{ textAlign: 'right' }}>{t('col.mktCap')}</th>
              <th style={{ textAlign: 'right' }}>{t('col.qty')}</th>
              <th style={{ textAlign: 'right' }}>{t('col.avgPrice')}</th>
              <th style={{ textAlign: 'right' }}>{t('col.invested')}</th>
              <th style={{ textAlign: 'right' }}>{t('col.livePrice')}</th>
              <th style={{ textAlign: 'right' }}>{t('watchlist.currentValue')}</th>
              <th style={{ textAlign: 'right' }}>{t('col.yield')}</th>
            </tr>
          </thead>
          <tbody>
            {enriched.map((h) => (
              <tr key={h.ticker} style={{ cursor: 'pointer' }} onClick={() => setSelectedRow(h)}>
                <td>
                  {h.logo_url
                    ? <img src={h.logo_url} alt="" style={{ width: 24, height: 24, borderRadius: 4, objectFit: 'contain', ...(WHITE_BG_LOGOS.has(h.ticker) ? { background: '#fff', padding: 2 } : {}) }} />
                    : <div style={{ width: 24, height: 24, borderRadius: 4, background: 'var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 8, fontWeight: 700 }}>{h.ticker.slice(0, 2)}</div>
                  }
                </td>
                <td><strong>{h.ticker}</strong></td>
                <td style={{ maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{h.company || '—'}</td>
                <td style={{ textAlign: 'right', color: 'var(--muted)' }}>{fmtMktCap(h.market_cap)}</td>
                <td style={{ textAlign: 'right' }}>{fmt(h.total_qty, 0)}</td>
                <td style={{ textAlign: 'right' }}>{fmt(h.avg_price)}</td>
                <td style={{ textAlign: 'right' }}>${fmt(h.total_cost)}</td>
                <td style={{ textAlign: 'right' }}>
                  {h.livePrice != null ? fmt(h.livePrice) : <span style={{ color: 'var(--muted)', fontSize: 11 }}>—</span>}
                </td>
                <td style={{ textAlign: 'right' }}>
                  {h.currentValue != null ? `$${fmt(h.currentValue)}` : '—'}
                </td>
                <td style={{ textAlign: 'right' }}>
                  {h.pnlPct != null
                    ? <span className={h.pnlPct >= 0 ? 'up' : 'down'}>{h.pnlPct >= 0 ? '+' : ''}{fmt(h.pnlPct)}%</span>
                    : '—'
                  }
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* What-If card */}
      {totalTrades > 0 && (
        <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10, padding: '16px 18px' }}>
          <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)', marginBottom: 12 }}>{t('portfolio.whatIf')}</div>
          <div style={{ display: 'flex', gap: 20, flexWrap: 'wrap', alignItems: 'center' }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('portfolio.winRate')}</div>
              <div style={{ fontSize: 18, fontWeight: 700, color: 'var(--green)' }}>{vesignWinRate != null ? `${vesignWinRate.toFixed(1)}%` : '—'}</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('portfolio.avgReturn')}</div>
              <div style={{ fontSize: 18, fontWeight: 700, color: 'var(--green)' }}>{totalReturn > 0 ? '+' : ''}{fmt(totalReturn)}%</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('portfolio.totalTrades')}</div>
              <div style={{ fontSize: 18, fontWeight: 700 }}>{totalTrades}</div>
            </div>
            <div style={{ borderLeft: '1px solid var(--border)', paddingLeft: 20, display: 'flex', gap: 20, alignItems: 'center', flexWrap: 'wrap' }}>
              <div>
                <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('portfolio.yourPortfolio')}</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: totalPnlPct != null && totalPnlPct >= 0 ? 'var(--green)' : 'var(--red)' }}>
                  {totalPnlPct != null ? `${totalPnlPct >= 0 ? '+' : ''}${fmt(totalPnlPct)}%` : '—'}
                </div>
              </div>
              <div>
                <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('portfolio.vesignSignals')}</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--green)' }}>+{fmt(totalReturn)}%</div>
              </div>
              {diff != null && (
                <div>
                  <div style={{ fontSize: 11, color: 'var(--muted)' }}>{t('portfolio.vsPortfolio')}</div>
                  <div style={{ fontSize: 16, fontWeight: 700, color: diff >= 0 ? 'var(--green)' : 'var(--red)' }}>
                    {diff >= 0 ? '▲' : '▼'} {Math.abs(diff).toFixed(1)}%
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {selectedRow && (
        <SignalModal
          row={selectedRow}
          onClose={() => setSelectedRow(null)}
        />
      )}
    </div>
  )
}
