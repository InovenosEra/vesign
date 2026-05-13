import { useState, useRef, useEffect, useLayoutEffect, useContext } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { getSignalsByTickers, getNews, WHITE_BG_LOGOS } from '../api'
import { MarketContext } from '../context/MarketContext'
import { useCurrency } from '../context/CurrencyContext'
import SignalChart, { PERIOD_LABEL } from './SignalChart'

function fmt(n, decimals = 2) {
  return n != null
    ? Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
    : '—'
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
  const { fmtPrice } = useCurrency()
  const priceScale = isIL ? 100 : 1  // IL prices stored in agorot, display in ₪

  const [descTab, setDescTab]     = useState('info')
  const [chartState, setChartState] = useState({ activePeriod: 12, chartStart: '', chartEnd: '', yieldPeriod: null })

  const { data: newsData = [], isLoading: newsLoading } = useQuery({
    queryKey: ['news', row.ticker],
    queryFn: () => getNews(row.ticker, 5),
    enabled: descTab === 'news',
    staleTime: 300_000,
  })

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
                    [t('col.signal'),          row.signal ? <span className={`badge badge-${row.signal}`}>{row.signal}{row.signal === 'BUY' && row.lot_seq > 1 ? ` ×${row.lot_seq}` : ''}</span> : '—'],
                    [t('modal.price'),         row.close != null ? fmtPrice(row.close / priceScale) : '—'],
                    [t('modal.rsi'),           row.rsi != null ? row.rsi.toFixed(1) : '—'],
                    [t('modal.analystTarget'), (() => { const base = row.target_mean_price; const close = row.close; if (!base || !close) return '—'; const pct = ((base - close) / close) * 100; return <span className={pct >= 0 ? 'up' : 'down'}>{pct >= 0 ? '+' : ''}{pct.toFixed(1)}%</span> })()],
                    [t('modal.mlScore'),       (() => { const s = row.prediction_score; if (s == null) return '—'; const pct = s * 100; return <span className={pct >= 0 ? 'up' : 'down'}>{pct >= 0 ? '▲' : '▼'} {Math.abs(pct).toFixed(1)}%</span> })()],
                    [chartState.activePeriod ? t('modal.yieldPeriod', { label: PERIOD_LABEL[chartState.activePeriod] || `${chartState.activePeriod}M` }) : t('modal.yieldCustom'), chartState.yieldPeriod != null ? <span className={chartState.yieldPeriod >= 0 ? 'up' : 'down'}>{chartState.yieldPeriod >= 0 ? '+' : ''}{fmt(chartState.yieldPeriod)}%</span> : '—'],
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
        <SignalChart ticker={row.ticker} onPeriodChange={setChartState} />
      </div>
    </div>
  )
}
