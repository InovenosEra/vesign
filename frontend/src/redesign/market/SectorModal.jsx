/* Sector drill-down modal — aggregates + 30d trend + breadth + constituents.
 * Ported from the mockup's sector-modal.js. Opens on a heatmap tile click;
 * a constituent row click opens the SignalModal via the ticker-modal context.
 * Styles live in redesign.css (.sm-* under .rd). */
import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSectorDetail, WHITE_BG_LOGOS } from '../../api'
import { num, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import './sector-modal.css'

const fmtNum = (v, fd = 2) => (v == null ? '—' : num(v, { fd }))
const fmtPct = (v) => (v == null ? '—' : (v >= 0 ? '+' : '') + fmtNum(v, 2) + '%')
const dir = (v) => (v == null ? '' : v > 0 ? 'up' : v < 0 ? 'down' : '')
const capB = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'

function SparkSVG({ values }) {
  if (!Array.isArray(values) || values.length < 2) return null
  const n = values.length, min = Math.min(...values), max = Math.max(...values), span = (max - min) || 1
  const up = values[n - 1] >= values[0]
  const color = up ? '#00d97e' : '#ff4d5c'
  const X = (i) => (i / (n - 1) * 200).toFixed(1)
  const Y = (v) => (4 + (1 - (v - min) / span) * 48).toFixed(1)
  const pts = values.map((v, i) => `${X(i)},${Y(v)}`)
  return (
    <svg viewBox="0 0 200 56" preserveAspectRatio="none">
      <defs><linearGradient id="sm-grad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={color} stopOpacity="0.28" /><stop offset="100%" stopColor={color} stopOpacity="0" />
      </linearGradient></defs>
      <path d={`M ${pts.join(' L ')} L 200,56 L 0,56 Z`} fill="url(#sm-grad)" />
      <polyline points={pts.join(' ')} fill="none" stroke={color} strokeWidth="1.6" />
    </svg>
  )
}

function Stat({ k, children, cls }) {
  return <div className="sm-stat"><div className="k">{k}</div><div className={'v ' + (cls || '')}>{children}</div></div>
}

export default function SectorModal({ sector, onClose }) {
  const openTicker = useTickerModal()
  const { data: d, isLoading } = useQuery({
    queryKey: ['market-sector', sector],
    queryFn: () => getSectorDetail(sector),
    enabled: !!sector,
  })

  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  if (!sector) return null
  const tot = d ? ((d.advancers + d.decliners) || 1) : 1

  return (
    <div className="sm-overlay open" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose() }}>
      <div className="sm-box" role="dialog" aria-label="Sector detail">
        <div className="sm-head">
          <div className="l">
            <div className="sm-sector">{d?.sector || sector}</div>
            {d && (
              <div className="sm-moves">
                <span className={'big ' + dir(d.change_pct)}>{fmtPct(d.change_pct)}</span><span className="lbl">today</span>
                <span className={dir(d.return_30d)}>{fmtPct(d.return_30d)}</span><span className="lbl">30d</span>
              </div>
            )}
          </div>
          <div className="sm-spark">{d && <SparkSVG values={d.sparkline} />}</div>
          <button className="sm-x" aria-label="Close" onClick={onClose}>×</button>
        </div>

        {d && (
          <div className="sm-stats">
            <Stat k="Companies">{d.count}</Stat>
            <Stat k="Market cap">{capB(d.total_market_cap)}</Stat>
            <Stat k="Median P/E">{d.median_pe == null ? '—' : fmtNum(d.median_pe, 1)}</Stat>
            <Stat k="Best today" cls={d.best && d.best.change_pct >= 0 ? 'up' : 'down'}>
              {d.best ? <>{d.best.ticker} <small>{fmtPct(d.best.change_pct)}</small></> : '—'}
            </Stat>
            <Stat k="Worst today" cls={d.worst && d.worst.change_pct >= 0 ? 'up' : 'down'}>
              {d.worst ? <>{d.worst.ticker} <small>{fmtPct(d.worst.change_pct)}</small></> : '—'}
            </Stat>
          </div>
        )}

        {d && (
          <div className="sm-breadth">
            <div className="lbl">In-sector breadth</div>
            <div className="sm-bar">
              <div className="a" style={{ width: (d.advancers / tot * 100).toFixed(1) + '%' }} />
              <div className="d" style={{ width: (d.decliners / tot * 100).toFixed(1) + '%' }} />
            </div>
            <div className="sm-bar-nums"><span className="up">{d.advancers} advancing</span><span className="down">{d.decliners} declining</span></div>
          </div>
        )}

        <div className="sm-tbl-wrap">
          <table className="sm-tbl">
            <thead><tr>
              <th className="l">Ticker</th><th className="r">Price</th><th className="r">Day</th>
              <th className="r">Mkt cap</th><th className="r">P/E</th>
            </tr></thead>
            <tbody>
              {isLoading || !d
                ? <tr><td className="l" colSpan="5" style={{ textAlign: 'center', color: 'var(--ink-3)', padding: 24 }}>{isLoading ? 'Loading…' : 'No data.'}</td></tr>
                : (d.constituents || []).map((c) => {
                  const wb = WHITE_BG_LOGOS.has(c.ticker) ? ' white-bg' : ''
                  return (
                    <tr key={c.ticker} data-ticker={c.ticker} onClick={() => openTicker(c.ticker, c.company)}>
                      <td className="l"><div className="sm-tk-cell"><img className={'sm-logo' + wb} src={LOGO(c.ticker)} alt={c.ticker} /><span className="sm-tk">{c.ticker}</span><span className="sm-co">{c.company || ''}</span></div></td>
                      <td>{fmtNum(c.close)}</td>
                      <td className={dir(c.change_pct)}>{fmtPct(c.change_pct)}</td>
                      <td>{capB(c.market_cap)}</td>
                      <td>{c.pe_ttm == null ? '—' : fmtNum(c.pe_ttm, 1)}</td>
                    </tr>
                  )
                })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
