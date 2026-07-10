import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'
import { fmtInt, fmtPercent, fmtAsOf } from './stats'
import { useCountUp, useReveal, useSectionView } from './hooks'
import { track } from './analytics'

/* ── Proof: the trust anchor, biggest number treatment on the page. Real
 * data only, from /api/stats — three explicit states (loading skeleton,
 * error/null em dash, loaded count-up) so a slow or failed fetch never
 * crashes the section, shows stale placeholder numbers, or collapses the
 * layout. */

function Cell({ label, loading, display }) {
  return (
    <div className="ld-proof-cell">
      {loading ? <div className="v rd-skel ld-proof-skel" /> : <div className="v">{display}</div>}
      <div className="k">{label}</div>
    </div>
  )
}

export function Proof({ stats }) {
  const { t, i18n } = useTranslation()
  const [sectionRef, visible] = useReveal({ threshold: 0.2 })
  const gaRef = useSectionView('proof')
  const loading = stats.status === 'loading'
  const ready = stats.status === 'ready'
  const d = stats.data

  const trigger = visible && ready
  const winRate = useCountUp(ready ? d.win_rate : null, { trigger })
  const closedTrades = useCountUp(ready ? d.closed_trades : null, { trigger })
  const avgYield = useCountUp(ready ? d.avg_yield : null, { trigger })
  const avgHoldDays = useCountUp(ready ? d.avg_hold_days : null, { trigger })

  const cells = [
    { label: t('ld.proof.winRate'), loading, display: fmtPercent(winRate, i18n.language) },
    { label: t('ld.proof.closedTrades'), loading, display: fmtInt(closedTrades, i18n.language) },
    { label: t('ld.proof.avgReturn'), loading, display: fmtPercent(avgYield, i18n.language, { signed: true }) },
    {
      label: t('ld.proof.avgHold'),
      loading,
      display: avgHoldDays == null ? '—' : t('ld.proof.avgHoldValue', { count: Math.round(avgHoldDays) }),
    },
  ]

  return (
    <section className="ld-section ld-proof" id="proof" ref={(el) => { sectionRef.current = el; gaRef.current = el }}>
      <div className="ld-section-inner">
        <div className="ld-section-head">
          <p className="tag">{t('ld.proof.tag')}</p>
          <h2>{t('ld.proof.title')}</h2>
          <p>{t('ld.proof.sub')}</p>
        </div>
        <div className="ld-proof-grid">
          {cells.map((c) => <Cell key={c.label} {...c} />)}
        </div>
        {/* Only rendered once real data has resolved — no placeholder date
         * while loading/failed, since a wrong-looking date is worse than no
         * date (see stats.js's null-safe fmtAsOf). */}
        {ready && d.as_of != null && (
          <p className="ld-proof-asof">{t('ld.proof.asOf', { date: fmtAsOf(d.as_of, i18n.language) })}</p>
        )}
        <div className="ld-proof-foot">
          <span className="note">{t('ld.proof.disclaimer')}</span>
          <Link
            to="/performance"
            className="ld-btn ghost"
            onClick={() => track('landing_performance_click', {})}
          >
            {ready ? t('ld.proof.browseTrades', { count: d.closed_trades }) : t('ld.proof.browseTradesGeneric')}
            {' '}<span className="ld-arrow" aria-hidden="true">→</span>
          </Link>
        </div>
      </div>
    </section>
  )
}
