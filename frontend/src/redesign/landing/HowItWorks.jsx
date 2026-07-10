import { useTranslation, Trans } from 'react-i18next'
import { useReveal, useSectionView } from './hooks'

/* ── How it works: a single vertical scroll-driven narrative, four steps.
 * Replaces the old 5-panel chevron/EngineConnectors rig — no DOM-measured
 * connector geometry here; the connecting line between steps is a plain
 * CSS ::before spanning the fixed-layout column, so there is nothing to
 * remeasure and nothing that can drift out of alignment on resize.
 * Each step's small visual is decorative/aria-hidden; the only real number
 * on the page outside Proof is the universe size, sourced from
 * stats.tickers_tracked rather than a hardcoded "1,800+". */

function ScreenVisual() {
  // 4x4 dot-matrix, dots light up in a staggered sweep via CSS animation —
  // "many stocks screened," nothing more literal than that.
  const dots = Array.from({ length: 16 })
  return (
    <div className="ld-how-visual ld-how-screen" aria-hidden="true">
      {dots.map((_, i) => <span key={i} style={{ '--i': i }} />)}
    </div>
  )
}

function ScoreVisual() {
  const factors = ['technicals', 'ml', 'analyst', 'health']
  return (
    <div className="ld-how-visual ld-how-score" aria-hidden="true">
      {factors.map((f, i) => <span key={f} className={`f f-${f}`} style={{ '--i': i }} />)}
      <span className="core" />
    </div>
  )
}

function SignalVisual() {
  return (
    <div className="ld-how-visual ld-how-signal" aria-hidden="true">
      <span className="row buy" />
      <span className="row sell" />
      <span className="row hold" />
    </div>
  )
}

function TrackVisual() {
  const ticks = Array.from({ length: 10 })
  return (
    <div className="ld-how-visual ld-how-track" aria-hidden="true">
      {ticks.map((_, i) => <span key={i} className={i % 3 === 1 ? 'neg' : 'pos'} style={{ '--i': i }} />)}
    </div>
  )
}

function Step({ index, titleKey, descKey, descValues, Visual }) {
  const { t } = useTranslation()
  const [ref, visible] = useReveal({ threshold: 0.35 })
  const StepVisual = Visual
  return (
    <li className={'ld-how-step' + (visible ? ' visible' : '')} ref={ref}>
      <div className="ld-how-num" aria-hidden="true">{String(index + 1).padStart(2, '0')}</div>
      <div className="ld-how-body">
        <h3>{t(titleKey)}</h3>
        {/* Trans (not plain t()) so the screen-count description's
         * `<ltr>{{count}}</ltr>` wrapper (see locale files) actually
         * renders as a real dir="ltr" span — the number must stay LTR
         * inside RTL prose. Works identically to t() for the other three
         * steps' plain-text keys, which contain no such tag. */}
        <p><Trans i18nKey={descKey} values={descValues} components={{ ltr: <span dir="ltr" /> }} /></p>
      </div>
      <StepVisual />
    </li>
  )
}

export function HowItWorks({ stats }) {
  const { t } = useTranslation()
  const sectionRef = useSectionView('how')
  // No hardcoded universe-size fallback — while /api/stats hasn't resolved
  // (or failed), the step reads a generic "the US market" instead of ever
  // showing a placeholder number that could be mistaken for a real one.
  const universe = stats.status === 'ready' ? stats.data?.tickers_tracked : null
  const screenDescKey = universe != null ? 'ld.how.steps.screen.desc' : 'ld.how.steps.screen.descGeneric'

  const steps = [
    { titleKey: 'ld.how.steps.screen.title', descKey: screenDescKey, descValues: { count: universe }, Visual: ScreenVisual },
    { titleKey: 'ld.how.steps.score.title', descKey: 'ld.how.steps.score.desc', Visual: ScoreVisual },
    { titleKey: 'ld.how.steps.signal.title', descKey: 'ld.how.steps.signal.desc', Visual: SignalVisual },
    { titleKey: 'ld.how.steps.track.title', descKey: 'ld.how.steps.track.desc', Visual: TrackVisual },
  ]

  return (
    <section className="ld-section ld-how" id="how" ref={sectionRef}>
      <div className="ld-section-inner">
        <div className="ld-section-head">
          <p className="tag">{t('ld.how.tag')}</p>
          <h2>{t('ld.how.title')}</h2>
          <p>{t('ld.how.sub')}</p>
        </div>
        <ol className="ld-how-steps">
          {steps.map((s, i) => <Step key={s.titleKey} index={i} {...s} />)}
        </ol>
      </div>
    </section>
  )
}
