import { useRef } from 'react'
import { useTranslation, Trans } from 'react-i18next'
import { Link } from 'react-router-dom'
import { track } from './analytics'
import { useSectionView, usePointerGlow, useMagnetic } from './hooks'

/* ── Hero backdrop: two large, soft ambient color blobs (pure CSS radial-
 * gradient, no canvas/JS at all). Replaces both prior attempts — a two-line
 * equity chart (read as distracting lines) and a scattered point field
 * (read as "dirt"/noise). Maximum restraint this time: just soft diffuse
 * blue/green atmosphere behind the headline, nothing granular or linear
 * enough to read as a graphic in its own right. Static — no entrance
 * animation, no pulse, nothing to get busy. */
function HeroGlowBlobs() {
  return (
    <div className="ld-hero-blobs" aria-hidden="true">
      <span className="b1" />
      <span className="b2" />
    </div>
  )
}

// Ambient signal-bar field — a wide band of bars behind the drawn curve,
// giving the top of the hero real visual density (the composition move
// borrowed from cinematic hero treatments — dense texture up top, fading to
// nothing, huge type in the middle). Heights come from a fixed sine
// composition, not Math.random: same "fixed, hand-tuned shape, not
// live/random data" rule as MODEL_Y/BENCH_Y above, computed once at module
// scope so the shape never differs between reloads.
const FIELD_BARS = Array.from({ length: 48 }, (_, i) =>
  Math.round(Math.abs(Math.sin(i * 0.7)) * 55 + Math.abs(Math.sin(i * 0.31)) * 30 + 8)
)

function SignalField() {
  return (
    <div className="ld-hero-field" aria-hidden="true">
      {FIELD_BARS.map((h, i) => <span key={i} style={{ '--h': h + '%' }} />)}
    </div>
  )
}

const TAPE_ITEMS = [
  ['NVDA', '+2.41%', 'up'], ['AAPL', '+0.84%', 'up'], ['GOOGL', '+1.12%', 'up'],
  ['TSLA', '-1.94%', 'down'], ['MSFT', '+0.63%', 'up'], ['AVGO', '+1.87%', 'up'],
  ['XOM', '-0.42%', 'down'], ['BAC', '+0.91%', 'up'],
]
function DecorativeTape() {
  const items = [...TAPE_ITEMS, ...TAPE_ITEMS]
  return (
    <div className="ld-tape" aria-hidden="true">
      <div className="ld-tape-track">
        {items.map(([tk, ch, dir], i) => (
          <span key={i} className={'ld-tape-item ' + dir}>
            {tk} <span dir="ltr">{ch}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

export function Hero() {
  const { t } = useTranslation()
  const gaRef = useSectionView('hero')
  const glowRef = useRef(null)
  usePointerGlow(glowRef)
  const ctaRef = useRef(null)
  useMagnetic(ctaRef)
  return (
    <header
      className="ld-hero"
      id="top"
      ref={(el) => { gaRef.current = el; glowRef.current = el }}
    >
      <SignalField />
      {/* Cursor-tracked radial glow — CSS var driven (see usePointerGlow),
       * so this repaints via `background`, never `filter`, keeping it clear
       * of the backdrop-filter+infinite-animation ghost-smear bug class
       * this page has hit before. Its own layer under the canvas/content so
       * it never competes with the drawn curve for contrast. */}
      <div className="ld-hero-glow" aria-hidden="true" />
      <HeroGlowBlobs />
      <div className="ld-hero-inner">
        <p className="ld-eyebrow">{t('ld.hero.eyebrow')}</p>
        <h1 className="ld-thesis">
          <Trans i18nKey="ld.hero.thesis" components={{ g: <span className="g" /> }} />
        </h1>
        <p className="ld-hero-sub">{t('ld.hero.sub')}</p>
        <div className="ld-hero-ctas">
          <Link
            to="/sign-up"
            className="ld-btn primary lg magnetic"
            ref={ctaRef}
            onClick={() => track('landing_cta_click', { location: 'hero', label: 'sign_up' })}
          >
            {t('ld.hero.ctaPrimary')}
          </Link>
        </div>
        <p className="ld-hero-trust">{t('ld.hero.trustLine')}</p>
      </div>
      <DecorativeTape />
      <div className="ld-scrollcue">{t('ld.hero.scrollCue')}<div className="chev" /></div>
    </header>
  )
}
