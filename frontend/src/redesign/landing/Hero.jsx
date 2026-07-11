import { useEffect, useRef } from 'react'
import { useTranslation, Trans } from 'react-i18next'
import { Link } from 'react-router-dom'
import { track } from './analytics'
import { useSectionView, usePointerGlow, useMagnetic } from './hooks'

/* ── Hero backdrop: a field of glowing points draws in once on load, each
 * one standing for "a signal, somewhere in the market" — not a specific
 * trade. Replaces the prior two-line equity chart (removed per feedback:
 * read as distracting green/grey lines competing with the headline); pure
 * blue/green points, no grey, no lines at all. Still DECORATIVE, not data:
 * aria-hidden, no axes, no labels, nothing that could be mistaken for a
 * real chart — the Proof section below is the only place actual numbers
 * appear, sourced from /api/stats. Draws once (~1.6s staggered reveal,
 * center outward), then rests with a slow synchronized glow pulse across a
 * handful of anchor points — ONE orchestrated moment, not scattered
 * twinkling. Pauses on tab-hidden, DPR clamped to 2, frame-gated to ~60fps. */
function HeroCanvas() {
  const ref = useRef(null)
  useEffect(() => {
    const canvas = ref.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    let W, H, DPR, raf, visible = true

    function resize() {
      DPR = Math.min(2, window.devicePixelRatio || 1)
      W = canvas.clientWidth; H = canvas.clientHeight
      canvas.width = W * DPR; canvas.height = H * DPR
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0)
    }
    window.addEventListener('resize', resize)
    resize()

    // Fixed point field (fraction of width/height) laid out via a golden-
    // angle spiral — an organic, non-grid scatter with zero randomness, so
    // the shape is identical on every load (same "fixed, hand-tuned, not
    // live/random data" rule the removed curve arrays followed).
    const N = 46
    const GOLD = 2.399963229728653
    const POINTS = Array.from({ length: N }, (_, i) => {
      const r = Math.sqrt((i + 0.5) / N)
      const theta = i * GOLD
      return {
        x: 0.5 + r * Math.cos(theta) * 0.58,
        y: 0.4 + r * Math.sin(theta) * 0.5,
        size: 1.3 + ((i * 53) % 7) * 0.28,
        green: i % 3 === 0,
      }
    })
    // A few anchor points (spread across the field, not clustered) that
    // breathe together on the SAME phase once revealed — a small, cohesive
    // moment rather than independent scattered blinking.
    const PULSE_IDX = new Set([2, 13, 24, 37])

    const REVEAL_MS = 1600
    let startTime = null
    let lastFrame = 0
    const FRAME_MIN = 1000 / 60

    function render(progress, pulsePhase) {
      ctx.clearRect(0, 0, W, H)
      POINTS.forEach((p, i) => {
        // Staggered pop-in, center point (i=0) first, outward by index.
        const appear = Math.min(1, Math.max(0, progress * N - i + 1))
        if (appear <= 0) return
        const x = p.x * W, y = p.y * H
        const color = p.green ? '0,217,126' : '96,165,250'
        const pulsing = pulsePhase != null && PULSE_IDX.has(i)
        const pulse = pulsing ? Math.sin(pulsePhase) * 0.25 + 0.75 : 1
        ctx.beginPath()
        ctx.fillStyle = `rgba(${color},${0.65 * appear * pulse})`
        ctx.shadowColor = `rgba(${color},0.8)`
        ctx.shadowBlur = (pulsing ? 8 + pulse * 4 : 5) * appear
        ctx.arc(x, y, p.size * (pulsing ? pulse : 1), 0, Math.PI * 2)
        ctx.fill()
        ctx.shadowBlur = 0
      })
    }

    function frame(now) {
      if (!visible) { raf = requestAnimationFrame(frame); return }
      if (now - lastFrame < FRAME_MIN) { raf = requestAnimationFrame(frame); return }
      lastFrame = now
      if (startTime == null) startTime = now
      const elapsed = now - startTime
      const revealP = Math.min(1, elapsed / REVEAL_MS)
      const eased = 1 - Math.pow(1 - revealP, 3)
      if (revealP < 1) {
        render(eased, null)
      } else {
        render(1, elapsed / 900)
      }
      raf = requestAnimationFrame(frame)
    }

    if (reduce) {
      render(1, null)
    } else {
      raf = requestAnimationFrame(frame)
    }

    const onVisibility = () => { visible = document.visibilityState === 'visible' }
    document.addEventListener('visibilitychange', onVisibility)

    return () => {
      window.removeEventListener('resize', resize)
      document.removeEventListener('visibilitychange', onVisibility)
      if (raf) cancelAnimationFrame(raf)
    }
  }, [])
  return <canvas className="ld-hero-canvas" ref={ref} aria-hidden="true" />
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
      <HeroCanvas />
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
