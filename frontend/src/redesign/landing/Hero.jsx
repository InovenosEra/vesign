import { useEffect, useRef } from 'react'
import { useTranslation, Trans } from 'react-i18next'
import { Link } from 'react-router-dom'
import { track } from './analytics'
import { useSectionView } from './hooks'

/* ── Hero backdrop: two smoothed curves drawing themselves once on load —
 * one climbing with real drawdowns, one flatter beneath it. This is the
 * page's signature moment (the visual form of "every signal comes with its
 * receipts"), but it is DECORATIVE, not data: aria-hidden, no axes, no tick
 * labels, no percentages or dollar figures, no legend text ("Alpha"/"vs
 * S&P"), no ticker symbols implying a real trade. The Proof section below
 * is the only place actual numbers appear, sourced from /api/stats — this
 * canvas must never look like it's asserting one. Draws once (~1.6s
 * ease-out reveal), then rests with a single slow ambient glow pulse at the
 * lead point — ONE orchestrated moment, not a scattered/looping effect.
 * Pauses on tab-hidden, DPR clamped to 2, frame-gated to ~60fps. */
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

    // Fixed, hand-tuned point sequences (fraction of width/height, not
    // literal values) — a deliberate shape, not live/random data. A gentle
    // random jitter is layered on at draw time so it doesn't read as a
    // rigid, obviously-synthetic zigzag, but the underlying path is fixed.
    const MODEL_Y = [0.72, 0.68, 0.70, 0.60, 0.63, 0.52, 0.55, 0.44, 0.47, 0.36, 0.40, 0.27, 0.30, 0.18, 0.22, 0.10]
    const BENCH_Y = [0.74, 0.73, 0.75, 0.72, 0.74, 0.70, 0.72, 0.68, 0.70, 0.66, 0.68, 0.64, 0.66, 0.61, 0.63, 0.58]
    const N = MODEL_Y.length
    const jitter = MODEL_Y.map(() => (Math.random() - 0.5) * 0.012)

    const pathAt = (ys, jit, progress) => {
      const count = Math.max(2, Math.floor(progress * (N - 1)) + 2)
      const pts = []
      for (let i = 0; i < Math.min(count, N); i++) {
        const x = (i / (N - 1)) * W
        const y = (ys[i] + (jit ? jitter[i] : 0)) * H
        pts.push([x, y])
      }
      // Partial-segment interpolation for the leading edge so the draw
      // reveal is smooth rather than snapping point-to-point.
      const exact = progress * (N - 1)
      const frac = exact - Math.floor(exact)
      if (frac > 0 && pts.length < N) {
        const i = pts.length - 1
        const x0 = ((i - 1) / (N - 1)) * W, y0 = (ys[i - 1] + (jit ? jitter[i - 1] : 0)) * H
        const x1 = (i / (N - 1)) * W, y1 = (ys[i] + (jit ? jitter[i] : 0)) * H
        pts[pts.length - 1] = [x0 + (x1 - x0) * frac, y0 + (y1 - y0) * frac]
      }
      return pts
    }

    const drawSmooth = (pts, strokeStyle, lineWidth, glow) => {
      if (pts.length < 2) return
      ctx.beginPath()
      ctx.moveTo(pts[0][0], pts[0][1])
      for (let i = 1; i < pts.length - 1; i++) {
        const mx = (pts[i][0] + pts[i + 1][0]) / 2
        const my = (pts[i][1] + pts[i + 1][1]) / 2
        ctx.quadraticCurveTo(pts[i][0], pts[i][1], mx, my)
      }
      const last = pts[pts.length - 1]
      ctx.lineTo(last[0], last[1])
      ctx.strokeStyle = strokeStyle
      ctx.lineWidth = lineWidth
      ctx.lineJoin = 'round'
      ctx.lineCap = 'round'
      if (glow) { ctx.shadowColor = strokeStyle; ctx.shadowBlur = glow }
      ctx.stroke()
      ctx.shadowBlur = 0
    }

    const REVEAL_MS = 1600
    let startTime = null
    let lastFrame = 0
    const FRAME_MIN = 1000 / 60

    function render(progress, pulsePhase) {
      ctx.clearRect(0, 0, W, H)
      const benchPts = pathAt(BENCH_Y, false, progress)
      drawSmooth(benchPts, 'rgba(168,176,190,0.30)', 1.5, 0)
      const modelPts = pathAt(MODEL_Y, true, progress)
      drawSmooth(modelPts, 'rgba(0,217,126,0.85)', 2.4, 10)
      if (modelPts.length) {
        const [x, y] = modelPts[modelPts.length - 1]
        const r = 3 + (pulsePhase != null ? Math.sin(pulsePhase) * 1.4 + 1.4 : 0)
        ctx.beginPath()
        ctx.fillStyle = 'rgba(0,217,126,0.95)'
        ctx.shadowColor = 'rgba(0,217,126,0.9)'
        ctx.shadowBlur = 14
        ctx.arc(x, y, r, 0, Math.PI * 2)
        ctx.fill()
        ctx.shadowBlur = 0
      }
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
  const sectionRef = useSectionView('hero')
  return (
    <header className="ld-hero" id="top" ref={sectionRef}>
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
            className="ld-btn primary lg"
            onClick={() => track('landing_cta_click', { location: 'hero', label: 'sign_up' })}
          >
            {t('ld.hero.ctaPrimary')}
          </Link>
          <a href="#pricing" className="ld-btn ghost lg">{t('ld.hero.ctaSecondary')}</a>
        </div>
      </div>
      <DecorativeTape />
      <div className="ld-scrollcue">{t('ld.hero.scrollCue')}<div className="chev" /></div>
    </header>
  )
}
