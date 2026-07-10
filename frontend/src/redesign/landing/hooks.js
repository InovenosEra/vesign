import { useEffect, useRef, useState } from 'react'
import { track } from './analytics'

const prefersReducedMotion = () =>
  typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches

// Scroll-triggered reveal: fires once (disconnects after), CSS does the
// actual translateY+opacity transition keyed off the returned boolean —
// this hook only ever flips false->true, never back, so nothing re-animates
// scrolling back up past a section. Reduced-motion is resolved into the
// lazy initial state (not a setState call inside the effect) so it never
// triggers an extra render.
export function useReveal({ threshold = 0.15 } = {}) {
  const ref = useRef(null)
  const [visible, setVisible] = useState(() => prefersReducedMotion())
  useEffect(() => {
    if (visible) return
    const el = ref.current
    if (!el) return
    const observer = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) {
        setVisible(true)
        observer.disconnect()
      }
    }, { threshold })
    observer.observe(el)
    return () => observer.disconnect()
  }, [threshold, visible])
  return [ref, visible]
}

// Fires GA4 landing_section_view the first time a section is >=50% visible.
// `extraEvent` lets one section also fire its own named event at the same
// crossing (only Pricing uses this today, for the spec's distinct
// landing_pricing_view alongside the generic section-view).
export function useSectionView(sectionName, extraEvent = null) {
  const ref = useRef(null)
  useEffect(() => {
    const el = ref.current
    if (!el || !sectionName) return
    const observer = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) {
        track('landing_section_view', { section: sectionName })
        if (extraEvent) track(extraEvent, {})
        observer.disconnect()
      }
    }, { threshold: 0.5 })
    observer.observe(el)
    return () => observer.disconnect()
  }, [sectionName, extraEvent])
  return ref
}

// Tracks pointer position as CSS custom properties (--px/--py, px relative
// to the element's own box) directly on the DOM node via ref, rAF-throttled
// — never through setState, so a fast mouse doesn't trigger React re-renders
// per pixel. Purely a hover embellishment (spotlight/glare effects), never a
// functional affordance, so it's disabled outright — not degraded — on
// coarse/touch pointers and under reduced-motion rather than left running
// pointlessly in the background.
export function usePointerGlow(ref) {
  useEffect(() => {
    const el = ref.current
    if (!el) return
    if (prefersReducedMotion() || !window.matchMedia('(pointer: fine)').matches) return
    let raf = null
    let last = null
    const apply = () => {
      raf = null
      if (!last) return
      const rect = el.getBoundingClientRect()
      el.style.setProperty('--px', `${last.clientX - rect.left}px`)
      el.style.setProperty('--py', `${last.clientY - rect.top}px`)
    }
    const onMove = (e) => {
      last = e
      if (raf == null) raf = requestAnimationFrame(apply)
    }
    el.addEventListener('pointermove', onMove)
    return () => {
      el.removeEventListener('pointermove', onMove)
      if (raf != null) cancelAnimationFrame(raf)
    }
  }, [ref])
}

const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v))

// Magnetic hover: the element eases toward the cursor within its own bounds
// (translate only, capped at `max`px), snapping back on pointerleave. Same
// disable-outright-on-touch/reduced-motion posture as usePointerGlow — this
// is pure delight, never load-bearing for the click itself (onClick still
// fires identically with or without it).
export function useMagnetic(ref, { strength = 0.3, max = 14 } = {}) {
  useEffect(() => {
    const el = ref.current
    if (!el) return
    if (prefersReducedMotion() || !window.matchMedia('(pointer: fine)').matches) return
    let raf = null
    let last = null
    const apply = () => {
      raf = null
      if (!last) return
      const rect = el.getBoundingClientRect()
      const dx = clamp((last.clientX - (rect.left + rect.width / 2)) * strength, -max, max)
      const dy = clamp((last.clientY - (rect.top + rect.height / 2)) * strength, -max, max)
      el.style.setProperty('--mx', `${dx}px`)
      el.style.setProperty('--my', `${dy}px`)
    }
    const onMove = (e) => {
      last = e
      if (raf == null) raf = requestAnimationFrame(apply)
    }
    const onLeave = () => {
      last = null
      el.style.setProperty('--mx', '0px')
      el.style.setProperty('--my', '0px')
    }
    el.addEventListener('pointermove', onMove)
    el.addEventListener('pointerleave', onLeave)
    return () => {
      el.removeEventListener('pointermove', onMove)
      el.removeEventListener('pointerleave', onLeave)
      if (raf != null) cancelAnimationFrame(raf)
    }
  }, [ref, strength, max])
}

// Scroll progress as a 0..1 fraction of the page's scrollable height,
// rAF-throttled the same way useScrolled (Nav.jsx) is — one listener, one
// pending frame at a time, never layout work per raw scroll event.
export function useScrollProgress() {
  const [progress, setProgress] = useState(0)
  useEffect(() => {
    let ticking = false
    const check = () => {
      const doc = document.documentElement
      const scrollable = doc.scrollHeight - doc.clientHeight
      setProgress(scrollable > 0 ? Math.min(1, Math.max(0, window.scrollY / scrollable)) : 0)
      ticking = false
    }
    const onScroll = () => {
      if (ticking) return
      ticking = true
      requestAnimationFrame(check)
    }
    check()
    window.addEventListener('scroll', onScroll, { passive: true })
    window.addEventListener('resize', onScroll)
    return () => {
      window.removeEventListener('scroll', onScroll)
      window.removeEventListener('resize', onScroll)
    }
  }, [])
  return progress
}

// Eases a number from 0 to `target` over `duration`ms once `trigger` flips
// true — null target (stats not loaded yet) is a no-op until it resolves.
// Under prefers-reduced-motion the hook never animates at all: it just
// returns `target` directly (computed at render time, not via setState),
// so the reduced-motion path never touches the rAF/animated-state machinery.
export function useCountUp(target, { trigger = true, duration = 900 } = {}) {
  const [animated, setAnimated] = useState(null)
  const startedRef = useRef(false)
  const reduce = prefersReducedMotion()
  useEffect(() => {
    if (reduce || target == null || !trigger || startedRef.current) return
    startedRef.current = true
    const start = performance.now()
    let raf
    const step = (now) => {
      const p = Math.min(1, (now - start) / duration)
      const eased = 1 - Math.pow(1 - p, 3)
      setAnimated(target * eased)
      if (p < 1) raf = requestAnimationFrame(step)
    }
    raf = requestAnimationFrame(step)
    return () => raf && cancelAnimationFrame(raf)
  }, [target, trigger, duration, reduce])
  return reduce ? target : animated
}
