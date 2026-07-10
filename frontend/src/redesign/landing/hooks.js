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
