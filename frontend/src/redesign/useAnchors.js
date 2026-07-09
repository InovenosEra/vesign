import { useLayoutEffect, useRef, useState, useCallback } from 'react'

/* Measures every [data-anchor] element inside `containerRef`, relative to
 * the container's own box (not the viewport). Recomputes on container
 * resize, window resize, and font load (font swap shifts row heights),
 * coalesced to at most once per animation frame so a resize drag doesn't
 * thrash. Returns { anchors, containerSize }; both are null until the first
 * measurement completes — consumers should render nothing until then.
 * Anchors present in the DOM but not yet measured, or removed later, simply
 * aren't in the returned object — no anchor is ever required to exist. */
export function useAnchors(containerRef) {
  const [anchors, setAnchors] = useState(null)
  const [containerSize, setContainerSize] = useState(null)
  const rafRef = useRef(null)

  const measure = useCallback(() => {
    const container = containerRef.current
    if (!container) return
    const containerRect = container.getBoundingClientRect()
    const found = container.querySelectorAll('[data-anchor]')
    console.log('ANCHORS FOUND:', found.length, [...found].map(el => el.dataset.anchor))
    const next = {}
    found.forEach((el) => {
      const name = el.getAttribute('data-anchor')
      if (!name) return
      const r = el.getBoundingClientRect()
      next[name] = { x: r.left - containerRect.left, y: r.top - containerRect.top, w: r.width, h: r.height }
    })
    console.log('ANCHORS MEASURED:', Object.keys(next).length, next)
    setAnchors(next)
    setContainerSize({ width: containerRect.width, height: containerRect.height })
  }, [containerRef])

  const scheduleMeasure = useCallback(() => {
    if (rafRef.current != null) return
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = null
      measure()
    })
  }, [measure])

  useLayoutEffect(() => {
    const container = containerRef.current
    if (!container) return

    // Double rAF tick before the FIRST measurement only. A single rAF (what
    // scheduleMeasure uses for ongoing recomputes) could still land before
    // the browser settles a layout pass triggered by CSS that finished
    // applying just after mount — this was the actual root cause of the
    // containerSize being stuck at ~1157px while .eng-panels was really
    // ~1600px wide (the full-bleed change). Two frames is the standard
    // "wait for layout to truly be settled" pattern.
    let raf1 = null, raf2 = null
    raf1 = requestAnimationFrame(() => {
      raf1 = null
      raf2 = requestAnimationFrame(() => {
        raf2 = null
        measure()
      })
    })

    const ro = new ResizeObserver(scheduleMeasure)
    ro.observe(container)
    window.addEventListener('resize', scheduleMeasure)

    // ResizeObserver only fires when .eng-panels' own OUTER box size changes.
    // Content that shifts INSIDE it without changing that outer size — e.g. the
    // net's node count/row-gap constants, which sit inside a fixed-aspect-ratio
    // SVG whose own box doesn't depend on how many circles are drawn in it —
    // never triggers a re-measure otherwise, silently leaving `anchors` stale
    // after exactly that kind of edit. MutationObserver catches it generally
    // (any node added/removed anywhere in the subtree).
    // Deliberately NOT observing `attributes`: EngineConnectors' own paths/
    // circles live inside this same container and update their d/cx/cy
    // attributes on every re-render (with stable keys, so React patches them
    // in place rather than adding/removing nodes) — observing attributes
    // would make every measure() trigger a re-render that re-triggers the
    // observer that triggers another measure(), forever.
    const mo = new MutationObserver(scheduleMeasure)
    mo.observe(container, { childList: true, subtree: true })

    let cancelled = false
    if (document.fonts && document.fonts.ready) {
      document.fonts.ready.then(() => { if (!cancelled) scheduleMeasure() })
    }

    return () => {
      cancelled = true
      if (raf1 != null) cancelAnimationFrame(raf1)
      if (raf2 != null) cancelAnimationFrame(raf2)
      ro.disconnect()
      mo.disconnect()
      window.removeEventListener('resize', scheduleMeasure)
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current)
    }
  }, [containerRef, scheduleMeasure, measure])

  return { anchors, containerSize }
}
