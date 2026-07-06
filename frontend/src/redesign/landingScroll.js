// Pure helpers for the landing page's eased section-to-section scroll — no
// DOM/window access here so they're unit-testable in isolation.
// LandingPage.jsx owns the wheel/keyboard event wiring and calls into these.

export const SCROLL_DURATION_MS = 500

// Fast start, soft landing.
export const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3)

export const clampIndex = (i, len) => Math.max(0, Math.min(len - 1, i))

// direction: +1 (down/forward) or -1 (up/backward).
export const nextIndex = (current, direction, len) =>
  clampIndex(current + (direction > 0 ? 1 : -1), len)

// Index of the section whose top is closest to the current scroll position.
export const nearestIndex = (tops, scrollY) => {
  let best = 0
  let bestDist = Infinity
  tops.forEach((top, i) => {
    const d = Math.abs(top - scrollY)
    if (d < bestDist) { bestDist = d; best = i }
  })
  return best
}

// Scroll position at `elapsed` ms into a `duration`-ms eased animation from
// `from` to `to`. Returns `to` exactly once `elapsed >= duration`.
export const scrollYAt = (from, to, elapsed, duration = SCROLL_DURATION_MS) => {
  if (elapsed >= duration) return to
  const p = easeOutCubic(Math.max(0, elapsed) / duration)
  return from + (to - from) * p
}
