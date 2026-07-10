// Thin GA4 wrapper — the only thing landing components call directly. Safe
// no-op whenever gtag isn't present (ad blockers, tests, a future SSR
// pass), so callers never need to guard for it themselves.
export function track(eventName, params = {}) {
  if (typeof window === 'undefined' || typeof window.gtag !== 'function') return
  window.gtag('event', eventName, params)
}
