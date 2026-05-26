/* Shared helpers for the News feed (hero, cards, controls). */
export const ago = (m) =>
  m == null ? '' : m < 60 ? `${m}m ago` : m < 1440 ? `${Math.floor(m / 60)}h ago` : `${Math.floor(m / 1440)}d ago`

export const newsSrc = (n) => `${n.source || ''}${n.age_minutes != null ? ` · ${ago(n.age_minutes)}` : ''}`

export const openUrl = (url) => { if (url) window.open(url, '_blank', 'noopener') }

export const bgImg = (img) => (img ? { backgroundImage: `url(${img})` } : undefined)
