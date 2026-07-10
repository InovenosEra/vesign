// Pure, locale-aware formatting for the /api/stats numbers shown in Proof.
// No DOM/window access — unit-testable in isolation. Every function returns
// an em dash for null/undefined so a slow or failed fetch never crashes the
// section or leaves a blank gap; callers interpolate the result into i18n
// strings rather than concatenating text here.

const EM_DASH = '—'

export function fmtInt(n, locale = 'en') {
  if (n == null || Number.isNaN(n)) return EM_DASH
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 0 }).format(n)
}

// avg_hold_days is a plain count, not a percent — same null handling, no
// unit suffix (the "days" wording lives in the i18n string that wraps it).
export function fmtDays(n, locale = 'en') {
  return fmtInt(n, locale)
}

// win_rate / avg_yield arrive from the API already multiplied by 100 (e.g.
// 64.2 meaning 64.2%), so we divide back down before handing to Intl's
// percent style, which expects a 0-1 fraction.
export function fmtPercent(n, locale = 'en', { signed = false } = {}) {
  if (n == null || Number.isNaN(n)) return EM_DASH
  return new Intl.NumberFormat(locale, {
    style: 'percent',
    maximumFractionDigits: 1,
    signDisplay: signed ? 'exceptZero' : 'auto',
  }).format(n / 100)
}

// as_of arrives as a SQLite datetime string ("2026-07-09 00:00:00.000000").
// Slicing to the date portion and appending T00:00:00 (rather than passing
// the raw "YYYY-MM-DD" straight to Date, or the space-separated string as
// -is) forces local-time parsing — a bare date-only ISO string parses as
// UTC midnight, which can display as the previous day west of UTC.
export function fmtAsOf(raw, locale = 'en') {
  if (!raw) return null
  const datePart = String(raw).slice(0, 10)
  const d = new Date(`${datePart}T00:00:00`)
  if (Number.isNaN(d.getTime())) return null
  return new Intl.DateTimeFormat(locale, { dateStyle: 'medium' }).format(d)
}
