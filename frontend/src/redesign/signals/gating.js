export const isLocked = (row) => !!row?.locked
export const lockedCount = (rows) => (rows || []).filter(isLocked).length
export const hasMoreLocked = (rows) => (rows || []).some(r => r?.locked && r.reason === 'pay')
export const fmtCents = (cents) =>
  '$' + (Math.round(cents) / 100).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })

// Bulk "See all" price: 50% of (n × per-row), floored to a whole dollar.
// Mirrors backend ent.see_all_price_cents so the displayed price == the charge.
export const seeAllCents = (n, perRowCents) =>
  Math.floor((Math.max(0, n) * perRowCents) / 2 / 100) * 100
