export const isLocked = (row) => !!row?.locked
export const lockedCount = (rows) => (rows || []).filter(isLocked).length
export const hasMoreLocked = (rows) => (rows || []).some(r => r?.locked && r.reason === 'pay')
export const fmtCents = (cents) =>
  '$' + (Math.round(cents) / 100).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
