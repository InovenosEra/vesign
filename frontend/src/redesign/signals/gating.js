export const isLocked = (row) => !!row?.locked
export const lockedCount = (rows) => (rows || []).filter(isLocked).length
export const hasMoreLocked = (rows) => (rows || []).some(r => r?.locked && r.reason === 'pay')
export const fmtCents = (cents) =>
  '$' + (Math.round(cents) / 100).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })

// Bulk "See all" price: exactly 50% of (n × per-row), to the cent.
// Mirrors backend ent.see_all_price_cents so the displayed price == the charge.
export const seeAllCents = (n, perRowCents) =>
  Math.floor((Math.max(0, n) * perRowCents) / 2)

// --- Tier unlock pricing (mirrors backend.entitlements; rates come from
// /api/signals/today/tiers so they stay single-sourced) -----------------------
export const PER_TIER_RATE_CENTS = { 1: 30, 2: 20, 3: 10 }
export const TIER_ALL_DISCOUNT_PCT = 40

// Bucket a BUY row to a pricing/legend tier: 1 Prime, 2 Strong, else 3 Promising.
export const tierOf = (row) => (row?.tier === 1 || row?.tier === 2) ? row.tier : 3

const rateFor = (tier, rates) => (rates?.[tier] ?? PER_TIER_RATE_CENTS[tier] ?? 10)

export const tierUnlockCents = (tier, nLocked, rates) =>
  rateFor(tier, rates) * Math.max(0, nLocked)

// Undiscounted sum of the per-tier prices — the "was" anchor for "Unlock all".
export const allTiersGrossCents = (lockedByTier, rates) =>
  [1, 2, 3].reduce((s, t) => s + tierUnlockCents(t, lockedByTier?.[t] || 0, rates), 0)

// 40% off the per-tier sum (round half-up), clamped ≥ the priciest single tier.
export const allTiersCents = (lockedByTier, rates) => {
  const per = [1, 2, 3].map(t => tierUnlockCents(t, lockedByTier?.[t] || 0, rates))
  const gross = per[0] + per[1] + per[2]
  if (gross <= 0) return 0
  const discounted = Math.floor((gross * (100 - TIER_ALL_DISCOUNT_PCT) + 50) / 100)
  return Math.max(discounted, ...per)
}
