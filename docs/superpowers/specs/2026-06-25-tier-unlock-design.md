# Tier-based signal unlocking — design

**Date:** 2026-06-25
**Branch:** `feat/ui-redesign`
**Status:** Approved design → ready for implementation plan

## Problem

The Signals page currently lets a Pro user unlock locked signals **one at a time**
(a slide-to-unlock per card: BUY 20¢, SELL 10¢) plus a bulk "Unlock all."

Per-row unlocking is poor UX: locked cards are **hazed** (fake ticker, fake
numbers), so each per-row purchase is a *blind* buy — the user can't see which
ticker they're buying and can't pick the one they care about. The only real use
of per-row was "spend 20¢ to reveal a random card." Now that BUY signals carry a
visible **quality tier** (Prime / Strong / Promising), the natural purchase unit
is a **tier**, chosen by quality — the one thing the user *can* see before paying.

## Goal

Replace one-by-one unlocking with **unlock-by-tier** and **unlock-all**. Remove
per-row unlocking entirely (BUY and SELL).

## Decisions (locked in)

| Decision | Choice |
|---|---|
| Pricing model | **Value-weighted per signal** — each tier has its own rate |
| Per-tier rates | Prime **30¢** · Strong **20¢** · Promising **10¢** per signal |
| Tier price | `rate × (signals still locked in that tier)` |
| Tiers are atomic | With per-row gone, a tier is **fully locked or fully unlocked** — never partial |
| "Unlock all" discount | **15% off** the sum of still-locked tier prices, clamped ≥ the priciest single locked tier |
| BUY interaction | The **tier-count chips become buy buttons** (legend doubles as the buy surface) |
| Per-row slide | **Removed** for BOTH BUY and SELL |
| `scope='row'` backend path | **Removed** entirely (endpoint, tokens, frontend slide) |
| SELL column | No tiers → free preview (5 rows) + a single "Unlock all" toggle |
| Open trades | **Unchanged** — already a flat $2 "Unlock all" |

## Pricing detail

```
tier_price(tier)   = PER_TIER_RATE[tier] × (# locked signals in that tier)
PER_TIER_RATE      = { Prime: 30, Strong: 20, Promising: 10 }  (cents)

all_price          = round( (1 − 0.15) × Σ tier_price(t) over still-locked tiers )
all_price          = max( all_price, max tier_price(t) )   # clamp: never undercut one tier
```

Worked example (today: 0 Prime, 3 Strong, 11 Promising, none unlocked):
- Strong = 20¢ × 3 = **$0.60**
- Promising = 10¢ × 11 = **$1.10**
- Sum = $1.70 → all = round(0.85 × 1.70) = **$1.45** (≥ $1.10 ✓)

Because tiers are atomic, "# locked in tier" is always either the full tier count
or zero, so chip price == full tier price whenever the chip is shown.

## UI / interaction

### BUY section head
```
[BUY] 14 signals │ ★ 3 Strong $0.60   ★ 11 Promising $1.10      [ Unlock all today $1.45 ⦿ ]
```
- Built on the existing tier-legend-with-counts (`TierLegend`, `tierStar.jsx`).
- A tier chip is a **button** iff: `plan === 'pro'` **and** that tier has ≥1
  locked signal. Clicking opens the shared `ConfirmUnlockDialog`
  ("Unlock all Strong signals? This unlocks 3 Strong signals and charges $0.60.").
- **Empty tier** (count 0, e.g. Prime today): dimmed, informational, no price, not clickable.
- **Fully-unlocked tier:** chip reverts to plain legend (star + count + label, no price/button).
- **Free user:** chips are plain legend (counts only); the existing `UpgradeBanner`
  remains the call-to-action.
- **Max user:** everything open → all chips plain legend.
- **"Unlock all today"** stays the blue switch + confirm (`UnlockAllToggle` +
  `ConfirmUnlockDialog`) on the right; charges `all_price`.

### Locked BUY card
- Keep the haze (text-shadow, NOT filter:blur — Chrome ghost-smear).
- **Remove** `SlideToUnlock`. Replace the per-card CTA with a **passive 🔒
  indicator** (no action). All purchasing happens from the section-head chips.

### SELL section head
- No tier chips. Free preview (`PRO_SELL_PREVIEW_ROWS = 5`) stays. Per-row slide
  **removed** → only the "Unlock all" toggle remains (existing SELL bulk price,
  `see_all_price_cents(count, 'sell')`, unchanged).

## Frontend ↔ backend price mirroring

Follows the existing pattern (`gating.js seeAllCents` mirrors
`entitlements.see_all_price_cents`). The frontend computes displayed prices; the
backend independently computes the authoritative charge; they must match.

**`GET /api/signals/today/tiers`** (extended) returns:
```json
{
  "total": 14,
  "all_discount_pct": 15,
  "tiers": {
    "prime":     { "tier": 1, "count": 0,  "rate_cents": 30 },
    "strong":    { "tier": 2, "count": 3,  "rate_cents": 20 },
    "promising": { "tier": 3, "count": 11, "rate_cents": 10 }
  }
}
```
This stays plan-independent/cache-friendly (totals + static rates only). The
frontend derives **locked-per-tier** from the gated BUY rows it already fetches
(`lockedInTier = count − (# unlocked rows with that tier)`), then computes each
chip price and `all_price` with the formula above.

## Backend changes

`backend/entitlements.py`
- `PER_TIER_RATE = {1: 30, 2: 20, 3: 10}`, `TIER_ALL_DISCOUNT_PCT = 15`.
- `tier_price_cents(tier, n_locked)` and `all_tier_price_cents(locked_by_tier)`
  (15%-off-sum, clamped ≥ max single tier).
- `gate_signals`: stop emitting per-row tokens for BUY/SELL locked rows
  (`_locked_row(..., reason='pay')` without `lock_token`). Remove `resolve_token`
  if it becomes unused.

`backend/main.py`
- Extend `/api/signals/today/tiers` to the shape above (counts + rates + discount).
- `unlock_signal`:
  - **Remove** `scope == 'row'` branch and the `lock_token` field handling.
  - Add **`scope == 'tier'`** (body carries `tier: int`): candidates = locked BUY
    rows of that tier for this user; `price = tier_price_cents(tier, len)`.
  - **`scope == 'all'`** for BUY: group still-locked candidates by tier → price =
    `all_tier_price_cents(...)`. (Open stays flat $2; SELL stays legacy bulk.)
  - `UnlockBody.scope` ∈ `{ all, tier }`; add optional `tier`; drop `lock_token`.

## Frontend changes

- `frontend/src/api.js` — `unlockSignal` body gains `scope:'tier'`/`tier`; drop
  the row path. `getSignalsTodayTiers` consumes the extended shape.
- `frontend/src/redesign/signals/tierStar.jsx` — `TierLegend` chips render as
  buttons when given a per-tier `{ locked, price, onUnlock }`; plain otherwise.
- `frontend/src/redesign/signals/SignalsSplit.jsx` — compute locked-per-tier from
  rows + tiers; wire chip buy + confirm; remove `unlockRow`/per-row plumbing.
- `frontend/src/redesign/signals/SignalCard.jsx` — `LockedSignalCard` drops
  `SlideToUnlock`, shows the passive 🔒 indicator.
- Delete `SlideToUnlock.jsx` if it becomes unused.

## Out of scope

- Open-trades flow (unchanged).
- Wallet top-up / payment processor (separate work).
- SELL tiering (SELL is untiered by design).

## Risks / notes

- **Price-mirroring drift:** frontend and backend both compute prices — a rate
  change must touch both (`PER_TIER_RATE` ↔ the `/tiers` `rate_cents`). The
  endpoint returning the rates keeps them single-sourced for the frontend.
- **Atomic-tier assumption:** holds only because per-row is fully removed. If
  per-row ever returns, the "locked-in-tier ∈ {0, full}" invariant breaks.
- **Clerk-gated SPA:** can't headless-render; verify via the live endpoint
  (token-reverse trick) + manual refresh on localhost.
- **Not deployed:** prod runs `main`; this ships at the go-live merge.
