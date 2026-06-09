# Plan & Billing — functional "Change plan" & "Cancel"

Date: 2026-06-09
Branch: `feat/ui-redesign`

## Problem

On the Account → **Plan & billing** pane (`frontend/src/redesign/AccountPage.jsx`,
`PlanPane`):

- **"Change plan"** is a `NavLink to="/market"` — clicking it navigates to the
  Market page (looks like going "home"). It does nothing plan-related.
- **"Cancel"** is a bare `<button>` with no handler — does nothing.

The whole pane is also mock data: it always renders `Free`'s name but a hardcoded
`$15 / month`, "unlimited everything" feature list, a fake renewal date, a fake
Mastercard, and six fake invoices — regardless of the user's actual plan.

## Constraints / reality

- Plans are a DB field only: `entitlements.get_plan(uid)` / `set_plan(uid, plan)`,
  `PLANS = ("free", "pro", "max")`. **No payment processor** (no Stripe/Clerk billing).
- Real tier differences enforced in `entitlements.py`:
  - **Free** — top-10 signal preview (yield only), rest locked.
  - **Pro** — wallet + pay-per-signal unlocks ($0.10/row, $0.50 see-all).
  - **Max** — everything unlocked, no redaction.
- `/api/me` already returns `{plan, balance_cents, ...}`; consumed via `MeContext`.
- Header plan-tier chip color: Free grey / Pro gold / Max purple (existing).

## Decision

Make the buttons **really switch the plan** (no payment). Real billing / Stripe is
a later pass.

## A. Tier catalog (single source of truth)

A frontend constant (id, label, price, tagline, features[], accent) drives BOTH
the plan card and the modal. Accent matches the header chip.

| id   | label | price   | tagline             | features |
|------|-------|---------|---------------------|----------|
| free | Free  | $0/mo   | Get started         | Top-10 daily signals preview · Real-time market data · 1 watchlist · Market & Research pages |
| pro  | Pro   | $19/mo  | Pay as you go       | Everything in Free · Unlock any signal from your wallet ($0.10 each) · 5 watchlists · Full fundamentals & ML predictions |
| max  | Max   | $49/mo  | Everything unlocked | Everything in Pro · All signals unlocked, no per-signal cost · Unlimited watchlists · API access · Priority support |

Accents: free → grey, pro → gold, max → purple (reuse existing chip color tokens).

## B. Backend

New protected endpoint:

```
POST /api/me/plan   body: { "plan": "free" | "pro" | "max" }
```

- Validate `plan in entitlements.PLANS` → else 400.
- `entitlements.set_plan(uid, plan)`.
- Return `{ "plan": plan }`.
- No payment. Honors the same auth as other `/api/me` routes.

Frontend `api.js`: add `setPlan(plan)` POST helper.

## C. Frontend behavior

### Plan card (PlanPane)
- Price, features, tagline, accent now read from the catalog keyed by `me.plan`
  (fixes the "$15 / unlimited" lie; Free correctly shows `$0`).
- **Hide billing on Free**: when `me.plan === 'free'`, hide the "Renews / Billed"
  meta line, the **Payment method** card, and the **Billing history** table. (Paid
  tiers keep the existing mock for now.)
- **"Change plan"** → opens `ChangePlanModal`.
- **"Cancel"** → hidden when `me.plan === 'free'`; otherwise opens `CancelPlanModal`.

### ChangePlanModal
- Three tier cards side-by-side (Free / Pro / Max) from the catalog.
- Current plan: badged "Current", button disabled.
- Other tiers: button "Switch to {label}".
- On click → `setPlan(id)` → on success: invalidate `['me']` query (header chip +
  card update live) → toast `Switched to {label}` → close. On error → toast error,
  stay open.
- Reuses existing `.acc-modal` overlay pattern (like `PictureModal`/`PasswordModal`)
  and the existing AccountPage toast.

### CancelPlanModal
- Confirm dialog: "Cancel subscription? You'll move to the Free plan."
- Confirm → `setPlan('free')` → invalidate `['me']` → toast "Moved to Free" → close.
- Dismiss → close, no change.

## D. Out of scope (later pass)

- Real payment processor (Stripe Checkout/Portal).
- Real renewal date / member-since / payment method / invoice history for paid tiers
  (still mock).
- Proration, period-end retention, downgrade scheduling.

## Testing

- Backend: `POST /api/me/plan` happy path for each of free/pro/max updates
  `get_plan`; invalid plan → 400; unauth → 401 (matches other protected routes).
- Frontend: manual — switch Free→Pro→Max→Free, confirm header chip + plan card +
  billing-section visibility update live; Cancel hidden on Free.
