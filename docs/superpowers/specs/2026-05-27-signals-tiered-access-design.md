# Signals Page — Tiered Access (Free / Pro / Max)

**Date:** 2026-05-27
**Branch:** `feat/ui-redesign`
**Status:** Design approved — pending implementation plan

## Problem

The Signals page is the core of the model. We want to monetize it across three
subscription tiers without leaking the model's output to users who haven't paid:

- **Free** — cannot see BUY/SELL tickers at all; sees faded rows with an
  "Upgrade to unlock" prompt. Limited teaser on Open trades.
- **Pro** — pays from a wallet to reveal individual BUY signals ($0.10/row) or
  bulk-unlock locked pages ($0.50 "see all"); gets a 10-row free preview on
  SELL and Open trades.
- **Max** — everything open, no limits.

## Access matrix

| Surface | **Free** | **Pro** | **Max** |
|---|---|---|---|
| **BUY signals** | All rows faded → "Upgrade to unlock" | All rows faded. Per-row **$0.10**, or **"Unlock all today · $0.50"** | Full |
| **SELL signals** | All rows faded → "Upgrade to unlock" | First 10 full; page 2+ faded → **"See all · $0.50"** | Full |
| **Open trades** | Rows faded; **Yield visible on top 10** only; page 2+ fully locked → "Upgrade" | First 10 full; page 2+ faded → **"See all · $0.50"** | Full |
| **Closed trades + 4 stat cards** | **TBD** (open to all for now) | TBD (open) | TBD (open) |

### Unlock semantics
- An unlock is tied to a **specific signal occurrence**: `(user_id, kind, ticker, signal_date)`.
- Permanent once paid — re-viewing the same occurrence is free forever.
- A **new** signal for the same ticker on a later date is a separate occurrence
  and costs again.
- **BUY** is purchasable **per-row ($0.10)** or **bulk ($0.50, all of today's
  new BUYs)**.
- **SELL** and **Open trades** are **bulk-only ($0.50)** — there is no per-row
  purchase; the first 10 rows are a free preview for Pro.
- **Open-trades bulk** unlock scope = **today** (the open list is live and
  re-priced daily); the unlock reveals all currently-locked open rows for that
  day.

### Pricing constants
- `PER_ROW_PRICE_CENTS = 10` (BUY only)
- `SEE_ALL_PRICE_CENTS = 50` (BUY bulk, SELL bulk, Open bulk)

## Security invariant (the whole point)

Gating is **server-side**. A locked row's payload MUST NOT contain the ticker,
company, logo, price, upside, health, VQS, ML, or any field that identifies the
signal. The fade is a veil over data the client never received — never a CSS
blur over data sitting in the network tab. An explicit test asserts this.

The single exception: Free users' Open-trades **top 10** rows include the
`yield` value (and nothing else identifying) as a teaser.

## Architecture (Approach A — redact inside existing endpoints)

The gated endpoints already receive the Clerk `user_id`. They resolve the
user's plan + unlock set and shape each row. We keep the **array return shape**
so existing consumers (`PageHead` counts, etc.) don't break; the client derives
"locked count" and the "see all" CTA from the rows plus the price constants from
`/api/me`.

### 1. Data model (new SQLite tables, created on startup like `blocked_users`)

```sql
CREATE TABLE IF NOT EXISTS user_plans (
  user_id    TEXT PRIMARY KEY,
  plan       TEXT NOT NULL DEFAULT 'free',   -- free | pro | max
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS wallets (
  user_id       TEXT PRIMARY KEY,
  balance_cents INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS signal_unlocks (
  user_id     TEXT NOT NULL,
  kind        TEXT NOT NULL,                 -- buy | sell | open
  ticker      TEXT NOT NULL,
  signal_date TEXT NOT NULL,
  created_at  TEXT DEFAULT (datetime('now')),
  UNIQUE(user_id, kind, ticker, signal_date)
);

CREATE TABLE IF NOT EXISTS wallet_txns (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id    TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,             -- negative = spend
  reason     TEXT NOT NULL,                  -- e.g. 'unlock_buy_row'
  ref        TEXT,                           -- e.g. 'AAPL:2026-05-26'
  created_at TEXT DEFAULT (datetime('now'))
);
```

- Plan source is **our DB table** (admin-controlled, like `blocked_users`).
  Future option: sync from Clerk Billing — out of scope here.

### 2. Plan / wallet resolution

- `get_plan(user_id) -> 'free'|'pro'|'max'` (default `free`).
- `get_balance(user_id) -> int cents` (default 0).
- `get_unlocks(user_id) -> set[(kind, ticker, signal_date)]`.
- **Dev override (localhost only):** honored ONLY when `BYPASS_USER_ID` is set.
  Env `DEV_PLAN` (free|pro|max) and `DEV_WALLET_CENTS` let us flip tiers and
  fund the wallet without prod writes. Production ignores both.

### 3. Redaction logic

Helper `redact_rows(rows, *, kind, plan, unlocks)` returns rows where each is
either full or:

```json
{ "locked": true, "kind": "buy", "signal_date": "2026-05-26",
  "reason": "pay", "unlock_price_cents": 10, "reveal": [] }
```

- `reason ∈ {'upgrade','pay'}` — `'upgrade'` for Free (needs a higher plan,
  no purchase possible), `'pay'` for Pro (purchasable now). Drives which CTA
  the row renders.
- `unlock_price_cents` is present only when `reason:'pay'` (10 for BUY per-row;
  bulk-only rows carry the bulk price via `/api/me`).
- `reveal` lists any fields that survive on a locked row (only `["yield"]` for
  Free Open-trades top 10; `[]` otherwise).
- Sensitive fields are omitted/nulled on locked rows.
- "First 10" = the first 10 rows in the endpoint's existing response order
  (server-controlled), so the preview boundary is deterministic.

Per endpoint:

- **`GET /api/signals/today?signal=BUY`**
  - max → all full
  - free → all locked, `reason:'upgrade'`
  - pro → row full iff `(buy,ticker,date) ∈ unlocks`, else locked
    `unlock_price_cents:10`
- **`GET /api/signals/today?signal=SELL`**
  - max → all full
  - free → all locked, `reason:'upgrade'`
  - pro → first 10 (server order) full; rows 11+ full iff unlocked, else locked
    bulk
- **`GET /api/trades/open`**
  - server sorts by `unrealized_pct` desc (was client-side; moved server-side
    for deterministic gating)
  - max → all full
  - free → all locked; rows 0–9 `reveal:['yield']` (yield present), rows 10+
    `reveal:[]`; `reason:'upgrade'`
  - pro → first 10 full; rows 11+ full iff unlocked, else locked bulk

### 4. New endpoints

- **`GET /api/me`** →
  `{ plan, balance_cents, per_row_price_cents, see_all_price_cents }`.
- **`POST /api/signals/unlock`** — body
  `{ kind:'buy'|'sell'|'open', scope:'row'|'all', ticker?, signal_date?, date? }`.
  - Reject if `plan != 'pro'` (max already full; free → 402/403 "upgrade").
  - `scope:'row'` only valid for `kind:'buy'` (10¢). `scope:'all'` → 50¢ for any
    kind.
  - Atomic transaction: re-check unlocks (idempotent — already-unlocked ⇒
    success, no charge), verify `balance >= price`, deduct, insert unlock
    row(s), insert `wallet_txns` entry.
  - Bulk (`scope:'all'`) recomputes today's locked set for that kind and inserts
    an unlock row per occurrence.
  - Returns `{ balance_cents, rows:[…revealed full rows] }` so the client patches
    in place.
  - Insufficient balance → `409`/`402` with a clear message (no top-up flow).

### 5. Frontend (React redesign, `frontend/src/redesign/signals/`)

- `useMe()` context: GET `/api/me` (plan, balance, price constants). A small
  **wallet balance chip** in the app header for Pro/Max.
- **Locked-row variant** in `SignalsSplit` / `OpenTrades`: faded content + lock
  pill.
  - BUY/Pro: per-row `$0.10` button + section-header "Unlock all today · $0.50".
  - SELL/Open page 2+: "See all · $0.50" CTA.
  - Free: "Upgrade to unlock" CTA → **placeholder destination, decided later**.
  - Free Open-trades: faded rows with `yield` shown on top 10; page 2 fully
    locked overlay.
- Unlock click → `POST /api/signals/unlock` → patch revealed rows in place (or
  invalidate the query). Insufficient balance → inline message.
- `Pager` stays 10/page; gating already aligns to its page boundaries.

### 6. Tests (mirror the existing suite patterns)

- Redaction per plan for BUY / SELL / Open (incl. the **"locked row carries no
  ticker/company/price"** invariant).
- Free Open-trades: top-10 yield present, page-2 yield absent.
- Pro preview: first-10 SELL/Open full, rest locked.
- `/api/signals/unlock`: deduction, idempotency (no double-charge),
  insufficient-balance, wrong-plan rejection, bulk reveals all locked rows.
- `/api/me` shape per plan.

## Out of scope / TBD

- **Closed trades + 4 stat cards** gating — open to all for now; revisit later.
- **Wallet top-up / real payments** — balance is admin/dev-funded for now.
- **Upgrade CTA destination** — placeholder now; Clerk billing vs `/pricing`
  page decided later.
- **Clerk Billing sync** of plan — future; DB table is source of truth now.

## Affected files (anticipated)

- `backend/auth.py` or new `backend/entitlements.py` — plan/wallet/unlock
  helpers + table creation.
- `backend/main.py` — redaction in `/api/signals/today` and `/api/trades/open`;
  new `/api/me`, `/api/signals/unlock`.
- `frontend/src/api.js` — `getMe`, `unlockSignal`.
- `frontend/src/redesign/signals/SignalsSplit.jsx`, `OpenTrades.jsx`,
  `signals.css` — locked-row UI + CTAs.
- App header — wallet chip.
- Tests under the backend test suite.
