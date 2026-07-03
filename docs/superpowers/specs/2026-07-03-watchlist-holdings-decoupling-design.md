# Watchlist / Holdings decoupling + per-list analysis + target price — design

Status: approved by user 2026-07-03. Implementation not started.

## Problem

Today, "owning" a ticker means having a lot recorded in `watchlist_holdings`,
which is keyed by `watchlist_id` — every lot must nominally belong to one of
the user's watchlists. `AddHoldingForm` surfaces this directly: adding a
holding requires picking which watchlist to file it under (defaulting
silently to the user's only list when there's just one). The Holdings tab is
really a rollup across every lot in every watchlist the user has.

This makes Holdings and Watchlists feel like the same data viewed two ways —
in practice, a user's default watchlist often ends up listing exactly the
tickers they already hold, adding nothing beyond what Holdings already shows.

The two pages are meant to serve different purposes:
- **Holdings** — the user's real portfolio: buy price, quantity, P&L,
  performance vs. Vesign, tailored analysis of what they actually hold.
- **Watchlist** — forward-looking. Tickers the user is curious about, not
  holding. No buy price, ever. Full Vesign research depth per ticker anyway,
  plus a new capability: a user-set **target price** per ticker ("if this
  reaches $Z, I might buy"), so the page becomes an active decision-support
  tool rather than a static list.

This design makes that separation real in the data model, not just the UI,
and redesigns the Watchlists tab around the target-price idea.

## Scope

**In scope:**
- Backend: holdings become their own user-scoped concept, independent of
  `watchlist_id`. Migrate existing data.
- `AddHoldingForm` drops the watchlist picker — adding a holding is just
  ticker/shares/price/date.
- All "owned" branching removed from the Watchlists tab — every ticker in a
  watchlist displays the same way, regardless of what the user separately
  holds.
- New `target_price` per ticker-in-a-watchlist, editable inline.
- New "Vesign's read on this watchlist" per-list analysis panel (signal mix,
  near-target count, avg health, biggest upside) — adapts the existing,
  currently-unwired `VesignRead.jsx` pattern.
- Ticker rows redesigned as richer "cockpit" cards (logo/ticker/company +
  verdict badge; Price+day%, Target price, Analyst upside, Health dots).
- Free-plan gating on the Vesign-model fields only (signal mix, avg health,
  per-ticker verdict, health dots) — same `plan !== 'pro' && plan !== 'max'`
  rule used everywhere else. Price, day-change, analyst upside, and target
  price are never gated (matches the existing "keep analyst Prediction
  visible for Free" rule).
- Page layout: unchanged shape — 2-column grid, one card per watchlist,
  unlimited watchlists (grid wraps to more rows). "+ New watchlist" dashed
  tile stays as-is.

**Out of scope (explicit, deferred):**
- Actual notification delivery (email/SMS/push) when a target price is
  crossed. `target_price` is stored and its proximity to the live price is
  displayed on the page; no alerting job or delivery channel is built here.
  This is a natural follow-up and should reuse/inform the parked SMS-alerts
  work (blocked previously on "what triggers an SMS" — this answers it).
- Cleaning up existing demo data where a watchlist happens to mirror Holdings
  1:1. That's data hygiene, not a code concern — the two are independent
  going forward; any overlap in ticker membership is the user's own curation
  choice.

## Data model changes

- **New table `holdings`**: `id, user_id, ticker, quantity, buy_price,
  buy_date`. Becomes the sole source of truth for owned lots, scoped by
  `user_id` instead of `watchlist_id`.
- **Migration**: for every existing `watchlist_holdings` row, resolve
  `user_id` via `watchlist_lists.user_id` (join on `watchlist_id`), insert
  into `holdings`. After migrating and verifying row counts match, drop
  `watchlist_holdings` — no reason to keep a superseded table around.
- **`watchlist` table** (ticker membership: `id, ticker, note, list_id`)
  gains one nullable column: `target_price REAL`. Nothing else changes here —
  it already represents "ticker is in this list," fully independent of
  ownership.

## Backend API changes

- New, user-scoped (no `list_id` in the path):
  - `GET /api/holdings` — all of the user's lots.
  - `POST /api/holdings` — add a lot (ticker, quantity, buy_price, buy_date).
  - `DELETE /api/holdings/{id}` — remove a lot.
  - These replace `GET|POST /api/watchlists/{list_id}/holdings` and
    `DELETE /api/watchlists/{list_id}/holdings/{holding_id}`, which are
    removed once the frontend no longer calls them.
- `GET /api/portfolio/holdings` — same response shape as today; reads
  directly from `holdings` now instead of joining through
  `watchlist_lists`/`watchlist_holdings`.
- `GET /api/watchlists/{list_id}/tickers` — each row gains `target_price`.
- `PATCH /api/watchlists/{list_id}/tickers/{ticker}` — body gains an optional
  `target_price` alongside the existing `note`; validate `target_price > 0`
  when provided (mirrors `validateHolding`'s existing price check).

## Frontend changes

- **`AddHoldingForm.jsx`** — remove the `watchlists` prop, `wlId` state, and
  the `<select>`. `addHolding()` no longer takes a list id; calls the new
  `/api/holdings` endpoint directly.
- **`HoldingsTable.jsx`** — stop passing `watchlists` into `AddHoldingForm`.
- **`watchlistDerive.js`** — delete `owned`, `lotCount`, `costBasis`,
  `pnlAbs`, `yieldPct` from `buildCards`; `upsidePct` is now computed
  unconditionally for every row (drop the `!owned` guard). Drop the
  `holdingsByList` parameter entirely — this module no longer needs lots
  data. Add `targetPrice` to each row (straight passthrough from the
  ticker-membership row).
- **`WatchlistsTab.jsx`** — remove the `holdingsQueries`/`getHoldings(l.id)`
  fetch (watchlists no longer touch ownership data at all).
- **`WatchlistCard.jsx`** — `invalidateAfterMutation` currently invalidates
  `watchlist-holdings`/`portfolio-comparison`/`portfolio-holdings`/
  `portfolio-lots` after ticker-membership mutations; none of those are
  affected by add/remove/rename anymore, so trim it to just
  `watchlist-tickers` + `watchlists`. Replace the current dense row list with a
  vertical stack of new ticker "cockpit" cards:
  - Header: logo, ticker, company, verdict badge (BUY/HOLD/SELL).
  - Cockpit strip (4 cells): Price + day % change · Target price (editable
    input, PATCHes `/api/watchlists/{list_id}/tickers/{ticker}` on
    blur/Enter) · Analyst upside % · Health dots.
  - "+ Add ticker" affordance stays at the bottom of the stack (existing
    pattern, reused).
  - Removing a ticker from the list simplifies: today, removing an "owned"
    row goes through a confirm dialog (`removeTarget` state) since it used to
    risk conflating "remove from this list" with "lose your position." With
    ownership fully decoupled, removing a ticker only ever removes list
    membership — never touches holdings — so every removal becomes the
    simple, no-confirm case. Drop the owned/confirm branch; keep
    `ConfirmDialog` only for delete-list (unchanged).
- **New `WatchlistRead.jsx`** (adapts `VesignRead.jsx`'s markup/CSS classes
  for a per-list aggregate instead of a whole-portfolio one), rendered above
  the ticker stack in each `WatchlistCard`:
  - Signal mix (BUY/HOLD/SELL chip counts, Vesign-model — gated)
  - Near-target count: how many tickers are within **5%** of their target
    price (user data — never gated)
  - Avg health (Vesign-model — gated)
  - Biggest upside: ticker + % (analyst-based — never gated, matches the
    existing "keep analyst Prediction visible for Free" rule)
- **Free-plan lock**: reuse the existing `modelLocked = me.plan !== 'pro' &&
  me.plan !== 'max'` check (identical to Screener/Deep-Dive/HoldingsTable).
  When locked: the whole `WatchlistRead` panel gets the solid-scrim + upgrade
  CTA overlay (matching the existing lock pattern), and each ticker card's
  verdict badge + health dots are hazed via `text-shadow` (never
  `filter:blur` — that property is what caused the Chrome ghost-smear
  compositing bug fixed earlier; see `project_signals_ghost_smear` memory).
  Price, day-change, target price, and analyst upside stay fully visible and
  interactive for Free users.

## Fallout: the "12M Yield" stat no longer means anything

Today, `WatchlistCard`'s header shows a "12M Yield" figure sourced from
`GET /api/portfolio/comparison`, and the toolbar's "Sort: Yield ↓" sorts
lists by it (`sortCards` in `watchlistDerive.js`). That figure is a
cost-basis-derived return — with no owned lots left in a watchlist, there's
nothing to compute a yield from. This has to change alongside everything
else:

- Drop the "12M Yield" stat from the card header entirely. Header becomes
  just the list name + ticker count + kebab menu.
- Replace "Sort: Yield ↓" with **"Sort: Avg upside ↓"** — average analyst
  upside across the list's tickers (already-available, never-gated data).
  Keeps a meaningful ranking ("which watchlist has the most promising
  prospects right now") without resurrecting any ownership concept.
- `sortCards`/`comparisonByName` wiring in `watchlistDerive.js` and
  `WatchlistsTab.jsx` is removed along with the `/api/portfolio/comparison`
  fetch on this page (that endpoint still exists for the Holdings tab's own
  bar chart, untouched).

## Page layout (unchanged from today, confirmed via mockup)

2-column grid (`grid-template-columns: 1fr 1fr`), one card per watchlist,
wrapping to additional rows as the user creates more lists — no cap on how
many watchlists exist. The dashed "+ Create new watchlist" tile keeps its
place in the grid. Each list card's internal layout is the only thing
changing (analysis panel + cockpit-card stack replacing the old dense rows).
