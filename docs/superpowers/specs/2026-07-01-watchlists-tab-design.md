# Portfolio › Watchlists tab — design

Status: approved by user 2026-07-01. Implementation not started.

## Problem

The redesigned Portfolio page has two tabs: Holdings (fully built) and
Watchlists (a stub). `WatchlistsTab.jsx` today only renders read-only cards
with a list name + aggregate 12m yield, search, and sort — no way to create a
list, rename/delete one, or add/remove tickers. That CRUD still exists, fully
built, on the pre-redesign `/watchlist-old` page, which isn't reachable from
the redesigned nav.

Meanwhile the Holdings tab already grew its own lot-management (`AddHoldingForm`
/ `HoldingLots`, with a watchlist-bucket picker) and all the aggregate views
(KPIs, performance chart, allocation donut, cost→value bridge). So "Watchlists"
in the redesign no longer needs to be a portfolio-summary page — it needs to be
the page where a user organizes tickers into named lists ("Core Tech",
"Dividend", "ML Picks", ...), independent of whether they own shares.

There is already unused CSS (`portfolio.css`) and a full mockup
(`portfolio-v1.html`) for a card-per-list design — per-ticker rows with logo,
price, day change, yield — that was never wired to real data. This design
finishes that wiring.

## Scope

**In scope**: list CRUD (create/rename/delete), per-list ticker membership
(add/remove), viewing each list's tickers with live price/day-change and
either P&L (if owned) or analyst upside (if watch-only), click-through to the
shared ticker modal, search across list names + ticker symbols, sort by yield.

**Out of scope** (stays on the Holdings tab, unchanged): buying/adding lots,
per-ticker lot editing, aggregate KPIs/charts, CSV/XLSX export. No backend
changes — every mutation reuses an existing `/api/watchlists/...` endpoint.

## Components

- **`WatchlistsTab.jsx`** (rewritten) — toolbar (`N watchlists · M tickers`,
  search, sort-by-yield toggle, "+ New watchlist") + card grid + trailing
  dashed "add new" card. Owns the create-list popover and list-level
  mutations (create/rename/delete now live here or are threaded to
  `WatchlistCard`; delete requires confirmation, see below).
- **`WatchlistCard.jsx`** (new) — one list: header (name, ticker count,
  aggregate 12m yield from the existing comparison endpoint, "⋯" menu with
  Rename/Delete), body (`WatchlistRow` per ticker, or an empty-state line),
  footer ("+ Add ticker" that expands into an inline ticker-search input,
  autocomplete via `searchTickers`, matching the existing `AddHoldingForm`
  suggestion-dropdown pattern/styling).
- **`WatchlistRow.jsx`** (new, function inside `WatchlistCard.jsx` is fine) —
  logo, ticker + company, live price, day % (from `useLivePrices`), and on the
  right either a P&L yield-pill (ticker has lots in this list) or the analyst
  upside % (watch-only). Row click opens the shared ticker modal
  (`useTickerModal()`); a small ✕ appears on hover to remove the ticker.
- **`ConfirmDialog.jsx`** (new, in `portfolio/`) — generic version of the
  `.confirm-overlay`/`.confirm-box` pattern already used by the signals
  unlock flow (`signals/UnlockAll.jsx` `ConfirmUnlockDialog`, styled in
  `signals.css`, global under `.rd`). Props: `title`, `body` (node, so callers
  can bold specifics), `confirmLabel`, `danger` (red confirm button),
  `onConfirm`, `onCancel`. Used for both delete-list and remove-ticker.
- **`portfolio/watchlistDerive.js`** (new, pure functions, unit-tested like
  `derive.js`/`holdingForm.js`) — `buildCards(lists, tickersByList,
  holdingsByList, signalsByTicker, livePrices)` → per-card view models (owned
  vs watch-only classification, cost/value/yield per owned ticker, day-change
  per row); `filterCards(cards, query)`; `sortCards(cards, dir)`.

## Data flow

- `getWatchlists()` → `[{id, name}]`. One card per list + the "add new" card.
- `useQueries` over `getWatchlistTickers(id)` for every list, in parallel (not
  sequential) → ticker symbols per card.
- `useQueries` over `getHoldings(id)` for every list → which tickers in that
  list have lots (owned) vs none (watch-only), plus cost basis for the
  yield-pill.
- One `getSignalsByTickers(allTrackedTickersDeduped)` call across the union of
  every list's tickers → company name, close, `target_mean_price` (for
  analyst upside on watch-only rows), same as the old page's approach.
- `useLivePrices(allTrackedTickersDeduped)` → live price + day change for
  every row.
- Card header's aggregate 12m yield keeps using `getPortfolioComparison('US')`
  exactly as today's stub does — no change there.
- Search matches list name OR any member ticker symbol, computed client-side
  in `filterCards` once ticker lists are loaded.
- Whole grid is gated on lists + tickers + holdings + signals all loaded
  (same synchronized-loading pattern used elsewhere in the redesign) — shows
  a skeleton until then, not a partially-populated grid.

## Mutations & destructive-action handling

- Create list → `createWatchlist(name)`, invalidate `['watchlists']`.
- Rename (via card menu → inline text input, not a double-click) →
  `renameWatchlist(id, name)`, invalidate `['watchlists']` +
  `['portfolio-comparison']`.
- Add ticker → `addTicker(listId, ticker)`, invalidate that list's tickers
  query + `['portfolio-comparison']`.
- **Remove ticker** → `removeTicker(listId, ticker)`. The backend already
  cascades this to delete any owned lots for that ticker in that list
  (`DELETE FROM watchlist_holdings WHERE watchlist_id = ... AND ticker = ...`).
  So removing an owned ticker is destructive, not just "stop watching." The
  `ConfirmDialog` body must say so when the ticker has lots — e.g. "AAPL has 2
  lots ($4,320 invested) in this list — removing it will also delete those
  holdings." — versus a plain "Remove AAPL from Core Tech?" when watch-only.
- **Delete list** → `deleteWatchlist(id)`, same cascade concern at the list
  level: the dialog states ticker count and, if any tickers in the list are
  owned, the total invested that will be deleted along with it.
- Invalidate `['portfolio-holdings']` / `['portfolio-lots', ticker]` too on
  any mutation that can change holdings (remove ticker, delete list), so the
  Holdings tab stays in sync.

## Empty states & edge cases

- No watchlists yet: grid shows only the dashed "add new" card; toolbar reads
  "0 watchlists · 0 tickers tracked".
- A list with zero tickers: card body shows a muted "No tickers yet" line
  instead of rows; footer's "+ Add ticker" still works.
- The mockup's card-footer "Created Aug 14, 2024 · Updated 09:31 ET" meta line
  is dropped — `watchlist_lists` has no `created_at` column, and adding one
  for a cosmetic line is out of scope. Footer keeps just "+ Add ticker".
- No entitlement/paywall gating needed on this tab: rows show price, day %,
  P&L, and analyst upside — none of those are gated `MODEL_FIELDS`
  (signal/health/ML/vqs/vesign_score), consistent with keeping analyst
  "Prediction" visible to Free plan everywhere else in the app.

## Testing

- Unit tests for `watchlistDerive.js` (`buildCards`/`filterCards`/`sortCards`)
  following the existing `derive.test.js` / `holdingForm.test.js` pattern —
  pure functions, no component rendering harness in this codebase today.
- Manual verification via the dev server: create/rename/delete a list, add/
  remove a watch-only ticker, add/remove an owned ticker (confirm the
  cascade-delete warning appears), search by list name and by ticker symbol,
  sort toggle, empty-list and zero-lists states, row click opens the ticker
  modal.
