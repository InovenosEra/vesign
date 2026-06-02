# Portfolio Holdings Management — Design

**Date:** 2026-06-02
**Branch:** `feat/ui-redesign` (not deployed to prod until go-live)
**Status:** Approved (design)

## Problem

The redesign Portfolio page's **Holdings** table is read-only. There is no way to
add a new ticker, add another lot to an existing ticker (dollar-cost averaging),
view the individual lots behind an aggregated position, or delete a lot.

## Goal

Let the user fully manage holdings from the Portfolio page:
- **Add** a new ticker (first lot).
- **Add a lot** to an existing ticker (DCA).
- **Expand** a holding row to see its individual lots.
- **Delete** a lot.

## Existing model (reused, unchanged)

- Lots live in `watchlist_holdings (id, watchlist_id, ticker, quantity, buy_price, buy_date)`.
- A "lot" = one row. Multiple lots per ticker = DCA.
- `GET /api/portfolio/holdings` aggregates lots **across all of the user's watchlists**, per ticker (total_qty, total_cost, avg_price, latest_close, prev_close, …). It returns **aggregates only — no lot IDs**.
- Existing endpoints (no change):
  - `POST /api/watchlists/{id}/holdings` — body `HoldingCreate {ticker, quantity, buy_price, buy_date}`, returns 201.
  - `DELETE /api/watchlists/{id}/holdings/{holding_id}` — returns 204.
  - `GET /api/watchlists` — `[{id, name}]`.
- API client helpers already exist: `addHolding(id, body)`, `deleteHolding(id, holdingId)`, `getWatchlists()`, `searchTickers(q, n)`.

## Design

### Backend

1. **New endpoint** `GET /api/portfolio/holdings/lots?ticker=XYZ&market=US`
   - Returns the ticker's individual lots across the **current user's** watchlists, newest first:
     ```json
     [{ "id": 27, "watchlist_id": 11, "watchlist_name": "Mine",
        "ticker": "AAPL", "quantity": 10, "buy_price": 180.5, "buy_date": "2026-01-15" }]
     ```
   - SQL: `watchlist_holdings wh JOIN watchlist_lists wl ON wh.watchlist_id = wl.id WHERE wl.user_id = :uid AND wh.ticker = :ticker AND <market filter>` (same `.TA` market filter as `/api/portfolio/holdings`).
   - Why: the aggregate endpoint can't drive per-lot view/delete (no IDs). This one provides lot IDs + their owning watchlist so delete works regardless of which list a lot is in.

2. **Server-side validation** on `add_holding` (harden the existing `HoldingCreate`):
   - `ticker` non-empty, upper-cased, must exist in `companies` → else 400.
   - `quantity > 0` → else 400.
   - `buy_price >= 0` → else 400.
   - `buy_date` parseable ISO date, not in the future → else 400.
   - The list must belong to the requesting user (the endpoint already scopes by user; keep that).

### Frontend (`redesign/portfolio/HoldingsTable.jsx` + small new pieces)

3. **"+ Add holding"** control in the Holdings section header (next to Export CSV) opens an inline add form (popover/row):
   - **Ticker** — autocomplete via `searchTickers` (same pattern as Deep Dive search); selecting fills the symbol.
   - **Shares** — number input, > 0.
   - **Buy price** — number input, pre-filled with the ticker's current/live price (editable).
   - **Buy date** — date input, default today, not future.
   - **Watchlist** — dropdown shown **only when the user has > 1 watchlist**; otherwise the single watchlist is used silently. Default = first/"Mine".
   - Submit → `addHolding(watchlistId, {ticker, quantity, buy_price, buy_date})` → on success invalidate `['portfolio-holdings']` (+ lots query) and close the form. Inline error on 400.

4. **Expandable rows**: each holding row gets an expand chevron (left or right).
   - Clicking the chevron toggles a lots sub-section for that ticker, fetched via the new lots endpoint (`['portfolio-lots', ticker]`).
   - Each lot shows: shares · buy price · buy date · (optional) per-lot P/L vs latest close, and a **delete** (🗑) action → `deleteHolding(lot.watchlist_id, lot.id)` → invalidate holdings + lots.
   - A **"+ Add lot"** button inside the expanded area opens the same add form with the ticker pre-filled (DCA).
   - **Row click still opens the ticker modal**; the chevron and expanded controls `stopPropagation` so they don't trigger the modal.

5. **Currency & refresh**: all monetary values use `useCurrency().fmtPrice`/`fmtAmount` (consistent with the rest of Portfolio). React-Query invalidation drives updates; the page's existing 3s live refresh continues to work.

### Data flow

```
Add:    form → addHolding(wlId, body) → 201 → invalidate [portfolio-holdings],[portfolio-lots,ticker]
Expand: chevron → getHoldingLots(ticker) → render lots
Delete: 🗑 → deleteHolding(lot.watchlist_id, lot.id) → 204 → invalidate [portfolio-holdings],[portfolio-lots,ticker]
```

### Error handling

- Add/delete failures surface an inline message near the form/lot; the table state is unchanged on failure (no optimistic mutation that could desync).
- New lots endpoint returns `[]` for an unknown/empty ticker (no 404 needed for the table use-case).

## Testing

- Backend: test the new `/api/portfolio/holdings/lots` endpoint (returns lots with IDs + watchlist for the user; excludes other users / other market); add+delete round-trip reflects in `/api/portfolio/holdings` aggregate; validation rejects bad input (qty<=0, future date, unknown ticker).
- Existing backend suite must stay green.

## Out of scope (YAGNI)

- Editing an existing lot in place (delete + re-add covers it).
- Creating/renaming/deleting whole watchlists from the Portfolio page (separate concern).
- Per-watchlist allocation views.

## Production safety

- All code on `feat/ui-redesign`; not merged/deployed until go-live.
- New endpoint is purely additive; reuses the existing `watchlist_holdings` table (already in prod). No schema change, no migration, no server/DB mutation from this work.
