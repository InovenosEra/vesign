# Edit Portfolio Lot — Design

## Goal

On the Portfolio page's Holdings table, under the per-ticker lot breakdown, a
user can currently **add** a new lot and **delete** an existing lot. This
feature adds the missing third operation: **edit** an existing lot's
quantity, cost basis (buy price), and purchase date, without deleting and
re-adding it.

## Scope

- Editable fields: `quantity`, `buy_price`, `buy_date`.
- Ticker is **not** editable in this flow. Moving a lot to a different
  ticker is effectively a different lot and stays covered by delete + add.
- No other scope. No changes to the add-lot or delete-lot behavior, no
  changes to how aggregated Holdings rows are computed.

## Background (confirmed via codebase exploration)

- Lots are user-owned rows in a single `holdings` table
  (`id, user_id, ticker, quantity, buy_price, buy_date`) — no `updated_at`,
  no soft-delete flag.
- This is a separate concept from the trading-engine's own `trade_lots` /
  `lot_seq` (DCA simulation bookkeeping for the Vesign model's simulated
  positions). Editing a user's holdings lot must not touch `trade_lots` —
  they are unrelated.
- All P&L / average-cost / weight figures shown in the Holdings table are
  computed **live** from the raw lot rows, both server-side (`avg_price` is
  a `SUM(quantity*buy_price)/SUM(quantity)` at query time) and client-side
  (`derive.js`'s `computeRows()`). Nothing is cached or persisted that an
  edit would need to explicitly recompute — invalidating the two existing
  react-query caches (`portfolio-holdings`, `portfolio-lots`) is sufficient,
  identical to what add-lot and delete-lot already do.
- No other table references a lot's row `id` (no FK, no join). An in-place
  `UPDATE ... WHERE id = :hid` is safe and preserves the id, which the
  frontend's lot rows are keyed by.
- Add/delete lot currently have no plan/entitlement gating — this stays
  true for edit as well (holdings CRUD is available on every plan; only
  model-derived *display* fields elsewhere are plan-gated).

## Backend

### `PATCH /api/holdings/{holding_id}`

- New endpoint under the existing `protected` router (auth required, same
  as add/delete).
- Request body (new Pydantic model, e.g. `HoldingUpdate`):
  ```
  { quantity: float, buy_price: float, buy_date: str }
  ```
  Ticker is intentionally excluded from the body — it is not editable.
- Validation: reuse the same rules the add-lot path already enforces
  (`quantity > 0`, `buy_price >= 0`, `buy_date` parses as a valid date and
  is not in the future). Factor the shared checks out of
  `_validate_and_normalize_holding` so both the create and update paths
  call the same validation, rather than duplicating the rules.
- Ownership + existence: `UPDATE holdings SET quantity=:q, buy_price=:p,
  buy_date=:d WHERE id=:hid AND user_id=:uid`. If the resulting row count
  is 0 (lot doesn't exist, or belongs to another user), return `404`.
  This is a deliberate difference from `DELETE /api/holdings/{id}`, which
  silently no-ops on a missing/foreign id — an edit is a targeted
  correction the user is actively watching the result of, so a failure
  should be visible rather than silently swallowed. This does not change
  delete's existing behavior.
- Response: `200` with the updated lot (`{id, ticker, quantity, buy_price,
  buy_date}`), so the frontend can use the response directly if useful
  without a second fetch.

### API client

- `frontend/src/api.js`: add `export const updateHolding = (id, body) =>
  patch(\`/holdings/${id}\`, body)`, mirroring the existing `updateTicker`
  helper that already uses the `patch()` wrapper.

## Frontend

### Interaction

- In `HoldingLots.jsx`, add an edit (pencil) icon next to the existing
  delete icon (`lot-del`) on each lot row.
- Clicking it turns that row into an inline edit form: quantity, buy
  price, and buy date fields, with Save/Cancel actions. This mirrors the
  lightweight inline style already used when adding a new lot (no modal
  — consistent with `feedback_dont_widen_modal` precedent of favoring
  minimal, non-modal UI where the existing pattern already avoids modals).
- Only one row can be in edit mode at a time (opening edit on a different
  row, or add-lot, cancels the current edit) — same "one open editor"
  convention as the existing add-lot toggle.
- Ticker is shown read-only in the edit row (not an editable field).

### Component reuse

- Extend `AddHoldingForm.jsx` to support an edit mode via a new optional
  `editingLot` prop (the existing lot object: `{id, ticker, quantity,
  buy_price, buy_date}`). When `editingLot` is provided:
  - `shares`/`price`/`date` state initializes from `editingLot.quantity` /
    `editingLot.buy_price` / `editingLot.buy_date` instead of the empty
    defaults, and the live-quote prefill effect (which only makes sense
    for a brand-new lot) is skipped.
  - Ticker is passed as `presetTicker={editingLot.ticker}` — this already
    hides the ticker input entirely (`{!presetTicker && (...)}` in the
    current component), so locking the ticker for edit needs no new
    logic, just reusing this existing prop.
  - The submit mutation calls `updateHolding(editingLot.id, body)`
    instead of `addHolding(body)` when `editingLot` is set.
  - Button label reads "Save"/"Saving…" instead of "Add"/"Adding…" when
    editing.
  - On success, invalidates the same two query keys
    (`['portfolio-holdings']`, `['portfolio-lots', ticker]`) the add and
    delete mutations already invalidate.
- This avoids building a second form component for what is functionally
  the same three fields with a different submit target.

### Validation / error handling

- Reuse `validateHolding()` from `holdingForm.js` for client-side
  validation before submit (same rules as add-lot).
- On a failed save (validation error or non-2xx response), the inline
  form shows the error message in place, the same way the add-lot form
  already surfaces errors — it does not close on failure.

## Out of scope

- Editing the ticker of a lot.
- Bulk edit of multiple lots at once.
- Any change to `trade_lots` / DCA `lot_seq` / signal engine state.
- Any change to delete-lot's existing silent-no-op behavior.
- Any entitlement/plan gating changes.

## Testing

- Backend: extend `tests/backend/test_portfolio_lots.py` with cases
  mirroring the existing add/delete coverage — successful edit updates
  the row and is reflected in `GET /api/portfolio/holdings/lots`;
  invalid quantity/price/future-date is rejected with the same messages
  as add-lot; editing another user's lot id (or a nonexistent id)
  returns 404; ticker is unchanged by an edit request that omits it.
- Frontend: extend `holdingForm.test.js`-style coverage if the shared
  validation function changes; otherwise rely on the existing
  `validateHolding()` tests since the edit form reuses it unchanged.
