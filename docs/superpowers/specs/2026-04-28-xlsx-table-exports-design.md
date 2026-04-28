# XLSX Table Exports — Design

## Goal

Add a "Download as XLSX" button to each of the five tables on the site. The
download must contain the **full set of DB columns per ticker** (not just the
visible ones), and must respect the user's current filters/sort while including
**all pages** (no pagination cutoff).

## Affected tables (5 buttons total)

| Page              | Table              | Button location                             |
| ----------------- | ------------------ | ------------------------------------------- |
| `SignalsPage`     | Signals            | Next to existing filter bar / table header  |
| `TradesPage`      | Historical Trades  | Next to that section's header               |
| `TradesPage`      | Open Trades        | Next to that section's header               |
| `WatchlistPage`   | Tickers            | Next to the tickers table header            |
| `PortfolioPage`   | Holdings           | Next to the holdings table header           |

## Architecture

**Server-side generation.** Each table gets a dedicated FastAPI endpoint that
returns `.xlsx` bytes via `StreamingResponse`. The frontend is a thin button
that builds a URL with the current filter params and calls
`window.location.assign(url)` — the browser handles the download natively.

Why backend (not SheetJS):
- Frontend currently only fetches the columns it renders. "Full DB columns per
  ticker" requires joins the frontend doesn't normally pull.
- Server-side generation bypasses the frontend's pagination/filtering view of
  data.
- Filter params already exist on the matching read endpoints — we reuse them.

**XLSX library:** `openpyxl` (small, pure-Python, no system deps). Add to
`requirements.txt`.

## Endpoints

Each endpoint accepts the same query params as its corresponding read endpoint
(filters), runs one SQL query that joins everything needed, writes one XLSX
sheet, and streams it back.

```
GET /api/signals/export.xlsx
    ?date=&market=&q=&signal=&min_score=&...      ← same filters as /api/signals
GET /api/trades/export.xlsx
    ?start=&end=&market=                          ← same as /api/trades
GET /api/trades/open/export.xlsx
    ?market=                                      ← same as /api/trades/open
GET /api/watchlists/{list_id}/export.xlsx
    ← scoped to one watchlist; auth + ownership check via _assert_owns_list
GET /api/portfolio/holdings/export.xlsx
    ← all US holdings across the user's watchlists, same shape as
      /api/portfolio/holdings
```

Each response sets:
```
Content-Type:        application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
Content-Disposition: attachment; filename="<resource>_<YYYY-MM-DD>.xlsx"
```

All endpoints require auth (the existing dependency) — except none should be
public; even the Signals export is gated behind login like the rest of `/api`.

## Column lists

### `/api/signals/export.xlsx`

Every column of the `signals` table:
`date, ticker, open, high, low, close, volume, rsi, bb_high, bb_low, macd,
rsi_factor, bb_factor, macd_factor, trend_factor, volume_sma_20, volume_ratio,
week52_high, pct_from_52w_high, target_mean_price, target_high_price,
target_low_price, number_of_analysts, last_update, health_score,
prediction_score, fair_value_upside, analyst_condition, bb_pct_b, bb_condition,
rsi_below_30, rsi_3day_flag, volume_flag, week52_condition, health_condition,
ml_condition, signal, score, vesign_score`

Plus joined enrichment:
- `company` (from `companies.company`)
- `sector` (from `companies.sector`)
- `industry` (from `companies.industry`)
- `market_cap` (from `fundamentals.market_cap`, latest)
- `logo_url`

### `/api/trades/export.xlsx` (Historical / closed)

Every column of `trade_log` for closed trades within the filter window:
`ticker, buy_date, buy_price, sell_date, sell_price, days_held, yield_pct,
shares, return_dollars, market`

Plus joined enrichment:
- `company`, `sector`, `industry`, `market_cap`, `logo_url`

One row per closed trade (matches the visible table — `PayPal × 2 closes` =
2 rows).

### `/api/trades/open/export.xlsx`

For each open position (BUY without subsequent SELL):
`ticker, company, sector, industry, market_cap, logo_url, buy_date, buy_price,
last_close, days_held, live_price, unrealized_yield_pct,
unrealized_return_dollars`

`live_price` uses the same `live_prices` cache the frontend uses (5s TTL).

### `/api/watchlists/{list_id}/export.xlsx`

Same column set as Signals, scoped to the tickers in this watchlist.
One row per ticker (latest signals row). Filename uses the watchlist name:
`watchlist_<name>_<YYYY-MM-DD>.xlsx`.

### `/api/portfolio/holdings/export.xlsx`

Aggregated per ticker (across all the user's watchlists), matching the data
returned by `/api/portfolio/holdings`:
`ticker, company, sector, industry, market_cap, total_shares, avg_buy_price,
total_invested, current_value, pnl_dollars, pnl_pct, watchlists` (a
comma-separated list of watchlist names containing this ticker).

## Frontend

A new component `frontend/src/components/DownloadXLSXButton.jsx`:

```
<DownloadXLSXButton url={`/api/signals/export.xlsx?${queryString}`}
                    filename="signals" />
```

- Renders an icon button (download icon) with tooltip "Download as XLSX".
- On click: passes through the user's auth token (since the API requires auth)
  and triggers a download. Implementation:
  1. `fetch(url, { headers: { Authorization: \`Bearer ${token}\` } })`
  2. `await res.blob()` → create object URL → `<a download>` click → revoke.

Auth token-bearing fetch is needed because `window.location.assign` does not
include the Clerk Bearer header. This is the standard pattern for authed
downloads.

Placement: each of the 5 tables gets one instance of the button next to its
header, mirroring whatever the page already uses for its filter bar layout.

## Error handling

- Any export endpoint that errors mid-stream returns a `500` JSON
  `{ "detail": "Export failed" }` (StreamingResponse hasn't started yet, so
  this is fine). The button shows a brief inline toast / error message.
- Empty result set: still return a valid XLSX with a header row only — the
  user gets a file confirming "no rows match my filters" rather than an
  ambiguous error.

## Memory / performance

- Largest export = the full Signals table for a single date (≈1500 rows × ~45
  columns) ≈ small file (well under 1 MB). No streaming complexity needed —
  build the workbook in memory, write to BytesIO, return.
- For ME exports (e.g., 5+ years of trade history), still capped at hundreds of
  rows. Same strategy.
- No new memory pressure on the 2 GB server.

## Out of scope

- Multi-sheet exports (e.g., one sheet per ticker) — Option B from
  brainstorming was rejected.
- Historical time-series exports (Option C) — also rejected.
- CSV / TSV variants — XLSX only.
- Bulk/cross-table exports.

## Testing

- Unit-level: a small fixture with 3 mock tickers; assert the workbook has
  expected sheet name, header row, and N data rows after applying a filter.
- Integration: hit each export endpoint with a known auth token + filter; load
  the response with `openpyxl.load_workbook` and check column count / first
  row matches.

## Files touched

| File                                              | Change                  |
| ------------------------------------------------- | ----------------------- |
| `backend/main.py`                                 | 5 new endpoints         |
| `backend/exports.py` *(new)*                      | XLSX builder helpers    |
| `frontend/src/components/DownloadXLSXButton.jsx` *(new)* | Button component  |
| `frontend/src/pages/SignalsPage.jsx`              | Add button              |
| `frontend/src/pages/TradesPage.jsx`               | Add 2 buttons           |
| `frontend/src/pages/WatchlistPage.jsx`            | Add button              |
| `frontend/src/pages/PortfolioPage.jsx`            | Add button              |
| `requirements.txt`                                | Add `openpyxl`          |
