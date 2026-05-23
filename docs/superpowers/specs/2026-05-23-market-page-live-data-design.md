# Market Page — wire mockup to live data (Phase 1)

**Date:** 2026-05-23
**Status:** Approved (user explicit go-ahead — "I want the new UI to reflect the data as it is on production")
**Scope:** Local implementation only. Production server untouched until the wider redesign deploys.
**Spec author:** Claude (Opus 4.7)

## Why

The redesign mockups at `.superpowers/brainstorm/7260-1779139139/content/` currently render with hardcoded placeholder data (SPY 486.84, NVDA 228.91, fake sector deltas, fake news headlines). The user wants every section of the new UI to reflect production data so the redesign can be evaluated and demoed against reality.

The DB on the local machine is already synced with production (last sync 2026-05-23 12:30, MAX(date)=2026-05-22 across signals/predictions/daily_prices). The FastAPI backend at `localhost:8000` reads from this synced DB. The static mockup server at `localhost:8080` serves the brainstorm directory. What's missing is the wiring between them.

This is Phase 1 of a 3-phase effort: **Market page first** (largest body of new endpoint work), then Trades + Portfolio + Research (mostly reuses existing endpoints), then Account + Onboarding + marketing pages.

## Decisions locked in (from brainstorm 2026-05-23)

| Decision | Value |
|---|---|
| Decomposition | Three phases by page-cluster. This spec covers Phase 1 only. |
| External data sources | Mix: FMP (we already pay for it) + yfinance (existing path for VIX) + free public APIs as fallback |
| Polling | Snapshot on page load — no live polling. Refresh = browser reload. |
| Auth model | All endpoints registered-users-only (`@protected.get`). Spotlight gets moved from `@app.get` to `@protected.get` as part of this. |
| Local dev auth | `BYPASS_AUTH=1` env flag treats the request as a fixed dev user (`laufer.israel@gmail.com`). Only set in local `.env`, gitignored — never reaches prod. |
| JS organization | Shared `assets/data.js` exposing `window.VesignAPI.*` fetchers and `window.VesignFmt.*` format helpers. Per-page inline `<script>` blocks consume it. |

## 1. Backend infra changes

### 1.1 `BYPASS_AUTH` env flag

In `backend/main.py`, the auth dependency (current Clerk verification) gains a single new short-circuit:

```python
if os.getenv("BYPASS_AUTH") == "1":
    return {"email": "laufer.israel@gmail.com", "sub": "dev-bypass"}
```

`.env.example` documents it; the user's local `.env` already has `CORS_ORIGINS=http://localhost:3000,http://localhost:8080` from the Spotlight cycle — `BYPASS_AUTH=1` is appended.

### 1.2 Spotlight visibility fix

`backend/main.py` currently has:

```python
@app.get("/api/spotlight/today")
def spotlight_today(): ...
```

Change to:

```python
@protected.get("/api/spotlight/today")
def spotlight_today(_user=Depends(...)): ...
```

The endpoint still works for the local mockup because `BYPASS_AUTH=1` is set. Production deploys without the bypass automatically require a session.

## 2. New endpoints

All under `/api/market/*`, all `@protected.get`, all snapshot-on-call. Caching is in-process TTL via the existing `threading.Lock` pattern.

| Endpoint | Method/Path | Data source | Cache | Notes |
|---|---|---|---|---|
| Indices | `GET /api/market/indices` | `daily_prices` last close + 30-trading-day history for sparkline | 60s | SPY/QQQ/DIA/IWM/VIX. Sparkline = list of 30 closes. |
| Cross-market | `GET /api/market/cross` | yfinance live: `DX-Y.NYB, ^TNX, GC=F, CL=F, BTC-USD, EURUSD=X` | 60s | If yfinance fails, fall back to FMP `/quote`. If both fail, return last cached value with a `stale: true` flag. |
| Movers | `GET /api/market/movers?type=gainers\|losers\|active&limit=5` | `daily_prices` last two trading days | 60s | Gainers/Losers ranked by `(close-prev_close)/prev_close`. Active ranked by `volume`. US-only. Excludes SPY/VOO. |
| Breadth | `GET /api/market/breadth` | `daily_prices` last two days + `companies.market='US'` filter | 60s | Returns `{advancers, decliners, week52_highs, week52_lows, above_50d_ma_pct}`. |
| Sectors | `GET /api/market/sectors` | `daily_prices` + `companies.industry` | 60s | One row per sector: weighted % change + top-3 movers by absolute %. |
| Top news | `GET /api/market/news/top?limit=5` | FMP `/stock-news?limit=...` | 5 min | Headline, source, ticker (if any), age in minutes. |
| Top analyst | `GET /api/market/analyst-changes/top?days=1&limit=5` | `analyst_targets_history` aggregated | 5 min | Latest change per ticker; UPGRADE/DOWNGRADE/INITIATE/RAISE-TP classification. |
| Earnings/week | `GET /api/market/earnings/week` | FMP `/earning-calendar?from=Mon&to=Fri` | 1 h | Ticker, company, EPS est, BMO/AMC, day-of-week. |
| Economic calendar | `GET /api/market/economic-calendar?days=7` | FMP `/economic-calendar?from=today&to=today+7` US-only | 1 h | Time, event name, importance (1–3 stars), estimate, prior. |
| Tape | `GET /api/market/tape` | `daily_prices` last 2 days for 15 well-known tickers | 60s | Combined payload for the 32px tape ticker. Single roundtrip. |

### 2.1 Existing endpoints reused (no changes)
- `GET /api/market/status` — top-nav market-open chip
- `GET /api/spotlight/today` — Spotlight panel (after visibility fix in 1.2)
- `GET /api/data/status` — stale-data banner (if used on Market page)

## 3. Shared `assets/data.js`

New file at `.superpowers/brainstorm/7260-1779139139/content/assets/data.js`. Exposes two globals:

```js
window.VesignAPI = {
  base: 'http://localhost:8000',
  // Returns Promise<null|object>. Logs errors to console; never throws.
  fetchSpotlight,
  fetchIndices,
  fetchCross,
  fetchMovers,           // type: 'gainers' | 'losers' | 'active'
  fetchBreadth,
  fetchSectors,
  fetchTopNews,
  fetchTopAnalyst,
  fetchEarningsWeek,
  fetchEconomicCalendar,
  fetchTape,
  fetchMarketStatus,
};

window.VesignFmt = {
  money,   // (1234.5) → "$1,234.50"
  pct,     // (0.0234) → "+2.34%"  /  (-0.012) → "-1.20%"
  num,     // (1_234_567) → "1.23M"
  date,    // ISO → "May 22, 2026"
  spark,   // [..prices] → inline SVG path string
  ago,     // ISO → "12 min ago"
};
```

Every fetcher does the same thing: `fetch(\`${base}/api/market/...\`).then(r => r.ok ? r.json() : null).catch(() => null)`. Failures degrade silently — the consuming section hides itself or shows a small "data unavailable" line.

Each mockup HTML includes `<script src="assets/data.js"></script>` near the top once.

## 4. Per-section wiring in `market-v1.html`

Each existing hardcoded block in `market-v1.html` is replaced with:
- The same DOM skeleton (so the layout is preserved)
- IDs on the data nodes
- An inline `<script>` block at the bottom of the page that fetches via `VesignAPI` and fills the nodes

Sections, in order, all in `market-v1.html`:

| Section | Hardcoded today | Replacement |
|---|---|---|
| Tape ticker | 16 hardcoded `<span>` items | Loop renders from `fetchTape()`. Duplicate the rendered list once (existing CSS animation needs the duplicate). |
| Top-nav market chip | "Market open · 04:21:18" hardcoded | `fetchMarketStatus()` → `{is_open, next_event_utc}`. The HH:MM:SS countdown uses `next_event_utc` — same logic as production. |
| Page-header date | "Tuesday, May 19, 2026" | `new Date()` client-side + breadth chips from `fetchBreadth()`. |
| 5 index cards | Fake prices + canned sparkline path strings | `fetchIndices()` → render each card with real close, change %, and the 30-day sparkline computed by `VesignFmt.spark()`. |
| Cross-market strip | 6 hardcoded cells | `fetchCross()` → render the 6 cells. Each cell shows value + change. |
| Movers (3 panels) | Hardcoded ticker lists | Three parallel `fetchMovers('gainers'\|'losers'\|'active')` calls. Each panel renders 5 rows with logo, ticker, company, price, change. |
| Breadth panel | Hardcoded bars | `fetchBreadth()` → render advancers/decliners bar, 52w highs/lows, above 50d MA, VIX big number. |
| Sectors heatmap | 10 hardcoded tiles | `fetchSectors()` → 10 tiles by % change; tile color class (`g0..g4`, `r0..r2`, `neutral`) computed from the change magnitude with the same thresholds the current mockup uses. |
| News + Analyst (two-up) | Hardcoded lists | `fetchTopNews()` + `fetchTopAnalyst()`. |
| Earnings + Economic (two-up) | Hardcoded lists | `fetchEarningsWeek()` + `fetchEconomicCalendar()`. |
| Spotlight panel | Already wired | No change. Existing inline `<script>` keeps working; it'll move into `assets/data.js` as `fetchSpotlight` for consistency. |

## 5. Edge cases

- **Empty data** — when an endpoint returns `null` (e.g. signals table empty, external API down), the corresponding section hides itself (`display: none`) or shows a single "data unavailable" line. Never breaks the page.
- **Stale external data** — yfinance/FMP outages: backend returns the last cached value with `stale: true`. Frontend shows the data with a small "stale" pill.
- **Pipeline mid-write** — the existing TTL caches handle this. New pipeline data invalidates each `/api/market/*` cache within 60s (or 5 min for news/analyst, 1 hr for calendars).
- **Mockup browser tab open while you `vesign-sync` again** — refresh the tab to pick up new data.

## 6. Out of scope (Phase 1)

- Trades / Portfolio / Research / Account / Onboarding / marketing-page wiring (Phases 2 and 3).
- Live polling on any section. Refresh = browser reload.
- WebSocket / SSE.
- Mobile-specific layout adjustments.
- Caching beyond in-process TTL (Redis, etc.).
- Visual changes to the existing mockup — only data wiring.

## 7. Testing

- **Backend unit tests:** `tests/backend/test_market_*.py` per endpoint, using the same temp-DB fixture pattern from `test_spotlight.py`. External-API endpoints (cross, news, earnings, economic) use mocked HTTP responses via `unittest.mock`.
- **Backend integration:** `BYPASS_AUTH=1` set in test env; each endpoint returns 200 with the expected shape against the synced DB.
- **Manual:** open `http://localhost:8080/market-v1.html` and visually verify each section renders real data. Hard refresh to confirm cache invalidation.

## 8. Rollback

- All 10 endpoints + the assets/data.js + the market-v1.html changes can be reverted by `git revert` on the relevant commits (endpoints) and deleting the mockup edits (which are gitignored anyway).
- The `BYPASS_AUTH` flag is gated behind an env var — leaving the code but unsetting the var disables the bypass.

## 9. Deployment

**Local only.** Production server untouched. When the wider platform redesign deploys, the new `/api/market/*` endpoints ship in the same release as the React port of the Market page. The `BYPASS_AUTH` flag is never set on prod.

## References

- Brainstorm session: 2026-05-23 (this doc)
- Mockup: `.superpowers/brainstorm/7260-1779139139/content/market-v1.html`
- Existing endpoint patterns: `backend/main.py:_get_signals_today_cached` (cache shape), `_build_spotlight_today` (compute-on-request shape)
- Sync workflow: `vesign-sync` alias in `~/.zshrc`
- Related: `docs/superpowers/specs/2026-05-23-daily-spotlight-design.md`
