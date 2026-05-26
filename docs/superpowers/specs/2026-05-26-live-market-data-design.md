# Live Market Data — whole-universe 3s live prices

**Date:** 2026-05-26
**Status:** Approved (design), pending implementation plan
**Branch:** `feat/ui-redesign`
**Spec author:** Claude (Opus 4.7)

## Why

The redesigned Market page (and the Signals table, Portfolio P&L) currently render
**end-of-day** prices read from `daily_prices` / `index_prices`, refreshed only by the
overnight pipeline. During pre-market, regular, and post-market hours they show the last
completed session's close and never move. Production's *legacy* UI already polls live
prices every ~3s (`useLivePrices`: 1s poll / 2s server cache) for the stock prices users
watch; the redesign lost that for everything except the signal modal's "Current" price.

Goal: make the whole Market page — plus the Signals table and Portfolio P&L — show **live
prices that update every 3s during pre/post/regular hours**, matching the production feel,
without exceeding FMP's rate limit.

This extends:
- `2026-05-14-pre-post-market-data-design.md` — the four-phase model (idle/pre/regular/post)
  and "live price" semantics. Unchanged; reused here.
- `2026-05-23-market-page-live-data-design.md` — the `/api/market/*` endpoints. This spec
  **supersedes that spec's "snapshot on page load — no live polling" decision** for the
  panels listed below; everything else about those endpoints stays.

## Feasibility (verified 2026-05-26, during a live pre-market session)

| Fact | Finding |
|---|---|
| FMP Premium rate limit | 750 calls/min |
| Regular-hours batch | `batch-quote?symbols=…` — 100 tickers/call → ~16 calls for the US universe |
| Pre/post extended price | `batch-quote` returns the **regular** price during pre/post (AAPL 308.82), NOT the extended trade. Real pre-market is in the extended endpoints (AAPL 310.38). |
| Batchable extended endpoint | `batch-aftermarket-quote?symbols=…` works and batches (returns bid/ask/volume). `aftermarket-trade` is per-ticker (1 call/ticker) and is **not** viable for the universe. |
| Indices on FMP | `batch-quote` for `^GSPC,^NDX,^DJI,^RUT,^VIX` → **null**. Not covered. Indices stay on yfinance. |
| Commodities/FX on FMP | `GCUSD`, `EURUSD` etc. are covered — but they're already live via yfinance, so no remap. |

**Call budget at 3s cadence:** universe ÷ 100 ≈ 16 calls/snapshot × (60/3) = **~320 calls/min**,
during pre/regular/post only, **0 when idle**. Comfortably under 750/min — *and only because
it is one shared server-side poll, not per-user.*

## Decisions locked in (from brainstorm 2026-05-26)

| Decision | Value |
|---|---|
| Architecture | **Shared server-side live snapshot** (approach A). One in-memory whole-universe snapshot; all derived endpoints read from it. FMP load decoupled from user count. |
| No-trade tickers (pre/post) | Fall back to last close → **0% move**. Panels stay full; only tickers with real extended-hours prints rank. |
| Refresh cadence | **3s** everywhere (stocks via FMP; top strip via yfinance — also 3s per user request, accepting yfinance's occasional throttle, handled by the existing `stale` fallback). |
| Scope | **Market panels + Signals table + Portfolio P&L**, all in this spec. |
| Change% baseline | Prior completed session's close (`MAX(daily_prices.date)`). Standard pre/post/intraday % change. |
| Sparklines / 52w windows | 30-day sparklines and 52w hi/lo do **not** move intraday → stay daily-cached; only the latest point / change% goes live. |

## Architecture

### Component 1 — `backend/live_snapshot.py` (new module)

A single shared, phase-aware, whole-universe live price cache.

```
get_snapshot() -> { "phase": str, "prices": {ticker: float} }
```

- **Single-flight + TTL:** a `threading.Lock` guards the fetch. The first call in a 3s
  window fetches; concurrent/subsequent calls within the TTL reuse the cached dict. TTL =
  `_SNAPSHOT_TTL = 3` seconds.
- **Phase-aware source** (phase from the existing `_phase_info()` / `exchange_calendars`):
  - `regular` → `fmp.live_prices(universe)` (batch-quote `price`).
  - `pre` / `post` → new `fmp.batch_aftermarket_quotes(universe)` → price = mid
    `(bidPrice + askPrice) / 2`; skip tickers where bid or ask is missing/≤0 (they fall back
    to prev_close downstream).
  - `idle` → no fetch; return empty `prices` (downstream uses prev_close → "Closed"/last
    close).
- **Phase-change flush:** if the detected phase differs from the cached phase, clear the
  snapshot before serving (mirrors the guard already in `/api/prices/live`,
  `backend/main.py:1315`).
- **Failure handling:** on FMP error, keep the previous snapshot (do **not** blank), log a
  warning. Stale-but-present beats empty.

### Component 2 — daily universe baseline (in the same module)

```
get_baseline() -> { ticker: {sector, company, market_cap, prev_close, hi52, lo52, logo_url} }
```

- Built once per latest-date; rebuilt when `MAX(daily_prices.date)` changes (guarded by the
  same lock, separate timestamp).
- `prev_close` = last completed session's close. Serves two roles:
  1. **change% baseline:** `change_pct = (price - prev_close) / prev_close * 100`.
  2. **fallback price** for tickers absent from the live snapshot → change% = 0.
- One query joining `companies` + `fundamentals` + `daily_prices` (MAX date) + a 252-row
  hi/lo window — the same data today's per-endpoint SQL already reads, hoisted to a single
  daily build.

### Component 3 — refactored stock endpoints (`backend/main.py`)

`movers`, `sectors` (heatmap change%/gainers/losers), `breadth`, `highs_lows` stop computing
change from two `daily_prices` rows. New shared helper:

```python
def _live_universe_rows():
    snap = live_snapshot.get_snapshot()["prices"]
    base = live_snapshot.get_baseline()
    for t, meta in base.items():
        price = snap.get(t) or meta["prev_close"]
        change = (price - meta["prev_close"]) / meta["prev_close"] * 100 if meta["prev_close"] else None
        yield {"ticker": t, "price": price, "change_pct": change, **meta}
```

Each endpoint ranks/aggregates these ~1600 in-memory rows (sub-ms):
- **movers** — sort by `change_pct` (gainers/losers) or live volume (active); top N.
- **sectors/heatmap** — group by sector, cap-weighted mean of live `change_pct`, live
  gainers/losers. **The 30-day cap/equal sparklines remain a separate daily-cached build**
  (`_build_market_sectors` pandas pivot runs once/day, not every 3s).
- **breadth** — advancers/decliners/unchanged counts from live `change_pct`.
- **highs_lows** — near-52w-high/low test uses live `price` vs daily-cached `hi52/lo52`.

The per-endpoint 60s `_market_cache` entries for these become a 3s TTL (or read straight
from the snapshot, which is itself TTL'd). Endpoints that are **not** price-driven (news,
analyst-changes, earnings, economic calendar) keep their existing slow caches.

### Component 4 — top strip (indices / commodities / currencies)

- **Indices** — switch `_build_market_indices` latest value from the `index_prices` table to
  live yfinance quotes (reuse `_fetch_yf_quotes`); the 30-day sparkline still comes from the
  table. Note: the cash index does not tick during pre/post (only futures do), so indices are
  naturally flat outside regular hours — expected, standard.
- **Commodities / currencies** — already live via yfinance; unchanged source.
- Strip server-cache TTL drops to ~3s.

### Component 5 — Signals table + Portfolio overlay

- **Signals endpoints** (`/api/signals/today`, `/api/signals`, …) currently use
  `COALESCE(lp.latest_close, s.close)` where `lp` is the latest `daily_prices` close. Overlay
  the snapshot: displayed price = `snapshot.get(ticker) or daily_close`. Implemented by
  applying the snapshot to the response rows (not by changing the SQL), keeping the live
  overlay in one place.
- **Portfolio** (`/api/portfolio/holdings`) — current value per holding uses the snapshot
  price when present; P&L recomputed from it. The performance/comparison line charts (53-week
  history) are **not** intraday and stay as-is.

### Component 6 — frontend (`frontend/src/redesign/…`)

- Bump `refetchInterval` to **3000ms** on the live panels: Indices, Commodities, Currencies,
  Movers, SectorHeatmap (change% only), Breadth, Highs/Lows, Signals table, Portfolio cards.
- Leave on slow refresh: sector sparklines, NewsFeed, AnalystChanges, EarningsWeek,
  EconomicCalendar (these are not intraday-price-driven).
- React Query already de-dupes in-flight requests; with the shared server snapshot, N panels
  polling at 3s still cause only ~16 FMP calls per 3s server-side.

## Edge cases

- **Idle / weekend / holiday:** snapshot empty → every panel shows prev_close, 0% change,
  status "Market closed". No FMP polling (0 calls).
- **Phase transition mid-session:** snapshot flushes on phase change; first post-transition
  call repopulates from the correct source.
- **FMP outage / partial response:** keep last good snapshot; tickers missing from a partial
  response fall back to prev_close (0%). Page never blanks.
- **New trading day rolls in:** baseline rebuilds when `MAX(date)` advances; the just-closed
  session's close becomes the new prev_close.
- **yfinance throttle on the strip:** existing `stale: true` flag already handles per-strip
  fetch failure; the card shows the last good value flagged stale.

## Testing

**Unit (`live_snapshot`):**
- single-flight: concurrent `get_snapshot()` calls trigger exactly one FMP fetch (mock fmp,
  assert call count == 1).
- change% math: `(price - prev_close)/prev_close*100`; prev_close==0 → None.
- no-trade fallback: ticker absent from snapshot → price == prev_close, change == 0.
- phase flush: changing phase clears the cached snapshot.
- idle: returns empty prices, no fetch.

**Integration (endpoints, inject a fake snapshot):**
- movers ranking reflects injected live prices (a ticker pushed up ranks to the top).
- sectors cap-weighted change reflects injected prices; sparkline unchanged (still daily).
- breadth counts shift with injected prices.
- highs_lows flips a ticker in/out of "near high" when its live price crosses the band.
- signals/today and portfolio holdings show the injected snapshot price, not the daily close.
- idle phase → all endpoints return prev_close / 0%.

**Load assertion:**
- with the shared snapshot, hitting all live endpoints within one 3s window triggers exactly
  one universe fetch (≤16 FMP calls), independent of endpoint count.

## Out of scope

- Changing the trading engine, signals, trade_log, or predictions — they remain on
  regular-session closes (per `2026-05-14` spec).
- Intraday history/sparklines (still daily).
- Commodity/FX migration to FMP (yfinance already live).
- Deploying to production (branch work; deploy with the wider redesign).
