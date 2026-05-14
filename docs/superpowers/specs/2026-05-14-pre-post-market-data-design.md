# Pre/Post-Market Live Data — Design Spec

**Date:** 2026-05-14
**Status:** Approved, pending implementation plan
**Owner:** Inovenos

---

## 1. Goal

Extend the website's "live price" experience beyond the NYSE regular session (09:30–16:00 ET) to also cover pre-market (04:00–09:30 ET) and post-market (16:00–20:00 ET) hours. After post-market closes, the latest extended-hours print is locked as the new "Last Day Price" for the day.

The change is purely a **display-layer enhancement** — the trading engine (signals, trade_log, predictions, charts) continues to operate on regular-session closes only.

---

## 2. Daily Phase Model

Four UTC timestamps derived daily from `exchange_calendars` (XNYS):

| Phase | Window (UTC, normal day) | "Live Price" header | "Live Price" cell | "Last Day Price" source | Yield basis |
|---|---|---|---|---|---|
| `idle` | post_close → next pre_open | "Live Price" | "Closed" | `extended_close ?? close` (locked) | Last Day Price |
| `pre` | pre_open → regular_open | **"Pre-Market"** | Live, polled | prior session's locked price | Live price |
| `regular` | regular_open → regular_close | "Live Price" | Live, polled | prior session's locked price | Live price |
| `post` | regular_close → post_close | **"Post-Market"** | Live, polled | prior session's locked price | Live price |

**No IDT/ET clock strings in code.** All transitions fire when the corresponding UTC timestamp passes. Early-close days, holidays, and weekends are handled automatically by `exchange_calendars`.

---

## 3. Layer Split (Critical)

| Layer | Definition | Uses post-market? |
|---|---|---|
| **Layer 1 — Display** | Anything labeled "Last Day Price", "current price", or current yield on Open Trades, Signals, Watchlist, Portfolio holdings, Signal Modal | **Yes** — reads `extended_close ?? close` |
| **Layer 2 — Engine / History** | RSI, MACD, BB indicators, BUY/SELL triggers, trade_log entries, predictions, Portfolio 53-week chart, closed-trades section, ML models, backtests | **No** — reads regular `close` only |

Rationale for keeping Layer 2 on regular close: industry convention, no historical post-market data to retrain on, thin/noisy post-market volume is poor input for technical indicators, signals fire at regular-session close prices (which is what was actually executable).

---

## 4. Architecture

### 4.1 Data source: FMP `/stable/aftermarket-trade`

A single endpoint serves both pre and post hours — FMP returns the most recent extended-hours print, whether from pre-market that morning or post-market the prior evening. Confirmed live on the user's Premium plan (2026-05-14 probe).

- Single-ticker calls; no batch support on the aftermarket endpoints. We parallelize with `ThreadPoolExecutor(max_workers=10)`, same pattern as today's `live_prices()`.
- Thinly-traded tickers return `[]` → represented as `None` → silent fallback to regular `close` per Q2 decision.

### 4.2 Database

**New table** (separate from `daily_prices` to eliminate any risk of the 07:00 cron clobbering snapshot data):

```sql
CREATE TABLE extended_hours_prices (
  date TEXT NOT NULL,
  ticker TEXT NOT NULL,
  extended_close REAL NOT NULL,
  source TEXT DEFAULT 'fmp_aftermarket',
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date, ticker)
);
CREATE INDEX idx_ext_date ON extended_hours_prices(date);
```

`date` matches the NYSE trading-session date (the regular-session date — extended hours roll up to that same session, not the next day).

### 4.3 Backend changes (`backend/main.py`)

**Phase calculator (pure function):**
```python
def _phase_info(now_utc: datetime) -> dict:
    """Returns {phase, next_event_utc, next_event_name}.
    phase ∈ {'idle','pre','regular','post'}.
    next_event_name ∈ {'pre_open','regular_open','regular_close','post_close'}."""
```

**`/api/market/status`** — extended response:

```json
{
  "phase": "regular",
  "next_event_utc": "2026-05-14T20:00:00+00:00",
  "next_event_name": "regular_close"
}
```

No external consumers — backend and frontend deploy together; the old `is_open` field is removed in the same release.

**`/api/prices/live`** — phase-aware:

```python
phase = _phase_info(now_utc())["phase"]
if phase == "idle":
    return {"phase": "idle", "prices": {t: None for t in tickers}}
if phase == "regular":
    prices = fetch_live_prices(tickers)        # /stable/quote
else:  # 'pre' or 'post'
    prices = fetch_aftermarket_trades(tickers) # /stable/aftermarket-trade
return {"phase": phase, "prices": prices}
```

5s in-memory cache keyed on `(phase, ticker)` to prevent stale regular-session prices leaking into post-market response.

**Endpoints returning a "current price" field** — add `LEFT JOIN extended_hours_prices`:

```sql
SELECT
  ...,
  COALESCE(eh.extended_close, dp.close) AS current_price
FROM trade_log t
JOIN daily_prices dp ON ...
LEFT JOIN extended_hours_prices eh
  ON eh.ticker = dp.ticker AND eh.date = dp.date
WHERE ...
```

Affected endpoints:
- `/api/trades/open`
- `/api/signals`, `/api/signals/today`, `/api/signals/by-tickers`
- `/api/portfolio/holdings`
- `/api/watchlists/...` ticker detail rows
- Any endpoint feeding the Signal Modal

**Unaffected endpoints (Layer 2):**
- `/api/portfolio/performance`, `/api/portfolio/comparison` (Vesign line/bar — built from trade_log, regular closes only)
- `/api/trades` (closed-trades history)
- `/api/signals/markers`, `/api/signals/success-rate`

### 4.4 `data/fmp.py` — new function

```python
def aftermarket_trades(tickers: list[str]) -> dict[str, float | None]:
    """Latest extended-hours trade price for each ticker.
    Returns None for tickers with no extended-hours activity."""
    # Parallel /stable/aftermarket-trade calls, ThreadPoolExecutor(max_workers=10)
```

### 4.5 Snapshot job — `production/snapshot_post_market.py`

```python
def snapshot_post_market():
    # 1. Determine the most-recent past trading session via xcals
    nyse = xcals.get_calendar("XNYS")
    session = nyse.previous_session(today_iso)
    post_close = nyse.session_close(session) + timedelta(hours=4)
    # Note: +4h is the standard NYSE post-market offset. Early-close days
    # may have shortened extended hours; +4h overshoots harmlessly because
    # FMP's aftermarket-trade returns whatever the last extended print was.

    # 2. Guard: only run if post_close happened in the last ~6 hours
    age = now_utc() - post_close
    if age < timedelta(0) or age > timedelta(hours=6):
        log("No fresh session to snapshot, exiting")
        return

    # 3. Load tickers of interest: all tickers in today's signals (~1,600).
    #    Superset of open positions, watchlist holdings, and surfaced UI rows.
    tickers = [r.ticker for r in conn.execute(
        "SELECT DISTINCT ticker FROM signals WHERE date(date) = :d", {"d": session}
    )]

    # 4. Pull extended-hours prices in parallel
    prices = aftermarket_trades(tickers)

    # 5. Upsert into extended_hours_prices for date=session (skip None)
    upsert_extended_hours(session, prices)
```

**Cron entry** (added to root crontab on the server):

```cron
5 3 * * * cd /opt/vesign && venv/bin/python -m production.snapshot_post_market >> /var/log/vesign-snapshot.log 2>&1; systemctl restart vesign
```

03:05 IDT is safe across regular post-close (03:00 IDT) and early-close-day post-close (24:00 IDT). The internal xcals guard handles weekends/holidays cleanly.

The `systemctl restart vesign` is required to bust the in-memory day-caches (`_build_vesign_cache`, `/api/trades/open` cache) so the new `extended_close` becomes visible immediately.

### 4.6 Frontend changes

**`useLivePrices` hook** — breaking change:
```js
// Before: { prices, marketOpen }
// After:  { prices, phase }   // phase ∈ 'idle'|'pre'|'regular'|'post'
```

Five consumer files update their destructuring (`TradesPage`, `SignalsPage`, `WatchlistPage`, `PortfolioPage`, `SignalModal`).

**Phase-aware column header (Open Trades — `TradesPage.jsx`):**
```jsx
const headerLabels = {
  idle: t('col.livePrice'),
  pre: t('col.preMarket'),
  regular: t('col.livePrice'),
  post: t('col.postMarket'),
}
<th>{headerLabels[phase]}</th>
```

**Cell rendering** (replaces `!isOpen ? 'Closed' : …` at TradesPage.jsx:367):
- `phase === 'idle'` → muted "Closed"
- `phase !== 'idle' && displayLive == null` → fallback to muted `displayClose`
- `phase !== 'idle' && displayLive != null` → live price + change

**Yield logic** (TradesPage.jsx:380):
```js
const priceForYield = (phase !== 'idle' && displayLive != null) ? displayLive : displayClose
```

`displayClose` (= `trade.current_price` from `/api/trades/open`) is already `extended_close ?? close` thanks to backend `COALESCE`. The frontend doesn't branch on which one it got.

**Same pattern applied to** SignalsPage, WatchlistPage, PortfolioPage, SignalModal wherever a "Live Price" column appears.

**Replace `marketOpen` reads everywhere** with `phase !== 'idle'` for equivalent boolean semantics.

**New i18n keys:**
```json
// en.json
"col.preMarket": "Pre-Market",
"col.postMarket": "Post-Market"
// he.json
"col.preMarket": "טרום־מסחר",
"col.postMarket": "אחר־מסחר"
```

---

## 5. Daily Timeline

| IDT | What happens |
|---|---|
| 03:00 | Post-market window ends (UTC: regular_close + 4h). Frontend phase flips `post` → `idle`. Live Price cell shows "Closed". Cached `current_price` is still last-session-value until snapshot runs. |
| 03:05 | Snapshot cron fires → writes today's `extended_hours_prices` → restarts service → caches rebuild with new `current_price`. Last Day Price column now reflects the post-market close. ≤ 5 minute UX gap between phase flip and Last Day Price update. |
| 07:00 | Regular engine cron runs (unchanged): downloads regular-session close from FMP, computes indicators/signals/trade_log, restarts service. Engine inputs are all Layer 2 — never touches `extended_hours_prices`. |
| 11:00 | Pre-market opens. Phase flips `idle` → `pre`. Header changes to "Pre-Market". Live polling begins. |
| 16:30 | Regular session opens. Phase flips `pre` → `regular`. Header back to "Live Price". |
| 23:00 | Regular session closes. Phase flips `regular` → `post`. Header changes to "Post-Market". Live polling continues. |
| 03:00+1 | Cycle repeats. |

---

## 6. Error Handling

| Failure | Behavior |
|---|---|
| FMP rate-limit / timeout on live poll | Existing 5s cache returns last value; cell shows last-known or "—". No user-facing error. |
| Snapshot job fails (FMP down at 03:05 IDT) | `extended_close` stays NULL for that session → silent fallback to regular `close`. Log line written. Add a daily check in the 07:00 pipeline that warns if `extended_hours_prices` for yesterday's session has < 100 rows (suggests systemic failure). |
| Holiday or weekend | xcals guard exits cleanly; no cron-spam alerts. |
| Early-close day | xcals returns the actual post_close (e.g., 17:00 ET → 24:00 IDT); guard recognizes it as "recent enough"; snapshot runs normally at 03:05 IDT. |
| Thinly-traded ticker, no extended print | `aftermarket_trades()` returns `None`; upsert skips that ticker; UI silently shows regular `close`. |
| Server restart mid-post-market | Live polling resumes within seconds; in-memory state rebuilds; no DB inconsistency. |
| Phase mismatch between server and client | Server is source of truth (returns `phase` on every `/api/prices/live` call). Client never decides phase locally. |

---

## 7. Testing

**Unit tests (pytest):**
- `_phase_info()` at each boundary minute, on a regular day, an early-close day, a weekend, and a major holiday (Christmas, July 4 falling on a weekday).
- `aftermarket_trades()` against fixture responses for: normal print, empty array, rate-limit error, mixed batch.
- `upsert_extended_hours()` idempotency (running twice for the same session doesn't duplicate or overwrite incorrectly).

**Integration tests:**
- `/api/market/status` returns correct phase across mocked UTC times.
- `/api/prices/live` returns extended-hours data during pre/post phases, `None` during idle.
- `/api/trades/open` returns `extended_close` value when one exists, else `close`.

**Frontend (Vitest + React Testing Library):**
- `useLivePrices` exposes correct `phase` shape; renders right header label per phase.
- Open Trades row renders "Closed" / "Pre-Market" / "Live Price" / "Post-Market" header based on the phase prop.
- Yield computation uses live price during non-idle phases, falls back during idle.

**Manual / smoke:**
- Dry-run `snapshot_post_market.py --dry-run` on prod data to confirm tickers list + FMP responses.
- Visual check on each affected page during pre-market the morning after deploy.

---

## 8. Out of Scope

- Layer 2 changes (engine, trade_log, predictions, ML models, historical charts) — explicitly stays on regular close.
- Replacing the 07:00 engine cron or changing its outputs.
- Real-time streaming via WebSockets (current 5s poll is sufficient).
- Post-market data for non-US markets (site is US-only).
- Historical backfill of `extended_hours_prices` for past dates (FMP doesn't provide historical extended-hours bars on this plan tier; not needed for the use case).
- Batch optimization of regular `/quote` calls (separate opportunity — `/stable/batch-quote` exists; tracked separately if desired).

---

## 9. Migration & Deployment

1. **Schema** — apply `CREATE TABLE extended_hours_prices …` to the prod DB once (no historical backfill).
2. **Backend** — deploy new `aftermarket_trades()`, phase calculator, modified endpoints, snapshot job script. Service restart.
3. **Cron** — add the 03:05 IDT crontab line.
4. **Frontend** — deploy new `useLivePrices` shape + phase-aware UI changes + i18n. Hard-refresh required for clients with cached old hook.
5. **Validation** — wait one full cycle (≥ 24h) and confirm: (a) snapshot ran successfully, (b) Open Trades shows "Pre-Market" header during pre-market, (c) Last Day Price updates after 03:05 IDT, (d) engine cron at 07:00 IDT runs unchanged.

Rollback: revert frontend + restore old `/api/prices/live` / `/api/market/status` shapes; the `extended_hours_prices` table can be left in place harmlessly.
