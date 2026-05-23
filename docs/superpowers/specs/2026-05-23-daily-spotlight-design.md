# Daily Spotlight — design spec

**Date:** 2026-05-23
**Status:** Approved (brainstorming complete)
**Scope:** Local implementation only. Production server stays untouched until the wider platform redesign is ready to deploy.

## Why

Some days the engine fires zero BUY signals — a normal consequence of the contrarian V1 (7-gate AND) + V2 (VQS=9, gated by VIX>22) rules. In low-volatility / strong-tape regimes, both paths sit idle for stretches of 1–5 trading days. The last such stretch began 2026-05-20 and is ongoing as of 2026-05-22 (3 zero-BUY days, last BUY 2026-05-19).

Empty-BUY days make the Market dashboard feel "dead" for Free users (who don't see real BUYs anyway, since BUYs are paid-only) and removes any daily reason for Free users to open the app. The strategy is sound — quiet markets *should* produce fewer BUYs — but the UX needs something to show.

**Goal:** always surface one ticker the engine is "watching" each day, framed clearly as a near-miss, not a BUY signal. Free users get a daily engagement hook; Paid users get an extra "runner-up" view alongside their real BUYs.

## Locked-in decisions (from brainstorm 2026-05-23)

| Decision | Value |
|---|---|
| Goal | UX engagement — **strategy stays untouched**. No effect on trade_log, win rate, or historical trades. |
| Tier name | **Spotlight** (separate label, never confused with BUY) |
| Selection | Highest V1 gates met, tiebreak by VQS, then by ML `prediction_score` |
| Audience | Free users see Spotlight only. Paid users see Spotlight + BUYs. |
| Placement | Dedicated panel on the Market page, directly after the page header (above the 5 index cards). |
| BUY-day behavior | Always show Spotlight — picks the highest-ranked non-BUY ticker. SELL tickers also excluded. |
| Approach | **A — compute-on-request, no schema changes**. |

## 1. Selection logic

Single SQL query against the latest pipeline output. No new tables, no new columns.

```sql
SELECT
  s.ticker, c.company, c.domain,
  s.close, s.pred_5d, s.prediction_score, s.vqs,
  s.rsi_3day_flag, s.bb_condition, s.analyst_condition,
  s.volume_flag, s.week52_condition, s.health_condition, s.ml_condition,
  prev.close AS prev_close,
  (CASE WHEN s.rsi_3day_flag = 3 THEN 1 ELSE 0 END
   + COALESCE(s.bb_condition, 0)
   + COALESCE(s.analyst_condition, 0)
   + COALESCE(s.volume_flag, 0)
   + COALESCE(s.week52_condition, 0)
   + COALESCE(s.health_condition, 0)
   + COALESCE(s.ml_condition, 0)) AS gates_met
FROM signals s
LEFT JOIN companies c ON c.ticker = s.ticker
LEFT JOIN signals prev
  ON prev.ticker = s.ticker
  AND prev.date = (
    SELECT MAX(date) FROM signals WHERE ticker = s.ticker AND date < s.date
  )
WHERE s.date = (SELECT MAX(date) FROM signals)
  AND s.signal NOT IN ('BUY', 'SELL')
  AND c.market = 'US'
ORDER BY gates_met DESC, s.vqs DESC, s.prediction_score DESC NULLS LAST, s.ticker ASC
LIMIT 1;
```

Notes:
- `signal NOT IN ('BUY', 'SELL')` ensures Spotlight is forward-looking and never collides with the canonical BUY/SELL surfaces.
- `c.market = 'US'` honors the existing US-only policy.
- `NULLS LAST` on `prediction_score` prevents NaN-ticker bias in the tiebreaker.
- `ticker ASC` as the final tiebreaker makes the result deterministic across requests on the same data.
- `prev.close` is joined for computing `day_change_pct = (close - prev_close) / prev_close` server-side before returning the response. Returned as `null` if no prior row exists for that ticker.
- Individual gate columns are returned so the API handler can build the `reasons[]` array without re-querying.

## 2. API endpoint — `GET /api/spotlight/today`

Lives in `backend/main.py` alongside the other read endpoints. Reuses the existing TTL-cache pattern.

**Response shape:**

```json
{
  "date": "2026-05-22",
  "ticker": "INTU",
  "company": "Intuit",
  "domain": "intuit.com",
  "close": 612.34,
  "day_change_pct": 1.2,
  "gates_met": 6,
  "gates_total": 7,
  "vqs": 4,
  "ml_pred_5d": 0.012,
  "reasons": [
    {"gate": "rsi_3day_flag",    "met": false, "label": "RSI<30 for 3 consecutive days", "value": 1, "needed": 3},
    {"gate": "bb_condition",     "met": true,  "label": "Bollinger oversold"},
    {"gate": "analyst_condition","met": true,  "label": "Analyst target upside"},
    {"gate": "volume_flag",      "met": true,  "label": "Volume confirmation"},
    {"gate": "week52_condition", "met": true,  "label": "Near 52-week low"},
    {"gate": "health_condition", "met": true,  "label": "Company health pass"},
    {"gate": "ml_condition",     "met": true,  "label": "ML model positive"}
  ]
}
```

**Behavior:**
- Returns `200` with body `null` when the signals table has no rows for today (e.g. pipeline hasn't run yet). The frontend hides the panel in that case.
- Server-side cache: 10 minute TTL, keyed by `(today_iso, MAX(signals.date))`. Invalidates automatically on next pipeline write.
- No auth required (matches Market page's other free endpoints).

## 3. Frontend

A new component `<SpotlightPanel />` rendered on the Market page directly after the page header and before the 5 index cards.

**Layout sketch:**

```
┌────────────────────────────────────────────────┐
│ ★ Today's Spotlight                            │
│ ┌────┐  INTU  Intuit                          │
│ │logo│  $612.34  +1.20%                       │
│ └────┘  6 of 7 BUY gates met                  │
│         Why not a BUY: RSI hasn't held <30 for │
│         3 consecutive days (currently 1)       │
│         [View details →]                       │
└────────────────────────────────────────────────┘
```

**Behavior:**
- Ticker logo from `https://ve-sign.com/logos/{T}.png` per [[feedback_always_show_ticker_logos]].
- Panel clickable as a whole — opens the existing `SignalModal` with the spotlighted ticker.
- "Why not a BUY" text generated client-side from the `reasons[]` array — picks the first `met: false` reason as the headline.
- When the API returns `null`, the panel renders nothing (no error state, no skeleton — it just isn't there).

## 4. Edge cases

| Case | Behavior |
|---|---|
| Signals table has no row for today | API returns `null`; panel hidden. |
| Every ticker has 0 of 7 gates met | VQS becomes the de-facto rank; if VQS is also flat across the board, ML `prediction_score` decides. |
| Multiple tickers tie on `(gates_met, vqs, prediction_score)` | Deterministic by alphabetical ticker (SQLite SELECT order). |
| Ticker is in the user's open positions | Still shown. Spotlight reflects the engine's view, not personalization. *(Future enhancement: "you already hold this" badge — out of scope here.)* |
| Pipeline writes mid-request | Server cache invalidates on next request because the cache key includes `MAX(signals.date)`. |

## 5. Out of scope (YAGNI)

- Historical Spotlight archive — Approach A doesn't persist picks. Upgrade path is Approach B/C if this becomes needed.
- Multiple Spotlights per day.
- Personalized picks (per-user filtering).
- Email / push notifications.
- Spotlight on Trades or Research pages.
- Mobile-specific layout (handled by the broader responsive system).

## 6. Testing

- **Unit (Python):** SQL ranking against fixture signals → expected ticker order under several scenarios (one near-miss, multiple ties, all-zero universe).
- **Integration:** endpoint returns `200` + valid JSON when signals exist; returns `null` when signals are empty for today.
- **Manual (local):** visit `http://localhost:3000/market` — Spotlight panel appears, click opens `SignalModal` with the correct ticker.

## 7. Rollback

Delete the endpoint handler in `backend/main.py` and the `<SpotlightPanel />` component. No DB migration, no schema change, no historical data to clean up.

## 8. Deployment

**Local only until the wider platform redesign deploys.** Production server (134.209.82.105) stays untouched. Implementation lives in the local checkout under `frontend/src/` (component, hook, types) and `backend/main.py` (endpoint), and is verified against the locally-synced DB and the http.server-hosted mockups on `localhost:8080`.

When the full redesign is ready for deploy, this endpoint ships in the same release as the rest of the new Market page.

## References

- Brainstorm session: 2026-05-23 (this doc captures the locked-in decisions)
- BUY-logic source: `signals/engine.py` lines 642–671 (V1 7-gate AND + V2 VQS==9)
- Market page mockup: `.superpowers/brainstorm/7260-1779139139/content/market-v1.html`
- Related memory: [[project_platform_redesign_2026_05]], [[project_v1_v2_hybrid_strategy]]
