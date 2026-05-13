# Compounded Vesign Yield — Design

**Date:** 2026-05-13
**Status:** Draft — pending implementation plan
**Scope:** Aggregate Vesign yield display + per-trade DCA cost basis

## Problem

Today the site shows aggregate Vesign yield as if every BUY signal pulls fresh capital from an infinite pocket. The denominator grows linearly with the number of trades and never recycles closed-trade proceeds back into new buys. This understates the strategy's true return on capital over multi-year windows.

A second, related inconsistency: the per-trade `avg_cost` shown in modals/tables is computed as a simple mean of lot prices — equivalent to assuming "1 share per lot." This doesn't correspond to any actual dollar amount of capital. It also gives a different per-trade yield than what the strategy would actually produce under "$1000 per BUY signal."

## Decision

Adopt **"$1000 per BUY signal"** as the consistent capital model across every Vesign-strategy yield surface. Each lot in `trade_lots` (including Path-B DCA add-on lots) is one $1000 BUY.

Two consequences:

1. **Per-trade `avg_cost`** = dollar-weighted = harmonic mean of lot prices = `n / Σ(1/p_i)`. Per-trade yield = `(sell_price − avg_cost) / avg_cost`. Cascades into modals, historical trades table, open trades table, exports.

2. **Aggregate yield** = bank/hand compounding simulation. Each BUY draws $1000 from hand (or bank if hand empty). Each SELL adds `$1000 × (1 + lot_yield)` to hand. Yield = `(final_hand + open_lots_MTM) / peak_bank_drawn − 1`. Peak bank drawn is computed but never displayed.

Untouched: trade log/engine/signal generation, user's manually-entered watchlist holdings (`watchlist_holdings.avg_price`), per-trade yield filter universe (`sell_date` in window), the user's blue portfolio line on the chart.

## Universe & Filtering

Same as today: every aggregate uses trades whose `sell_date` lies in the requested window. A trade's BUY events happen at each lot's `lot_date`, even if those dates predate the window. SELL events all share the trade's `sell_date`. Same-day events resolve SELL-before-BUY (matches the user's mental model: proceeds available same day).

Trades with no rows in `trade_lots` (rare — e.g. a close that hasn't propagated yet) fall back to a synthetic single lot from `trade_log.buy_date / buy_price`.

## Math

```
avg_cost(lots)            = n / Σ(1 / p_i)
per_trade_yield(t)        = (sell_price − avg_cost(lots_of_t)) / avg_cost(lots_of_t)
per_lot_yield(lot, sell)  = (sell − lot.price) / lot.price

simulate_bank_hand(lots, price_at, eval_dates):
    events = sort by (date, SELL_before_BUY) over all BUY/SELL events of all lots
    bank, hand, equity_curve = 0, 0, []
    for each event:
        if BUY:
            if hand >= 1000: hand -= 1000
            else:            bank += (1000 - hand); hand = 0
        if SELL:
            hand += 1000 * (1 + per_lot_yield(lot, sell_price))
        if event.date in eval_dates:
            mtm = sum 1000 * price_at(t, d) / lot.price for each open lot
            equity_curve.append((d, hand + mtm))
    return peak_bank, hand_final, equity_curve
```

`yield(d) = (equity_curve[d] / bank_drawn_at(d)) − 1` for each evaluation date.

## Affected Surfaces

### Backend (`backend/main.py`)

| Endpoint | What changes |
|---|---|
| `GET /api/trades` (line 1353) | `pair["avg_cost"]` uses harmonic mean (line 1541) |
| `GET /api/trades/export*` (line 1546) | `df["dca_return_pct"]` uses new avg_cost (line 1638) |
| `GET /api/trades/open` (line 1902) | `r["avg_cost"]` + unrealized yield use new avg_cost (lines 1885, 1924) |
| `_build_vesign_cache()` (line 2143) | Load lots, not just trades; same daily TTL |
| `GET /api/portfolio/performance` (line 2227) | Replace `vesign_yield_at()` with bank/hand simulator |
| `GET /api/portfolio/comparison` (line 2378) | `vesign_val` = last point of the simulator output |

### Backend new helper

`backend/yield_calcs.py` (new file) — three pure functions:

- `avg_cost_dollar_weighted(lot_prices: list[float]) -> float`
- `per_trade_yield_dca(sell_price: float, lot_prices: list[float]) -> float`
- `simulate_bank_hand(lots, price_at, eval_dates) -> SimResult`

All testable in isolation, no DB access inside them.

### Frontend

No code changes expected. All affected components read computed values from the API. One optional touch: i18n label tweak if we want to signal "compounded" — recommendation is to leave labels alone.

### Cache

`_vesign_cache` already has a daily TTL and background warm on uvicorn startup. We extend the cached payload to include lots and (optionally) precompute the equity curve once per day per market. No new caching infrastructure.

## Validation

Pre-deploy checks on the production server:

1. Copy `analysis/sim_compounded_yield.py` to `/opt/vesign/analysis/`, run it, record baseline numbers.
2. Run the endpoints after the change; numbers must match the simulator within rounding tolerance.
3. Spot-check **FDS** modal: `avg_cost = $251.33`, `yield = −7.40%`.
4. Spot-check **1Y Vesign bar** on `ve-sign.com/portfolio` (signed in): must equal the simulator's bank/hand 1Y value.
5. Spot-check **1Y "Avg Yield" chip** on TradesPage: should now read the corrected per-trade simple mean (≈ +27% — lower than today's +30.37% because per-lot DCA losers now properly weighted).

Edge cases to exercise manually:

- Single-lot trade: per-trade yield must equal `return_pct` exactly (n=1 → harmonic mean = the one price).
- 2-lot DCA winner.
- Trade with no `trade_lots` rows (synthetic fallback).
- Vesign line week[0]: finite percentage, not NaN (pre-window buys contribute MTM).
- Empty universe (very short window): graceful empty response, no division by zero.

## Rollout

- All edits on the production server, no localhost detour.
- One PR, no feature flag (per project preference for clean cut-overs).
- After deploy: hard-refresh ve-sign.com, walk through TradesPage 1Y chip → Portfolio 1Y line/bar → FDS modal.
- Watch uvicorn logs for ~10 min.

## Rollback

Revert the commit, restart uvicorn. No DB schema changes, no migrations — rollback is instant.

## Non-Goals

- Showing peak-bank-drawn anywhere in the UI (internal-only).
- Changing the engine, signal generation, or trade_log.
- Changing user's actual portfolio yield calculation (their lots are real, not Vesign signals).
- A/B testing or gradual rollout — the change is purely in calc paths.

## Numbers (1Y window, as of 2026-05-13)

| Metric | Today | After change |
|---|---|---|
| Trades | 370 | 370 |
| Lots | 458 | 458 |
| $ Invested | $370,000 (implicit) | $458,000 (explicit, $1000 × lots) |
| $ Profit | +$112,357 (site) | +$123,809 (corrected per-lot weighting) |
| Yield % (TradesPage chip) | +30.37% | +27.03% (simple-mean of new per-trade yields) |
| Yield % (Vesign bar/line endpoint) | +30.37% (same calc today) | +34.68% (bank/hand compounded) |
| FDS modal avg_cost | $261.43 | $251.33 |
| FDS modal yield | −10.98% | −7.40% |

The 1Y chip and the Vesign bar **diverge after this change** — they're answering different questions: chip = average per-trade outcome; bar/line = compounded capital return. That's intentional and consistent.
