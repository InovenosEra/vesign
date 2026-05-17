# yfinance Analyst Data Migration — Design Spec

**Date:** 2026-05-17
**Author:** Israel Laufer (with Claude assist)
**Status:** Approved (pending implementation plan)

---

## Problem

Our analyst-target data comes from FMP's `/stable/price-target-consensus` endpoint. Coverage and quality have meaningful gaps:

- **Thin coverage on foreign cross-listings.** NGG (National Grid, $80B) has only 2 analyst target events in FMP's database — *ever*. With our 90-day staleness rule, NGG shows NULL analyst data for the last 2 months. Same problem for TEVA, CHKP, ESLT, NVMI, TSEM, BAM, and other international large-caps cross-listed on US exchanges.
- **Stale outliers contaminate consensus.** FMP's consensus aggregation keeps stale targets indefinitely. Visa shows `targetLow=$160` from a phantom analyst while recent calls are $350–$415, inflating the displayed range and depressing the upside calculation (FMP: +11.9% vs real Refinitiv: +23.4%).
- **Root cause: data source tier.** Analyst data is an oligopoly (Refinitiv, FactSet, Bloomberg, $10–30k/yr/seat). Yahoo has a legacy ad-funded Refinitiv I/B/E/S license, which `yfinance` pipes for free. FMP at $50/mo aggregates cheaper feeds.

Coverage survey (2026-05-12, 1,612-ticker universe):

| Source | Full data coverage | Notes |
|---|---|---|
| FMP (current) | 1,598 (99.1%) | What we use today |
| yfinance | 1,585 (98.3%) | 3-pass sequential retry, ≥0.7 sec between requests |
| Hybrid (yfinance primary, FMP fallback) | ~99.5% | 13 tickers covered only by FMP |

## Goals

1. **Switch live analyst pipeline from FMP to yfinance** (forward-only).
2. Preserve all historical analyst data — no rewriting of existing `analyst_targets_history` rows.
3. Keep FMP as automatic fallback for the ~13 tickers yfinance can't cover (mostly ETFs, dual-class shares).
4. Add data-provenance column (`source`) so each row records who provided it.
5. Single-flag rollback path (one ENV var change + restart).

## Non-goals

- **Historical analyst backfill.** yfinance only exposes current snapshots; historical Refinitiv data requires paid licensing. Out of scope.
- **Re-running historical signals or trade_log.** Per memory's `feedback_analyst_forward_fill.md` rule, we don't rewrite old data. The chart's historical line stays FMP-sourced for dates ≤ migration day; only future dates show yfinance.
- **Switching prices/fundamentals/ratios/cash flow to yfinance.** FMP is reliable for those; only analyst data needs the switch.
- **Field-level multi-source merge.** Approach C from brainstorming — over-engineered for our needs.

---

## Architecture

Three files touch this change:

```
data/yfinance_analyst.py       ← NEW (~80 lines)
    └── get_targets(ticker) -> dict | None
    └── get_targets_batch(tickers, max_workers=4) -> dict[ticker, dict|None]

data/fmp.py                    ← EXISTING, no change
    └── price_target_consensus(ticker)  (kept as fallback)

production/run_daily.py        ← MODIFIED (~10 lines)
    └── update_analyst_targets() — switched to yfinance with FMP fallback
```

**Module responsibilities:**

- `data/yfinance_analyst.py` is a thin adapter: handles rate limiting (Yahoo blocks above ~5 req/sec), parses Yahoo's response shape into our schema's column names, returns `None` on empty so the caller knows to fall back. No DB writes inside the module.
- `production/run_daily.py:update_analyst_targets()` orchestrates: batch-fetch yfinance, per-empty-result FMP retry, write to both `analyst_targets_history` and `analyst_expectations` with explicit `source` field.

**Behavioral contract:**

- Each successful run produces ≥1 row per ticker in `analyst_targets_history` for the run date, plus REPLACES the corresponding row in `analyst_expectations`.
- Every written row carries a non-NULL `source` value: `'yfinance'`, `'fmp'`, or (rare) `'none'` (when both empty).

---

## Data flow

```
Daily pipeline 7:00 AM Israel time (Mon–Sat)
┌─────────────────────────────────────────────────────────────┐
│ 1. Universe load     (S&P 500/400/600 + NASDAQ-100)         │
│ 2. Price update      (FMP — unchanged)                      │
│ 3. Fundamentals      (FMP — unchanged)                      │
│ 4. Predictions       (XGBoost — unchanged)                  │
│ 5. Analyst targets   ◄── CHANGED                            │
│      │                                                       │
│      ├── yfinance batch fetch (4 workers, 1-sec delay)      │
│      │     returns: dict[ticker, {low, mean, high, count}]  │
│      │                                                       │
│      ├── For empty results (~13 tickers):                   │
│      │     fmp.price_target_consensus(t)  ← fallback        │
│      │                                                       │
│      └── INSERT into analyst_targets_history (today's row)  │
│          REPLACE into analyst_expectations (current snap)   │
│          source = 'yfinance' | 'fmp' | 'none'               │
│                                                             │
│ 6. Health update     (Claude — unchanged)                   │
│ 7. Signal scoring    (engine reads analyst_expectations)    │
│ 8. Cache invalidate  (unchanged)                            │
└─────────────────────────────────────────────────────────────┘
```

**Two write paths:**

- `analyst_targets_history`: one row per (ticker, run_date). Matches AAPL's existing pattern (~250 rows/year per ticker).
- `analyst_expectations`: REPLACE-INTO, one row per ticker, refreshed each run.

**Engine reads (unchanged):**

- Historical signal scoring (target_date set): reads `analyst_targets_history` with point-in-time MAX(date) ≤ target_date AND date ≥ DATE(target_date, '-90 days').
- Live signal scoring (target_date=None): reads `analyst_expectations` (current snapshot).
- `/api/signals/today`: reads `analyst_expectations`.

**Runtime budget:**

- Yahoo rate limit: blocks above ~5 req/sec
- Strategy: 4 parallel workers, each sleeps 1 sec between its requests → ~4 req/sec aggregate, comfortably under the threshold
- 1,616 tickers / 4 req/sec ≈ 7 minutes added to the daily pipeline
- Total pipeline runtime: ~22 → ~29 min, comfortably inside the daily window

Coverage numbers below come from the 2026-05-12 survey on a 1,612-ticker universe. Today's universe is 1,616 (+BN/+NGG added 2026-05-17), so deploy-day counts may shift by ±5. The dry-run script in §Testing produces the authoritative current numbers before flipping.

---

## Error handling

| Failure | Response |
|---|---|
| yfinance returns empty / no analysts | Fall back to FMP for that ticker. Log it. |
| yfinance throws (timeout, JSON error) | Retry once after 2-sec sleep. Then fall back to FMP. |
| Yahoo HTTP 429 (rate limit) | Pause 30 sec, resume. After 3 consecutive 429s, abort yfinance step, finish remaining tickers via FMP. |
| Both yfinance AND FMP return empty | Write NULL row with `source='none'`. Engine handles NULL gracefully (auto-passes analyst_condition). |
| yfinance response shape changes | Parser raises explicit error early; pipeline aborts before any writes (failsafe — better to fail loud than silently corrupt). |
| Schema mismatch (missing `source` column) | Migration runs at module load via `_ensure_analyst_source_column()`, idempotent. |
| FMP fallback also fails (network) | Skip ticker. Log it. Next pipeline run retries — same self-healing pattern as existing repair loops. |
| Partial pipeline crash | Per-ticker transactions: already-fetched tickers stay written. Restart re-fetches only missing tickers. |

**Logging:**

Per-pipeline summary line, e.g.:
```
analyst: 1583 yfinance | 13 fmp_fallback | 20 missing
```
If counts deviate sharply from baseline (e.g., 200+ missing), surface via `/api/data/status` so the staleness banner can warn.

**Source-truth invariant:** the `source` column always reflects which API actually delivered the value for that row. Never write `source='yfinance'` if the value came from FMP. This protects future debugging — "why did NGG's target change in October 2026?" must be answerable from `analyst_targets_history` alone.

---

## Schema change

```sql
ALTER TABLE analyst_targets_history ADD COLUMN source TEXT;
ALTER TABLE analyst_expectations    ADD COLUMN source TEXT;
```

**Migration mechanism:**

Helper in `data/loaders.py` (following existing `_ensure_signals_columns()` pattern):

```python
def _ensure_analyst_source_column():
    with engine.begin() as conn:
        for tbl in ('analyst_targets_history', 'analyst_expectations'):
            cols = {r[1] for r in conn.execute(text(f"PRAGMA table_info({tbl})"))}
            if 'source' not in cols:
                conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN source TEXT"))
```

Called once on module load. Idempotent — safe on every uvicorn restart.

**Legacy data:**

- 1,616 rows in `analyst_expectations`: `source` stays NULL. Interpretation: unknown / pre-migration. No backfill — new yfinance writes will REPLACE these rows on the first pipeline run.
- ~600k rows in `analyst_targets_history`: `source` stays NULL on existing rows. New rows get `source='yfinance'` or `'fmp'`. We can query `WHERE source IS NULL` to identify legacy rows if ever needed.

**No data backfill** — we don't fabricate a `source='fmp'` value on legacy rows because we don't have first-hand provenance. Per memory's "no fabrication" rule.

---

## Testing & verification

### Pre-deploy (gate before flipping the call site)

```python
# scripts/dry_run_yfinance_analyst.py
# Fetches yfinance data for ALL tickers, compares to current FMP data.
# NO DB writes. Outputs a comparison report.
```

Expected output:
```
=== yfinance vs FMP coverage comparison ===
Total tickers:                     1616
yfinance returned data:            1585  (98.1%)
FMP would still be needed:           31  (1.9%)
Both returned empty (true gap):      12

=== Signal-shift simulation ===
Tickers where analyst_condition would FLIP:
  Currently passing → would fail:    87
  Currently failing → would pass:   142
Most extreme upside corrections:
  V:   FMP +11.9%  →  yfinance +23.4%
  NGG: FMP   NULL  →  yfinance +14.8%
  …

=== Per-ticker problem cases ===
[tickers with absurd deltas (>50% change) for manual review]
```

Go/no-go: eyeball the report. Sample 20 random tickers manually against Yahoo Finance website. If any look bizarre → fix before deploying.

### Deploy-time

Single ENV flag in `/opt/vesign/.env`:
```
ANALYST_SOURCE=yfinance     # was implicit 'fmp'
```

`update_analyst_targets()` reads this flag. Instant rollback by editing the flag.

### Post-deploy monitoring (first 3 daily runs)

- Per-run summary log: yfinance / fmp / missing counts should stabilize near 1583 / 13 / 20
- yfinance step duration: <10 min
- `/api/data/status` returns healthy
- Spot-check 5 random tickers on website vs Yahoo Finance

If any go sideways within first 3 runs → flip flag back, investigate.

---

## Rollout plan

**Recommended: wait for tomorrow's 7am pipeline.**

1. Deploy code + schema migration today.
2. Tomorrow morning's normal pipeline run is the first yfinance fetch.
3. We watch logs in real-time as the normal pipeline executes.
4. No manual triggers, no special handling — just the normal daily flow doing the work.

Alternative (rejected): one-shot manual refresh today. Skips one day of waiting but adds another moving piece on deploy day. Not worth the risk.

---

## Rollback

```
Risk level:        LOW
Mechanism:         one ENV flag
Roll-back time:    30 seconds (edit .env + systemctl restart vesign)
Data damage:       none — yfinance rows stay, FMP writes resume tomorrow
```

Step-by-step:
1. Edit `/opt/vesign/.env` → `ANALYST_SOURCE=fmp`
2. `systemctl restart vesign`
3. Next pipeline run writes FMP rows again. Already-written yfinance rows untouched.
4. Open issue, debug.

---

## Out of scope

These are deliberate non-decisions for this spec; revisit in separate projects:

- **Historical analyst backfill.** yfinance can't provide pre-today snapshots; needs paid Refinitiv access ($15k/yr) or Tiingo/Polygon evaluation.
- **Other data sources.** yfinance/Refinitiv is already > FMP; no need to add Tiingo, IEX Cloud, etc.
- **yfinance for non-analyst data.** FMP is reliable for prices/fundamentals/ratios; only analyst data needs switching.
- **Per-ticker source routing.** Approach B from brainstorming (foreign-only yfinance, US-only FMP) — adds complexity without enough benefit.

---

## Memory note (post-deploy)

After successful deploy, save to `feedback_yfinance_analyst_active.md`:

- Active source: yfinance (since YYYY-MM-DD)
- Fallback: FMP
- Schema: `source` column on `analyst_targets_history` + `analyst_expectations`
- Rollback flag: `ANALYST_SOURCE` env var

---

## Open questions

None. All design decisions made during brainstorming.

---

## Related memory

- `project_yfinance_vs_fmp_analyst.md` — original migration scope (2026-05-12)
- `feedback_analyst_forward_fill.md` — no-fabrication rule
- `project_split_adjustment_for_analyst_data.md` — split-adjustment issue (orthogonal to this change)
- `project_analyst_backfill_pattern.md` — UPSERT pattern (still applies for analyst_targets_history)
- `project_bn_ngg_add_2026_05_17.md` — surface that exposed NGG's coverage gap
