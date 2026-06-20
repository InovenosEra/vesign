# Tiered BUY Signals — Design

**Date:** 2026-06-20
**Branch:** `feat/ui-redesign`
**Status:** Design approved (analysis complete). Not yet implemented.

---

## 1. Problem

Vesign generates far more SELL signals than BUY signals, and BUYs are clustered into market panics with long dead stretches in between. Users open the app for days or weeks and see only sells — there is nothing to act on.

Measured reality (US universe, excludes `.TA`):

| Year | BUY signal-rows | SELL signal-rows | Ratio |
|---|--:|--:|--:|
| 2024 | 215 | 15,253 | 1 : 71 |
| 2025 | 512 | 6,977 | 1 : 14 |

Cadence is worse than the totals suggest. In 2025, **June had zero days with any BUY** (703 sells that month); May had one. Most months only 5–10 of ~21 trading days fire even a single BUY.

### Root cause

A BUY requires a **maxed-out** score. The engine fires BUY only if **either**:
- **V1** — 7 conditions all true at once (`rsi_3day_flag==3 & bb_condition & analyst_condition & volume_flag & week52_condition & health_condition & ml_condition`), or
- **V2** — `vqs == 9`, a *perfect* 9/9 contrarian score (requires VIX>29, deep momentum crashes, etc.).

Both paths only trigger in extreme oversold/panic conditions. There is no "8/9" — it is BUY or silence. Yet `vqs` (0–9) is **already computed for every stock every day** and then discarded below 9. The tiering mechanism already exists in the data; we simply throw away everything under the top.

## 2. Goal

Surface the existing `vqs` score as **three quality tiers**, so there is a BUY to show on (nearly) every trading day, ordered by quality. Accept that more BUYs → eventually more SELLs; that is fine.

**Non-goal:** changing the SELL logic, the exit/stop logic, the DCA logic, or the underlying scoring. This is purely about *which* signals we surface and *how* we label them.

## 3. The design

### 3.1 Three tiers (mutually exclusive by `vqs` band)

Each signal is labeled by its `vqs` value. A signal belongs to exactly one tier.

| Tier | Public label | Rule | Trades (8y backtest) | Win Rate | Avg Yield |
|---|---|---|--:|--:|--:|
| **① Strongest** | "Strongest" *(public)* | `vqs == 9` | 1,244 | 74.6% | +40.5% |
| **② Recommended** | "Recommended" | `vqs == 8` | 4,151 | 72.2% | +30.5% |
| **③ High Potential** | "High Potential" | `vqs 6–7` | 11,309 | 72.2% | +21.5% |

- Volume steps are smooth: ×3.3 then ×2.7 (no cliff).
- Yield ladder is clean and monotonic: **+40.5% → +30.5% → +21.5%**.
- `vqs ≤ 5` remains HOLD (not surfaced as a BUY), **except** a V1 7-gate signal (see below).
- **Tiers are assigned purely by `vqs` band.** Tier 1 is `vqs == 9` (the backtested 1,244-trade figure is pure `vqs 9`, not "vqs 9 or V1").
- **V1 is retained as a BUY trigger** — a V1 7-gate signal still fires even if `vqs < 6`, and is **labeled by its `vqs` band** like everything else. A V1 signal that fires with `vqs ≤ 5` is assigned **Tier 3** (the floor) so it is never unlabeled. V1 entries are rare; nearly all tier volume is `vqs`-driven.

### 3.2 What is shown where

- **Public / marketing pages:** only **Tier 1** statistics (74.6% win rate, +40.5% avg yield). This is the flagship number.
- **In-app signal lists:** all three tiers, each rendered with its **label/badge** only. Tiers 2 and 3 do **not** display win-rate/yield headline stats — they are presented as quality labels ("Recommended", "High Potential"), not as performance claims.

### 3.3 Year-by-year (Tier 1, the public face)

| Year | Trades | Win Rate | Avg Yield |
|---:|---:|---:|---:|
| 2020 | 628 | 73.9% | +48.0% |
| 2021 | 14 | 71.4% | +11.5% |
| 2022 | 361 | 73.1% | +24.1% |
| 2024 | 30 | 70.0% | +26.1% |
| 2025 | 202 | 80.2% | +51.7% |
| 2026* | 9 | 77.8% | +17.1% |
| **TOTAL** | **1,244** | **74.6%** | **+40.5%** |

\*2026 partial (through ~June). 2018/2019/2023 had no `vqs==9` days (calm years).

## 4. Key findings that shaped the design

These are the non-obvious conclusions from the analysis; they justify the choices above and prevent us from re-litigating them.

1. **Win rate is largely a "house effect," ~69%.** Random entries run through the real trade engine score **68.7% win rate / +5.1% yield**; "always enter when flat" scores 70.7%. The −25% stop plus let-winners-run-to-RSI70/365d, in a market that drifted up 2018–2026, manufactures ~70% wins from coin-flip entries. **Therefore win rate barely separates strategies — average yield is the real edge metric.** (Random +5.1% vs Tier 1 +40.5% = ~8× edge.)

2. **`vqs` is the best available engine.** It forms a clean, monotonic quality ladder on yield. The V1 multi-criteria gate, scored as a graded count (0–7), is shallow and *non-monotonic*, adds no unique edge over `vqs`, and one of its conditions (`health`) actively *reduces* forward returns. We keep V1 firing (continuity) but do not build the tiers on it.

3. **No high-quality non-contrarian "second engine" exists in calm markets.** In calm regimes (VIX<20, ~63% of days) nothing beats coin-flip win rate on the 20-day horizon. Through the real engine, analyst-upside ≥25% (73% / +13%) and ML `prediction_score` ≥0.05 (70% / +30%) are viable *alternative* engines if we later want non-contrarian coverage — but they are out of scope here.

4. **Bollinger depth (`bb_pct_b`) is the only lever that lifts win rate** (kept in reserve). `(V1 or vqs9) AND bb ≤ −0.20` → 82% WR / +38.5% (278 trades); `bb ≤ −0.25` → 84% (163 trades). Win rate plateaus ~84%. We did **not** adopt this for Tier 1 because it reintroduces a ~27× volume gap to Tier 2. It remains available if public win rate is later judged more important than volume balance.

## 5. Implementation approach

### 5.1 Engine (`signals/engine.py`)

The `vqs` score is already computed and persisted per signal row. The change is to **derive a tier label from `vqs`** and widen the BUY gate from "`vqs == 9` only" to "`vqs >= 6`", while keeping the V1 path.

- New BUY condition (conceptual): `v1_buy_cond | (vqs >= 6)` (was `v1_buy_cond | (vqs == 9)`).
- New persisted column on `signals`: `tier` (small int): `1` for `vqs == 9`, `2` for `vqs == 8`, `3` for `vqs 6–7`, and `3` (floor) for any V1-gate BUY whose `vqs ≤ 5`. HOLD/SELL rows have null tier.
- DCA, stop, SELL, and 365-day exit logic are **unchanged**.
- Tier assignment is a pure function of the existing `vqs` value — no new inputs, no new data sources.

Because this is a rule change to the BUY gate, it triggers the standard historical rebuild (per project rule: threshold changes apply historically). That means re-running the signal backfill + `build_trade_log` so `signals.tier` and the trade history are point-in-time correct across all dates.

### 5.2 Mutual exclusivity / dedup

Tiers are defined by disjoint `vqs` bands (9 / 8 / 6–7), so a given signal row maps to exactly one tier with no overlap. The backtest "trades" counts per tier were simulated independently (entry-when-in-band) and are approximate; the **exact** per-tier counts after the unified rule must be measured during implementation from the rebuilt `signals.tier` column. (Trade-count totals will shift slightly from the independent-simulation figures; win-rate/yield are expected to hold.)

### 5.3 API

- `signals/today`, `/api/signals*`: include `tier` in each BUY row's payload.
- Public `/api/stats`: continue to report a single flagship number, now defined as **Tier 1 only** (vqs 9). (Note: today `/api/stats` and the TradesPage panel already disagree on win-rate definition; align both to the Tier 1 definition as part of this work.)

### 5.4 UI

- BUY signal cards/rows: a **tier badge** ("Strongest" / "Recommended" / "High Potential") with distinct styling. Tier 1 visually dominant.
- Tiers 2–3 show the **label only**, no per-tier headline win-rate/yield.
- Public/marketing pages: flagship stats = Tier 1 (74.6% / +40.5%).
- (Interaction with the existing Free/Pro/Max access tiers — see `project_signals_tiered_access` — is a separate axis; tier-label gating to be decided then.)

### 5.5 Position-sizing guidance (product)

More tiers means users could over-deploy on lower-conviction Tier 3 signals. Provide per-tier guidance (e.g. suggested weighting Strongest > Recommended > High Potential). Exact mechanism TBD — flagged, not specified here.

## 6. Backtest evidence & methodology

All analysis was **read-only** (scripts in `/tmp`, no DB writes, no product-code changes).

- **Faithful re-implementation** of `backtesting/engine.py:build_trade_log`: open on first BUY at close; stop = fixed 25% below first-buy price; exit = `((close ≤ stop OR rsi ≥ 70) AND (pred < 0 or pred is NULL)) OR held ≥ 365d`; DCA add-on lot when a qualifying BUY fires while open AND close ≤ 90% of last lot; per-trade yield = `(sell − harmonic-avg-cost) / harmonic-avg-cost`.
- **Validation:** the simulator reproduces the live baseline exactly — 1,933 trades, `return_pct` 68.4% WR / +21.6% avg (to the decimal), DCA 73.5% / +30.4%, 178-day avg hold. This is why the tier numbers can be trusted.
- **Outcome proxy for never-traded signals:** the `forward_returns` table (`fwd_5d` / `fwd_20d`, 3.6M rows, 2018–2026) was used to rank signal quality where no real trade existed; final tier stats use the real-engine simulation, not the proxy.

## 7. Testing

- **Regression:** after the engine change, re-running the backfill must still reproduce the validated baseline for the `vqs == 9` subset (1,244 trades / 74.6% / +40.5%).
- **Tier integrity:** every surfaced BUY row has exactly one tier in {1,2,3}; no BUY with `vqs ≤ 5`; HOLD/SELL rows have null tier.
- **Per-year stats:** regenerate the year × tier matrices (trades / win rate / yield) from the rebuilt tables and confirm they match this document within expected dedup drift.
- **API contract:** `tier` present on BUY payloads; public `/api/stats` returns the Tier 1 number.

## 8. Out of scope (YAGNI)

- Changing SELL / stop / DCA / 365-day exit logic.
- A non-contrarian second engine (analyst-upside / ML) — viable but deferred.
- The `bb_pct_b` deep-dip Tier 1 (82–84% WR) — deferred; available if priorities change.
- Risk-adjusted (drawdown / volatility) tier metrics — not computed here.
- Changes to the Free/Pro/Max access paywall.

## 9. Open items to resolve during implementation

1. Exact per-tier trade counts after the unified rule + dedup (re-measure from rebuilt `signals.tier`).
2. Per-tier position-sizing guidance mechanism.
3. Align `/api/stats` and TradesPage win-rate definitions to the Tier 1 definition.
4. Tier label × Free/Pro/Max gating interaction.
