# Holdings "Vault" — Design Spec

**Date:** 2026-06-29
**Branch:** `feat/ui-redesign`
**Scope:** The Portfolio → **Holdings** tab only (`frontend/src/redesign/PortfolioPage.jsx` + `frontend/src/redesign/portfolio/*`). Watchlists tab unchanged.

## Goal

Make the Holdings page the user's **vault**: the single best place to track, analyze, and understand their investments — good enough that they stop opening their broker. The differentiator is that we layer **Vesign's intelligence (health, ML, analyst target, live signal) onto the user's own positions**, alongside performance analytics a broker doesn't give (benchmark vs Vesign *and* SPY, concentration/diversification, net-worth-over-time).

Direction chosen: **Layered Vault** (evolve the existing structure top-to-bottom), reusing the components already built.

## Data reality (drives scope)

Available per holding (from `/api/portfolio/holdings`): ticker, company, logo, industry, **sector** (to be added), market_cap, **health_score**, **fair_value_upside** (model), **prediction_score** (ML), **target_mean_price** (analyst), **signal** (to be added), total_qty, total_cost, avg_price, latest_close (live/phase-aware), prev_close (day baseline), last_close, first_buy_date, DCA lots.

Portfolio-level: weekly return series from `/api/portfolio/performance` (supports `months` 1–60; currently UI hardcodes 1Y), per-watchlist comparison, and **SPY daily prices** in `daily_prices` (enables a market benchmark).

**Explicitly out of scope (no data — will NOT be faked):** dividends/income, and realized P&L / sell history. Users have only buy lots, so the vault tracks **current open positions**.

## Section-by-section design

The page renders (top → bottom): **Net-worth hero → Performance → Allocation+Concentration → Vesign's Read → Holdings table.** Watchlist-comparison bars move below the table (secondary). All sections gate together on load (existing `useReady` pattern) so they appear at once.

### 1. Net-worth hero (replaces `KpiStrip`'s 5-card row)

A commanding band, two zones:
- **Left (hero):** big **Current value** (mono, ~34px). Below it today's move `▲ +$1,240 (+0.97%)` (green/red), and an all-time line `+$23,400 · +22.3% since inception` (muted label).
- **Right (stat chips):** four compact chips — **Invested** (+ `N holdings · M watchlists`), **vs Vesign (1Y)**, **Best / Worst** (top + bottom ticker by yield), **Largest holding** (ticker + % of value).

All values already computed in `computeRows()`. Currency-aware via `useCurrency()`.

### 2. Performance — range + benchmark + %/$ toggle (`PerformanceChart`)

- **Range chips:** `1M · 3M · 6M · 1Y · 2Y · All` → `months` = `1 / 3 / 6 / 12 / 24 / 60`. Wires the existing `getPortfolioPerformance(market, months)` (currently hardcoded 12). Default **1Y**.
- **Mode toggle:** **Return %** (default) vs **Value $**.
  - *Return % mode:* three toggleable series — **Your portfolio** (blue `#60a5fa`), **Vesign** (green `#00d97e`), **SPY** (amber `#f59e0b`), each normalized to 0 at window start. Default all on.
  - *Value $ mode:* single **portfolio market-value** line (net worth over time; benchmarks have no $ value for the user, so they hide in this mode). Currency-aware.
- Existing **hover crosshair + tooltip** extends to whichever series are visible.
- **Backend change:** `/api/portfolio/performance` adds two fields per weekly point: `spy` (normalized buy-and-hold return %, from SPY in `daily_prices`) and `value` (portfolio market value $ that week = Σ qty×price for lots held as of that week). `portfolio` and `vesign` unchanged.

### 3. Allocation + concentration (`AllocationDonut`)

- Donut chips become **Sector / Industry / Ticker** (Sector is new and more meaningful for diversification; uses the `sector` field added to the endpoint). Existing top-7 + "Other" rollup retained.
- New **concentration readout** beside/under the donut: **Top holding %**, **Top-5 weight %**, **# positions**, **Largest sector %**, and a one-word **diversification read** — *Concentrated* (top-5 ≥ 70%), *Balanced* (40–70%), *Diversified* (< 40%). All derived client-side from `rows`.

### 4. "Vesign's Read on your holdings" — NEW panel (the differentiator)

A horizontal panel summarizing the model's view of the **current** holdings, left→right:
- **Signal mix:** colored chips `5 BUY · 4 HOLD · 1 SELL` (counts of `signal` across holdings; BUY green / HOLD grey / SELL red). Holdings with no signal counted as "—"/unrated, not shown as a chip.
- **Avg health:** `3.8 / 5` rendered with the 5-dot health widget (value-weighted by position value).
- **Avg ML quality:** mean `prediction_score` (as %).
- **Biggest upside:** ticker + analyst upside % (max of `(target_mean_price − price)/price`).
- **Watch-outs:** weakest-health holding (min health_score) and/or any holding currently flagged **SELL**.

New component `portfolio/VesignRead.jsx`. Data: holdings `rows` (needs the `signal` field). No new endpoint.

### 5. Holdings table — sortable, searchable, deeper (`HoldingsTable`)

- **Sortable:** every data column is click-to-sort with ▲/▼, reusing the `Th` pattern + `.sortable`/`.sort-ar` CSS already shipped on Open/Closed Trades. **Default sort: Market value ↓** (biggest positions first; replaces A→Z).
- **Search box:** filter by ticker/company (same control/UX as Closed Trades).
- **Export:** restore the CSV/XLSX/ZIP button (endpoint `/api/portfolio/holdings/export` exists; reuse prod's `DownloadButton` approach in the redesign style).
- **Columns** (left→right). M. Cap and sector are **subtitles, not columns**, to keep width sane:
  1. ▸ chevron (DCA lot expand)
  2. **Ticker** — logo + ticker + a small **signal pill** (BUY/HOLD/SELL) when present
  3. **Company** — with a tiny subtitle `Sector · $XXB` (sector + market cap)
  4. **Weight %** — position value ÷ total, with a thin inline micro-bar
  5. **Health** — dots
  6. **Prediction** — analyst-target upside vs live price (existing `arrowPct1`)
  7. **ML Score** — `prediction_score` %
  8. **Qty**
  9. **Avg Price**
  10. **Price** — phase-aware live (+ day Δ $ and %); shows last close when market closed. *(Merges the old separate "Last Price" + "Live Price" columns.)*
  11. **Invested**
  12. **Market value**
  13. **Total P&L** — $ (bold) + % beneath

  Sortable on every column incl. Weight %, Health, Prediction, ML, and the money columns; the Company column sorts by ticker. (M.Cap/sector live in the subtitle, so sorting by size isn't offered — weight % covers "how big is this position".)
- Retain: DCA **lot expansion** (`HoldingLots`), **add-holding** form (`AddHoldingForm`), **row → SignalModal**.
- **Backend change:** `/api/portfolio/holdings` adds `sector` (from `companies.sector`) and `signal` (latest `signals.signal` for the ticker — the meta subquery already selects from `signals`, add the column).

## Backend changes (complete list — both additive, no breaking changes)

1. **`/api/portfolio/performance`** — add `spy` (normalized return %) and `value` (portfolio $ value) to each weekly point. SPY series = buy-and-hold of `SPY` from `daily_prices`, normalized to 0% at the window's first week.
2. **`/api/portfolio/holdings`** — add `sector` and `signal` to each row (extend the existing `meta` subquery; both columns are one-line additions to a query that already joins `companies` and `signals`).

## Component / file plan

- `PortfolioPage.jsx` — reorder holdings tab: hero → perf → alloc → VesignRead → table → (watchlist comparison moved below). Extend `computeRows` with weight %, concentration aggregates, signal-mix counts, value-weighted avg health.
- `portfolio/KpiStrip.jsx` → reworked into the **net-worth hero** (rename to `NetWorthHero.jsx`).
- `portfolio/PerformanceChart.jsx` — range state, mode toggle, SPY/value series, multi-series toggle.
- `portfolio/AllocationDonut.jsx` — Sector mode + concentration readout.
- `portfolio/VesignRead.jsx` — **new**.
- `portfolio/HoldingsTable.jsx` — sort + search + export + new columns/subtitles/signal pill.
- `portfolio/portfolio.css` — styles for hero, range/mode chips, concentration readout, VesignRead, weight micro-bar, signal pill.
- Backend: `backend/main.py` (`portfolio_performance`, `portfolio_holdings`).

## Testing

- Backend: extend `tests/backend/` for the two endpoints — `performance` returns `spy` + `value` keys with sane values; `holdings` returns `sector` + `signal`. SPY normalization starts at 0.
- Frontend: unit-test the new pure helpers — concentration/diversification classification, weight %, signal-mix counts, value-weighted avg health — and the table sort comparator (numbers numeric, strings lexical, nulls last), mirroring the Closed Trades sort tests.

## Non-goals

Dividends, realized P&L, tax lots beyond the existing DCA view, $-value benchmark lines, and any Watchlists-tab changes.
