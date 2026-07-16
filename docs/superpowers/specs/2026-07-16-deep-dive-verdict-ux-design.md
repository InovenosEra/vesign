# Deep-Dive "Verdict-First" UX Redesign

**Date:** 2026-07-16
**Branch:** `feat/ui-redesign`
**Scope:** `frontend/src/redesign/research/DeepDive.jsx` + `research.css`, one backend endpoint change (`/api/signals/markers`). Screener, hero search, fundamentals grid, AI Report generation behavior, and news are unchanged.

## Goal

Deep-Dive's UI is fine; its UX isn't. The page is evidence-first and conclusion-last: a user doing pre-buy due diligence or checking in on a holding has to read six sections of raw stat panels before reaching anything synthesized — and the one section that *is* synthesized (the AI Research Report, shipped earlier this session) sits second-to-last, requires a manual click, and is easy to never notice. Along the way, three separate panels repeat the same "Predicted upside" and "ML 5-day" numbers with no added context between appearances, and the signal-history table's return column is a hardcoded "—" for every row despite the underlying win/loss data existing in `trade_log`.

This redesign reorders the page around one synthesized verdict, removes the redundant restatements, and wires real numbers into the one table that's currently faking emptiness.

## Constraint carried through the whole design

Deep-Dive's model fields (signal, health, ML, AI report) are gated at Free-vs-Pro+ (`modelLocked = plan !== 'pro' && plan !== 'max'`) — **not** Max-only like the Screener. Every new/changed section must work for locked users too: a compelling single locked line, not a wall of blur, and never fewer real values than the Free plan already gets today (analyst-based "Predicted upside" stays visible regardless of plan, per existing convention).

## Section-by-section design

### 1. New verdict band (`frontend/src/redesign/research/DeepDive.jsx`, new sub-component)

Sits directly under the hero, above the chart row. **No LLM call** — assembled client-side from fields already fetched via `getResearch`:

- **Left:** signal tag (locked pill for Free) + `"{trade_count} historical trades · WR {win_rate}%"` (moved here from the panel being removed, §2).
- **Center:** one synthesized sentence built from real fields, e.g. *"Analysts see +18% upside to $210. Health strong (4/5). ML leans bullish (+2.1% 5-day)."* Upside fragment always renders (not model-gated). Health/ML fragments render normally for Pro+/Max; for Free they collapse into a single locked fragment — `"Vesign model 🔒 Upgrade to Pro"` — inline in the same sentence rather than three separate blurred stat boxes.
- **Right:** next-earnings date + a `Get the full AI written take →` button. Clicking scrolls/expands to the AI Report section (§4) — does not fire it.

### 2. Dedup + relocate

- **Remove** the current side-by-side verdict panel next to the chart (`dd-verdict`/`dd-vstat` block) — its content is now in the band above. The chart panel (`dd-chart-panel`) becomes full-width standalone; its own content (range chips, BUY/SELL markers, legend) is unchanged.
- **Move** "In your watchlists" chips into the hero, next to the existing Watchlist toggle button — same concern, one place.
- **Remove** the ML-predictions panel's "Direction (5d)" row (`dd-ml-row` for `Direction (5d)` / `UP`/`DOWN`/`FLAT`) — it's the sign of the number two lines above it in the same panel, zero new information.
- Analyst-targets range bar and the ML confidence bar **stay** — those show range/context a flat number doesn't, unlike the plain duplicate stat cards being removed.

### 3. Signal history — real returns

`/api/signals/markers` (`backend/main.py:1505`) currently returns `date, signal, lot_seq, close, vqs, fair_value_upside, health_score` — no outcome. Verified join path exists: `signals.lot_seq` + ticker matches `trade_lots(ticker, lot_seq)` → gives `(buy_date, sell_date)` → keys `trade_log(ticker, buy_date, sell_date)` → `return_pct` (spot-checked against real rows in `vesign.db`, e.g. ticker A / lot_seq 1 / 2020-02-27→2020-06-01 / +16.1%).

- **Closed trades:** attach the matched `trade_log.return_pct` to each marker row.
- **Still-open position** (no matching `sell_date`): compute unrealized return from the live/last-close price vs the lot's buy price — same live-overlay pattern already used elsewhere (`_get_live_snapshot()`).
- **Frontend:** `DeepDive.jsx`'s `dd-hrow` currently hardcodes `<div className="ret open">—</div>` for every row (`.dd-hrow .ret.up`/`.ret.down`/`.ret.open` CSS already exists, unused). Wire the real value in — green/red pill for closed, a distinct "open" treatment for the unrealized case.

### 4. AI Report — unchanged behavior, new visibility

Stays exactly where it is structurally and stays manual/on-demand (confirmed: no auto-fire, since it's an uncached, billed Anthropic call per generation — auto-firing on every page view was explicitly rejected). Only change: the verdict band's CTA (§1) makes it discoverable instead of something you only find by scrolling past five sections.

## Net diff

| Change | Where |
|---|---|
| New verdict-band component | `DeepDive.jsx` (frontend only, no backend) |
| Remove old side verdict panel | `DeepDive.jsx` + `research.css` |
| Remove ML panel's "Direction (5d)" row | `DeepDive.jsx` + `research.css` |
| Relocate watchlist-membership chips into hero | `DeepDive.jsx` |
| Real returns on signal-history rows | `backend/main.py` (`/api/signals/markers`) + `DeepDive.jsx` |

Untouched: fundamentals grid, chart internals, AI Report generation/rendering, news, search bar.

## Out of scope (explicitly deferred, not forgotten)

- Tabbed/progressive-disclosure restructuring (Approach B) — rejected in favor of same-page reorder.
- Caching the AI Report cache-aside per (ticker, date) to make auto-generation affordable — a real option if "always-on AI verdict" becomes a priority later, but is backend work beyond this pass.
- Peer/sector comparison, price alerts from Deep-Dive, personalized "recently viewed" pills — noted during the audit as possible future value, not part of this redesign.
