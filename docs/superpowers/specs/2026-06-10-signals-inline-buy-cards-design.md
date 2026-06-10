# Signals Page — Inline BUY Cards Redesign

**Date:** 2026-06-10
**Status:** Approved (brainstorm), pending implementation plan
**Branch:** `feat/ui-redesign`
**Builds on:** [BUY-signal explainer](2026-06-10-buy-signal-explainer-design.md)

## Problem

The Signals page renders BUY and SELL as two **equal side-by-side columns**
(`redesign/signals/SignalsSplit.jsx`). The two sides are asymmetric:

- **BUY** — few (often 1), high-value, *paid*, and now has a rich AI "Why this
  signal" explanation. Today it's a one-line table row; the explanation is only
  reachable by a second click into the modal.
- **SELL** — many (e.g. 43), scannable, no per-signal explanation (the explainer
  is BUY-only), paginated.

Equal columns waste a half-screen on a single BUY while cramming 43 SELLs into a
paginated half. And the thing a user *pays for* — the BUY signal + its rationale
— is hidden behind an extra click.

## Goal

Show each BUY's metrics **and** its AI explanation inline on the Signals page —
the payoff for paying, in one place — while keeping SELL a compact list. Keep
the two sections, stacked vertically.

## Decisions (from brainstorm)

| Question | Decision |
|---|---|
| Layout | **Stacked, asymmetric**: BUY section (rich cards) on top, SELL section (compact table) below |
| BUY card contents | **Metrics + AI explanation inline.** Chart/fundamentals/news stay in the modal |
| Locked BUY card | **Blurred full-card shape + unlock CTA.** No real data or model call until unlocked |
| SELL section | **Unchanged** compact paginated table, below; row click opens modal |
| Modal | **Kept** for the deep dive; reached via `Open full analysis →` on each BUY card |
| Backend gating | **Unchanged** — reuse `unlockSignal`, `isLocked`, see-all wallet flow |

## Layout

```
PageHead (unchanged)
┌─ BUY SIGNALS · <date> · N signals ─────────────────────────────┐
│  [BuySignalCard]   ← one full-width card per BUY                │
│  [BuySignalCard]                                                │
└────────────────────────────────────────────────────────────────┘
┌─ SELL SIGNALS · <date> · M signals ────────────────────────────┐
│  [compact paged table — today's SignalColumn body, reused]     │
└────────────────────────────────────────────────────────────────┘
OpenTrades / ClosedTrades (unchanged)
```

## Components

### `SignalExplanation` (new, shared) — `redesign/SignalExplanation.jsx`
Extract the explanation UI currently inline in `SignalModalRd.jsx` into one
presentational+self-fetching component, so the modal and the new BUY card share
it (no duplication).
- **Props:** `ticker` (required). Internally runs the `getSignalExplanation(ticker)`
  query (ticker-centric, no date), `staleTime: 600_000`.
- **Renders:** loading ("Generating…"), `locked` teaser, error, and the data view
  (headline · ✓strengths · ⚠risks · key-numbers · disclaimer).
- **CSS:** co-located `signal-explanation.css` with generic classes (e.g.
  `.sig-why*`), replacing the modal-scoped `.m-why*` rules.
- **Mounting rule:** the parent only renders it for BUY signals (modal keeps its
  `r?.signal === 'BUY'` guard; `BuySignalCard` only exists for BUYs). The
  component itself does not re-check signal type.

### `BuySignalCard` (new) — `redesign/signals/BuySignalCard.jsx`
Full-width card for one **unlocked** BUY (`s` = a signal row).
- Header: logo · ticker · company · price · ▲upside · health dots · ML (same
  fields as today's `FullRow`).
- Body: `<SignalExplanation ticker={s.ticker} />`.
- Footer: `Open full analysis →` → `useTickerModal()(s.ticker, s.company)`.

### `LockedBuyCard` (new) — `redesign/signals/BuySignalCard.jsx` (same file)
Full-card shape, **blurred**, for a locked BUY (`s` = a locked row).
- Renders the card skeleton with placeholder/blurred metrics + a blurred
  rationale block (placeholder text — never real data); ticker identity hidden
  (reuse the existing `FAKE_SIG` placeholder approach).
- CTA: per-row `🔓 Unlock · <price>` when `s.reason === 'pay'`; else `🔒 See all` /
  `Upgrade` — mirrors current `LockedRow` logic and calls the same `unlockRow`.
- **No `getSignalExplanation` call** is made for locked cards.

### `SignalsSplit` → restructured — `redesign/signals/SignalsSplit.jsx`
- Renders `<BuySection>` then `<SellSection>` vertically (replaces the
  `.signals-split` two-column flex).
- **BuySection:** the BUY query + section header (with `Unlock all today` see-all
  CTA, unchanged) + a list of `BuySignalCard`/`LockedBuyCard` (per `isLocked`).
- **SellSection:** the existing `SignalColumn` table body for SELL, reused
  verbatim (header + `PagedTable` of `FullRow`/`LockedRow`).
- Shared unlock handlers (`unlockRow`, `unlockAll`) stay; factor the bits both
  sections need so they aren't duplicated.

### `SignalModalRd.jsx` — refactor only
Replace the inline `r?.signal === 'BUY' && (<div className="m-why">…)` block with
`{r?.signal === 'BUY' && <SignalExplanation ticker={ticker} />}`. Behaviour
unchanged; remove the now-dead `.m-why*` markup. (Keep `.m-why*` CSS only if
still referenced; otherwise migrate to `signal-explanation.css`.)

## Data / gating / performance

- **Tiers:** Max → all BUYs unlocked (full cards). Pro → BUYs locked until paid
  (per-row 10¢ / see-all 50¢) → blurred cards → full card on unlock. Free →
  blurred cards + Upgrade CTA.
- **Explanation cost:** generated only for **unlocked** BUY cards, in parallel
  (react-query, one query per card, each with its own "Generating…"), cached
  server-side per ticker/day. BUYs are few by design, so this is a handful of
  calls at most, and zero for locked cards.
- **CSS:** new card styles in `redesign/signals/signals.css`; explanation styles
  in `signal-explanation.css`.

## Error handling

- Explanation fetch error → the card body shows "Explanation unavailable —
  please try again" (the `SignalExplanation` error state); the rest of the card
  (metrics, Open full analysis) still works.
- Unlock failure (402) → existing alert("Not enough wallet balance.") path,
  unchanged.

## Testing

- `gating.js` logic (`isLocked`, `hasMoreLocked`) is unchanged and keeps its
  existing tests.
- Frontend rendering is not headlessly verifiable (Clerk-gated SPA) — verify via
  `npm run build` (green) + manual check: as Max, BUY shows a full card with the
  explanation inline; as Pro pre-unlock, a blurred card + working unlock CTA that
  reveals the full card; SELL still a compact paged table below.
- Backend explanation endpoint/tests are unchanged (15 passing).

## Out of scope (v1)

- No chart / fundamentals / news inline (stay in the modal).
- No SELL explanations.
- No backend gating/wallet changes.
- No change to OpenTrades/ClosedTrades sections.

## Deploy notes

Frontend-only (plus the already-shipped explainer backend). Stays on
`feat/ui-redesign`; merge to `main` at deploy.
