# BUY-Signal Explainer — Design

**Date:** 2026-06-10
**Status:** Approved (brainstorm), pending implementation plan
**Branch:** `feat/ui-redesign` (all work stays here until deploy)

## Summary

An LLM-generated, plain-English explanation for each signal, shown in the
`SignalModal`. It **narrates the structured data Vesign already trusts** (ML
score, health, fundamentals, analyst targets, recent news) into a scannable
card. It does **not** produce new signals, predictions, or analysis — the
verdict always comes from our engine; the LLM only restates and interprets the
evidence.

This is the first LLM-powered feature in Vesign. The Anthropic API is used via
the official `anthropic` Python SDK; `ANTHROPIC_API_KEY` is already in `.env`.

## Decisions (from brainstorm)

| Question | Decision |
|---|---|
| Feature | Per-signal "why did this fire" explainer in SignalModal |
| Generation timing | **On-demand + cache** (generate on first modal open, cache per ticker+signal-date) |
| Output shape | **Structured bullets**: headline + strengths[] + risks[] + key_numbers[] |
| Access tier | **Pro + Max only** (reuse existing paywall/redaction in `backend/entitlements.py`) |
| Model | **`claude-sonnet-4-6`** (high-volume summarization; cheap, fast, sufficient) |
| Output contract | **Structured outputs** (`output_config.format` + JSON schema) — validated server-side |

## Architecture & Data Flow

```
SignalModal (Pro/Max user opens a signal)
  → GET /api/signals/{ticker}/explanation?date=YYYY-MM-DD
  → backend/main.py endpoint:
      1. tier guard (reuse entitlements: Pro/Max only; Free → 403/locked teaser)
      2. cache lookup in signal_explanations (ticker, signal_date)
           hit  → return cached JSON (common path; shared across all users)
           miss → assemble evidence packet → call Claude → store → return
  → data/explanations.py (new module, mirrors data/fundamentals.py):
      assemble_evidence(ticker, signal_date) → compact dict from EXISTING data:
        - signal: action, ml_score, ★ strong-buy marker (derived from vqs===9,
          number never shown), close
        - health score + dots
        - fundamentals: pe_ttm, margins, roe, de_ratio (data/fundamentals.py)
        - analyst: consensus target upside (LIVE, forward-filled; may be null)
        - news: 1–3 recent FMP headlines (already fetched for the News tab)
  → anthropic SDK: claude-sonnet-4-6, structured outputs
      returns {headline, strengths[], risks[], key_numbers[]}
  → store row in signal_explanations
```

**Cache key: `ticker + signal_date`.** One row per signal per day, shared
across all users — the second and every later viewer pays nothing. This matches
the existing `(ticker, signal_date)` convention already used by the
`signal_unlocks` table in `backend/entitlements.py`.

## Components

### `data/explanations.py` (new)

Mirrors `data/fundamentals.py` conventions (pure functions + thin I/O,
auditable field mapping, design-notes docstring).

- `assemble_evidence(ticker, signal_date) -> dict` — **pure-ish** read of
  existing tables into a compact evidence packet. Null fields stay null (e.g.
  V2 VQS=9 BUYs with no analyst data — by design, not a bug). Unit-testable
  with a seeded DB.
- `generate_explanation(evidence: dict) -> dict` — builds the system+user
  prompt, calls Claude with structured outputs, returns validated dict. No DB
  I/O (so it tests with a mocked client).
- `get_or_create(ticker, signal_date) -> dict` — cache-aside: lookup →
  generate → store → return. The only function the endpoint calls.
- `_ensure_table()` — `CREATE TABLE IF NOT EXISTS signal_explanations`,
  following the `_ensure_tables()` idiom in `entitlements.py`.

### `signal_explanations` table (new)

```sql
CREATE TABLE IF NOT EXISTS signal_explanations (
    ticker      TEXT NOT NULL,
    signal_date TEXT NOT NULL,
    payload     TEXT NOT NULL,          -- JSON: {headline, strengths, risks, key_numbers}
    model       TEXT NOT NULL,          -- e.g. claude-sonnet-4-6 (provenance)
    created_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(ticker, signal_date)
);
```

Immutable once written (point-in-time-truth: the evidence was true as of the
signal date; no regeneration logic).

### Endpoint: `GET /api/signals/{ticker}/explanation?date=` (in `backend/main.py`)

- Reuses the existing auth + entitlements guard pattern from the tiered Signals
  work. **Pro/Max only**; Free users get a 403 (frontend shows the same
  locked/blurred teaser used elsewhere on Signals).
- Validates `date` is a real signal date for the ticker (404 otherwise).
- Returns `{headline, strengths[], risks[], key_numbers[], model, cached: bool}`.

### Frontend: `SignalModal.jsx`

- New "Why" card rendered from the structured fields (no markdown parsing).
- Skeleton loader on first open (~2–4s for a cache miss); instant on cache hit.
- Pro/Max gating reuses the existing paywall component; Free sees the teaser.
- One disclaimer line under the card: *"AI summary of model data, not new
  analysis."*

## The Model Call & Anti-Hallucination Contract

- **Model:** `claude-sonnet-4-6`. Adaptive thinking is unnecessary for
  summarization; keep the call simple. ~800 input + ~250 output tokens ≈
  **$0.006 per uncached explanation**, paid once per ticker/day.
- **Structured outputs** via `output_config.format` with this JSON schema
  (`additionalProperties: false` on every object; arrays bounded):
  - `headline`: string (one-line takeaway)
  - `strengths`: array of string, max 3
  - `risks`: array of string, max 2
  - `key_numbers`: array of `{label: string, value: string}`, max 4
- **System-prompt hard rules:**
  1. Only restate/interpret numbers present in the evidence packet.
  2. Never invent data, never predict price, never contradict the signal's
     action.
  3. If a field is null, omit it — do not guess (critical for analyst-less V2
     BUYs).
  4. Never surface VQS as a number (internal-only; the ★ marker is fine).
- **Parsing:** always consume validated SDK output; never raw-string-match the
  model's JSON.

## Error Handling

- Anthropic API error / timeout → endpoint returns 503 with a friendly
  message; frontend shows "Explanation unavailable, try again" (no cache write
  on failure).
- Missing/partial evidence → still generate; the prompt handles nulls by
  omission. An evidence packet with too little signal (e.g. only an ML score)
  still yields a minimal valid card.
- `ANTHROPIC_API_KEY` absent → endpoint 503 + logged warning (feature degrades
  cleanly; rest of app unaffected).

## Testing

- `assemble_evidence` — unit tests over a seeded DB (full data; analyst-null
  V2 case; missing fundamentals).
- `generate_explanation` — mocked Anthropic client; assert schema validity and
  that null evidence fields are absent from the prompt.
- `get_or_create` — cache miss writes a row + returns; second call is a pure
  cache hit (mock client asserted **not** called).
- Endpoint — Free → 403; Pro/Max → 200; bad date → 404; API failure → 503 with
  no cache write.

## Scope Boundaries (NOT building)

- No streaming (structured JSON renders atomically; skeleton loader covers the
  first open).
- No precompute cron (on-demand chosen).
- No new tier infrastructure (reuse `entitlements.py`).
- No regeneration / cache invalidation (explanations are immutable per
  signal-day).
- No SELL/HOLD explanations in v1 — BUY signals only (the high-value case).

## Deploy Notes

- New table is created lazily via `_ensure_table()` (no migration step needed,
  consistent with `entitlements.py`).
- Prod must have `ANTHROPIC_API_KEY` set in its `.env` (already present
  locally; verify on server before deploy).
- Stays on `feat/ui-redesign`; merge to `main` only at deploy time.
