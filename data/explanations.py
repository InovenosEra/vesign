"""BUY-signal explainer — assemble trusted evidence, call Claude, cache result.

This is Vesign's first LLM feature. The model only RESTATES and interprets the
numbers we already trust (signal, health, fundamentals, analyst target, news);
it never invents data, predicts price, or contradicts the engine's action.

Design notes (mirrors data/fundamentals.py):
- `assemble_evidence` reads existing tables into a compact dict; null fields are
  omitted so the prompt can't be tempted to fill them in (critical for V2
  VQS=9 BUYs that legitimately have no analyst data).
- `generate_explanation` does the model I/O only (no DB), so it tests with a
  mocked client. Structured outputs guarantee a valid JSON shape.
- `get_or_create` is cache-aside on (ticker, signal_date); rows are immutable
  (point-in-time-truth: the evidence was true as of the signal date).
- Table is created lazily via `_ensure_table()` — no migration step, matching
  backend/entitlements.py.
"""
from __future__ import annotations

import json

from sqlalchemy import text

from data import fmp
from data.loaders import engine

MODEL = "claude-sonnet-4-6"


def _ensure_table() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS signal_explanations (
                ticker      TEXT NOT NULL,
                signal_date TEXT NOT NULL,
                payload     TEXT NOT NULL,
                model       TEXT NOT NULL,
                created_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(ticker, signal_date)
            )
        """))
