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


_FUND_FIELDS = ("pe_ttm", "gross_margin", "op_margin", "net_margin", "roe", "de_ratio")


def _recent_headlines(ticker: str, limit: int = 3) -> list[str]:
    """Best-effort recent headlines from FMP. Never raises; empty on any failure."""
    try:
        items = fmp.stock_news(ticker, limit=limit) or []
    except Exception:
        return []
    out = []
    for it in items[:limit]:
        title = (it or {}).get("title")
        if title:
            out.append(title)
    return out


def assemble_evidence(ticker: str, signal_date: str) -> dict | None:
    """Compact evidence packet from existing tables. None if no signal that day."""
    with engine.begin() as conn:
        s = conn.execute(text("""
            SELECT s.close, s.prediction_score, s.vqs, s.signal, s.health_score,
                   s.target_mean_price, c.company
            FROM signals s LEFT JOIN companies c ON c.ticker = s.ticker
            WHERE s.ticker = :t AND substr(s.date, 1, 10) = :d
            ORDER BY s.date DESC LIMIT 1
        """), {"t": ticker, "d": signal_date}).fetchone()
        if not s:
            return None
        f = conn.execute(text("""
            SELECT pe_ttm, gross_margin, op_margin, net_margin, roe, de_ratio
            FROM fundamentals WHERE ticker = :t
        """), {"t": ticker}).fetchone()

    close, pred, vqs, action, health, tmp, company = s
    fundamentals = {}
    if f:
        for k, v in zip(_FUND_FIELDS, f):
            if v is not None:
                fundamentals[k] = v
    upside = round((tmp - close) / close * 100, 1) if (tmp and close) else None

    ev = {
        "ticker": ticker,
        "company": company or ticker,
        "action": action,
        "signal_date": signal_date,
        "close": close,
        "ml_score": pred,
        "strong_buy": (vqs == 9),
        "health_score": health,
        "fundamentals": fundamentals,
        "analyst_upside_pct": upside,
        "news": _recent_headlines(ticker),
    }
    # Drop top-level nulls so the prompt never sees a field to fill in.
    return {k: v for k, v in ev.items() if v is not None}
