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

    close, pred, vqs, action, health, target_mean, company = s
    fundamentals = {}
    if f:
        for k, v in zip(_FUND_FIELDS, f):
            if v is not None:
                fundamentals[k] = v
    upside = round((target_mean - close) / close * 100, 1) if (target_mean and close) else None

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


_SCHEMA = {
    "type": "object",
    "properties": {
        "headline": {"type": "string"},
        "strengths": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
        "key_numbers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "label": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["label", "value"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["headline", "strengths", "risks", "key_numbers"],
    "additionalProperties": False,
}

_SYSTEM = (
    "You explain why Vesign's model flagged a stock, for a retail investor. "
    "You are given a JSON evidence packet of data Vesign already computed. "
    "Rules you must never break:\n"
    "1. Use ONLY numbers present in the packet. Never invent data or figures.\n"
    "2. Never predict a future price and never contradict the 'action'.\n"
    "3. If a field is absent from the packet, do not mention it or guess it.\n"
    "4. Never output an internal score named VQS or any 0-9/0-10 quality number; "
    "'strong_buy: true' may be phrased as a strong signal, without a number.\n"
    "Write a one-line headline, up to 3 strengths, up to 2 risks, and up to 4 "
    "key_numbers (label + short value) drawn from the packet. Be concise and factual."
)


def generate_explanation(evidence: dict, *, client=None) -> dict:
    """Call Claude with structured outputs; return the validated, trimmed dict."""
    if client is None:
        from anthropic import Anthropic
        client = Anthropic()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=600,
        system=_SYSTEM,
        messages=[{"role": "user", "content": json.dumps(evidence)}],
        output_config={"format": {"type": "json_schema", "schema": _SCHEMA}},
    )
    body = next((b.text for b in resp.content if getattr(b, "type", None) == "text"), None)
    if body is None:
        # e.g. a safety refusal returns no text block — surface it clearly so the
        # endpoint maps it to a 503 with a useful log line rather than a bare error.
        raise ValueError("Claude returned no text block")
    data = json.loads(body)
    # Structured outputs can't enforce array maxItems — trim server-side.
    data["strengths"] = list(data.get("strengths", []))[:3]
    data["risks"] = list(data.get("risks", []))[:2]
    data["key_numbers"] = list(data.get("key_numbers", []))[:4]
    return data


def get_or_create(ticker: str, signal_date: str, *, client=None) -> dict | None:
    """Cache-aside: return cached payload, else assemble->generate->store. None if
    no signal exists for (ticker, signal_date)."""
    _ensure_table()
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT payload FROM signal_explanations
            WHERE ticker = :t AND signal_date = :d
        """), {"t": ticker, "d": signal_date}).fetchone()
    if row:
        return json.loads(row[0])

    evidence = assemble_evidence(ticker, signal_date)
    if evidence is None:
        return None
    payload = generate_explanation(evidence, client=client)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT OR IGNORE INTO signal_explanations (ticker, signal_date, payload, model)
            VALUES (:t, :d, :p, :m)
        """), {"t": ticker, "d": signal_date, "p": json.dumps(payload), "m": MODEL})
    return payload
