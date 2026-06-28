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
import logging

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
    "You explain why Vesign's model flagged a stock with a BUY or SELL signal, "
    "for a retail investor. You are given a JSON evidence packet of data Vesign "
    "already computed; the 'action' field is BUY or SELL.\n"
    "Action-aware framing:\n"
    "- If action is BUY: `strengths` are the bull case (why to buy); `risks` are "
    "the downside/cautions.\n"
    "- If action is SELL: `strengths` are the bearish drivers (why to sell — e.g. "
    "trading above the analyst target, weak/negative momentum, soft fundamentals); "
    "`risks` are counterpoints (what could go right / reasons it might not fall).\n"
    "Rules you must never break:\n"
    "1. Use ONLY numbers present in the packet. Never invent data or figures.\n"
    "2. Never predict a future price and never contradict the 'action'.\n"
    "3. If a field is absent from the packet, do not mention it or guess it.\n"
    "4. Never output an internal score named VQS or any 0-9/0-10 quality number; "
    "'strong_buy: true' may be phrased as a strong signal, without a number.\n"
    "5. The `headline` is a short stand-alone phrase. Do NOT prefix it with the "
    "ticker, company name, or 'Flagged BUY/SELL' — those are shown separately in "
    "the UI. Lead with the SINGLE most distinctive fact for THIS specific stock, "
    "chosen from whichever dimension stands out most in the packet: valuation "
    "(P/E), profitability (gross/operating/net margin or ROE), balance sheet "
    "(debt/equity), momentum or model conviction, a notable recent headline, an "
    "exceptionally strong signal, or analyst upside. Do NOT reflexively open every "
    "BUY with analyst upside, nor every SELL with 'above analyst target' or "
    "'fading momentum' — choose the lead that genuinely fits each stock so that no "
    "two headlines read alike. Vary the opening word too: avoid starting with "
    "'Trades' or 'Trading' unless the price-versus-analyst-target relationship is "
    "the single most important fact for that stock; otherwise open with the "
    "standout metric, fundamental, or driver itself. Examples of the VARIETY "
    "expected (illustrative — do not copy verbatim): 'Expanding margins on a clean "
    "balance sheet', 'Trades at a steep discount to its earnings', 'Strong model "
    "conviction plus 54% analyst upside', 'High ROE backs a strong buy signal', "
    "'Stretched valuation after a sharp run-up', 'Margins have slipped into the "
    "red', 'Priced well above what its fundamentals support', 'Momentum fading "
    "despite a rich multiple'.\n"
    "Write the headline, up to 3 strengths, up to 2 risks, and up to 4 "
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


def latest_signal_date(ticker: str) -> str | None:
    """Most recent signal date (YYYY-MM-DD) for a ticker, or None if it has none."""
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT max(substr(date, 1, 10)) FROM signals WHERE ticker = :t"
        ), {"t": ticker}).fetchone()
    return row[0] if row and row[0] else None


def global_latest_signal_date() -> str | None:
    """The most recent signal date across the whole universe (the active day)."""
    with engine.begin() as conn:
        return conn.execute(text(
            "SELECT max(substr(date, 1, 10)) FROM signals")).scalar()


def get_or_create(ticker: str, signal_date: str | None = None, *, client=None) -> dict | None:
    """Cache-aside: return cached payload, else assemble->generate->store. None if
    no signal exists for (ticker, signal_date). When signal_date is omitted, the
    ticker's latest signal date is used (the redesign modal is ticker-centric)."""
    _ensure_table()
    if signal_date is None:
        signal_date = latest_signal_date(ticker)
        if signal_date is None:
            return None
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
    # Money guard: only ever call the model for an ACTIVE signal — the global
    # latest signal date AND a BUY/SELL action. Historical trades (delisted /
    # acquired tickers whose latest signal is old) and HOLD requests serve the
    # cache if present (returned above) but never trigger a new paid generation.
    if evidence.get("action") not in ("BUY", "SELL") or signal_date != global_latest_signal_date():
        return None
    payload = generate_explanation(evidence, client=client)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT OR IGNORE INTO signal_explanations (ticker, signal_date, payload, model)
            VALUES (:t, :d, :p, :m)
        """), {"t": ticker, "d": signal_date, "p": json.dumps(payload), "m": MODEL})
    return payload


def precompute_today(*, limit: int | None = None, client=None) -> dict:
    """Generate + cache explanations for every BUY/SELL signal on the latest
    signal date, so the Signals page loads instantly. Idempotent: already-cached
    tickers are skipped (no model call). Per-ticker failures are logged and
    skipped — the batch never aborts. Run daily after the pipeline writes signals.
    Returns counts: {date, total, generated, cached, failed}."""
    log = logging.getLogger(__name__)
    _ensure_table()
    with engine.begin() as conn:
        day = conn.execute(text("SELECT max(substr(date,1,10)) FROM signals")).scalar()
        if not day:
            return {"date": None, "total": 0, "generated": 0, "cached": 0, "failed": 0}
        tickers = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT ticker FROM signals
            WHERE substr(date,1,10) = :d AND signal IN ('BUY','SELL')
            ORDER BY ticker
        """), {"d": day})]
        done = {r[0] for r in conn.execute(text(
            "SELECT ticker FROM signal_explanations WHERE signal_date = :d"), {"d": day})}
    if limit is not None:
        tickers = tickers[:limit]
    generated = cached = failed = 0
    for t in tickers:
        if t in done:
            cached += 1
            continue
        try:
            payload = get_or_create(t, day, client=client)
            if payload is None:
                failed += 1
            else:
                generated += 1
        except Exception:
            failed += 1
            log.exception("precompute_today: explanation failed for %s @ %s", t, day)
    result = {"date": day, "total": len(tickers),
              "generated": generated, "cached": cached, "failed": failed}
    log.info("precompute_today: %s", result)
    return result
