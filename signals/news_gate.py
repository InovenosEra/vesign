"""News-veto gate for fresh BUY signals.

Runs ONLY on rows already labelled BUY by `signals.engine.run_scoring`. For each
BUY, fetches the last N days of news from FMP and asks Claude Haiku whether the
news indicates *catastrophic continuation risk* (FDA recall of a primary product,
fraud/SEC enforcement, bankruptcy/going-concern, restatement, major lawsuit
affecting solvency, CEO/CFO sudden resignation under cloud, acquisition pricing
in the upside). If yes → flip BUY to HOLD and persist the reason in
`signals.news_block_reason`.

Cost is tiny because the gate runs only on the few BUYs the prior 7 gates already
let through — typically 0–10 tickers/day. At Haiku 4.5 prices that's well under
$0.05/day.

Defaults to PASS (don't block) when:
  - no recent news at all (insufficient signal),
  - the FMP fetch errors,
  - or Claude returns malformed JSON.

That intentional default-allow keeps the gate from silently suppressing BUYs on
infrastructure problems. The pipeline already has 7 strict gates; this one is
purely a defensive veto layer.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from data import fmp
from data.loaders import engine

DEFAULT_LOOKBACK_DAYS = 14
HAIKU_MODEL = "claude-haiku-4-5"

_SYSTEM_PROMPT = (
    "You evaluate whether to BLOCK an oversold-mean-reversion BUY based on recent news. "
    "BLOCK only if catastrophic continuation risk is present: confirmed fraud, "
    "bankruptcy / Chapter 11 / going-concern doubt, SEC enforcement action, "
    "FDA recall of a primary product, major lawsuit affecting solvency, "
    "restatement of financials, CEO/CFO sudden resignation under cloud, "
    "acquisition that prices in the upside (no further mean-reversion possible).\n"
    "Do NOT block on: routine analyst downgrades, missed earnings, ordinary business "
    "volatility, macro fears, sector rotation, competitor news, or vague concerns.\n"
    "Respond with ONLY valid JSON: "
    '{"block": true|false, "reason": "<one short sentence citing the specific headline>"}'
)


def _parse_news_date(s):
    try:
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _strip_code_fence(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return raw.strip()


def check_news_gate(ticker: str, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> tuple[bool, str | None]:
    """Return (block, reason). On any failure, returns (False, None) — default-allow."""
    try:
        news = fmp.stock_news(ticker, limit=10) or []
    except Exception:
        return False, None

    cutoff = datetime.now() - timedelta(days=lookback_days)
    recent = []
    for n in news:
        d = _parse_news_date(n.get("date", ""))
        if d and d >= cutoff:
            recent.append((d, n))
    if not recent:
        return False, None

    recent.sort(key=lambda dn: dn[0], reverse=True)
    recent = recent[:5]

    items = "\n".join(
        f"- [{n.get('date', '?')[:10]}] {n.get('title', '')}\n  "
        f"{(n.get('summary') or '').strip()[:300]}"
        for _, n in recent
    )
    prompt = (
        f"Ticker: {ticker}\n"
        f"Recent headlines (last {lookback_days} days):\n{items}\n\n"
        "Should we BLOCK this BUY?"
    )

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        msg = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=200,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        result = json.loads(_strip_code_fence(msg.content[0].text))
        block = bool(result.get("block", False))
        reason = (result.get("reason") or "")[:500] if block else None
        return block, reason
    except Exception:
        return False, None


def apply_news_gate(target_date: str | None = None, *, verbose: bool = True) -> dict:
    """Run the news gate on every BUY row for `target_date` whose news_block_reason
    is still NULL. Flips blocked rows to HOLD and persists the reason.

    `target_date` is 'YYYY-MM-DD'. If omitted, uses MAX(DATE(date)) from signals.

    Returns {checked, blocked, errors}.
    """
    with engine.begin() as conn:
        cols = [r[1] for r in conn.execute(text("PRAGMA table_info(signals)")).fetchall()]
        if "news_block_reason" not in cols:
            conn.execute(text("ALTER TABLE signals ADD COLUMN news_block_reason TEXT"))

    with engine.connect() as conn:
        if not target_date:
            target_date = conn.execute(
                text("SELECT MAX(DATE(date)) FROM signals")
            ).scalar()

        rows = conn.execute(
            text(
                "SELECT ticker FROM signals "
                "WHERE DATE(date) = :d AND signal = 'BUY' "
                "AND news_block_reason IS NULL "
                "AND tier <= 2 "
                "ORDER BY ticker"
            ),
            {"d": target_date},
        ).fetchall()
    tickers = [r[0] for r in rows]

    if not tickers:
        if verbose:
            print(f"News gate: 0 BUYs to check on {target_date}.")
        return {"checked": 0, "blocked": 0, "errors": 0}

    if verbose:
        print(f"News gate: checking {len(tickers)} BUY(s) on {target_date}...")

    blocked = 0
    errors = 0
    for t in tickers:
        try:
            block, reason = check_news_gate(t)
        except Exception as e:
            errors += 1
            if verbose:
                print(f"  {t}: ERROR {e}")
            continue
        if block:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE signals SET signal = 'HOLD', news_block_reason = :r "
                        "WHERE ticker = :t AND DATE(date) = :d"
                    ),
                    {"r": reason, "t": t, "d": target_date},
                )
            blocked += 1
            if verbose:
                print(f"  {t}: BLOCK — {reason}")
        elif verbose:
            print(f"  {t}: pass")

    if verbose:
        print(f"News gate: blocked {blocked}/{len(tickers)} BUY(s).")
    return {"checked": len(tickers), "blocked": blocked, "errors": errors}


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    apply_news_gate(target)
