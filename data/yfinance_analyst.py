"""yfinance adapter for analyst price targets.

Pulls Refinitiv I/B/E/S consensus via yfinance — the same data Bloomberg/
Reuters terminals show. Active under ANALYST_SOURCE=yfinance; FMP is the
fallback per-ticker when this returns None.
"""
from __future__ import annotations
import logging
import yfinance as yf

log = logging.getLogger(__name__)


def get_targets(ticker: str) -> dict | None:
    """Fetch current analyst targets for a single ticker.

    Returns a dict with keys (target_mean_price, target_high_price,
    target_low_price, number_of_analysts) on success, or None when there is
    no usable data (empty response, missing mean, exception, etc.). Never
    raises.
    """
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as e:
        log.debug("yfinance fetch failed for %s: %s", ticker, e)
        return None

    mean = info.get("targetMeanPrice")
    # Treat 0/None/negative as no data — yfinance occasionally returns 0.0
    # for stubs (ETFs, dual-class shares, etc.)
    if not mean or mean <= 0:
        return None

    return {
        "target_mean_price":  float(mean),
        "target_high_price":  float(info["targetHighPrice"]) if info.get("targetHighPrice") else None,
        "target_low_price":   float(info["targetLowPrice"])  if info.get("targetLowPrice")  else None,
        "number_of_analysts": int(info["numberOfAnalystOpinions"]) if info.get("numberOfAnalystOpinions") else None,
    }
