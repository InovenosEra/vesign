"""yfinance adapter for analyst price targets.

Pulls Refinitiv I/B/E/S consensus via yfinance — the same data Bloomberg/
Reuters terminals show. Active under ANALYST_SOURCE=yfinance; FMP is the
fallback per-ticker when this returns None.
"""
from __future__ import annotations
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


_PER_REQUEST_SLEEP_SEC = 1.0  # per-worker rate limit


def get_targets_batch(tickers: list[str], max_workers: int = 4,
                      sleep_sec: float = _PER_REQUEST_SLEEP_SEC) -> dict[str, dict | None]:
    """Parallel-fetch analyst targets for many tickers.

    Yahoo blocks above ~5 req/sec aggregate. With 4 workers each sleeping
    1 sec between calls, effective rate is ~4 req/sec — comfortably under
    the threshold. Per-ticker failures return None rather than raising so
    the orchestrator can fall back per ticker.
    """
    results: dict[str, dict | None] = {}

    def _one(t):
        try:
            res = get_targets(t)
        except Exception as e:
            log.debug("get_targets raised unexpectedly for %s: %s", t, e)
            res = None
        time.sleep(sleep_sec)
        return t, res

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed(ex.submit(_one, t) for t in tickers):
            ticker, res = fut.result()
            results[ticker] = res

    return results
