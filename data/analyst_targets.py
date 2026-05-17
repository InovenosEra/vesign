"""Orchestrator: fetch analyst targets for a list of tickers using yfinance
primary + FMP fallback. Returns rows with explicit `source` provenance.

Single entry point for the live pipeline. Never raises — per-ticker failures
isolate so one bad ticker doesn't break the run.
"""
from __future__ import annotations
import logging
from data import yfinance_analyst, fmp

log = logging.getLogger(__name__)


def _from_fmp_consensus(c: dict | None) -> dict | None:
    """Map FMP price_target_consensus shape to our schema."""
    if not c:
        return None
    mean = c.get("targetConsensus")
    if not mean or mean <= 0:
        return None
    return {
        "target_mean_price":  float(mean),
        "target_high_price":  float(c["targetHigh"]) if c.get("targetHigh") else None,
        "target_low_price":   float(c["targetLow"])  if c.get("targetLow")  else None,
        "number_of_analysts": int(c["numberOfAnalysts"]) if c.get("numberOfAnalysts") else None,
    }


def fetch_with_fallback(tickers: list[str]) -> dict[str, dict]:
    """For each ticker, return {<our schema fields>, source: 'yfinance'|'fmp'|'none'}.

    Strategy:
      1. Batch-fetch via yfinance.get_targets_batch
      2. For any ticker where yfinance returned None, retry per-ticker via
         FMP's price_target_consensus
      3. Tickers neither source covered get a 'none' row with all fields NULL

    Source column always reflects actual provenance — never lies.
    """
    yf_out = yfinance_analyst.get_targets_batch(tickers)

    results: dict[str, dict] = {}
    for t in tickers:
        yf_row = yf_out.get(t)
        if yf_row is not None:
            results[t] = {**yf_row, "source": "yfinance"}
            continue

        # yfinance empty — fall back to FMP
        try:
            fmp_consensus = fmp.price_target_consensus(t)
        except Exception as e:
            log.debug("FMP fallback raised for %s: %s", t, e)
            fmp_consensus = None

        fmp_row = _from_fmp_consensus(fmp_consensus)
        if fmp_row is not None:
            results[t] = {**fmp_row, "source": "fmp"}
        else:
            results[t] = {
                "target_mean_price": None,
                "target_high_price": None,
                "target_low_price": None,
                "number_of_analysts": None,
                "source": "none",
            }
    return results
