#!/usr/bin/env python3
"""Daily precompute of BUY/SELL signal explanations.

Generates and caches an AI explanation for every BUY/SELL signal on the latest
signal date so the Signals page loads instantly (no live model calls per card).
Idempotent — re-running only fills gaps. Safe to run standalone or chained after
the daily pipeline (`run_daily.py` already calls `precompute_today()` at its tail).

Usage:
    venv/bin/python -m production.precompute_explanations
"""
import logging

from data import explanations


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    res = explanations.precompute_today()
    logging.getLogger(__name__).info("done: %s", res)


if __name__ == "__main__":
    main()
