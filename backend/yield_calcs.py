"""Pure $1000-per-BUY-signal yield math used by every aggregate Vesign-yield
surface. No I/O, no DB. Imported by backend/main.py."""
from __future__ import annotations

from datetime import date
from typing import Callable, Iterable, NamedTuple, Optional

INVEST = 1000.0  # dollars per BUY signal


def avg_cost_dollar_weighted(lot_prices: Iterable[float]) -> float:
    """Harmonic mean of lot prices — the effective average cost per share when
    each lot receives an equal $INVEST. Raises ValueError if empty or any
    price <= 0."""
    prices = [float(p) for p in lot_prices]
    if not prices:
        raise ValueError("at least one lot price required")
    if any(p <= 0 for p in prices):
        raise ValueError("all lot prices must be positive")
    return len(prices) / sum(1.0 / p for p in prices)
