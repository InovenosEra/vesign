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


def per_trade_yield_dca(sell_price: float, lot_prices: Iterable[float]) -> float:
    """Per-trade yield against dollar-weighted avg_cost. Returns a fraction.
    For n=1 this collapses to return_pct."""
    ac = avg_cost_dollar_weighted(lot_prices)
    return (float(sell_price) - ac) / ac


class Lot(NamedTuple):
    ticker:     str
    buy_date:   date
    sell_date:  Optional[date]   # None when the trade is still open
    lot_price:  float
    sell_price: Optional[float]  # None when still open


class SimResult(NamedTuple):
    peak_bank:      float
    final_hand:     float
    equity_curve:   list[tuple[date, float]]
    max_concurrent: int


def simulate_bank_hand(
    lots: list[Lot],
    price_at: Callable[[str, date], Optional[float]],
    eval_dates: list[date],
) -> SimResult:
    """Replay BUY/SELL events for every lot. $1000 per BUY signal.
    Same-day SELL processed before BUY so closed proceeds are reusable
    immediately. Equity at each eval_date = hand + MTM of open lots
    valued via `price_at(ticker, date)`.

    Lots with sell_date=None are treated as still-open: their BUY event
    is processed, but no SELL event ever fires. They contribute MTM to
    every eval_date >= their buy_date.
    """
    if not lots:
        return SimResult(0.0, 0.0, [(d, 0.0) for d in sorted(eval_dates)], 0)

    # Build event list. Each event: (date, order, kind, lot_index)
    # order: 0 = SELL (process first), 1 = BUY
    events: list[tuple[date, int, str, int]] = []
    for i, lot in enumerate(lots):
        events.append((lot.buy_date, 1, "BUY", i))
        if lot.sell_date is not None:
            events.append((lot.sell_date, 0, "SELL", i))
    events.sort(key=lambda e: (e[0], e[1]))

    sorted_evals = sorted(set(eval_dates))
    bank_drawn = 0.0
    hand       = 0.0
    open_set: set[int] = set()
    max_open   = 0
    eq_curve: list[tuple[date, float]] = []
    ev_idx = 0

    def equity_on(d: date) -> float:
        mtm = 0.0
        for i in open_set:
            lot = lots[i]
            p = price_at(lot.ticker, d)
            if p is not None and lot.lot_price > 0:
                mtm += INVEST * (p / lot.lot_price)
            else:
                mtm += INVEST  # no price → flat
        return hand + mtm

    for ev_date, _order, kind, idx in events:
        # Snapshot equity at any eval_dates strictly before this event date
        while ev_idx < len(sorted_evals) and sorted_evals[ev_idx] < ev_date:
            eq_curve.append((sorted_evals[ev_idx], equity_on(sorted_evals[ev_idx])))
            ev_idx += 1
        lot = lots[idx]
        if kind == "BUY":
            if hand >= INVEST:
                hand -= INVEST
            else:
                bank_drawn += (INVEST - hand)
                hand = 0.0
            open_set.add(idx)
            if len(open_set) > max_open:
                max_open = len(open_set)
        else:  # SELL
            assert lot.sell_price is not None
            lot_yield = (lot.sell_price - lot.lot_price) / lot.lot_price
            hand += INVEST * (1.0 + lot_yield)
            open_set.discard(idx)

    # Remaining eval_dates after last event
    while ev_idx < len(sorted_evals):
        eq_curve.append((sorted_evals[ev_idx], equity_on(sorted_evals[ev_idx])))
        ev_idx += 1

    return SimResult(bank_drawn, hand, eq_curve, max_open)
