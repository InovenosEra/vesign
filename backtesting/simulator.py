"""
Backtesting simulator using real historical signals.

The baseline (no config) exactly replicates build_trade_log() logic:
  - Enter on FIRST BUY of each streak (open_trade = None pattern)
  - Exit on the original SELL signal from the signals table
  - Return = (sell_close - first_buy_close) / first_buy_close

Baseline must match production trade_log: ~1,277 trades, ~81.2% win rate.

Variants can add:
  Entry filters    → min_analysts, health_score_min
                     (skips a BUY streak if the condition isn't met on entry day)
  Early exit       → profit_target_pct, max_hold_days
                     (exits BEFORE the original SELL signal using daily_prices)
  Signal override  → no_analyst_upside
                     (re-labels rows passing 6 non-analyst criteria as BUY)

Usage:
    from backtesting.simulator import simulate, compare

    print(simulate())                            # baseline — must match trade_log
    print(simulate({"min_analysts": 3}))         # entry filter variant
    print(simulate({"no_analyst_upside": True})) # 6-criteria scenario
"""

import pandas as pd
from data.loaders import engine

_cache = {}


def _load_data(refresh=False):
    global _cache
    if _cache and not refresh:
        return _cache

    print("Loading simulation data...")

    signals = pd.read_sql(
        "SELECT date, ticker, signal, close, "
        "rsi_3day_flag, bb_condition, volume_flag, volume_ratio, week52_condition, "
        "health_condition, ml_condition, analyst_condition "
        "FROM signals ORDER BY ticker, date",
        engine,
    )
    prices = pd.read_sql(
        "SELECT date, ticker, close FROM daily_prices ORDER BY ticker, date",
        engine,
    )
    analyst = pd.read_sql(
        "SELECT ticker, target_mean_price, number_of_analysts FROM analyst_expectations",
        engine,
    )
    health = pd.read_sql(
        "SELECT ticker, score AS health_score FROM company_health",
        engine,
    )

    signals["date"] = pd.to_datetime(signals["date"])
    prices["date"]  = pd.to_datetime(prices["date"])

    _cache = {
        "signals": signals,
        "prices":  prices,
        "analyst": analyst,
        "health":  health,
    }

    print(
        f"Signals: {len(signals):,} rows, {signals['ticker'].nunique():,} tickers | "
        f"Prices: {len(prices):,} rows"
    )
    return _cache


DEFAULTS = {
    # Entry filters (default off — baseline uses no filters)
    "min_analysts":      None,   # e.g. 3
    "health_score_min":  None,   # e.g. 5
    # Early-exit overrides (default off — baseline uses original SELL signals)
    "profit_target_pct": None,   # e.g. 0.20 → exit when up 20% before SELL
    "max_hold_days":     None,   # e.g. 45  → exit after 45 days before SELL
    # Signal override
    "no_analyst_upside": False,  # True → use 6 criteria only (ignore analyst upside)
    "volume_ratio_min":  None,   # e.g. 1.2 → override volume threshold (3-day rolling max)
}


def simulate(config=None, us_only=True):
    """
    Run simulation. Returns a metrics dict.

    Baseline (config=None) must match production trade_log exactly.
    """
    cfg = {**DEFAULTS, **(config or {})}

    data = _load_data()

    signals = data["signals"].copy()
    if us_only:
        signals = signals[~signals["ticker"].str.endswith(".TA")]

    # Signal override: re-label rows based on custom criteria as BUY
    if cfg["no_analyst_upside"] or cfg["volume_ratio_min"] is not None:
        signals = signals.copy()

        # Volume condition: use custom threshold (3-day rolling max) or original flag
        if cfg["volume_ratio_min"] is not None:
            vol_3d_max = signals.groupby("ticker")["volume_ratio"].transform(
                lambda x: x.rolling(3, min_periods=1).max()
            )
            volume_cond = vol_3d_max >= cfg["volume_ratio_min"]
        else:
            volume_cond = signals["volume_flag"] == 1

        # Analyst condition: use original flag or ignore it
        if cfg["no_analyst_upside"]:
            analyst_cond = True  # always passes
        else:
            analyst_cond = signals["analyst_condition"] == 1

        buy_crit = (
            (signals["rsi_3day_flag"]    == 1) &
            (signals["bb_condition"]     == 1) &
            volume_cond &
            (signals["week52_condition"] == 1) &
            (signals["health_condition"] == 1) &
            (signals["ml_condition"]     == 1) &
            analyst_cond
        )
        signals.loc[buy_crit, "signal"] = "BUY"

    # Lookup maps for entry filters
    analyst_map = data["analyst"].set_index("ticker").to_dict("index")
    health_map  = data["health"].set_index("ticker")["health_score"].to_dict()

    # Per-ticker daily price series (only built if early-exit rules are active)
    need_prices = cfg["profit_target_pct"] is not None or cfg["max_hold_days"] is not None
    prices_map  = {}
    if need_prices:
        prices = data["prices"].copy()
        if us_only:
            prices = prices[~prices["ticker"].str.endswith(".TA")]
        for ticker, grp in prices.groupby("ticker"):
            prices_map[ticker] = grp.set_index("date")["close"]

    trades = []

    for ticker, grp in signals.groupby("ticker"):
        grp = grp.reset_index(drop=True)
        open_trade = None  # {"date": ..., "price": ...} or None

        for _, row in grp.iterrows():
            sig = row["signal"]

            if sig == "BUY" and open_trade is None:
                # Apply entry filters
                if cfg["min_analysts"] is not None:
                    n = (analyst_map.get(ticker) or {}).get("number_of_analysts") or 0
                    if n < cfg["min_analysts"]:
                        continue

                if cfg["health_score_min"] is not None:
                    score = health_map.get(ticker) or 0
                    if score < cfg["health_score_min"]:
                        continue

                open_trade = {"date": row["date"], "price": row["close"]}

            elif sig == "SELL" and open_trade is not None:
                exit_date   = row["date"]
                exit_price  = row["close"]
                exit_reason = "signal_sell"

                # Check for early exit via daily prices
                if need_prices and ticker in prices_map:
                    px = prices_map[ticker]
                    mask = (px.index > open_trade["date"]) & (px.index <= exit_date)
                    for d, p in px[mask].items():
                        days = (d - open_trade["date"]).days
                        ret  = (p - open_trade["price"]) / open_trade["price"]

                        if cfg["profit_target_pct"] is not None and ret >= cfg["profit_target_pct"]:
                            exit_date, exit_price, exit_reason = d, p, "profit_target"
                            break

                        if cfg["max_hold_days"] is not None and days >= cfg["max_hold_days"]:
                            exit_date, exit_price, exit_reason = d, p, "time_stop"
                            break

                ret_pct   = (exit_price - open_trade["price"]) / open_trade["price"]
                days_held = (exit_date  - open_trade["date"]).days

                trades.append({
                    "ticker":      ticker,
                    "entry_date":  open_trade["date"],
                    "exit_date":   exit_date,
                    "entry_price": open_trade["price"],
                    "exit_price":  exit_price,
                    "return_pct":  ret_pct,
                    "days_held":   days_held,
                    "exit_reason": exit_reason,
                })
                open_trade = None

    if not trades:
        return {"n_trades": 0, "win_rate": 0}

    tdf = pd.DataFrame(trades)
    r   = tdf["return_pct"]

    return {
        "n_trades":      len(tdf),
        "win_rate":      round((r > 0).mean() * 100, 1),
        "avg_return":    round(r.mean() * 100, 2),
        "median_return": round(r.median() * 100, 2),
        "avg_days_held": round(tdf["days_held"].mean(), 1),
        "total_return":  round(r.sum() * 100, 1),
        "sharpe":        round(r.mean() / r.std(), 3) if r.std() > 0 else 0,
        "best_trade":    round(r.max() * 100, 1),
        "worst_trade":   round(r.min() * 100, 1),
        "exit_reasons":  tdf["exit_reason"].value_counts().to_dict(),
    }


def compare(variants, us_only=True):
    """Run multiple variants and return a comparison DataFrame."""
    _load_data()  # warm cache once
    results = []
    for name, config in variants.items():
        print(f"  Simulating: {name}...")
        m = simulate(config, us_only=us_only)
        results.append({"strategy": name, **{k: v for k, v in m.items() if k != "exit_reasons"}})
    return pd.DataFrame(results).set_index("strategy")
