"""Hybrid backfill: keep V1's BUY/SELL signals as-is, ADD V2 VQS=9 BUYs.

Steps:
  1. Compute VQS for every historical (ticker, date) row using already-stored
     indicators + a one-time load of vix + market_cap_history + a per-ticker
     mom/sma/vol/atr computation.  Bulk UPDATE signals.vqs.
  2. Walk forward per ticker; whenever VQS=9 and V1 says HOLD and the ticker
     isn't already in an open V1 position, set signal='BUY' (the V2 strong-buy
     addition). Persist via bulk UPDATE.
  3. Rebuild trade_log via the V1 build_trade_log().

Much faster than running run_scoring date-by-date — typically <10min total
on a ~6-year history.
"""
from __future__ import annotations

import time
import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
from sqlalchemy import text

from data.loaders import engine
from signals.engine import _ensure_signals_columns
from backtesting.engine import build_trade_log


def main() -> None:
    _ensure_signals_columns()
    t0 = time.time()
    print("Hybrid backfill starting...")

    # --- 1. Load everything we need ---
    print("Loading prices, signals, predictions, vix, market_cap_history ...")
    db = Path(__file__).resolve().parents[1] / "vesign.db"
    con = sqlite3.connect(db)
    prices = pd.read_sql_query(
        "SELECT date, ticker, close, high, low FROM daily_prices "
        "WHERE date >= '2020-01-01' AND ticker NOT LIKE '%.TA' "
        "ORDER BY ticker, date",
        con, parse_dates=["date"]
    )
    sig = pd.read_sql_query(
        "SELECT date, ticker, signal, rsi FROM signals WHERE ticker NOT LIKE '%.TA'",
        con, parse_dates=["date"]
    )
    pred = pd.read_sql_query(
        "SELECT date, ticker, pred_5d FROM predictions WHERE ticker NOT LIKE '%.TA'",
        con, parse_dates=["date"]
    )
    vix = pd.read_sql_query(
        "SELECT date, close AS vix_close FROM vix",
        con, parse_dates=["date"]
    )
    mch = pd.read_sql_query(
        "SELECT ticker, date AS mc_date, market_cap FROM market_cap_history "
        "WHERE date >= '2019-01-01' AND ticker NOT LIKE '%.TA'",
        con, parse_dates=["mc_date"]
    )
    con.close()
    print(f"  loaded in {time.time()-t0:.1f}s — "
          f"prices {len(prices):,}, signals {len(sig):,}, preds {len(pred):,}, "
          f"vix {len(vix):,}, mch {len(mch):,}")

    # --- 2. Compute V2 indicators per ticker (vectorized) ---
    t = time.time()
    print("Computing V2 indicators (mom, sma, vol, atr) ...")
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = prices.groupby("ticker", sort=False)
    prices["mom_5d"]  = g["close"].transform(lambda c: c / c.shift(5)  - 1)
    prices["mom_60d"] = g["close"].transform(lambda c: c / c.shift(60) - 1)
    prices["sma_50_dist"] = prices["close"] / g["close"].transform(
        lambda c: c.rolling(50, min_periods=20).mean()
    ) - 1
    prices["log_ret_tmp"] = g["close"].transform(lambda c: np.log(c / c.shift(1)))
    prices["realized_vol_20"] = prices.groupby("ticker", sort=False)[
        "log_ret_tmp"
    ].transform(lambda s: s.rolling(20).std()) * (252 ** 0.5)
    prev_close = g["close"].shift(1)
    prices["tr_tmp"] = pd.concat([
        (prices["high"] - prices["low"]).abs(),
        (prices["high"] - prev_close).abs(),
        (prices["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    prices["atr_14_pct"] = prices.groupby("ticker", sort=False)["tr_tmp"].transform(
        lambda s: s.ewm(alpha=1/14, adjust=False).mean()
    ) / prices["close"]
    prices = prices.drop(columns=["log_ret_tmp", "tr_tmp"])
    print(f"  done in {time.time()-t:.1f}s")

    # --- 3. Merge inputs into a single VQS table ---
    t = time.time()
    print("Merging inputs ...")
    df = prices[["date","ticker","close","mom_5d","mom_60d","sma_50_dist",
                 "realized_vol_20","atr_14_pct"]].merge(
        sig[["date","ticker","signal","rsi"]], on=["date","ticker"], how="inner"
    )
    df = df.merge(pred, on=["date","ticker"], how="left")
    df = df.merge(vix, on="date", how="left")
    # As-of merge for market_cap (most recent on or before date, per ticker)
    df = df.sort_values(["date","ticker"]).reset_index(drop=True)
    mch = mch.sort_values(["mc_date","ticker"]).reset_index(drop=True)
    df = pd.merge_asof(df, mch, left_on="date", right_on="mc_date",
                       by="ticker", direction="backward").drop(columns=["mc_date"])
    df["log_market_cap"] = np.log(df["market_cap"].replace(0, np.nan))
    print(f"  merged in {time.time()-t:.1f}s — {len(df):,} rows")

    # --- 4. Compute VQS for every row (vectorized) ---
    t = time.time()
    df["vqs"] = (
        (df["vix_close"] > 22.0).astype(int)
        + (df["vix_close"] > 29.0).astype(int)
        + (df["mom_60d"] < -0.15).astype(int)
        + (df["mom_5d"]  < -0.05).astype(int)
        + (df["rsi"] < 35.0).astype(int)
        + ((df["realized_vol_20"] > 0.50) | (df["atr_14_pct"] > 0.04)).astype(int)
        + (df["log_market_cap"] < 22.0).astype(int)
        + (df["pred_5d"] > 0.005).astype(int)
        + (df["sma_50_dist"] < -0.07).astype(int)
    )
    n_vqs9 = (df["vqs"] == 9).sum()
    print(f"  VQS computed in {time.time()-t:.1f}s — VQS=9 rows: {n_vqs9:,}")

    # --- 5. Walk forward per ticker; emit V2 VQS=9 BUYs that don't conflict
    #        with an already-open V1 position. Track new BUY rows.
    # Skip ETFs (sector='ETF') — they live in `companies` for tracking but
    # should never fire BUY/SELL. Same guard as in signals/engine.py.
    try:
        etf_tickers = set(pd.read_sql(
            "SELECT ticker FROM companies WHERE sector = 'ETF'", engine
        )["ticker"].tolist())
    except Exception:
        etf_tickers = set()
    t = time.time()
    print("Identifying V2 VQS=9 BUY additions (no-overlap) ...")
    df = df.sort_values(["ticker","date"]).reset_index(drop=True)
    new_buys = []  # list of (date, ticker)
    for ticker, g in df.groupby("ticker", sort=False):
        if ticker in etf_tickers:
            continue
        open_trade = False  # is there an open position right now?
        for _, row in g.iterrows():
            sig_v1 = row["signal"]
            if sig_v1 == "BUY":
                open_trade = True   # V1 BUY opens position
                continue
            if sig_v1 == "SELL" and open_trade:
                open_trade = False  # V1 SELL closes
                continue
            # HOLD or unrelated SELL — check if V2 VQS=9 should fire
            if not open_trade and row["vqs"] == 9:
                new_buys.append((row["date"], ticker))
                open_trade = True   # the new V2 BUY opens position
    print(f"  V2 VQS=9 BUYs to add: {len(new_buys):,}  "
          f"(elapsed {time.time()-t:.1f}s)")

    # --- 6. Bulk-update signals: write VQS for every row + flip new BUYs ---
    t = time.time()
    print("Updating signals.vqs in bulk ...")
    df["date_str"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:26]
    update_rows = list(zip(df["vqs"].astype(int).tolist(),
                           df["date_str"].tolist(),
                           df["ticker"].tolist()))
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE signals SET vqs = :vqs WHERE date = :date AND ticker = :t"
        ), [{"vqs": v, "date": d, "t": t_} for v, d, t_ in update_rows])
    print(f"  vqs updated for {len(update_rows):,} rows in {time.time()-t:.1f}s")

    if new_buys:
        t = time.time()
        print("Flipping signal='BUY' for new V2 VQS=9 entries ...")
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE signals SET signal = 'BUY' "
                "WHERE DATE(date) = DATE(:d) AND ticker = :t AND signal = 'HOLD'"
            ), [{"d": d.strftime("%Y-%m-%d"), "t": t_} for d, t_ in new_buys])
        print(f"  flipped {len(new_buys):,} HOLD→BUY in {time.time()-t:.1f}s")

    # --- 7. Rebuild trade_log via V1's logic (unchanged) ---
    t = time.time()
    print("\nRebuilding trade_log...")
    build_trade_log()
    print(f"  trade_log rebuilt in {time.time()-t:.1f}s")

    # --- 8. Quick stats ---
    con = sqlite3.connect(db)
    n_trades = con.execute("SELECT COUNT(*) FROM trade_log").fetchone()[0]
    if n_trades > 0:
        m, w = con.execute(
            "SELECT AVG(return_pct), AVG(CASE WHEN return_pct > 0 THEN 1.0 ELSE 0.0 END) "
            "FROM trade_log WHERE return_pct IS NOT NULL"
        ).fetchone()
        print(f"\ntrade_log: N={n_trades:,}  mean={m*100:+.2f}%  WR={w:.3f}")
    con.close()

    print(f"\nTOTAL hybrid backfill time: {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
