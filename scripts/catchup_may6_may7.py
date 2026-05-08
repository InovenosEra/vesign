"""Catch-up for May 6 and May 7: prices, VIX, features, predictions, signals.

Used after the engine-patch rebuild to bring the leading edge of the DB up to
yesterday's (May 7) US close. Caller must stop vesign first.
"""
import time
import pandas as pd
from sqlalchemy import text
from datetime import datetime

from data.loaders import engine
from data.fmp import historical_prices

START = "2026-05-06"
END = "2026-05-07"


def log(msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


def fetch_prices_for_dates(start: str, end: str) -> None:
    log(f"Fetching prices for {start}..{end}")
    with engine.connect() as c:
        tickers = [r[0] for r in c.execute(text(
            "SELECT DISTINCT ticker FROM companies WHERE ticker NOT LIKE '%.TA' ORDER BY ticker"
        ))]
    log(f"  {len(tickers)} US tickers")

    ok = missing = errors = 0
    for i, t in enumerate(tickers, 1):
        try:
            df = historical_prices(t, start, end)
        except Exception as e:
            log(f"  [{i}/{len(tickers)}] {t}: ERROR {e}")
            errors += 1
            time.sleep(1)
            continue
        if df is None or df.empty:
            missing += 1
            continue
        try:
            df.to_sql("daily_prices", engine, if_exists="append", index=False)
            ok += 1
        except Exception as e:
            log(f"  [{i}/{len(tickers)}] {t}: WRITE-ERROR {e}")
            errors += 1
        if i % 200 == 0:
            log(f"  [{i}/{len(tickers)}] ok={ok} missing={missing} errors={errors}")
    log(f"DONE prices: ok={ok} missing={missing} errors={errors}")


def main():
    log("=" * 60)
    log("CATCH-UP STARTING (May 6 + May 7)")
    log("=" * 60)

    # 1. Prices
    fetch_prices_for_dates(START, END)

    # 2. VIX (yfinance, fetches latest available incrementally)
    log("Updating VIX...")
    from data.market_data import update_vix
    update_vix()

    # 3. Features (recomputes the latest 280 days, picks up new dates)
    log("Computing features...")
    from features.technical import compute_and_save_features_chunked
    compute_and_save_features_chunked(engine, days=280, chunk_size=100)

    # 4. Forward returns (full rebuild)
    log("Computing forward_returns...")
    from features.forward_returns import compute_forward_returns
    compute_forward_returns()

    # 5. ML walk-forward retrain check (idempotent — Q2 2026 model already exists)
    log("Walk-forward retrain check...")
    from models.walk_forward import maybe_retrain_for_today
    maybe_retrain_for_today()

    # 6. Predictions (will find May 6 + May 7 as pending)
    log("Running prediction engine...")
    from models.predict import run_prediction_engine
    run_prediction_engine()

    # 7. Scoring — May 6 then May 7
    from signals.engine import run_scoring
    log("Scoring 2026-05-06...")
    run_scoring(target_date="2026-05-06")
    log("Scoring 2026-05-07...")
    run_scoring(target_date="2026-05-07")

    # 8. Trade_log
    log("Rebuilding trade_log...")
    from backtesting.engine import build_trade_log
    build_trade_log()

    log("=" * 60)
    log("CATCH-UP COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()
