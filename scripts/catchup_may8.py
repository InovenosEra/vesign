"""Catch-up for May 8 only — bypasses universe loader (FMP IJH/IJR is down).
Fetches prices for current `companies` table, then runs the rest of the pipeline.
"""
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import text

from data.loaders import engine
from data.fmp import historical_prices

START = "2026-05-08"
END = "2026-05-08"


def log(msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


def fetch_prices():
    log(f"Fetching prices for {START}..{END}")
    with engine.connect() as c:
        tickers = [r[0] for r in c.execute(text(
            "SELECT DISTINCT ticker FROM companies WHERE ticker NOT LIKE '%.TA' ORDER BY ticker"
        ))]
    log(f"  {len(tickers)} US tickers")

    ok = missing = errors = 0
    for i, t in enumerate(tickers, 1):
        try:
            df = historical_prices(t, START, END)
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
    log(f"CATCH-UP STARTING ({START})")
    log("=" * 60)

    fetch_prices()

    log("Updating VIX...")
    from data.market_data import update_vix
    update_vix()

    log("Computing features...")
    from features.technical import compute_and_save_features_chunked
    compute_and_save_features_chunked(engine, days=280, chunk_size=100)

    log("Computing forward_returns...")
    from features.forward_returns import compute_forward_returns
    compute_forward_returns()

    log("Walk-forward retrain check...")
    from models.walk_forward import maybe_retrain_for_today
    maybe_retrain_for_today()

    log("Running prediction engine...")
    from models.predict import run_prediction_engine
    run_prediction_engine()

    from signals.engine import run_scoring
    log(f"Scoring {START}...")
    run_scoring(target_date=START)

    log("Rebuilding trade_log...")
    from backtesting.engine import build_trade_log
    build_trade_log()

    log("=" * 60)
    log("CATCH-UP COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()
