import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------- Data pipelines ----------
from data.market_data import update_prices, update_vix, update_company_info, summarize_descriptions, update_company_health, _download_and_save

# ---------- Feature engineering ----------
from data.loaders import load_prices, save_features, engine
from features.technical import compute_features

# ---------- Modeling ----------
from models.predict import run_prediction_engine
from signals.engine import run_scoring

from backtesting.engine import build_trade_log

# ---------- Portfolio ----------
from portfolio.ranking import run_ranking
from portfolio.allocator import run_allocator


# ── Self-healing repair passes ──────────────────────────────────────────────

def _repair_price_gaps():
    """Re-download any US tickers missing recent price data (FMP rate-limit gaps)."""
    from datetime import date, timedelta
    import pandas as pd

    today = date.today()
    # Find the most common latest date among US tickers (= the expected date)
    df = pd.read_sql(
        "SELECT ticker, DATE(MAX(date)) AS max_date FROM daily_prices "
        "WHERE ticker NOT LIKE '%.TA' GROUP BY ticker",
        engine,
    )
    if df.empty:
        return
    expected = df["max_date"].mode()[0]
    lagging = df[df["max_date"] < expected]["ticker"].tolist()
    if not lagging:
        print("Price gaps: none found.")
        return
    print(f"Price gaps: {len(lagging)} tickers behind {expected} — re-downloading…")
    _download_and_save(lagging, expected, expected)
    print("Price gap repair done.")


def _repair_market_caps():
    """Re-fetch market_cap from FMP for any ticker where it is NULL."""
    import pandas as pd
    from data import fmp

    df = pd.read_sql(
        "SELECT ticker FROM fundamentals WHERE market_cap IS NULL",
        engine,
    )
    tickers = [t for t in df["ticker"].tolist() if not t.endswith(".TA")]
    if not tickers:
        print("Market cap repair: none needed.")
        return
    print(f"Market cap repair: {len(tickers)} tickers with NULL market_cap…")
    fixed = 0
    with engine.begin() as conn:
        for t in tickers:
            try:
                profile = fmp.company_profile(t) or {}
                mc = profile.get("marketCap")
                if mc:
                    result = conn.execute(
                        __import__("sqlalchemy").text(
                            "UPDATE fundamentals SET market_cap=:mc WHERE ticker=:t"
                        ),
                        {"mc": mc, "t": t},
                    )
                    if result.rowcount == 0:
                        conn.execute(
                            __import__("sqlalchemy").text(
                                "INSERT INTO fundamentals (ticker, market_cap) VALUES (:t, :mc)"
                            ),
                            {"t": t, "mc": mc},
                        )
                    fixed += 1
            except Exception:
                pass
    print(f"Market cap repair done: {fixed}/{len(tickers)} fixed.")


def _repair_analyst_targets():
    """Fallback to yfinance for US tickers with NULL analyst targets."""
    import pandas as pd
    import sqlalchemy as sa
    from datetime import datetime, timezone

    df = pd.read_sql(
        "SELECT ticker FROM analyst_expectations WHERE target_mean_price IS NULL",
        engine,
    )
    tickers = [t for t in df["ticker"].tolist() if not t.endswith(".TA")]
    if not tickers:
        print("Analyst target repair: none needed.")
        return
    print(f"Analyst target repair: {len(tickers)} tickers with NULL targets…")
    import yfinance as yf
    fixed = 0
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        for t in tickers:
            try:
                info = yf.Ticker(t).info or {}
                mean = info.get("targetMeanPrice") or None
                low  = info.get("targetLowPrice") or None
                high = info.get("targetHighPrice") or None
                n    = info.get("numberOfAnalystOpinions") or None
                if mean:
                    conn.execute(
                        sa.text(
                            "UPDATE analyst_expectations SET "
                            "target_mean_price=:mean, target_low_price=:low, "
                            "target_high_price=:high, number_of_analysts=:n, last_update=:ts "
                            "WHERE ticker=:t"
                        ),
                        {"mean": mean, "low": low, "high": high, "n": n, "ts": now, "t": t},
                    )
                    fixed += 1
            except Exception:
                pass
    print(f"Analyst target repair done: {fixed}/{len(tickers)} fixed.")


# ────────────────────────────────────────────────────────────────────────────

def run_daily():
    import gc

    update_prices()
    gc.collect()

    update_vix()
    gc.collect()

    update_company_info()
    gc.collect()

    summarize_descriptions()
    gc.collect()

    update_company_health()
    gc.collect()

    # ── Price gap repair before signal engine so no dates are skipped ────────
    _repair_price_gaps()

    prices = load_prices()
    features_df = compute_features(prices)
    save_features(features_df)
    del prices, features_df
    gc.collect()

    run_prediction_engine()
    run_scoring()

    build_trade_log()
    gc.collect()

    run_ranking()
    run_allocator()

    # ── Remaining self-healing repairs ───────────────────────────────────────
    _repair_market_caps()
    _repair_analyst_targets()


if __name__ == "__main__":
    run_daily()
