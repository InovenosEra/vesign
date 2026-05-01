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
    """Re-download any US tickers missing recent price data (FMP rate-limit gaps).

    Catches two kinds of gaps:
    1. Tickers whose max date is behind the expected latest date (tail gaps).
    2. Tickers missing data for an interior date — e.g. have Mar 24 and Mar 27
       but not Mar 25 or Mar 26 (the bug that caused this fix to be written).
    """
    import pandas as pd

    # ── 1. Tail gaps: tickers whose max date is behind the mode ─────────────
    df = pd.read_sql(
        "SELECT ticker, DATE(MAX(date)) AS max_date FROM daily_prices "
        "WHERE ticker NOT LIKE '%.TA' GROUP BY ticker",
        engine,
    )
    if df.empty:
        return
    mode_vals = df["max_date"].mode()
    if mode_vals.empty:
        return
    expected = mode_vals[0]
    lagging = df[df["max_date"] < expected]["ticker"].tolist()
    if lagging:
        print(f"Price gaps (tail): {len(lagging)} tickers behind {expected} — re-downloading…")
        _download_and_save(lagging, expected, expected)
    else:
        print("Price gaps (tail): none found.")

    # ── 2. Interior gaps: tickers present on latest date but missing on ──────
    #       intermediate dates within the last 10 calendar days
    recent_dates = pd.read_sql(
        f"SELECT DISTINCT DATE(date) AS d FROM daily_prices "
        f"WHERE ticker NOT LIKE '%.TA' AND date >= DATE('{expected}', '-10 days') "
        f"ORDER BY d",
        engine,
    )["d"].tolist()

    if len(recent_dates) < 2:
        print("Price gaps (interior): not enough dates to check.")
        return

    # Single query: load all (ticker, date) pairs in the recent window at once,
    # then compute missing combos in Python — avoids N separate DB round-trips.
    present_df = pd.read_sql(
        f"SELECT ticker, DATE(date) AS d FROM daily_prices "
        f"WHERE ticker NOT LIKE '%.TA' "
        f"AND date >= DATE('{expected}', '-10 days') "
        f"AND date <= '{expected}'",
        engine,
    )
    universe = set(present_df[present_df["d"] == expected]["ticker"].tolist())

    gaps_by_date = {}
    for d in recent_dates[:-1]:  # all dates except the latest
        present_on_date = set(present_df[present_df["d"] == d]["ticker"].tolist())
        missing = sorted(universe - present_on_date)
        if missing:
            gaps_by_date[d] = missing

    if not gaps_by_date:
        print("Price gaps (interior): none found.")
        return

    for gap_date, tickers in gaps_by_date.items():
        print(f"Price gaps (interior): {len(tickers)} tickers missing on {gap_date} — re-downloading…")
        _download_and_save(tickers, gap_date, gap_date)

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
    """yfinance fallback for tickers FMP missed during today's run.

    Targeted scope: only fetches tickers with NULL target_mean_price in
    analyst_expectations (typically 50-150 tickers after FMP completes).
    Single-threaded with sleeps — avoids the OOM that bulk concurrent
    yfinance caused on 2026-04-22/24/25. Hard cap at 300 tickers and a
    consecutive-failure circuit breaker as additional safety nets.

    Updates analyst_expectations and re-stamps today's analyst_targets_history
    snapshot so downstream signals join the recovered values.
    """
    import time
    import sqlalchemy as sa
    import yfinance as yf
    from datetime import datetime, timezone

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with engine.begin() as conn:
        rows = conn.execute(sa.text(
            "SELECT ticker FROM analyst_expectations "
            "WHERE target_mean_price IS NULL AND ticker NOT LIKE '%.TA' "
            "LIMIT 300"
        )).fetchall()
    tickers = [r[0] for r in rows]

    if not tickers:
        print("Analyst target refresh: no gaps to repair.")
        return

    print(f"Analyst target refresh (yfinance, gap-only): {len(tickers)} tickers")

    fixed = 0
    no_data = 0
    consec_fail = 0
    now = datetime.now(timezone.utc)

    for i, t in enumerate(tickers, 1):
        try:
            info = yf.Ticker(t).info or {}
            mean = info.get("targetMeanPrice")
            if not mean:
                no_data += 1
                consec_fail += 1
                if consec_fail >= 30:
                    print(f"  yfinance appears blocked ({consec_fail} consecutive misses) — aborting at {i}/{len(tickers)}")
                    break
                time.sleep(0.4)
                continue
            consec_fail = 0
            low = info.get("targetLowPrice")
            high = info.get("targetHighPrice")
            n = info.get("numberOfAnalystOpinions")
            with engine.begin() as conn:
                conn.execute(sa.text(
                    "UPDATE analyst_expectations SET "
                    "target_mean_price=:m, target_low_price=:l, target_high_price=:h, "
                    "number_of_analysts=:n, last_update=:ts WHERE ticker=:t"
                ), {"m": mean, "l": low, "h": high, "n": n, "ts": now, "t": t})
                conn.execute(sa.text(
                    "DELETE FROM analyst_targets_history WHERE date=:d AND ticker=:t"
                ), {"d": today_str, "t": t})
                conn.execute(sa.text(
                    "INSERT INTO analyst_targets_history "
                    "(date, ticker, target_mean_price, target_high_price, target_low_price, number_of_analysts) "
                    "VALUES (:d, :t, :m, :h, :l, :n)"
                ), {"d": today_str, "t": t, "m": mean, "h": high, "l": low, "n": n})
            fixed += 1
        except Exception as e:
            consec_fail += 1
            if consec_fail >= 30:
                print(f"  yfinance errors ({consec_fail} consecutive) — aborting at {i}/{len(tickers)}: {e}")
                break
        time.sleep(0.4)
        if i % 25 == 0:
            print(f"  yfinance: {i}/{len(tickers)} done, fixed={fixed}, no_data={no_data}")

    print(f"Analyst target refresh done: {fixed}/{len(tickers)} recovered, {no_data} no data.")


# ────────────────────────────────────────────────────────────────────────────

def _backfill_missing_signal_dates():
    """Score any dates within the last 10 days that have features but no signals.

    Runs after run_scoring() to catch interior gaps left by the price repair.
    """
    import pandas as pd
    missing = pd.read_sql(
        """
        SELECT DISTINCT DATE(f.date) AS d FROM features f
        WHERE f.date >= DATE('now', '-10 days')
          AND NOT EXISTS (
              SELECT 1 FROM signals s WHERE DATE(s.date) = DATE(f.date)
          )
        ORDER BY d
        """,
        engine,
    )
    dates = missing["d"].tolist()
    if not dates:
        return
    print(f"Backfilling signals for {len(dates)} missing date(s): {dates}")
    for d in dates:
        run_scoring(target_date=d)


def _validate_pipeline():
    """Check that all key data is current after the pipeline run.

    Determines the last closed trading session for US (NYSE) and TASE, then
    verifies prices, VIX, features, and signals all reach that date.
    Logs a PASS/FAIL line for each check and returns True if everything passed.
    """
    import pandas as pd
    import exchange_calendars as xcals
    from datetime import datetime, UTC, timedelta

    print("\n── Pipeline validation ──────────────────────────────────────────────────")
    passed = True
    today = datetime.now(UTC).date()

    def _last_closed(xcal_id):
        cal = xcals.get_calendar(xcal_id)
        sessions = cal.sessions_in_range(
            pd.Timestamp(today - timedelta(days=14)),
            pd.Timestamp(today - timedelta(days=1)),
        )
        return sessions[-1].date() if len(sessions) > 0 else today - timedelta(days=1)

    expected_us = _last_closed("XNYS")

    def check(label, actual, expected):
        nonlocal passed
        ok = str(actual) == str(expected)
        if not ok:
            passed = False
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}: {actual} (expected {expected})")

    row = pd.read_sql(
        "SELECT COUNT(DISTINCT ticker) AS cnt, DATE(MAX(date)) AS latest "
        "FROM daily_prices WHERE ticker NOT LIKE '%.TA'",
        engine,
    ).iloc[0]
    check(f"US prices ({row['cnt']} tickers)", row["latest"], expected_us)

    row = pd.read_sql("SELECT DATE(MAX(date)) AS latest FROM vix", engine).iloc[0]
    check("VIX", row["latest"], expected_us)

    row = pd.read_sql(
        "SELECT DATE(MAX(date)) AS latest, COUNT(*) AS cnt FROM signals", engine
    ).iloc[0]
    check(f"Signals ({row['cnt']} total)", row["latest"], expected_us)

    row = pd.read_sql("SELECT DATE(MAX(date)) AS latest FROM features", engine).iloc[0]
    check("Features", row["latest"], expected_us)

    if passed:
        print("  ✓ All checks passed — pipeline complete.")
    else:
        print("  ✗ One or more checks FAILED — see above.")
    print("────────────────────────────────────────────────────────────────────────\n")
    return passed


def _download_missing_logos():
    """Fetch logo PNGs for any tickers that don't have an on-disk file yet."""
    from production.download_logos import download_all
    download_all(missing_only=True)


def run_daily():
    import gc

    update_prices()
    gc.collect()

    update_vix()
    gc.collect()

    update_company_info()
    gc.collect()

    _repair_analyst_targets()
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

    # ── Backfill any interior dates still missing signals after repair ────────
    _backfill_missing_signal_dates()

    # ── News-veto gate: catastrophic-news check on the few BUYs that fired ───
    # Runs only on the latest scored date. Default-allows on any error so it
    # can never silently suppress BUYs because of an API hiccup.
    from signals.news_gate import apply_news_gate
    try:
        apply_news_gate()
    except Exception as e:
        print(f"News gate skipped: {e}")
    gc.collect()

    build_trade_log()
    gc.collect()

    run_ranking()
    run_allocator()

    # ── Remaining self-healing repairs ───────────────────────────────────────
    _repair_market_caps()
    _download_missing_logos()


if __name__ == "__main__":
    run_daily()
