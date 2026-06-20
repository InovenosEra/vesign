import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------- Data pipelines ----------
from data.market_data import update_prices, update_vix, update_company_info, summarize_descriptions, update_company_health, _download_and_save, fill_analyst_consensus_from_events

# ---------- Feature engineering ----------
from data.loaders import load_prices, save_features, engine
from features.technical import compute_features

# ---------- Modeling ----------
from models.predict import run_prediction_engine
from signals.engine import run_scoring

from production.fast_rebuild_tiers import build_trade_log_fast, build_trade_lots

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
    """Two-stage repair for NULL market_cap rows in fundamentals.

    Stage 1: re-fetch FMP /profile (timing-flaky — sometimes returns marketCap
             on retry when the original update_company_info call missed it).
    Stage 2: fallback compute from close × latest shares_outstanding using
             daily_prices and market_cap_history. Catches the case where FMP
             /profile chronically omits marketCap for a ticker (we observed
             ~84 such cases at one point — DASH, AXP, AZN, DE, DDOG, etc.).
    """
    import pandas as pd
    from sqlalchemy import text
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

    # Stage 1: re-fetch FMP profile
    stage1 = 0
    with engine.begin() as conn:
        for t in tickers:
            try:
                profile = fmp.company_profile(t) or {}
                mc = profile.get("marketCap")
                if mc:
                    result = conn.execute(
                        text("UPDATE fundamentals SET market_cap=:mc WHERE ticker=:t"),
                        {"mc": mc, "t": t},
                    )
                    if result.rowcount == 0:
                        conn.execute(
                            text("INSERT INTO fundamentals (ticker, market_cap) VALUES (:t, :mc)"),
                            {"t": t, "mc": mc},
                        )
                    stage1 += 1
            except Exception:
                pass
    print(f"  Stage 1 (FMP profile re-fetch): {stage1}/{len(tickers)} filled")

    # Stage 2: anything still NULL — compute from close × shares_outstanding
    still_null = pd.read_sql(
        "SELECT ticker FROM fundamentals WHERE market_cap IS NULL",
        engine,
    )["ticker"].tolist()
    if not still_null:
        print(f"Market cap repair done: {stage1} fixed.")
        return

    stage2 = 0
    with engine.begin() as conn:
        for t in still_null:
            try:
                close_row = conn.execute(
                    text("SELECT close FROM daily_prices WHERE ticker=:t ORDER BY date DESC LIMIT 1"),
                    {"t": t},
                ).fetchone()
                if not close_row or not close_row[0]:
                    continue
                shares_row = conn.execute(
                    text("SELECT shares_outstanding FROM market_cap_history WHERE ticker=:t ORDER BY date DESC LIMIT 1"),
                    {"t": t},
                ).fetchone()
                if not shares_row or not shares_row[0]:
                    continue
                mc = float(close_row[0]) * float(shares_row[0])
                conn.execute(
                    text("UPDATE fundamentals SET market_cap=:mc WHERE ticker=:t"),
                    {"mc": mc, "t": t},
                )
                stage2 += 1
            except Exception:
                pass
    print(f"  Stage 2 (close × shares): {stage2} additional filled")
    print(f"Market cap repair done: {stage1 + stage2}/{len(tickers)} fixed.")


def _repair_analyst_targets():
    """Fill gaps in analyst_expectations after the primary daily pass.

    Targets only tickers with NULL target_mean_price. Uses the standard
    yfinance-primary + FMP-fallback orchestrator so the source column is
    populated truthfully. Re-stamps today's analyst_targets_history row to
    match.
    """
    import sqlalchemy as sa
    from datetime import datetime, timezone
    from data.analyst_targets import fetch_with_fallback

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

    print(f"Analyst target refresh (orchestrator, gap-only): {len(tickers)} tickers")
    out = fetch_with_fallback(tickers)
    now = datetime.now(timezone.utc)
    fixed = 0
    for t, row in out.items():
        if row["source"] == "none":
            continue
        with engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE analyst_expectations SET
                    target_mean_price=:m, target_low_price=:l, target_high_price=:h,
                    number_of_analysts=:n, last_update=:ts, source=:src
                WHERE ticker=:t
            """), {
                "m": row["target_mean_price"], "l": row["target_low_price"],
                "h": row["target_high_price"], "n": row["number_of_analysts"],
                "ts": now, "src": row["source"], "t": t,
            })
            conn.execute(sa.text(
                "DELETE FROM analyst_targets_history WHERE date=:d AND ticker=:t"
            ), {"d": today_str, "t": t})
            conn.execute(sa.text("""
                INSERT INTO analyst_targets_history
                    (date, ticker, target_mean_price, target_high_price,
                     target_low_price, number_of_analysts, source)
                VALUES (:d, :t, :m, :h, :l, :n, :src)
            """), {
                "d": today_str, "t": t, "m": row["target_mean_price"],
                "h": row["target_high_price"], "l": row["target_low_price"],
                "n": row["number_of_analysts"], "src": row["source"],
            })
        fixed += 1
    print(f"Analyst target refresh done: {fixed}/{len(tickers)} recovered.")


# ────────────────────────────────────────────────────────────────────────────

def _backfill_missing_signal_dates():
    """Score any dates within the last 10 days that have features but no signals.

    Runs after run_scoring() to catch interior gaps left by the price repair.

    Implementation note: the previous `NOT EXISTS` query wrapped both sides in
    `DATE()`, which prevented SQLite from using `idx_signals_date` and triggered
    a full-table scan on signals (~2.3M rows) for every features row. On the
    production server with a 1700-day, 1.5k-ticker universe this ran for hours
    without finishing. We now compute the two date sets separately (each uses
    the per-table date index) and diff in Python — typically <1s end-to-end.
    """
    import pandas as pd
    feat_dates = pd.read_sql(
        """
        SELECT DISTINCT date FROM features
        WHERE date >= DATE('now', '-10 days')
        """,
        engine,
    )["date"].astype(str).str[:10].tolist()
    sig_dates = pd.read_sql(
        """
        SELECT DISTINCT date FROM signals
        WHERE date >= DATE('now', '-10 days')
        """,
        engine,
    )["date"].astype(str).str[:10].tolist()
    dates = sorted(set(feat_dates) - set(sig_dates))
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

    # Final fallback: synthesize consensus from our own analyst_target_changes
    # events for tickers FMP /price-target-consensus and yfinance both miss.
    # Strict guard: never overwrites existing non-NULL targets.
    fill_analyst_consensus_from_events()
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

    # Memory-light trade tables: build_trade_log_fast pairs run_scoring's BUY/SELL
    # rows (no 3.5M-row price merge → no OOM on the widened gate), and build_trade_lots
    # maintains trade_lots, which the canonical build_trade_log never did — without
    # this the DCA-aware /api/stats Prime yield decays after deploy day.
    build_trade_log_fast()
    build_trade_lots()
    gc.collect()

    run_ranking()
    run_allocator()

    # ── Remaining self-healing repairs ───────────────────────────────────────
    _repair_market_caps()
    _download_missing_logos()


if __name__ == "__main__":
    run_daily()
