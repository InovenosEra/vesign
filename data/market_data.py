import os
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, UTC
from utils.universe_loader import load_universe
from data.loaders import engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import exchange_calendars as xcals
from utils.update_guard import should_run, mark_run
from data import fmp

# Batch size for backfilling large numbers of new tickers (avoids memory/timeout issues)
_BACKFILL_BATCH = 200

# Earliest date for which we keep price history. Newly-added tickers are
# backfilled from this date so they match the depth of the rest of the DB.
# Same constant as production/rebuild_from_2020.py — keep in sync.
_HISTORY_START = date(2020, 1, 1)


def _download_and_save(tickers: list, start_date, end_date, batch_size: int = 0):
    """
    Download price data for *tickers* between *start_date* and *end_date*
    via FMP (parallel, one request per ticker), then upsert into daily_prices.

    `batch_size` is unused — kept for API compatibility with old callers.
    """
    if not tickers:
        return

    all_frames = []
    today_ts   = pd.Timestamp(datetime.now(UTC).date())

    def _fetch_us(ticker):
        return fmp.historical_prices(ticker, start_date, end_date)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_us, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            done += 1
            df = f.result()
            if df is not None and not df.empty:
                df = df[df["date"] < today_ts]
                if not df.empty:
                    all_frames.append(df)
            if done % 200 == 0:
                print(f"  FMP: {done}/{len(tickers)} tickers fetched")

    if not all_frames:
        print("  No data downloaded.")
        return

    final_df = pd.concat(all_frames, ignore_index=True)
    final_df.drop_duplicates(subset=["date", "ticker"], inplace=True)

    # Delete only the rows for the specific tickers being saved (not all tickers on those dates,
    # which would wipe unrelated markets' price history when doing a partial backfill).
    dates = final_df["date"].dt.strftime("%Y-%m-%d").unique().tolist()
    batch_tickers = final_df["ticker"].unique().tolist()
    if dates and batch_tickers:
        date_params = {f"d{i}": d for i, d in enumerate(dates)}
        tick_params = {f"t{j}": t for j, t in enumerate(batch_tickers)}
        date_phs = ",".join(f":d{i}" for i in range(len(dates)))
        tick_phs = ",".join(f":t{j}" for j in range(len(batch_tickers)))
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    f"DELETE FROM daily_prices "
                    f"WHERE date(date) IN ({date_phs}) "
                    f"AND ticker IN ({tick_phs})"
                ), {**date_params, **tick_params})
        except OperationalError:
            # Table doesn't exist yet — fresh DB rebuild. First to_sql will create it.
            pass

    final_df.to_sql("daily_prices", engine, if_exists="append", index=False)
    print(f"  Saved {len(final_df):,} rows for {final_df['ticker'].nunique():,} tickers.")


def update_prices():

    print("Updating prices…")

    try:
        tickers = load_universe()
    except RuntimeError as e:
        # FMP transient endpoint outage (e.g. IJH/IJR ETF holdings returning 0)
        # would otherwise fail the entire daily pipeline. Fall back to the
        # existing companies table so prices still update for known tickers.
        # Universe drift will get picked up the next time FMP is healthy.
        print(f"⚠️  Universe rebuild failed ({e}); falling back to existing companies table.")
        with engine.connect() as c:
            tickers = [r[0] for r in c.execute(text(
                "SELECT DISTINCT ticker FROM companies WHERE ticker NOT LIKE '%.TA' ORDER BY ticker"
            ))]
        print(f"Using {len(tickers)} existing US tickers from companies table.")

    today    = datetime.now(UTC).date()
    nyse     = xcals.get_calendar("XNYS")
    sessions = nyse.sessions_in_range(
        pd.Timestamp(today - timedelta(days=10)), pd.Timestamp(today)
    )
    end_date = sessions[-1].date() if len(sessions) > 0 else today - timedelta(days=1)

    # ── Step 1: backfill any tickers that have never been downloaded ──────────
    # New tickers (e.g. newly added NASDAQ stocks) won't appear in daily_prices
    # at all, so the MIN(MAX(date)) logic below would only give them a few days
    # of data.  We backfill them separately with 3 years of history first.
    try:
        initialized = set(
            pd.read_sql("SELECT DISTINCT ticker FROM daily_prices", engine)["ticker"].tolist()
        )
    except Exception:
        initialized = set()

    new_tickers = [t for t in tickers if t not in initialized]

    if new_tickers:
        # Backfill new tickers from the same anchor date used by the full
        # rebuild (2020-01-01) so the DB stays uniform. Was 3 years previously,
        # which left newly-added tickers (e.g. NASDAQ-100 expansions) shallower
        # than the rest and broke point-in-time joins on older signals.
        backfill_start = _HISTORY_START
        print(f"Backfilling {len(new_tickers):,} new tickers from {backfill_start} to {end_date}…")
        _download_and_save(new_tickers, backfill_start, end_date, batch_size=_BACKFILL_BATCH)

    # ── Step 2: incremental update for all tickers ────────────────────────────
    # Use MIN(MAX(date) per ticker) so that even if one ticker is slightly ahead,
    # the others still get refreshed.
    # IMPORTANT: filter MIN(MAX) to the current universe — otherwise a delisted
    # ticker (e.g. MODG with last close 2022-09-06) drags the floor back years
    # and forces unnecessary re-fetch of all tickers from that point. (2026-05-09)
    try:
        from sqlalchemy import bindparam
        stmt = text(
            "SELECT MIN(max_date) as last_date FROM "
            "(SELECT ticker, MAX(date) as max_date FROM daily_prices "
            " WHERE ticker IN :tickers GROUP BY ticker)"
        ).bindparams(bindparam("tickers", expanding=True))
        existing = pd.read_sql(stmt, engine, params={"tickers": tickers})
        last_date  = pd.to_datetime(existing["last_date"][0]).date()
        start_date = last_date + timedelta(days=1)
    except Exception:
        start_date = end_date - timedelta(days=3 * 365)

    if start_date >= end_date:
        print("Database already up to date.")
        return

    print(f"Incremental update: {start_date} → {end_date} ({len(tickers):,} tickers)")
    _download_and_save(tickers, start_date, end_date, batch_size=20)

    print("Prices updated successfully.")


def update_vix():

    print("Updating VIX data incrementally...")

    try:
        existing = pd.read_sql("SELECT MAX(date) as last_date FROM vix", engine)
        last_date = pd.to_datetime(existing["last_date"][0]).date()
        start_date = last_date + timedelta(days=1)
    except Exception:
        start_date = datetime.now(UTC).date() - timedelta(days=3 * 365)

    today = datetime.now(UTC).date()

    if start_date >= today:
        print("VIX already up to date")
        return

    print(f"Downloading VIX from {start_date} to {today}")

    try:
        data = yf.download(
            "^VIX",
            start=start_date,
            end=today,
            auto_adjust=False,
            progress=False
        )

        # Fallback: period-based download when date range returns empty
        if data is None or data.empty:
            print("VIX date-range download empty, trying period fallback...")
            data = yf.download("^VIX", period="5d", auto_adjust=False, progress=False)

        if data is None or data.empty:
            print("VIX download returned empty data")
            return

        data.reset_index(inplace=True)

        # Handle MultiIndex columns from yfinance
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] if col[1] == "" else col[0] for col in data.columns]

        data.rename(columns={"Date": "date", "Close": "close"}, inplace=True)
        data = data[["date", "close"]].dropna()

        today_ts = pd.Timestamp(datetime.now(UTC).date())
        data = data[data["date"] < today_ts]

        # Strict cutoff against existing rows: yfinance often returns the most
        # recent trading day even outside the requested range (e.g. asked for
        # Mon-only, returns Fri data because the new week hasn't traded yet).
        # Appending without this check creates duplicate (date) rows that the
        # signal engine then joins against, doubling every ticker's row and
        # crashing the daily pipeline (see project_engine_pitfalls.md #2).
        start_ts = pd.Timestamp(start_date)
        data = data[data["date"] >= start_ts]

        data.drop_duplicates(subset=["date"], inplace=True)
        if data.empty:
            print("VIX: no new dates to insert (all fetched rows already in DB)")
            return
        data.to_sql("vix", engine, if_exists="append", index=False)
        print(f"VIX updated successfully ({len(data)} new rows)")

    except Exception as e:
        print(f"VIX update failed: {e}")


# Real index levels (not the SPY/QQQ/DIA/IWM ETF proxies). yfinance symbols:
#   ^GSPC = S&P 500, ^NDX = Nasdaq-100, ^DJI = Dow Jones, ^RUT = Russell 2000.
INDEX_SYMBOLS = ["^GSPC", "^NDX", "^DJI", "^RUT"]

# Commodity futures + macro cross + FX (USD-anchored: USD<CCY>=X = units of CCY
# per 1 USD, so any display base derives as cross(c, base) = u[base] / u[c]).
COMMODITY_SYMBOLS = ["GC=F", "SI=F", "PL=F", "PA=F", "CL=F", "BZ=F", "NG=F", "HG=F"]
CROSS_SYMBOLS = ["DX-Y.NYB", "^TNX", "BTC-USD"]
FX_USD_SYMBOLS = ["USDEUR=X", "USDGBP=X", "USDJPY=X", "USDCHF=X", "USDCNY=X",
                  "USDAUD=X", "USDCAD=X", "USDILS=X"]
MARKET_QUOTE_SYMBOLS = COMMODITY_SYMBOLS + CROSS_SYMBOLS + FX_USD_SYMBOLS


def update_indices():
    """Fetch real index levels into the index_prices table — mirrors update_vix().
    The market page shows these instead of the ETF proxies so 'S&P 500' reads
    ~5,900 (the index) rather than ~745 (the SPY ETF)."""
    print("Updating index levels incrementally...")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS index_prices (date DATETIME, ticker TEXT, close FLOAT)"
        ))

    try:
        existing = pd.read_sql("SELECT MAX(date) as last_date FROM index_prices", engine)
        last = existing["last_date"][0]
        start_date = (pd.to_datetime(last).date() + timedelta(days=1)) if last is not None \
            else datetime.now(UTC).date() - timedelta(days=3 * 365)
    except Exception:
        start_date = datetime.now(UTC).date() - timedelta(days=3 * 365)

    today = datetime.now(UTC).date()
    if start_date >= today:
        print("Index levels already up to date")
        return

    print(f"Downloading index levels from {start_date} to {today}")
    try:
        data = yf.download(INDEX_SYMBOLS, start=start_date, end=today, auto_adjust=False, progress=False)
        if data is None or data.empty:
            print("Index date-range download empty, trying period fallback...")
            data = yf.download(INDEX_SYMBOLS, period="5d", auto_adjust=False, progress=False)
        if data is None or data.empty:
            print("Index download returned empty data")
            return

        # Multi-ticker download → columns are a (field, ticker) MultiIndex.
        close = data["Close"].reset_index().rename(columns={"Date": "date"})
        long = close.melt(id_vars=["date"], var_name="ticker", value_name="close").dropna()

        today_ts = pd.Timestamp(datetime.now(UTC).date())
        start_ts = pd.Timestamp(start_date)
        long = long[(long["date"] < today_ts) & (long["date"] >= start_ts)]
        long.drop_duplicates(subset=["date", "ticker"], inplace=True)
        if long.empty:
            print("Index levels: no new rows to insert")
            return
        long[["date", "ticker", "close"]].to_sql("index_prices", engine, if_exists="append", index=False)
        print(f"Index levels updated successfully ({len(long)} new rows)")

    except Exception as e:
        print(f"Index update failed: {e}")


def update_market_quotes():
    """Store daily closes for commodity futures, the macro cross strip, and FX
    (USD-anchored) in market_quotes — mirrors update_indices. Lets the
    commodities/currencies/cross strips read a DB baseline + history instead of
    hammering yfinance on every web request (which caused live-contention bugs)."""
    print("Updating market quotes (commodities / cross / FX) incrementally...")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS market_quotes (date DATETIME, ticker TEXT, close FLOAT)"
        ))

    try:
        existing = pd.read_sql("SELECT MAX(date) as last_date FROM market_quotes", engine)
        last = existing["last_date"][0]
        start_date = (pd.to_datetime(last).date() + timedelta(days=1)) if last is not None \
            else datetime.now(UTC).date() - timedelta(days=3 * 365)
    except Exception:
        start_date = datetime.now(UTC).date() - timedelta(days=3 * 365)

    today = datetime.now(UTC).date()
    if start_date >= today:
        print("Market quotes already up to date")
        return

    print(f"Downloading market quotes from {start_date} to {today}")
    try:
        data = yf.download(MARKET_QUOTE_SYMBOLS, start=start_date, end=today,
                           auto_adjust=False, progress=False)
        if data is None or data.empty:
            data = yf.download(MARKET_QUOTE_SYMBOLS, period="5d", auto_adjust=False, progress=False)
        if data is None or data.empty:
            print("Market quotes download returned empty data")
            return

        close = data["Close"].reset_index().rename(columns={"Date": "date"})
        long = close.melt(id_vars=["date"], var_name="ticker", value_name="close").dropna()

        today_ts = pd.Timestamp(datetime.now(UTC).date())
        start_ts = pd.Timestamp(start_date)
        long = long[(long["date"] < today_ts) & (long["date"] >= start_ts)]
        long.drop_duplicates(subset=["date", "ticker"], inplace=True)
        if long.empty:
            print("Market quotes: no new rows to insert")
            return
        long[["date", "ticker", "close"]].to_sql("market_quotes", engine, if_exists="append", index=False)
        print(f"Market quotes updated successfully ({len(long)} new rows)")

    except Exception as e:
        print(f"Market quotes update failed: {e}")


def snapshot_analyst_targets(date_str: str) -> None:
    """Copy current analyst_expectations into analyst_targets_history for date_str.

    Persists the `source` column so the chart can attribute each historical
    row to its origin (yfinance / fmp / none / NULL=legacy).
    Idempotent (INSERT OR REPLACE). Only rows with a non-NULL target_mean_price
    are snapshotted.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS analyst_targets_history (
                date               TEXT NOT NULL,
                ticker             TEXT NOT NULL,
                target_mean_price  REAL,
                target_high_price  REAL,
                target_low_price   REAL,
                number_of_analysts REAL,
                source             TEXT,
                PRIMARY KEY (date, ticker)
            )
        """))
        result = conn.execute(text("""
            INSERT OR REPLACE INTO analyst_targets_history
                (date, ticker, target_mean_price, target_high_price,
                 target_low_price, number_of_analysts, source)
            SELECT :date, ticker, target_mean_price, target_high_price,
                   target_low_price, number_of_analysts, source
            FROM analyst_expectations
            WHERE target_mean_price IS NOT NULL
        """), {"date": date_str})
    print(f"  Snapshotted analyst targets for {date_str}: {result.rowcount} rows inserted")


def update_company_info():
    """Fetch fundamentals + analyst targets in a single parallel FMP pass."""

    needs_fundamentals = should_run("fundamentals_update", 168)   # weekly
    needs_analyst      = should_run("analyst_update", 24)         # daily

    if not needs_fundamentals and not needs_analyst:
        return

    print("Updating company info (fundamentals + analyst)...")

    from utils.sectors import normalize_sector

    tickers = pd.read_sql("SELECT ticker FROM companies", engine)["ticker"].tolist()
    now     = datetime.now(UTC)

    rows = []

    def _fetch_us(t):
        try:
            profile = fmp.company_profile(t) if needs_fundamentals else {}
            profile = profile or {}

            # Analyst fetch moved to fetch_analyst_targets_routed (after the parallel loop)
            target_mean = target_high = target_low = n_analysts = None

            return {
                "ticker":             t,
                "market_cap":         profile.get("marketCap"),
                "industry":           profile.get("industry"),
                "sector":             normalize_sector(profile.get("sector")),  # FMP→GICS
                "description":        profile.get("description"),
                "target_mean_price":  target_mean,
                "target_high_price":  target_high,
                "target_low_price":   target_low,
                "number_of_analysts": n_analysts,
                "source":             None,
                "last_update":        now,
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_us, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            done += 1
            result = f.result()
            if result:
                rows.append(result)
            if done % 200 == 0:
                print(f"  FMP: {done}/{len(tickers)} done, {len(rows)} fetched")

    # ── Analyst targets: routed through orchestrator (yfinance|fmp via ANALYST_SOURCE env)
    if needs_analyst:
        from data.analyst_targets import fetch_analyst_targets_routed
        from collections import Counter
        fetched_tickers = [r["ticker"] for r in rows]
        analyst_rows = fetch_analyst_targets_routed(fetched_tickers)
        by_ticker = {r["ticker"]: r for r in rows}
        for t, a in analyst_rows.items():
            if t in by_ticker:
                by_ticker[t]["target_mean_price"]  = a["target_mean_price"]
                by_ticker[t]["target_high_price"]  = a["target_high_price"]
                by_ticker[t]["target_low_price"]   = a["target_low_price"]
                by_ticker[t]["number_of_analysts"] = a["number_of_analysts"]
                by_ticker[t]["source"]             = a["source"]
        src_counts = Counter(a["source"] for a in analyst_rows.values())
        print(f"  analyst: " + " | ".join(f"{n} {s}" for s, n in src_counts.most_common()))

    if not rows:
        print("No company info downloaded")
        return

    df = pd.DataFrame(rows)

    if needs_fundamentals:
        fund_df = df[["ticker", "market_cap"]]
        fetched = fund_df["ticker"].tolist()
        try:
            with engine.begin() as conn:
                tick_params = {f"t{j}": t for j, t in enumerate(fetched)}
                tick_phs = ",".join(f":t{j}" for j in range(len(fetched)))
                conn.execute(text(f"DELETE FROM fundamentals WHERE ticker IN ({tick_phs})"), tick_params)
        except OperationalError:
            # Table doesn't exist yet — fresh DB rebuild. First to_sql will create it.
            pass
        fund_df.to_sql("fundamentals", engine, if_exists="append", index=False)
        # Update industry and description in companies table.
        # logo_url is owned by production/download_logos.py — do NOT touch it
        # here, or every pipeline run would clobber the self-hosted /logos/{T}.png
        # path back to the deprecated FMP CDN URL from profile["image"].
        with engine.begin() as conn:
            for _, row in df[["ticker", "industry", "sector", "description"]].iterrows():
                if row["industry"] or row["description"] or row["sector"]:
                    # COALESCE on sector: never clobber an existing sector with a blank fetch.
                    conn.execute(text(
                        "UPDATE companies SET "
                        "industry = :ind, "
                        "sector = COALESCE(:sec, sector), "
                        "description = :desc "
                        "WHERE ticker = :t"
                    ), {"ind": row["industry"], "sec": row["sector"],
                        "desc": row["description"], "t": row["ticker"]})
        mark_run("fundamentals_update")

    if needs_analyst:
        analyst_df = df[["ticker", "target_mean_price", "target_high_price",
                          "target_low_price", "number_of_analysts", "last_update", "source"]]
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analyst_expectations (
                    ticker             TEXT PRIMARY KEY,
                    target_mean_price  REAL,
                    target_high_price  REAL,
                    target_low_price   REAL,
                    number_of_analysts REAL,
                    last_update        TEXT,
                    source             TEXT
                )
            """))
            for _, row in analyst_df.iterrows():
                conn.execute(text("""
                    INSERT INTO analyst_expectations
                        (ticker, target_mean_price, target_high_price, target_low_price,
                         number_of_analysts, last_update, source)
                    VALUES (:ticker, :mean, :high, :low, :n, :upd, :src)
                    ON CONFLICT(ticker) DO UPDATE SET
                        target_mean_price  = excluded.target_mean_price,
                        target_high_price  = excluded.target_high_price,
                        target_low_price   = excluded.target_low_price,
                        number_of_analysts = excluded.number_of_analysts,
                        last_update        = excluded.last_update,
                        source             = excluded.source
                """), {"ticker": row["ticker"], "mean": row["target_mean_price"],
                       "high": row["target_high_price"], "low": row["target_low_price"],
                       "n": row["number_of_analysts"], "upd": str(row["last_update"]),
                       "src": row.get("source") or "fmp"})
        mark_run("analyst_update")
        today_str = datetime.now(UTC).strftime("%Y-%m-%d")
        snapshot_analyst_targets(today_str)

    print(f"Company info updated ({len(rows)} tickers)")


def fill_analyst_consensus_from_events(window_days: int = 365) -> int:
    """Fallback: for tickers where FMP /price-target-consensus returns nothing
    (typical for foreign ADRs), compute a current consensus from the existing
    analyst_target_changes events table — same window + one-target-per-analyst
    methodology as apply_historical_analyst.compute_per_date_consensus.

    Strict safety:
      - ONLY updates rows where target_mean_price IS NULL
        (never overwrites FMP's authoritative consensus)
      - Only synthesizes from >= 1 events within the window
      - Idempotent (re-running is a no-op once values are filled)

    Returns the number of rows filled.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=window_days)).strftime("%Y-%m-%d")

    # Tickers needing fill: row exists but target is NULL, or no row at all
    missing = pd.read_sql(text("""
        SELECT c.ticker FROM companies c
        LEFT JOIN analyst_expectations a ON c.ticker = a.ticker
        WHERE a.target_mean_price IS NULL
    """), engine)["ticker"].tolist()
    if not missing:
        print("Consensus-from-events: no tickers need filling")
        return 0

    ph = ",".join(f":t{i}" for i in range(len(missing)))
    params = {f"t{i}": t for i, t in enumerate(missing)}
    params["cutoff"] = cutoff
    events = pd.read_sql(text(
        f"SELECT ticker, published_date, price_target, analyst_company "
        f"FROM analyst_target_changes "
        f"WHERE ticker IN ({ph}) AND published_date >= :cutoff"
    ), engine, params=params)
    if events.empty:
        print(f"Consensus-from-events: 0 of {len(missing)} have events in window")
        return 0

    events["analyst_company"] = events["analyst_company"].fillna("").astype(str)
    rows_to_write = []
    now_iso = datetime.now(UTC).isoformat()
    for ticker, grp in events.groupby("ticker"):
        # One target per analyst — take their latest published_date
        latest = (grp.sort_values("published_date")
                     .drop_duplicates("analyst_company", keep="last"))
        if latest.empty:
            continue
        rows_to_write.append({
            "ticker": ticker,
            "mean":   float(latest["price_target"].mean()),
            "high":   float(latest["price_target"].max()),
            "low":    float(latest["price_target"].min()),
            "n":      int(len(latest)),
            "upd":    now_iso,
        })

    if not rows_to_write:
        return 0

    # UPSERT — but only fill rows where target_mean_price is currently NULL.
    # Split the bulk-insert and the per-ticker UPDATEs into separate
    # transactions: an earlier version mixed batch executemany (INSERT OR
    # IGNORE with a list of dicts) and single-execute UPDATEs in one
    # `engine.begin()` block. SQLAlchemy occasionally lost the binding for
    # `number_of_analysts` mid-transaction (78 rows came out with
    # mean/high/low set but n_analysts NULL). Two separate transactions
    # avoid the issue.
    with engine.begin() as conn:
        conn.execute(text("INSERT OR IGNORE INTO analyst_expectations (ticker) VALUES (:t)"),
                     [{"t": r["ticker"]} for r in rows_to_write])

    filled = 0
    for r in rows_to_write:
        with engine.begin() as conn:
            res = conn.execute(text("""
                UPDATE analyst_expectations
                SET target_mean_price = :mean,
                    target_high_price = :high,
                    target_low_price  = :low,
                    number_of_analysts = :n,
                    last_update       = :upd,
                    source            = :src
                WHERE ticker = :t AND target_mean_price IS NULL
            """), {"t": r["ticker"], "mean": r["mean"], "high": r["high"],
                   "low": r["low"], "n": r["n"], "upd": r["upd"],
                   "src": "events_synthetic"})
            filled += res.rowcount or 0
    print(f"Consensus-from-events: filled {filled} ticker(s) from analyst_target_changes")
    return filled


def update_company_health():
    """Score company financial health (1-5) via Claude Sonnet from FMP data.
    Runs weekly; only re-scores tickers whose score is missing or older than 7 days.
    """
    import json as _json
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set — skipping health scoring.")
        return

    try:
        import anthropic
    except ImportError:
        print("anthropic package not installed — skipping.")
        return

    all_tickers = pd.read_sql("SELECT ticker FROM companies", engine)["ticker"].tolist()

    try:
        existing = pd.read_sql("SELECT ticker, last_update FROM company_health", engine)
        existing["last_update"] = pd.to_datetime(existing["last_update"])
        cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7)
        fresh = set(existing[existing["last_update"] >= cutoff]["ticker"].tolist())
    except Exception:
        fresh = set()

    pending = [t for t in all_tickers if t not in fresh]
    if not pending:
        print("Company health scores are up to date.")
        return

    print(f"Scoring health for {len(pending)} companies…")
    client = anthropic.Anthropic(api_key=api_key)
    now = datetime.now(UTC)

    _SYSTEM = (
        "You are a strict financial analyst rating company health on a FULL 1-5 scale. "
        "Use the ENTIRE range — do NOT cluster scores around 2-3.\n\n"
        "Scale definition (use each level freely):\n"
        "  1 = Weak:      Negative or near-zero margins, heavy debt load, negative/weak cash flow, "
        "shrinking revenue, or near-distress signals.\n"
        "  2 = Fair:      Below-average profitability, elevated leverage, modest or inconsistent cash flow, "
        "slow/flat growth. Survivable but uninspiring.\n"
        "  3 = Good:      Solid, average performance for the industry. Profitable, manageable debt, "
        "positive cash flow, stable growth.\n"
        "  4 = Great:     Above-average margins, strong free cash flow, low-to-moderate debt, "
        "healthy revenue/earnings growth. Financially sound.\n"
        "  5 = Excellent: Exceptional across ALL metrics — industry-leading margins, minimal debt, "
        "strong growing free cash flow, consistent double-digit growth.\n\n"
        "Rules:\n"
        "- If debtToEquity > 2.0 or profitMargins < 0, lean toward 1-2.\n"
        "- If freeCashFlow < 0 and revenueGrowth < 0, that is a 1 or 2.\n"
        "- If profitMargins > 0.20 and debtToEquity < 0.5 and revenueGrowth > 0.10, lean toward 4-5.\n"
        "- Score 5 requires excellence in ALL dimensions simultaneously.\n"
        "- If the company had a net loss in the prior year (one year ago), the score MUST be 3 or lower. No exceptions.\n"
        "- A single strong recovery year after a loss does NOT warrant a 4 or 5.\n"
        "- Context matters: benchmark within the company's industry.\n\n"
        "Respond with ONLY valid JSON: {\"score\": <integer 1-5>, \"reason\": \"<one concise sentence>\"}"
    )

    us_pending = pending

    done    = 0
    success = 0

    def _parse_claude(raw):
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return _json.loads(raw.strip())

    def _write_score(r):
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT OR REPLACE INTO company_health (ticker, score, reason, last_update)
                VALUES (:ticker, :score, :reason, :last_update)
            """), r)
            # Archive to history — one entry per ticker per calendar day (idempotent)
            conn.execute(text("""
                INSERT INTO company_health_history (ticker, score, reason, recorded_at)
                SELECT :ticker, :score, :reason, :last_update
                WHERE NOT EXISTS (
                    SELECT 1 FROM company_health_history
                    WHERE ticker = :ticker
                    AND date(recorded_at) = date(:last_update)
                )
            """), r)

    # ── US path: FMP data + Claude ────────────────────────────────────────────
    def _score_us(ticker):
        try:
            profile   = fmp.company_profile(ticker) or {}
            ratios    = fmp.ratios_ttm(ticker) or {}
            km        = fmp.key_metrics_ttm(ticker) or {}
            growth    = fmp.financial_growth(ticker) or {}
            cf        = fmp.cash_flow(ticker) or {}
            stmts     = fmp.income_statement(ticker, limit=2)
            headlines = fmp.stock_news(ticker, limit=5)

            metrics = {}
            for k, v in [
                ("profitMargins",      ratios.get("netProfitMarginTTM")),
                ("operatingMargins",   ratios.get("operatingProfitMarginTTM")),
                ("grossMargins",       ratios.get("grossProfitMarginTTM")),
                ("returnOnEquity",     km.get("returnOnEquityTTM")),
                ("returnOnAssets",     km.get("returnOnAssetsTTM")),
                ("currentRatio",       ratios.get("currentRatioTTM")),
                ("quickRatio",         ratios.get("quickRatioTTM")),
                ("debtToEquity",       ratios.get("debtToEquityRatioTTM")),  # true ratio, not ×100
                ("revenueGrowth",      growth.get("revenueGrowth")),
                ("netIncomeGrowth",    growth.get("netIncomeGrowth")),
                ("freeCashFlow",       cf.get("freeCashFlow")),
                ("operatingCashFlow",  cf.get("operatingCashFlow")),
            ]:
                if v is not None:
                    metrics[k] = v

            prompt = (
                f"Company: {profile.get('companyName') or ticker} ({ticker})\n"
                f"Industry: {profile.get('industry') or 'Unknown'}\n\n"
            )
            if metrics:
                prompt += "TTM financial metrics:\n"
                for k, v in metrics.items():
                    prompt += f"  {k}: {round(v, 4) if isinstance(v, float) else f'{v:,}'}\n"

            # Add YoY income history so Claude can detect recent losses / volatility
            if stmts:
                prompt += "\nAnnual income history (most recent first):\n"
                for s in stmts:
                    rev = s.get("revenue")
                    ni  = s.get("netIncome")
                    yr  = s.get("fiscalYear") or s.get("date", "")[:4]
                    margin = round(ni / rev * 100, 1) if rev and ni is not None else None
                    margin_str = f"{margin}%" if margin is not None else "N/A"
                    ni_str = f"{ni:,}" if ni is not None else "N/A"
                    prompt += f"  {yr}: revenue={rev:,}, netIncome={ni_str}, margin={margin_str}\n"

            if headlines:
                prompt += "\nRecent news:\n" + "".join(
                    f"  - {h.get('title', '') if isinstance(h, dict) else h}\n" for h in headlines
                )

            msg = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=120,
                system=_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _parse_claude(msg.content[0].text.strip())
            return {"ticker": ticker, "score": max(1, min(5, int(result["score"]))),
                    "reason": result["reason"], "last_update": now}
        except Exception:
            return None

    if us_pending:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(_score_us, t): t for t in us_pending}
            for f in as_completed(futures):
                done += 1
                r = f.result()
                if r:
                    success += 1
                    _write_score(r)
                if done % 100 == 0:
                    print(f"  {done}/{len(pending)} scored ({success} successful)")

    print(f"Health scoring complete: {success}/{len(pending)} scored.")


def update_company_health_batch():
    """Score company financial health (1-5) via Claude Batches API.
    Builds all prompts in parallel from FMP, submits them as a single batch,
    polls until complete, then saves results. Skips tickers scored within the
    last 7 days.
    """
    import json as _json
    import time as _time
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set — skipping health scoring.")
        return

    try:
        import anthropic
    except ImportError:
        print("anthropic package not installed — skipping.")
        return

    all_tickers = pd.read_sql("SELECT ticker FROM companies", engine)["ticker"].tolist()

    try:
        existing = pd.read_sql("SELECT ticker, last_update FROM company_health", engine)
        existing["last_update"] = pd.to_datetime(existing["last_update"])
        cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7)
        fresh = set(existing[existing["last_update"] >= cutoff]["ticker"].tolist())
    except Exception:
        fresh = set()

    pending = [t for t in all_tickers if t not in fresh]
    if not pending:
        print("Company health scores are up to date.")
        return

    print(f"Building prompts for {len(pending)} companies…")
    client = anthropic.Anthropic(api_key=api_key)
    now = datetime.now(UTC)

    _SYSTEM = (
        "You are a strict financial analyst rating company health on a FULL 1-5 scale. "
        "Use the ENTIRE range — do NOT cluster scores around 2-3.\n\n"
        "Scale definition (use each level freely):\n"
        "  1 = Weak:      Negative or near-zero margins, heavy debt load, negative/weak cash flow, "
        "shrinking revenue, or near-distress signals.\n"
        "  2 = Fair:      Below-average profitability, elevated leverage, modest or inconsistent cash flow, "
        "slow/flat growth. Survivable but uninspiring.\n"
        "  3 = Good:      Solid, average performance for the industry. Profitable, manageable debt, "
        "positive cash flow, stable growth.\n"
        "  4 = Great:     Above-average margins, strong free cash flow, low-to-moderate debt, "
        "healthy revenue/earnings growth. Financially sound.\n"
        "  5 = Excellent: Exceptional across ALL metrics — industry-leading margins, minimal debt, "
        "strong growing free cash flow, consistent double-digit growth.\n\n"
        "Rules:\n"
        "- If debtToEquity > 2.0 or profitMargins < 0, lean toward 1-2.\n"
        "- If freeCashFlow < 0 and revenueGrowth < 0, that is a 1 or 2.\n"
        "- If profitMargins > 0.20 and debtToEquity < 0.5 and revenueGrowth > 0.10, lean toward 4-5.\n"
        "- Score 5 requires excellence in ALL dimensions simultaneously.\n"
        "- If the company had a net loss in the prior year (one year ago), the score MUST be 3 or lower. No exceptions.\n"
        "- A single strong recovery year after a loss does NOT warrant a 4 or 5.\n"
        "- Context matters: benchmark within the company's industry.\n\n"
        "Respond with ONLY valid JSON: {\"score\": <integer 1-5>, \"reason\": \"<one concise sentence>\"}"
    )

    def _parse_claude(raw):
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return _json.loads(raw.strip())

    def _write_score(r):
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT OR REPLACE INTO company_health (ticker, score, reason, last_update)
                VALUES (:ticker, :score, :reason, :last_update)
            """), r)
            # Archive to history — one entry per ticker per calendar day (idempotent)
            conn.execute(text("""
                INSERT INTO company_health_history (ticker, score, reason, recorded_at)
                SELECT :ticker, :score, :reason, :last_update
                WHERE NOT EXISTS (
                    SELECT 1 FROM company_health_history
                    WHERE ticker = :ticker
                    AND date(recorded_at) = date(:last_update)
                )
            """), r)

    # ── Build prompts in parallel ─────────────────────────────────────────────
    us_pending = pending

    # Map custom_id → ticker for result processing
    prompt_map: dict[str, str] = {}  # custom_id → ticker
    batch_requests = []

    def _build_us_prompt(ticker):
        try:
            profile   = fmp.company_profile(ticker) or {}
            ratios    = fmp.ratios_ttm(ticker) or {}
            km        = fmp.key_metrics_ttm(ticker) or {}
            growth    = fmp.financial_growth(ticker) or {}
            cf        = fmp.cash_flow(ticker) or {}
            stmts     = fmp.income_statement(ticker, limit=2)
            headlines = fmp.stock_news(ticker, limit=5)

            metrics = {}
            for k, v in [
                ("profitMargins",      ratios.get("netProfitMarginTTM")),
                ("operatingMargins",   ratios.get("operatingProfitMarginTTM")),
                ("grossMargins",       ratios.get("grossProfitMarginTTM")),
                ("returnOnEquity",     km.get("returnOnEquityTTM")),
                ("returnOnAssets",     km.get("returnOnAssetsTTM")),
                ("currentRatio",       ratios.get("currentRatioTTM")),
                ("quickRatio",         ratios.get("quickRatioTTM")),
                ("debtToEquity",       ratios.get("debtToEquityRatioTTM")),
                ("revenueGrowth",      growth.get("revenueGrowth")),
                ("netIncomeGrowth",    growth.get("netIncomeGrowth")),
                ("freeCashFlow",       cf.get("freeCashFlow")),
                ("operatingCashFlow",  cf.get("operatingCashFlow")),
            ]:
                if v is not None:
                    metrics[k] = v

            prompt = (
                f"Company: {profile.get('companyName') or ticker} ({ticker})\n"
                f"Industry: {profile.get('industry') or 'Unknown'}\n\n"
            )
            if metrics:
                prompt += "TTM financial metrics:\n"
                for k, v in metrics.items():
                    prompt += f"  {k}: {round(v, 4) if isinstance(v, float) else f'{v:,}'}\n"

            if stmts:
                prompt += "\nAnnual income history (most recent first):\n"
                for s in stmts:
                    rev = s.get("revenue")
                    ni  = s.get("netIncome")
                    yr  = s.get("fiscalYear") or s.get("date", "")[:4]
                    margin = round(ni / rev * 100, 1) if rev and ni is not None else None
                    margin_str = f"{margin}%" if margin is not None else "N/A"
                    ni_str = f"{ni:,}" if ni is not None else "N/A"
                    prompt += f"  {yr}: revenue={rev:,}, netIncome={ni_str}, margin={margin_str}\n"

            if headlines:
                prompt += "\nRecent news:\n" + "".join(f"  - {h}\n" for h in headlines)

            return ticker, prompt
        except Exception:
            return ticker, None

    # Fetch US prompts in parallel (FMP, 10 workers)
    if us_pending:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_build_us_prompt, t): t for t in us_pending}
            done = 0
            for f in as_completed(futures):
                done += 1
                ticker, prompt = f.result()
                if prompt:
                    custom_id = f"health-{ticker}".replace(".", "_")
                    prompt_map[custom_id] = ticker
                    batch_requests.append({
                        "custom_id": custom_id,
                        "params": {
                            "model": "claude-sonnet-4-6",
                            "max_tokens": 120,
                            "system": _SYSTEM,
                            "messages": [{"role": "user", "content": prompt}],
                        },
                    })
                if done % 200 == 0:
                    print(f"  US prompts built: {done}/{len(us_pending)}")
        print(f"  US prompts ready: {len(prompt_map)}")

    if not batch_requests:
        print("No prompts built — nothing to score.")
        return

    # ── Check for existing batch (crash recovery) ─────────────────────────────
    _BATCH_ID_FILE = "/tmp/vesign_health_batch_id.txt"
    batch_id = None

    if os.path.exists(_BATCH_ID_FILE):
        with open(_BATCH_ID_FILE) as f:
            saved_id = f.read().strip()
        if saved_id:
            try:
                existing_batch = client.messages.batches.retrieve(saved_id)
                if existing_batch.processing_status != "ended":
                    print(f"Resuming existing batch {saved_id} (status: {existing_batch.processing_status})")
                    batch_id = saved_id
                else:
                    print(f"Saved batch {saved_id} already ended — submitting new batch.")
            except Exception:
                print(f"Could not retrieve saved batch {saved_id} — submitting new batch.")

    # ── Submit batch ──────────────────────────────────────────────────────────
    if batch_id is None:
        print(f"Submitting batch of {len(batch_requests):,} requests…")
        batch = client.messages.batches.create(requests=batch_requests)
        batch_id = batch.id
        with open(_BATCH_ID_FILE, "w") as f:
            f.write(batch_id)
        print(f"Batch submitted: {batch_id}")

    # ── Poll until complete ───────────────────────────────────────────────────
    print("Polling for batch completion (every 60s)…")
    while True:
        batch_status = client.messages.batches.retrieve(batch_id)
        status = batch_status.processing_status
        counts = batch_status.request_counts
        print(f"  Status: {status} | processing={counts.processing} succeeded={counts.succeeded} errored={counts.errored}")
        if status == "ended":
            break
        _time.sleep(60)

    # ── Process results ───────────────────────────────────────────────────────
    print("Processing batch results…")
    success = 0
    errors  = 0

    for result in client.messages.batches.results(batch_id):
        custom_id = result.custom_id
        ticker = prompt_map.get(custom_id)
        if not ticker:
            continue

        if result.result.type == "succeeded":
            try:
                raw = result.result.message.content[0].text.strip()
                parsed = _parse_claude(raw)
                _write_score({
                    "ticker":      ticker,
                    "score":       max(1, min(5, int(parsed["score"]))),
                    "reason":      parsed["reason"],
                    "last_update": now,
                })
                success += 1
            except Exception as e:
                print(f"  Parse error for {ticker}: {e}")
                errors += 1
        else:
            errors += 1

    # Clean up batch ID file after successful processing
    try:
        os.remove(_BATCH_ID_FILE)
    except Exception:
        pass

    print(f"Health scoring complete: {success} scored, {errors} errors (of {len(batch_requests)} submitted).")


def summarize_descriptions():
    """Use Claude Haiku to generate short (2-sentence) summaries for company descriptions.
    Only processes rows that have a raw description but no summary yet.
    """
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set — skipping description summarization.")
        return

    try:
        import anthropic
    except ImportError:
        print("anthropic package not installed — skipping.")
        return

    pending = pd.read_sql(
        "SELECT ticker, description FROM companies "
        "WHERE description IS NOT NULL AND description != '' "
        "AND (description_short IS NULL OR description_short = '')",
        engine
    )

    if pending.empty:
        print("All descriptions already summarized.")
        return

    print(f"Summarizing {len(pending)} company descriptions…")
    client = anthropic.Anthropic(api_key=api_key)

    updated = 0
    for _, row in pending.iterrows():
        try:
            msg = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=80,
                messages=[{
                    "role": "user",
                    "content": (
                        f"Summarize the following company description in exactly 1-2 short sentences. "
                        f"Be factual and concise. No fluff.\n\n{row['description']}"
                    ),
                }],
            )
            summary = msg.content[0].text.strip()
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE companies SET description_short = :s WHERE ticker = :t"
                ), {"s": summary, "t": row["ticker"]})
            updated += 1
        except Exception as e:
            print(f"  {row['ticker']}: {e}")

    print(f"Summarized {updated}/{len(pending)} descriptions.")
