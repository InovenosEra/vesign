import os
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, UTC
from utils.universe_loader import load_universe
from data.loaders import engine
from sqlalchemy import text
import exchange_calendars as xcals
from utils.update_guard import should_run, mark_run
from data import fmp

# Batch size for backfilling large numbers of new tickers (avoids memory/timeout issues)
_BACKFILL_BATCH = 200

# Custom logo overrides — FMP returns placeholder/missing images for these tickers.
# These are applied after every company info update to prevent FMP from overwriting them.
LOGO_OVERRIDES = {
    "PENG": "https://cdn.prod.website-files.com/6764579f0a24e5a0083f25bb/67bb88245ce879aaca499ddb_schema--penguin-logo.jpg",
    "HWKN": "https://www.hawkinsinc.com/wp-content/uploads/2025/10/Hawkins-logo-300-x-300.jpg",
}


def _build_ticker_df(raw, ticker, start_date, end_date, single=False):
    """
    Extract and clean one ticker's DataFrame from a yfinance download result.
    Returns a clean DataFrame or None if the ticker had no usable data.
    """
    try:
        df = raw.copy() if single else raw[ticker].copy()
    except (KeyError, TypeError):
        return None

    if df is None or df.empty:
        return None

    # Flatten MultiIndex columns (yfinance returns these for single-ticker downloads too)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df = df.reset_index()
    df["ticker"] = ticker
    df.rename(columns={
        "Date": "date", "Open": "open", "High": "high",
        "Low": "low", "Close": "close", "Volume": "volume",
    }, inplace=True)

    today_ts = pd.Timestamp(datetime.now(UTC).date())
    df = df[df["date"] < today_ts]

    # Keep only the standard columns that exist
    keep = [c for c in ("date", "ticker", "open", "high", "low", "close", "volume") if c in df.columns]
    df = df[keep]

    # Drop rows with no close price — these are non-trading days or incomplete data
    # that would corrupt feature computation downstream.
    if "close" in df.columns:
        df = df[df["close"].notna()]

    return df if not df.empty else None


def _download_and_save(tickers: list, start_date, end_date, batch_size: int = 0):
    """
    Download price data for *tickers* between *start_date* and *end_date*,
    then upsert into daily_prices.

    US tickers: fetched from FMP (parallel, one request per ticker).
    TASE tickers (.TA suffix): fetched from yfinance (existing logic).
    batch_size only applies to TASE batches (ignored for US).
    """
    if not tickers:
        return

    us_tickers   = [t for t in tickers if not t.endswith('.TA')]
    tase_tickers = [t for t in tickers if t.endswith('.TA')]

    all_frames = []
    today_ts   = pd.Timestamp(datetime.now(UTC).date())

    # ── US path: FMP parallel fetch ──────────────────────────────────────────
    if us_tickers:
        def _fetch_us(ticker):
            return fmp.historical_prices(ticker, start_date, end_date)

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_fetch_us, t): t for t in us_tickers}
            done = 0
            for f in as_completed(futures):
                done += 1
                df = f.result()
                if df is not None and not df.empty:
                    df = df[df["date"] < today_ts]
                    if not df.empty:
                        all_frames.append(df)
                if done % 200 == 0:
                    print(f"  FMP: {done}/{len(us_tickers)} US tickers fetched")

    # ── TASE path: existing yfinance batch logic ──────────────────────────────
    if tase_tickers:
        batches = (
            [tase_tickers[i:i + batch_size] for i in range(0, len(tase_tickers), batch_size)]
            if batch_size > 0
            else [tase_tickers]
        )

        for b_idx, batch in enumerate(batches):
            if len(batches) > 1:
                print(f"  TASE Batch {b_idx + 1}/{len(batches)} ({len(batch)} tickers)…")

            query = batch[0] if len(batch) == 1 else batch
            try:
                data = yf.download(
                    query,
                    start=start_date,
                    end=end_date,
                    group_by="ticker",
                    auto_adjust=False,
                    progress=len(batches) == 1,
                )
            except Exception as e:
                print(f"  TASE batch download failed: {e}")
                continue

            single = len(batch) == 1
            for ticker in batch:
                df = _build_ticker_df(data, ticker, start_date, end_date, single=single)
                if df is None and not single:
                    try:
                        retry = yf.download(
                            ticker, start=start_date, end=end_date,
                            auto_adjust=False, progress=False,
                        )
                        df = _build_ticker_df(retry, ticker, start_date, end_date, single=True)
                    except Exception:
                        pass
                if df is not None and not df.empty:
                    all_frames.append(df)

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
        with engine.begin() as conn:
            conn.execute(text(
                f"DELETE FROM daily_prices "
                f"WHERE date(date) IN ({date_phs}) "
                f"AND ticker IN ({tick_phs})"
            ), {**date_params, **tick_params})

    final_df.to_sql("daily_prices", engine, if_exists="append", index=False)
    print(f"  Saved {len(final_df):,} rows for {final_df['ticker'].nunique():,} tickers.")


def update_prices():

    print("Updating prices…")

    tickers = load_universe()

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
        backfill_start = end_date - timedelta(days=3 * 365)
        print(f"Backfilling {len(new_tickers):,} new tickers from {backfill_start} to {end_date}…")
        _download_and_save(new_tickers, backfill_start, end_date, batch_size=_BACKFILL_BATCH)

    # ── Step 2: incremental update for all tickers ────────────────────────────
    # Use MIN(MAX(date) per ticker) so that even if one ticker is slightly ahead,
    # the others still get refreshed.
    try:
        existing = pd.read_sql(
            "SELECT MIN(max_date) as last_date FROM "
            "(SELECT ticker, MAX(date) as max_date FROM daily_prices GROUP BY ticker)",
            engine
        )
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

        data.drop_duplicates(subset=["date"], inplace=True)
        data.to_sql("vix", engine, if_exists="append", index=False)
        print("VIX updated successfully")

    except Exception as e:
        print(f"VIX update failed: {e}")


def snapshot_analyst_targets(date_str: str) -> None:
    """Copy current analyst_expectations into analyst_targets_history for date_str.

    Idempotent (INSERT OR IGNORE). Only rows with a non-NULL target_mean_price
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
                PRIMARY KEY (date, ticker)
            )
        """))
        result = conn.execute(text("""
            INSERT OR REPLACE INTO analyst_targets_history
                (date, ticker, target_mean_price, target_high_price,
                 target_low_price, number_of_analysts)
            SELECT :date, ticker, target_mean_price, target_high_price,
                   target_low_price, number_of_analysts
            FROM analyst_expectations
            WHERE target_mean_price IS NOT NULL
        """), {"date": date_str})
    print(f"  Snapshotted analyst targets for {date_str}: {result.rowcount} rows inserted")


def update_company_info():
    """Fetch fundamentals + analyst targets in a single parallel pass.
    US tickers: FMP (company_profile + price_target_consensus).
    TASE tickers: yfinance .info (unchanged).
    """

    needs_fundamentals = should_run("fundamentals_update", 168)   # weekly
    needs_analyst      = should_run("analyst_update", 24)         # daily

    if not needs_fundamentals and not needs_analyst:
        return

    print("Updating company info (fundamentals + analyst)...")

    tickers      = pd.read_sql("SELECT ticker FROM companies", engine)["ticker"].tolist()
    us_tickers   = [t for t in tickers if not t.endswith('.TA')]
    tase_tickers = [t for t in tickers if t.endswith('.TA')]
    now          = datetime.now(UTC)

    rows = []

    # ── US path: FMP ──────────────────────────────────────────────────────────
    def _fetch_us(t):
        try:
            # Only fetch company profile when fundamentals need updating
            profile = fmp.company_profile(t) if needs_fundamentals else {}
            profile = profile or {}

            target_mean = target_high = target_low = n_analysts = None
            if needs_analyst:
                consensus = fmp.price_target_consensus(t) or {}
                target_mean = consensus.get("targetConsensus") or None
                target_high = consensus.get("targetHigh") or None
                target_low  = consensus.get("targetLow") or None

                # yfinance: fallback for targets, always used for analyst count (FMP doesn't provide it)
                if target_mean is None:
                    try:
                        import yfinance as yf
                        info = yf.Ticker(t).info or {}
                        target_mean = info.get("targetMeanPrice") or None
                        target_low  = info.get("targetLowPrice") or None
                        target_high = info.get("targetHighPrice") or None
                        n_analysts  = info.get("numberOfAnalystOpinions") or None
                    except Exception:
                        pass
                else:
                    try:
                        import yfinance as yf
                        n_analysts = (yf.Ticker(t).info or {}).get("numberOfAnalystOpinions") or None
                    except Exception:
                        pass

            return {
                "ticker":             t,
                "market_cap":         profile.get("marketCap"),
                "industry":           profile.get("industry"),
                "description":        profile.get("description"),
                "logo_url":           profile.get("image"),
                "target_mean_price":  target_mean,
                "target_high_price":  target_high,
                "target_low_price":   target_low,
                "number_of_analysts": n_analysts,
                "last_update":        now,
            }
        except Exception:
            return None

    if us_tickers:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_fetch_us, t): t for t in us_tickers}
            done = 0
            for f in as_completed(futures):
                done += 1
                result = f.result()
                if result:
                    rows.append(result)
                if done % 200 == 0:
                    print(f"  FMP: {done}/{len(us_tickers)} US done, {len(rows)} fetched")

    # ── TASE path: yfinance + FMP fallback for dual-listed tickers ────────────
    # Get USD/ILS rate once for converting FMP USD targets to agorot
    try:
        _usd_ils = yf.Ticker("ILS=X").fast_info.last_price or 3.6
    except Exception:
        _usd_ils = 3.6

    # Pre-fetch FMP consensus sequentially (avoids rate-limit errors from parallel calls)
    import time as _time
    _fmp_targets: dict = {}
    if needs_analyst and tase_tickers:
        print(f"  Fetching FMP consensus for {len(tase_tickers)} TASE tickers...")
        scale = _usd_ils * 100  # USD → agorot
        for _t in tase_tickers:
            try:
                c = fmp.price_target_consensus(_t.replace(".TA", ""))
                if c and c.get("targetConsensus"):
                    _fmp_targets[_t] = {
                        "target_mean_price": c["targetConsensus"] * scale,
                        "target_low_price":  c.get("targetLow",  c["targetConsensus"]) * scale,
                        "target_high_price": c.get("targetHigh", c["targetConsensus"]) * scale,
                    }
            except Exception:
                pass
            _time.sleep(0.15)
        print(f"  FMP TASE coverage: {len(_fmp_targets)}/{len(tase_tickers)}")

    def _fetch_tase(t):
        try:
            info = yf.Ticker(t).info
            if t in _fmp_targets:
                targets = _fmp_targets[t]
            else:
                targets = {
                    "target_mean_price":  info.get("targetMeanPrice"),
                    "target_low_price":   info.get("targetLowPrice"),
                    "target_high_price":  info.get("targetHighPrice"),
                }
            return {
                "ticker":             t,
                "market_cap":         info.get("marketCap"),
                "industry":           info.get("industry"),
                "description":        info.get("longBusinessSummary"),
                "logo_url":           None,
                **targets,
                "number_of_analysts": info.get("numberOfAnalystOpinions"),
                "last_update":        now,
            }
        except Exception:
            return None

    if tase_tickers:
        # Warm up yfinance session (crumb) before threading
        try:
            yf.Ticker(tase_tickers[0]).info
        except Exception:
            pass

        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(_fetch_tase, t): t for t in tase_tickers}
            done = 0
            for f in as_completed(futures):
                done += 1
                result = f.result()
                if result:
                    rows.append(result)
                if done % 200 == 0:
                    print(f"  yfinance: {done}/{len(tase_tickers)} TASE done")

    if not rows:
        print("No company info downloaded")
        return

    df = pd.DataFrame(rows)

    if needs_fundamentals:
        fund_df = df[["ticker", "market_cap"]]
        fetched = fund_df["ticker"].tolist()
        with engine.begin() as conn:
            tick_params = {f"t{j}": t for j, t in enumerate(fetched)}
            tick_phs = ",".join(f":t{j}" for j in range(len(fetched)))
            conn.execute(text(f"DELETE FROM fundamentals WHERE ticker IN ({tick_phs})"), tick_params)
        fund_df.to_sql("fundamentals", engine, if_exists="append", index=False)
        # Update industry, description, and logo_url in companies table
        with engine.begin() as conn:
            for _, row in df[["ticker", "industry", "description", "logo_url"]].iterrows():
                if row["industry"] or row["description"] or row["logo_url"]:
                    conn.execute(text(
                        "UPDATE companies SET "
                        "industry = :ind, "
                        "description = :desc, "
                        "logo_url = COALESCE(:logo, logo_url) "
                        "WHERE ticker = :t"
                    ), {"ind": row["industry"], "desc": row["description"],
                        "logo": row["logo_url"], "t": row["ticker"]})
        mark_run("fundamentals_update")

    # Re-apply custom logo overrides (FMP placeholder images would otherwise overwrite them)
    with engine.begin() as conn:
        for ticker, url in LOGO_OVERRIDES.items():
            conn.execute(text("UPDATE companies SET logo_url = :url WHERE ticker = :t"),
                         {"url": url, "t": ticker})

    if needs_analyst:
        analyst_df = df[["ticker", "target_mean_price", "target_high_price",
                          "target_low_price", "number_of_analysts", "last_update"]]
        with engine.begin() as conn:
            for _, row in analyst_df.iterrows():
                conn.execute(text("""
                    INSERT INTO analyst_expectations
                        (ticker, target_mean_price, target_high_price, target_low_price,
                         number_of_analysts, last_update)
                    VALUES (:ticker, :mean, :high, :low, :n, :upd)
                    ON CONFLICT(ticker) DO UPDATE SET
                        target_mean_price  = excluded.target_mean_price,
                        target_high_price  = excluded.target_high_price,
                        target_low_price   = excluded.target_low_price,
                        number_of_analysts = excluded.number_of_analysts,
                        last_update        = excluded.last_update
                """), {"ticker": row["ticker"], "mean": row["target_mean_price"],
                       "high": row["target_high_price"], "low": row["target_low_price"],
                       "n": row["number_of_analysts"], "upd": str(row["last_update"])})
        mark_run("analyst_update")
        today_str = datetime.now(UTC).strftime("%Y-%m-%d")
        snapshot_analyst_targets(today_str)

    print(f"Company info updated ({len(rows)} tickers)")


def update_company_health():
    """Score company financial health (1-5) via Claude Haiku.
    US tickers: FMP fundamentals + news. TASE tickers: yfinance (unchanged).
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

    _SYSTEM_IL = (
        "You are a strict financial analyst rating company health on a FULL 1-5 scale. "
        "Use the ENTIRE range — do NOT cluster scores around 2-3.\n\n"
        "You are evaluating Israeli (TASE) companies. Apply these market-specific norms:\n"
        "- Israeli banks, real estate, and infrastructure companies structurally carry high debt — "
        "D/E > 2.0 is normal for these sectors. Do NOT penalize unless D/E > 5.0.\n"
        "- The Tel Aviv market is heavily weighted toward real estate, banking, pharma, and defense — "
        "benchmark against sector peers, not US norms.\n"
        "- Stock prices may be quoted in agorot (1/100 shekel) — ignore absolute price levels.\n\n"
        "Scale definition (use each level freely):\n"
        "  1 = Weak:      Negative or near-zero margins, severe debt overload, negative/weak cash flow, "
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
        "- If debtToEquity > 5.0 or profitMargins < 0, lean toward 1-2.\n"
        "- If freeCashflow < 0 and revenueGrowth < 0, that is a 1 or 2.\n"
        "- If profitMargins > 0.15 and revenueGrowth > 0.08, lean toward 4-5.\n"
        "- Score 5 requires excellence in ALL dimensions simultaneously.\n"
        "- If the company had a net loss in the prior year (one year ago), the score MUST be 3 or lower. No exceptions.\n"
        "- A single strong recovery year after a loss does NOT warrant a 4 or 5.\n"
        "- Context matters: benchmark within the company's industry and Israeli market.\n\n"
        "Respond with ONLY valid JSON: {\"score\": <integer 1-5>, \"reason\": \"<one concise sentence>\"}"
    )

    us_pending   = [t for t in pending if not t.endswith('.TA')]
    tase_pending = [t for t in pending if t.endswith('.TA')]

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
                model="claude-haiku-4-5-20251001",
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

    # ── TASE path: yfinance data + Claude ─────────────────────────────────────
    def _score_tase(ticker):
        try:
            info = yf.Ticker(ticker).info
            metrics = {k: info.get(k) for k in (
                "profitMargins", "operatingMargins", "grossMargins",
                "returnOnEquity", "returnOnAssets",
                "currentRatio", "quickRatio",
                "debtToEquity", "totalDebt", "totalCash",
                "freeCashflow", "operatingCashflow",
                "revenueGrowth", "earningsGrowth",
            )}
            metrics = {k: v for k, v in metrics.items() if v is not None}

            try:
                headlines = [n["title"] for n in (yf.Ticker(ticker).news or [])[:5] if n.get("title")]
            except Exception:
                headlines = []

            prompt = (
                f"Company: {info.get('longName') or ticker} ({ticker})\n"
                f"Industry: {info.get('industry') or 'Unknown'}\n\n"
            )
            if metrics:
                prompt += "Financial metrics:\n"
                for k, v in metrics.items():
                    prompt += f"  {k}: {round(v, 4) if isinstance(v, float) else f'{v:,}'}\n"
            if headlines:
                prompt += "\nRecent news:\n" + "".join(f"  - {h}\n" for h in headlines)

            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=120,
                system=_SYSTEM_IL,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _parse_claude(msg.content[0].text.strip())
            return {"ticker": ticker, "score": max(1, min(5, int(result["score"]))),
                    "reason": result["reason"], "last_update": now}
        except Exception:
            return None

    if tase_pending:
        # Warm up yfinance session (crumb) before threading
        try:
            yf.Ticker(tase_pending[0]).info
        except Exception:
            pass

        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(_score_tase, t): t for t in tase_pending}
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
    Builds all prompts in parallel (FMP for US, yfinance for TASE), submits them
    as a single batch, polls until complete, then saves results.
    Skips tickers scored within the last 7 days.
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

    _SYSTEM_IL = (
        "You are a strict financial analyst rating company health on a FULL 1-5 scale. "
        "Use the ENTIRE range — do NOT cluster scores around 2-3.\n\n"
        "You are evaluating Israeli (TASE) companies. Apply these market-specific norms:\n"
        "- Israeli banks, real estate, and infrastructure companies structurally carry high debt — "
        "D/E > 2.0 is normal for these sectors. Do NOT penalize unless D/E > 5.0.\n"
        "- The Tel Aviv market is heavily weighted toward real estate, banking, pharma, and defense — "
        "benchmark against sector peers, not US norms.\n"
        "- Stock prices may be quoted in agorot (1/100 shekel) — ignore absolute price levels.\n\n"
        "Scale definition (use each level freely):\n"
        "  1 = Weak:      Negative or near-zero margins, severe debt overload, negative/weak cash flow, "
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
        "- If debtToEquity > 5.0 or profitMargins < 0, lean toward 1-2.\n"
        "- If freeCashflow < 0 and revenueGrowth < 0, that is a 1 or 2.\n"
        "- If profitMargins > 0.15 and revenueGrowth > 0.08, lean toward 4-5.\n"
        "- Score 5 requires excellence in ALL dimensions simultaneously.\n"
        "- If the company had a net loss in the prior year (one year ago), the score MUST be 3 or lower. No exceptions.\n"
        "- A single strong recovery year after a loss does NOT warrant a 4 or 5.\n"
        "- Context matters: benchmark within the company's industry and Israeli market.\n\n"
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
    us_pending   = [t for t in pending if not t.endswith('.TA')]
    tase_pending = [t for t in pending if t.endswith('.TA')]

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

    def _build_tase_prompt(ticker):
        try:
            info = yf.Ticker(ticker).info
            metrics = {k: info.get(k) for k in (
                "profitMargins", "operatingMargins", "grossMargins",
                "returnOnEquity", "returnOnAssets",
                "currentRatio", "quickRatio",
                "debtToEquity", "totalDebt", "totalCash",
                "freeCashflow", "operatingCashflow",
                "revenueGrowth", "earningsGrowth",
            )}
            metrics = {k: v for k, v in metrics.items() if v is not None}

            try:
                headlines = [n["title"] for n in (yf.Ticker(ticker).news or [])[:5] if n.get("title")]
            except Exception:
                headlines = []

            prompt = (
                f"Company: {info.get('longName') or ticker} ({ticker})\n"
                f"Industry: {info.get('industry') or 'Unknown'}\n\n"
            )
            if metrics:
                prompt += "Financial metrics:\n"
                for k, v in metrics.items():
                    prompt += f"  {k}: {round(v, 4) if isinstance(v, float) else f'{v:,}'}\n"
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
                            "model": "claude-haiku-4-5-20251001",
                            "max_tokens": 120,
                            "system": _SYSTEM,
                            "messages": [{"role": "user", "content": prompt}],
                        },
                    })
                if done % 200 == 0:
                    print(f"  US prompts built: {done}/{len(us_pending)}")
        print(f"  US prompts ready: {sum(1 for c in prompt_map if not prompt_map[c].endswith('.TA'))}")

    # Fetch TASE prompts in parallel (yfinance, 3 workers)
    if tase_pending:
        try:
            yf.Ticker(tase_pending[0]).info
        except Exception:
            pass

        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(_build_tase_prompt, t): t for t in tase_pending}
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
                            "model": "claude-haiku-4-5-20251001",
                            "max_tokens": 120,
                            "system": _SYSTEM_IL,
                            "messages": [{"role": "user", "content": prompt}],
                        },
                    })
                if done % 50 == 0:
                    print(f"  TASE prompts built: {done}/{len(tase_pending)}")
        print(f"  TASE prompts ready: {sum(1 for c in prompt_map if prompt_map[c].endswith('.TA'))}")

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
                model="claude-haiku-4-5-20251001",
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
