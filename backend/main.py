import math
import os
import time
import re
import requests
import subprocess
import sys
import tempfile
from datetime import datetime, UTC, date
from typing import Optional

import pandas as pd
import pandas_market_calendars as mcal
import pytz
import yfinance as yf
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text, event as sa_event
from sqlalchemy.pool import NullPool
from backend.auth import get_current_user

# ---------------------------------------------------------------------------
# Config  (.env overrides defaults; .env is gitignored)
# ---------------------------------------------------------------------------

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_APP_ROOT, ".env"))

DB_PATH = os.getenv("DB_PATH", os.path.join(_APP_ROOT, "vesign.db"))
_CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")]

app = FastAPI(title="Vesign Trading API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

engine = create_engine(f"sqlite:///{DB_PATH}", poolclass=NullPool)


@sa_event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=30000")
    cur.close()


def _ensure_indexes():
    """Create performance indexes if they don't already exist."""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_companies_ticker ON companies(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_company_health_history_ticker_date ON company_health_history(ticker, recorded_at)",
    ]
    with engine.begin() as conn:
        for sql in indexes:
            conn.execute(text(sql))

_ensure_indexes()


def _send_access_request_email(requester_email: str, message: str):
    api_key     = os.getenv("RESEND_API_KEY")
    admin_email = os.getenv("ADMIN_EMAIL")
    from_addr   = os.getenv("RESEND_FROM", "noreply@ve-sign.com")
    if not all([api_key, admin_email]):
        return  # not configured — skip silently
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "from": from_addr,
                "to": [admin_email],
                "subject": f"[Vesign] Access request from {requester_email}",
                "text": f"New access request on Vesign:\n\nEmail: {requester_email}\nMessage: {message or '(none)'}",
            },
            timeout=10,
        )
        if resp.status_code >= 400:
            print(f"[email] Resend error {resp.status_code}: {resp.text}")
    except Exception as exc:
        print(f"[email] Failed to send access-request notification: {exc}")


def _init_tables():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS access_requests (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                email      TEXT NOT NULL,
                message    TEXT DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS watchlist_lists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL,
                UNIQUE(user_id, name)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                list_id INTEGER NOT NULL REFERENCES watchlist_lists(id),
                ticker TEXT NOT NULL,
                note TEXT DEFAULT '',
                UNIQUE(list_id, ticker)
            )
        """))

    # Schema migration in its own connection so a failure doesn't poison the
    # watchlist transaction above (SQLite marks a connection as "needs rollback"
    # after any failed statement, even if the exception is caught).
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE companies ADD COLUMN market TEXT DEFAULT 'US'"))
    except Exception:
        pass  # column already exists or table doesn't exist yet

    # Migrate watchlist_lists: add user_id + change UNIQUE(name) → UNIQUE(user_id, name)
    try:
        with engine.begin() as conn:
            cols = [r[1] for r in conn.execute(text("PRAGMA table_info(watchlist_lists)")).fetchall()]
            if "user_id" not in cols:
                conn.execute(text("""
                    CREATE TABLE watchlist_lists_new (
                        id      INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL DEFAULT '',
                        name    TEXT NOT NULL,
                        UNIQUE(user_id, name)
                    )
                """))
                conn.execute(text("""
                    INSERT INTO watchlist_lists_new (id, user_id, name)
                    SELECT id, '', name FROM watchlist_lists
                """))
                conn.execute(text("DROP TABLE watchlist_lists"))
                conn.execute(text("ALTER TABLE watchlist_lists_new RENAME TO watchlist_lists"))
    except Exception:
        pass


_init_tables()

# ---------------------------------------------------------------------------
# Market helpers
# ---------------------------------------------------------------------------

_nyse_cal = mcal.get_calendar("NYSE")
_tase_cal = mcal.get_calendar("TASE")
_sched_cache: dict = {}  # (cal_name, date) → schedule DataFrame


def _get_schedule(cal, d):
    key = (cal.name, d)
    if key not in _sched_cache:
        _sched_cache[key] = cal.schedule(start_date=d, end_date=d)
    return _sched_cache[key]


def _market_info(cal) -> dict:
    """Return {is_open, next_event_utc} for a calendar, holiday-aware."""
    from datetime import timedelta
    today = datetime.now(UTC).date()
    now = datetime.now(UTC)

    sched_today = _get_schedule(cal, today)
    if not sched_today.empty:
        row = sched_today.iloc[0]
        open_ts = row["market_open"].to_pydatetime()
        close_ts = row["market_close"].to_pydatetime()
        if open_ts <= now <= close_ts:
            return {"is_open": True, "next_event_utc": close_ts.isoformat()}
        if now < open_ts:
            return {"is_open": False, "next_event_utc": open_ts.isoformat()}

    # Session over or today is holiday — find next trading session (up to 10 days)
    from datetime import timedelta
    for delta in range(1, 11):
        sched = _get_schedule(cal, today + timedelta(days=delta))
        if not sched.empty:
            open_ts = sched.iloc[0]["market_open"].to_pydatetime()
            return {"is_open": False, "next_event_utc": open_ts.isoformat()}

    return {"is_open": False, "next_event_utc": None}


def market_is_open() -> bool:
    return _market_info(_nyse_cal)["is_open"]


def tase_is_open() -> bool:
    return _market_info(_tase_cal)["is_open"]


def _extract_close_series(raw: pd.DataFrame, ticker: str | None = None) -> pd.Series:
    """Extract a Close price Series from a yf.download result, handling MultiIndex columns."""
    if raw.empty:
        return pd.Series(dtype=float)
    # Flatten MultiIndex → pick "Close" level
    if isinstance(raw.columns, pd.MultiIndex):
        if "Close" not in raw.columns.get_level_values(0):
            return pd.Series(dtype=float)
        close_df = raw["Close"]
        if ticker:
            return close_df[ticker].dropna() if ticker in close_df.columns else pd.Series(dtype=float)
        return close_df.iloc[:, 0].dropna()
    # Flat columns
    if ticker is None:
        col = "Close" if "Close" in raw.columns else raw.columns[0]
        return raw[col].dropna()
    return raw.get("Close", pd.Series(dtype=float)).dropna()


_live_price_cache: dict = {}      # ticker -> price
_live_price_cache_ts: float = 0.0 # last fetch timestamp
_LIVE_CACHE_TTL = 60              # seconds


def fetch_live_prices(tickers: list[str]) -> dict:
    """Fetch latest prices. US tickers via FMP batch; TASE (.TA) via yfinance in parallel."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from data import fmp as _fmp

    us_tickers   = [t for t in tickers if not t.endswith('.TA')]
    il_tickers   = [t for t in tickers if t.endswith('.TA')]

    prices: dict = {}

    # US: single FMP batch call
    if us_tickers:
        prices.update(_fmp.live_prices(us_tickers))

    # TASE: yfinance fast_info in parallel
    if il_tickers:
        def _get(t):
            try:
                price = yf.Ticker(t).fast_info.last_price
                return t, float(price) if price else None
            except Exception:
                return t, None

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_get, t): t for t in il_tickers}
            for f in as_completed(futures):
                t, price = f.result()
                prices[t] = price

    return prices


# ---------------------------------------------------------------------------
# Pipeline state (module-level, single-process)
# ---------------------------------------------------------------------------

_pipeline_proc: Optional[subprocess.Popen] = None
_pipeline_log_file: Optional[str] = None


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class WatchlistCreate(BaseModel):
    name: str


class TickerAdd(BaseModel):
    ticker: str
    note: str = ""


class NoteUpdate(BaseModel):
    note: str


class AccessRequestBody(BaseModel):
    email: str
    message: str = ""


# ---------------------------------------------------------------------------
# Helper: convert DataFrame to JSON-safe records
# ---------------------------------------------------------------------------

def _records(df: pd.DataFrame) -> list[dict]:
    records = df.to_dict(orient="records")
    return [
        {k: (None if (isinstance(v, float) and math.isnan(v)) else v)
         for k, v in row.items()}
        for row in records
    ]


# ===========================================================================
# Auth endpoints (public)
# ===========================================================================

@app.get("/api/auth/me")
def me(user=Depends(get_current_user)):
    return user


@app.post("/api/auth/request-access")
def request_access(payload: dict):
    email   = str(payload.get("email", "")).strip()
    message = str(payload.get("message", "")).strip()
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO access_requests (email, message) VALUES (:e, :m)"),
                     {"e": email, "m": message})
    _send_access_request_email(email, message)
    return {"ok": True}


@app.post("/api/access-request", status_code=201)
def access_request(body: AccessRequestBody):
    email = body.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email address")
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO access_requests (email, message) VALUES (:email, :msg)"),
            {"email": email, "msg": body.message.strip()},
        )
    _send_access_request_email(email, body.message.strip())
    return {"ok": True}


# ===========================================================================
# Protected router — all existing routes require a valid JWT
# ===========================================================================

protected = APIRouter(dependencies=[Depends(get_current_user)])


@protected.get("/api/access-requests")
def list_access_requests():
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT id, email, message, created_at FROM access_requests ORDER BY created_at DESC"), conn)
    return _records(df)


# ===========================================================================
# Endpoints
# ===========================================================================

# --- Health (public — for deployment health checks) -------------------------

@app.get("/api/health")
def health():
    return {"status": "ok", "db_exists": os.path.exists(DB_PATH)}


# --- Market status ----------------------------------------------------------

@protected.get("/api/market/status")
def market_status(market: Optional[str] = None):
    if market and market.upper() == "IL":
        return _market_info(_tase_cal)
    return _market_info(_nyse_cal)


# --- Signals ----------------------------------------------------------------

_MARKET_CAP_JOIN = """
    LEFT JOIN (
        SELECT ticker, MAX(market_cap) AS market_cap
        FROM fundamentals GROUP BY ticker
    ) f ON s.ticker = f.ticker
    LEFT JOIN company_health h ON s.ticker = h.ticker
    LEFT JOIN (
        SELECT p1.ticker, p1.close AS latest_close
        FROM daily_prices p1
        INNER JOIN (SELECT ticker, MAX(date) AS max_date FROM daily_prices GROUP BY ticker) p2
            ON p1.ticker = p2.ticker AND p1.date = p2.max_date
    ) lp ON s.ticker = lp.ticker
    LEFT JOIN analyst_expectations ae ON s.ticker = ae.ticker
"""

_ANALYST_UPSIDE_SQL = """CASE WHEN COALESCE(ae.target_mean_price, s.target_mean_price) IS NOT NULL AND lp.latest_close IS NOT NULL AND lp.latest_close > 0
                    THEN (COALESCE(ae.target_mean_price, s.target_mean_price) - lp.latest_close) / lp.latest_close
                    ELSE s.fair_value_upside END AS fair_value_upside"""

@protected.get("/api/signals/today")
def signals_today(signal: Optional[str] = None, market: Optional[str] = None):
    """Today's signals (latest date in DB). Optional ?signal=BUY|SELL|HOLD&market=US|IL filter."""
    mkt = (market or "US").upper()
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT s.date, s.ticker, COALESCE(lp.latest_close, s.close) AS close, s.rsi,
                   {_ANALYST_UPSIDE_SQL},
                   COALESCE(ae.target_mean_price, s.target_mean_price) AS target_mean_price, COALESCE(ae.target_low_price, s.target_low_price) AS target_low_price, COALESCE(ae.target_high_price, s.target_high_price) AS target_high_price,
                   s.prediction_score,
                   s.signal, c.company, c.logo_url, c.industry, c.description, c.description_short, COALESCE(s.health_score, h.score) AS health_score, h.reason AS health_reason,
                   f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
            WHERE COALESCE(c.market, 'US') = :market
            AND DATE(s.date) = (
                SELECT DATE(MAX(s2.date))
                FROM signals s2
                LEFT JOIN companies c2 ON s2.ticker = c2.ticker
                WHERE COALESCE(c2.market, 'US') = :market
            )
        """), conn, params={"market": mkt})

    if signal:
        df = df[df["signal"] == signal.upper()]

    return _records(df)


_SORTABLE = {"date", "ticker", "company", "close", "rsi", "fair_value_upside", "signal", "target_mean_price", "market_cap", "prediction_score"}
_TICKER_RE = re.compile(r'^[A-Z0-9.\-]{1,10}$')
_SORT_COL_SQL = {
    "company":    "c.company",
    "market_cap": "f.market_cap",
}


@protected.get("/api/signals")
def signals(
    signal: Optional[str] = None,
    search: Optional[str] = None,
    months: int = Query(default=12, ge=1, le=120),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=10, le=500),
    market: Optional[str] = None,
):
    """Signals for the last N months with server-side sort and pagination."""
    _key = sort_by if sort_by in _SORTABLE else "date"
    sort_col = _SORT_COL_SQL.get(_key, f"s.{_key}")
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
    mkt = (market or "US").upper()

    conditions = [
        f"DATE(s.date) >= DATE('now', '-{months} months')",
        "COALESCE(c.market, 'US') = :market",
    ]
    params: dict = {"market": mkt}

    if signal:
        conditions.append("s.signal = :signal")
        params["signal"] = signal.upper()

    if search:
        conditions.append("(LOWER(s.ticker) LIKE :search OR LOWER(c.company) LIKE :search)")
        params["search"] = f"%{search.lower()}%"

    where = "WHERE " + " AND ".join(conditions)

    with engine.connect() as conn:
        count_row = conn.execute(text(f"""
            SELECT COUNT(*) AS total
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {where}
        """), params).fetchone()
        total = count_row[0] if count_row else 0

        df = pd.read_sql(text(f"""
            SELECT s.date, s.ticker, s.close, s.rsi,
                   s.fair_value_upside,
                   COALESCE(s.target_mean_price, ae.target_mean_price) AS target_mean_price, COALESCE(s.target_low_price, ae.target_low_price) AS target_low_price, COALESCE(s.target_high_price, ae.target_high_price) AS target_high_price,
                   s.prediction_score,
                   s.signal, c.company, c.logo_url, c.industry, c.description, c.description_short,
                   COALESCE(
                       (SELECT score FROM company_health_history
                        WHERE ticker = s.ticker AND DATE(recorded_at) <= DATE(s.date)
                        ORDER BY recorded_at DESC LIMIT 1),
                       s.health_score, h.score
                   ) AS health_score, h.reason AS health_reason,
                   f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f ON s.ticker = f.ticker
            LEFT JOIN company_health h ON s.ticker = h.ticker
            LEFT JOIN analyst_expectations ae ON s.ticker = ae.ticker
            {where}
            ORDER BY {sort_col} {direction}
            LIMIT :limit OFFSET :offset
        """), conn, params={**params, "limit": page_size, "offset": (page - 1) * page_size})

    return {
        "data": _records(df),
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": max(1, -(-total // page_size)),  # ceiling division
    }


@protected.get("/api/signals/by-tickers")
def signals_by_tickers(tickers: str = Query(..., description="Comma-separated ticker symbols")):
    """Latest signal row for each of the given tickers (used by watchlist)."""
    ticker_list = [
        t.strip().upper() for t in tickers.split(",")
        if t.strip() and _TICKER_RE.match(t.strip().upper())
    ]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="No valid tickers provided")

    placeholders = "','".join(ticker_list)
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT s.ticker, c.company, c.logo_url, c.industry,
                   c.description, c.description_short,
                   COALESCE(lp.latest_close, s.close) AS close, s.signal, s.rsi,
                   {_ANALYST_UPSIDE_SQL},
                   COALESCE(ae.target_mean_price, s.target_mean_price) AS target_mean_price, COALESCE(ae.target_low_price, s.target_low_price) AS target_low_price, COALESCE(ae.target_high_price, s.target_high_price) AS target_high_price,
                   s.prediction_score,
                   f.market_cap,
                   h.score AS health_score, h.reason AS health_reason
            FROM signals s
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM signals
                WHERE ticker IN ('{placeholders}')
                GROUP BY ticker
            ) latest ON s.ticker = latest.ticker AND s.date = latest.max_date
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
        """), conn)
    return _records(df)


# --- Signal success rate ----------------------------------------------------

@protected.get("/api/search")
def search_tickers(q: str = Query(..., min_length=1), limit: int = Query(default=10, ge=1, le=50)):
    """Full-text search across ticker and company name."""
    pattern = f"%{q.strip()}%"
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT c.ticker, c.company, c.logo_url, c.industry,
                   c.description, c.description_short,
                   s.signal, s.close, s.rsi,
                   COALESCE(ae.target_mean_price, s.target_mean_price) AS target_mean_price, COALESCE(ae.target_low_price, s.target_low_price) AS target_low_price, COALESCE(ae.target_high_price, s.target_high_price) AS target_high_price,
                   s.prediction_score,
                   f.market_cap,
                   h.score AS health_score, h.reason AS health_reason
            FROM companies c
            LEFT JOIN (
                SELECT s1.ticker, s1.signal, s1.close, s1.rsi,
                       s1.target_mean_price, s1.target_low_price, s1.target_high_price,
                       s1.prediction_score
                FROM signals s1
                INNER JOIN (
                    SELECT ticker, MAX(date) AS max_date FROM signals GROUP BY ticker
                ) s2 ON s1.ticker = s2.ticker AND s1.date = s2.max_date
            ) s ON c.ticker = s.ticker
            LEFT JOIN (
                SELECT ticker, MAX(market_cap) AS market_cap
                FROM fundamentals GROUP BY ticker
            ) f ON c.ticker = f.ticker
            LEFT JOIN company_health h ON c.ticker = h.ticker
            LEFT JOIN analyst_expectations ae ON c.ticker = ae.ticker
            WHERE c.ticker LIKE :pat OR c.company LIKE :pat
            ORDER BY
                CASE WHEN UPPER(c.ticker) = UPPER(:q) THEN 0
                     WHEN UPPER(c.ticker) LIKE UPPER(:q) || '%' THEN 1
                     ELSE 2 END,
                f.market_cap DESC NULLS LAST
            LIMIT :lim
        """), conn, params={"pat": pattern, "q": q.strip(), "lim": limit})
    return _records(df)


@protected.get("/api/signals/markers")
def signal_markers(ticker: str, months: int = Query(default=13, ge=1, le=60)):
    """Return all BUY/SELL signals for a ticker over the last N months (for chart overlay)."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT DATE(date) AS date, signal, close
            FROM signals
            WHERE ticker = :t
              AND signal IN ('BUY', 'SELL')
              AND DATE(date) >= DATE('now', :offset)
            ORDER BY date ASC
        """), conn, params={"t": ticker, "offset": f"-{months} months"})
    return df.to_dict(orient="records")


@protected.get("/api/signals/success-rate")
def signals_success_rate(months: int = Query(default=12, ge=1, le=120)):
    """BUY→SELL success rate from trade_log (US only) over the last N months."""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT tl.ticker, tl.buy_date, tl.sell_date,
                   tl.buy_price, tl.sell_price, tl.return_pct,
                   c.company, c.logo_url, f.market_cap
            FROM trade_log tl
            LEFT JOIN companies c ON tl.ticker = c.ticker
            LEFT JOIN (
                SELECT ticker, MAX(market_cap) AS market_cap
                FROM fundamentals GROUP BY ticker
            ) f ON tl.ticker = f.ticker
            WHERE DATE(tl.sell_date) >= DATE('now', '-{months} months')
              AND tl.ticker NOT LIKE '%.TA'
            ORDER BY tl.ticker, tl.sell_date
        """), conn)

    if df.empty:
        return []

    rows = []
    for ticker, grp in df.groupby("ticker", sort=False):
        meta = grp.iloc[0]
        total = len(grp)
        wins = int((grp["return_pct"] > 0).sum())
        avg_return = float(grp["return_pct"].mean()) * 100
        buy_dates = pd.to_datetime(grp["buy_date"])
        sell_dates = pd.to_datetime(grp["sell_date"])
        avg_days = float((sell_dates - buy_dates).dt.days.mean())
        rows.append({
            "ticker":         ticker,
            "company":        meta["company"],
            "logo_url":       meta["logo_url"],
            "market_cap":     int(meta["market_cap"]) if pd.notna(meta["market_cap"]) else None,
            "total_trades":   total,
            "wins":           wins,
            "success_rate":   round(wins / total * 100, 1),
            "avg_return_pct": round(avg_return, 2),
            "avg_days_held":  round(avg_days, 1),
        })

    return sorted(rows, key=lambda x: x["success_rate"], reverse=True)


# --- Live prices ------------------------------------------------------------

@protected.get("/api/prices/history")
def price_history(
    ticker: str = Query(..., description="Ticker symbol"),
    months: int = Query(default=12, ge=1, le=60),
    start: Optional[str] = Query(default=None),
    end:   Optional[str] = Query(default=None),
):
    """Daily close prices. If start/end are provided, use them; otherwise use last N months."""
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    with engine.connect() as conn:
        if start and end:
            df = pd.read_sql(text("""
                SELECT date(date) AS date, close
                FROM daily_prices
                WHERE ticker = :ticker
                  AND date >= :start
                  AND date <= :end
                  AND close IS NOT NULL
                ORDER BY date ASC
            """), conn, params={"ticker": ticker, "start": start, "end": end})
        else:
            df = pd.read_sql(text("""
                SELECT date(date) AS date, close
                FROM daily_prices
                WHERE ticker = :ticker
                  AND date >= date('now', :offset)
                  AND close IS NOT NULL
                ORDER BY date ASC
            """), conn, params={"ticker": ticker, "offset": f"-{months} months"})
    return _records(df)


@protected.get("/api/analyst-history")
def analyst_history_endpoint(
    ticker: str = Query(..., description="Ticker symbol"),
    start: Optional[str] = Query(default=None),
    end:   Optional[str] = Query(default=None),
):
    """Historical analyst targets (Low/Base/High) for a ticker from the signals table."""
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    start_date = start or "2026-01-01"
    end_date   = end   or date.today().isoformat()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT DATE(date) AS date,
                   target_mean_price, target_low_price, target_high_price
            FROM signals
            WHERE ticker = :ticker
              AND target_mean_price IS NOT NULL
              AND DATE(date) BETWEEN :start AND :end
            ORDER BY date ASC
        """), conn, params={"ticker": ticker, "start": start_date, "end": end_date})
    return _records(df)


@protected.get("/api/prices/live")
def live_prices(tickers: str = Query(..., description="Comma-separated ticker symbols")):
    """Fetch real-time prices. Handles US and IL market hours independently."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="No tickers provided")

    us_open = market_is_open()
    il_open = tase_is_open()

    active = []
    result_prices: dict = {}
    for t in ticker_list:
        if t.endswith('.TA'):
            if il_open:
                active.append(t)
            else:
                result_prices[t] = None
        else:
            if us_open:
                active.append(t)
            else:
                result_prices[t] = None

    if active:
        global _live_price_cache, _live_price_cache_ts
        now = time.time()
        cached    = {t: _live_price_cache[t] for t in active if t in _live_price_cache}
        stale     = [t for t in active if t not in _live_price_cache or now - _live_price_cache_ts > _LIVE_CACHE_TTL]
        if stale:
            fresh = fetch_live_prices(stale)
            _live_price_cache.update(fresh)
            _live_price_cache_ts = now
            cached.update(fresh)
        result_prices.update(cached)

    # market_open reflects the market relevant to the majority of requested tickers
    il_count = sum(1 for t in ticker_list if t.endswith('.TA'))
    market_open = il_open if il_count > len(ticker_list) / 2 else us_open
    return {"market_open": market_open, "prices": result_prices}


# --- Watchlists -------------------------------------------------------------

def _assert_owns_list(conn, list_id: int, user_id: str):
    """Raise 403 if the list doesn't belong to this user."""
    r = conn.execute(
        text("SELECT id FROM watchlist_lists WHERE id = :lid AND user_id = :uid"),
        {"lid": list_id, "uid": user_id},
    ).fetchone()
    if not r:
        raise HTTPException(status_code=403, detail="List not found")


_EHUD_LIST_NAME = "Ehud"


@protected.get("/api/watchlists")
def get_watchlists(user=Depends(get_current_user)):
    uid = user["id"]

    with engine.begin() as conn:
        has_lists = conn.execute(
            text("SELECT COUNT(*) FROM watchlist_lists WHERE user_id = :uid"), {"uid": uid}
        ).fetchone()[0] > 0

        if not has_lists:
            # Try to claim all unclaimed legacy lists (first user to arrive gets them all)
            claimed = conn.execute(
                text("UPDATE watchlist_lists SET user_id = :uid WHERE user_id = ''"),
                {"uid": uid},
            ).rowcount

            # New users start with no lists; they can create their own

    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT id, name FROM watchlist_lists WHERE user_id = :uid ORDER BY id"),
            conn, params={"uid": uid}
        )
    return _records(df)


@protected.post("/api/watchlists", status_code=201)
def create_watchlist(body: WatchlistCreate, user=Depends(get_current_user)):
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name cannot be empty")
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("INSERT OR IGNORE INTO watchlist_lists (user_id, name) VALUES (:uid, :name)"),
                {"uid": user["id"], "name": name},
            )
        if result.rowcount == 0:
            raise HTTPException(status_code=409, detail=f"List '{name}' already exists")
        with engine.connect() as conn:
            row = pd.read_sql(
                text("SELECT id, name FROM watchlist_lists WHERE user_id = :uid AND name = :name"),
                conn, params={"uid": user["id"], "name": name}
            )
        return row.iloc[0].to_dict()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@protected.delete("/api/watchlists/{list_id}", status_code=204)
def delete_watchlist(list_id: int, user=Depends(get_current_user)):
    with engine.begin() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        conn.execute(text("DELETE FROM watchlist WHERE list_id = :lid"), {"lid": list_id})
        conn.execute(text("DELETE FROM watchlist_lists WHERE id = :lid AND user_id = :uid"), {"lid": list_id, "uid": user["id"]})


@protected.get("/api/watchlists/{list_id}/tickers")
def get_watchlist_tickers(list_id: int, user=Depends(get_current_user)):
    with engine.connect() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        df = pd.read_sql(
            text("SELECT id, ticker, note FROM watchlist WHERE list_id = :lid ORDER BY id"),
            conn, params={"lid": list_id}
        )
    return _records(df)


@protected.post("/api/watchlists/{list_id}/tickers", status_code=201)
def add_ticker(list_id: int, body: TickerAdd, user=Depends(get_current_user)):
    ticker = body.ticker.strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    try:
        with engine.begin() as conn:
            _assert_owns_list(conn, list_id, user["id"])
            result = conn.execute(
                text("INSERT OR IGNORE INTO watchlist (list_id, ticker, note) VALUES (:lid, :ticker, :note)"),
                {"lid": list_id, "ticker": ticker, "note": body.note},
            )
        if result.rowcount == 0:
            raise HTTPException(status_code=409, detail=f"{ticker} already in this watchlist")
        return {"ticker": ticker, "note": body.note}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@protected.patch("/api/watchlists/{list_id}/tickers/{ticker}")
def update_ticker_note(list_id: int, ticker: str, body: NoteUpdate, user=Depends(get_current_user)):
    with engine.begin() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        conn.execute(
            text("UPDATE watchlist SET note = :note WHERE list_id = :lid AND ticker = :ticker"),
            {"note": body.note, "lid": list_id, "ticker": ticker.upper()},
        )
    return {"ok": True}


@protected.delete("/api/watchlists/{list_id}/tickers/{ticker}", status_code=204)
def remove_ticker(list_id: int, ticker: str, user=Depends(get_current_user)):
    with engine.begin() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        conn.execute(
            text("DELETE FROM watchlist WHERE list_id = :lid AND ticker = :ticker"),
            {"lid": list_id, "ticker": ticker.upper()},
        )


# --- Watchlist holdings ------------------------------------------------------

class HoldingCreate(BaseModel):
    ticker: str
    quantity: float
    buy_price: float
    buy_date: str

@protected.get("/api/watchlists/{list_id}/holdings")
def get_holdings(list_id: int, user=Depends(get_current_user)):
    with engine.connect() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        rows = conn.execute(
            text("SELECT id, ticker, quantity, buy_price, buy_date FROM watchlist_holdings WHERE watchlist_id = :lid ORDER BY ticker, buy_date"),
            {"lid": list_id},
        ).fetchall()
    return [{"id": r[0], "ticker": r[1], "quantity": r[2], "buy_price": r[3], "buy_date": r[4]} for r in rows]

@protected.post("/api/watchlists/{list_id}/holdings", status_code=201)
def add_holding(list_id: int, body: HoldingCreate, user=Depends(get_current_user)):
    with engine.begin() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        result = conn.execute(
            text("INSERT INTO watchlist_holdings (watchlist_id, ticker, quantity, buy_price, buy_date) VALUES (:lid, :ticker, :qty, :price, :date)"),
            {"lid": list_id, "ticker": body.ticker.upper(), "qty": body.quantity, "price": body.buy_price, "date": body.buy_date},
        )
    return {"id": result.lastrowid}

@protected.delete("/api/watchlists/{list_id}/holdings/{holding_id}", status_code=204)
def delete_holding(list_id: int, holding_id: int, user=Depends(get_current_user)):
    with engine.begin() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        conn.execute(
            text("DELETE FROM watchlist_holdings WHERE id = :hid AND watchlist_id = :lid"),
            {"hid": holding_id, "lid": list_id},
        )


# --- Historical trades ------------------------------------------------------

@protected.get("/api/trades")
def historical_trades(
    start: Optional[str] = None,
    end: Optional[str] = None,
    market: Optional[str] = None,
):
    """BUY→SELL trade pairs from pre-built trade_log table."""
    mkt = (market or "US").upper()
    conditions = ["COALESCE(c.market, 'US') = :market"]
    params: dict = {"market": mkt}
    if start:
        conditions.append("DATE(tl.sell_date) >= :start")
        params["start"] = start
    if end:
        conditions.append("DATE(tl.sell_date) <= :end")
        params["end"] = end

    where = "WHERE " + " AND ".join(conditions)

    def _v(v):
        return None if (isinstance(v, float) and math.isnan(v)) else v

    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT tl.ticker, tl.buy_date, tl.buy_price, tl.sell_date, tl.sell_price, tl.return_pct,
                   c.company, c.logo_url, c.industry, c.description, c.description_short,
                   f.market_cap,
                   COALESCE(
                       (SELECT score FROM company_health_history
                        WHERE ticker = tl.ticker AND DATE(recorded_at) <= DATE(tl.buy_date)
                        ORDER BY recorded_at DESC LIMIT 1),
                       sb.health_score, h.score
                   ) AS health_score, h.reason AS health_reason
            FROM trade_log tl
            LEFT JOIN companies c ON tl.ticker = c.ticker
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f
                ON tl.ticker = f.ticker
            LEFT JOIN company_health h ON tl.ticker = h.ticker
            LEFT JOIN signals sb ON sb.ticker = tl.ticker AND DATE(sb.date) = DATE(tl.buy_date)
            {where}
            ORDER BY tl.ticker, tl.buy_date
        """), conn, params=params)

    def _num(v, decimals=2):
        """Return rounded float or None for NaN/None values."""
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return round(float(v), decimals)

    # Group by ticker
    ticker_trades: dict = {}
    for ticker, grp in df.groupby("ticker"):
        pairs = []
        for _, row in grp.iterrows():
            raw_ret    = row["return_pct"]
            ret        = _num(raw_ret * 100) if raw_ret is not None and not (isinstance(raw_ret, float) and math.isnan(raw_ret)) else None
            buy_price  = _num(row["buy_price"])
            sell_price = _num(row["sell_price"])
            buy_date   = str(row["buy_date"])[:10]
            sell_date  = str(row["sell_date"])[:10] if _v(row["sell_date"]) is not None else None
            days_held  = int((pd.to_datetime(row["sell_date"]) - pd.to_datetime(row["buy_date"])).days) \
                         if sell_date is not None else None
            pairs.append({
                "buy_date":   buy_date,
                "sell_date":  sell_date,
                "buy_price":  buy_price,
                "sell_price": sell_price,
                "return_pct": ret,
                "days_held":  days_held,
                "result":     "Win" if ret is not None and ret > 0 else ("Loss" if ret is not None else "Open"),
            })

        closed_pairs = [p for p in pairs if p["result"] != "Open"]
        if not closed_pairs:
            continue
        mc    = grp.iloc[0].get("market_cap")
        score = grp.iloc[0].get("health_score")
        wins  = sum(1 for p in closed_pairs if p["result"] == "Win")
        avg_ret  = sum(p["return_pct"] for p in closed_pairs if p["return_pct"] is not None) / len(closed_pairs)
        days_list = [p["days_held"] for p in closed_pairs if p["days_held"] is not None]
        avg_days = sum(days_list) / len(days_list) if days_list else 0

        buy_dates  = sorted(p["buy_date"]  for p in closed_pairs if p["buy_date"])
        sell_dates = sorted(p["sell_date"] for p in closed_pairs if p["sell_date"])

        ticker_trades[ticker] = {
            "ticker":            ticker,
            "company":           _v(grp.iloc[0]["company"]),
            "logo_url":          _v(grp.iloc[0]["logo_url"]),
            "industry":          _v(grp.iloc[0]["industry"]),
            "description":       _v(grp.iloc[0]["description"]),
            "description_short": _v(grp.iloc[0]["description_short"]),
            "market_cap":        int(mc) if mc is not None and not (isinstance(mc, float) and math.isnan(mc)) else None,
            "health_score":      int(score) if score is not None and not (isinstance(score, float) and math.isnan(score)) else None,
            "health_reason":     _v(grp.iloc[0]["health_reason"]),
            "trade_count":       len(closed_pairs),
            "win_count":         wins,
            "avg_return":        round(avg_ret, 2),
            "avg_days":          round(avg_days, 1),
            "first_buy_date":    buy_dates[0]  if buy_dates  else None,
            "last_sell_date":    sell_dates[-1] if sell_dates else None,
            "trades":            pairs,
        }

    # Enrich with organic_yield over date range
    if ticker_trades and start and end:
        tickers = list(ticker_trades.keys())
        placeholders = ", ".join([f":t{i}" for i in range(len(tickers))])
        p = {f"t{i}": t for i, t in enumerate(tickers)}
        p["start"] = start
        p["end"]   = end
        with engine.connect() as conn:
            df_org = pd.read_sql(text(f"""
                WITH bounds AS (
                    SELECT ticker,
                           MAX(CASE WHEN date(date) <= :start THEN date(date) END) AS fd,
                           MAX(CASE WHEN date(date) <= :end   THEN date(date) END) AS ld
                    FROM daily_prices
                    WHERE date(date) >= date(:start, '-30 days')
                      AND date(date) <= :end
                      AND ticker IN ({placeholders})
                    GROUP BY ticker
                )
                SELECT b.ticker, d1.close AS first_close, d2.close AS last_close
                FROM bounds b
                JOIN daily_prices d1 ON d1.ticker = b.ticker AND date(d1.date) = b.fd
                JOIN daily_prices d2 ON d2.ticker = b.ticker AND date(d2.date) = b.ld
            """), conn, params=p)
        for _, org_row in df_org.iterrows():
            t = org_row["ticker"]
            fc, lc = org_row["first_close"], org_row["last_close"]
            if t in ticker_trades and fc and float(fc) > 0:
                ticker_trades[t]["organic_yield"] = round(float((lc - fc) / fc * 100), 2)

    # Enrich with current signal
    if ticker_trades:
        tickers_list = list(ticker_trades.keys())
        ph = ", ".join([f":h{i}" for i in range(len(tickers_list))])
        hp = {f"h{i}": t for i, t in enumerate(tickers_list)}
        with engine.connect() as conn:
            df_curr = pd.read_sql(text(f"""
                SELECT s.ticker, s.signal AS current_signal
                FROM signals s
                JOIN (SELECT ticker, MAX(date) AS md FROM signals
                      WHERE ticker IN ({ph}) GROUP BY ticker) l
                  ON s.ticker = l.ticker AND s.date = l.md
            """), conn, params=hp)
        for _, sr in df_curr.iterrows():
            t = sr["ticker"]
            if t in ticker_trades:
                ticker_trades[t]["current_signal"] = sr["current_signal"]

    return list(ticker_trades.values())


@protected.get("/api/trades/open")
def open_trades(market: Optional[str] = None):
    """Tickers with a BUY signal and no SELL since — currently open positions."""
    mkt = (market or "US").upper()

    def _v(v):
        return None if (isinstance(v, float) and math.isnan(v)) else v

    with engine.connect() as conn:
        # Step 1: fast open-position lookup via covering index (signal, ticker, date)
        open_rows = conn.execute(text("""
            WITH last_sells AS (
                SELECT ticker, MAX(date) AS d FROM signals WHERE signal='SELL' GROUP BY ticker
            ),
            buys AS (
                SELECT b.ticker, MIN(b.date) AS d
                FROM signals b
                LEFT JOIN last_sells ls ON b.ticker = ls.ticker
                WHERE b.signal = 'BUY'
                AND (ls.d IS NULL OR b.date > ls.d)
                GROUP BY b.ticker
            ),
            lasts AS (SELECT ticker, MAX(date) AS d FROM signals GROUP BY ticker)
            SELECT b.ticker, b.d AS buy_date, l.d AS last_date
            FROM buys b
            JOIN lasts l ON b.ticker = l.ticker
        """)).fetchall()

        if not open_rows:
            return []

        buy_date_map  = {r[0]: r[1] for r in open_rows}
        last_date_map = {r[0]: r[2] for r in open_rows}
        tickers = list(buy_date_map.keys())
        ph = ", ".join([f":t{i}" for i in range(len(tickers))])
        tp = {f"t{i}": t for i, t in enumerate(tickers)}

        # Step 2: point lookups via CTE VALUES (ticker+date index hit per row)
        buy_vals = ", ".join(f"('{t}', '{buy_date_map[t]}')"  for t in tickers)
        cur_vals = ", ".join(f"('{t}', '{last_date_map[t]}')" for t in tickers)

        buy_prices = {r[0]: r[1] for r in conn.execute(text(f"""
            WITH pairs(t, d) AS (VALUES {buy_vals})
            SELECT s.ticker, s.close FROM signals s
            JOIN pairs ON s.ticker = pairs.t AND s.date = pairs.d
        """)).fetchall()}

        cur_signals = {r[0]: r[1] for r in conn.execute(text(f"""
            WITH pairs(t, d) AS (VALUES {cur_vals})
            SELECT s.ticker, s.signal FROM signals s
            JOIN pairs ON s.ticker = pairs.t AND s.date = pairs.d
        """)).fetchall()}

        # Step 4: latest price per ticker (IN query, ticker_date index)
        latest_prices = {r[0]: r[1] for r in conn.execute(text(f"""
            SELECT dp.ticker, dp.close FROM daily_prices dp
            JOIN (SELECT ticker, MAX(date) AS md FROM daily_prices WHERE ticker IN ({ph}) GROUP BY ticker) lp
                ON dp.ticker = lp.ticker AND dp.date = lp.md
        """), tp).fetchall()}

        # Step 5: company info + health + market cap (IN query, small result)
        df_meta = pd.read_sql(text(f"""
            SELECT c.ticker, c.company, c.logo_url, c.industry, c.market,
                   c.description_short, f.market_cap,
                   h.score AS health_score, h.reason AS health_reason
            FROM companies c
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f
                ON c.ticker = f.ticker
            LEFT JOIN company_health h ON c.ticker = h.ticker
            WHERE c.ticker IN ({ph})
        """), conn, params=tp)
        meta = {r["ticker"]: r for _, r in df_meta.iterrows()}

    result = []
    for ticker in tickers:
        m = meta.get(ticker)
        if m is None:
            continue
        if (m.get("market") or "US").upper() != mkt:
            continue
        cur_sig = cur_signals.get(ticker, "HOLD")
        if cur_sig not in ("BUY", "HOLD"):
            continue

        buy_price     = buy_prices.get(ticker)
        current_price = latest_prices.get(ticker)
        buy_date_raw  = buy_date_map[ticker]
        buy_date_str  = str(buy_date_raw)[:10]
        buy_date_obj  = pd.to_datetime(buy_date_raw).date()
        days_held     = (date.today() - buy_date_obj).days
        unrealized    = round(float((current_price - buy_price) / buy_price * 100), 2) \
                        if buy_price and current_price and float(buy_price) > 0 else None
        mc    = m.get("market_cap")
        score = m.get("health_score")
        result.append({
            "ticker":            ticker,
            "company":           _v(m.get("company")),
            "logo_url":          _v(m.get("logo_url")),
            "industry":          _v(m.get("industry")),
            "description_short": _v(m.get("description_short")),
            "market_cap":        int(mc) if mc is not None and not (isinstance(mc, float) and math.isnan(mc)) else None,
            "buy_date":          buy_date_str,
            "buy_price":         round(float(buy_price), 2) if buy_price is not None else None,
            "current_price":     round(float(current_price), 2) if current_price is not None else None,
            "days_held":         days_held,
            "unrealized_pct":    unrealized,
            "current_signal":    cur_sig,
            "health_score":      int(score) if score is not None and not (isinstance(score, float) and math.isnan(score)) else None,
            "health_reason":     _v(m.get("health_reason")),
        })

    result.sort(key=lambda x: x["buy_date"], reverse=True)
    return result


# --- News & analyst endpoints -----------------------------------------------

@protected.get("/api/news")
def stock_news_endpoint(ticker: str = Query(...), limit: int = Query(default=5, ge=1, le=20)):
    from data import fmp as _fmp
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    return _fmp.stock_news(ticker, limit)


@protected.get("/api/earnings")
def earnings_endpoint(ticker: str = Query(...)):
    from data import fmp as _fmp
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    return _fmp.earnings_calendar(ticker)


@protected.get("/api/analyst-changes")
def analyst_changes_endpoint(ticker: str = Query(...), limit: int = Query(default=8, ge=1, le=20)):
    from data import fmp as _fmp
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    return _fmp.analyst_upgrades(ticker, limit)


# --- Portfolio holdings ------------------------------------------------------

@protected.get("/api/portfolio/holdings")
def portfolio_holdings(user=Depends(get_current_user)):
    """All holdings across all user's watchlists, aggregated per ticker."""
    uid = user["id"]
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT wh.ticker,
                   SUM(wh.quantity) AS total_qty,
                   SUM(wh.quantity * wh.buy_price) AS total_cost,
                   MIN(wh.buy_date) AS first_buy_date
            FROM watchlist_holdings wh
            JOIN watchlist_lists wl ON wh.watchlist_id = wl.id
            WHERE wl.user_id = :uid
            GROUP BY wh.ticker
            ORDER BY total_cost DESC
        """), {"uid": uid}).fetchall()

        if not rows:
            return []

        tickers = [r[0] for r in rows]
        ph = ", ".join([f":t{i}" for i in range(len(tickers))])
        tp = {f"t{i}": t for i, t in enumerate(tickers)}

        meta = {r[0]: r for r in conn.execute(text(f"""
            SELECT c.ticker, c.company, c.logo_url, c.industry,
                   f.market_cap,
                   lp.latest_close
            FROM companies c
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f
                ON c.ticker = f.ticker
            LEFT JOIN (
                SELECT p1.ticker, p1.close AS latest_close
                FROM daily_prices p1
                INNER JOIN (SELECT ticker, MAX(date) AS max_date FROM daily_prices GROUP BY ticker) p2
                    ON p1.ticker = p2.ticker AND p1.date = p2.max_date
            ) lp ON c.ticker = lp.ticker
            WHERE c.ticker IN ({ph})
        """), tp).fetchall()}

    result = []
    for r in rows:
        ticker, total_qty, total_cost, first_buy_date = r
        avg_price = (total_cost / total_qty) if total_qty else None
        m = meta.get(ticker)
        mc = m[4] if m else None
        latest_close = m[5] if m else None
        result.append({
            "ticker": ticker,
            "company": m[1] if m else None,
            "logo_url": m[2] if m else None,
            "industry": m[3] if m else None,
            "market_cap": int(mc) if mc is not None and not (isinstance(mc, float) and math.isnan(mc)) else None,
            "total_qty": total_qty,
            "total_cost": round(total_cost, 2) if total_cost is not None else None,
            "avg_price": round(avg_price, 4) if avg_price is not None else None,
            "latest_close": round(float(latest_close), 4) if latest_close is not None else None,
            "first_buy_date": first_buy_date,
        })
    return result


# --- Portfolio performance ---------------------------------------------------

@protected.get("/api/portfolio/performance")
def portfolio_performance(user=Depends(get_current_user)):
    """Weekly yield %: user portfolio + Vesign equal-weight model (last 52 weeks)."""
    from datetime import date as _date, timedelta
    from collections import defaultdict

    uid = user["id"]
    today = _date.today()
    start_date = today - timedelta(weeks=52)
    weeks = [start_date + timedelta(weeks=i) for i in range(53)]

    with engine.connect() as conn:
        # User US holdings — individual lots with buy_date
        user_rows = conn.execute(text("""
            SELECT wh.ticker, wh.quantity, wh.buy_price, DATE(wh.buy_date) AS buy_date
            FROM watchlist_holdings wh
            JOIN watchlist_lists wl ON wh.watchlist_id = wl.id
            WHERE wl.user_id = :uid AND wh.ticker NOT LIKE '%.TA'
              AND wh.quantity > 0
            ORDER BY wh.buy_date
        """), {"uid": uid}).fetchall()

        if not user_rows:
            return []

        # Vesign: first BUY signal per ticker in the 52-week window
        signal_rows = conn.execute(text("""
            WITH first_buy AS (
                SELECT ticker, MIN(date) AS signal_date
                FROM signals
                WHERE signal = 'BUY' AND date >= :start AND ticker NOT LIKE '%.TA'
                GROUP BY ticker
            )
            SELECT fb.ticker, DATE(fb.signal_date) AS signal_date, s.close AS entry_price
            FROM first_buy fb
            JOIN signals s ON s.ticker = fb.ticker AND s.date = fb.signal_date AND s.signal = 'BUY'
            WHERE s.close IS NOT NULL AND s.close > 0
        """), {"start": start_date.isoformat()}).fetchall()

        # Matching closed trades
        sig_tickers = [r[0] for r in signal_rows]
        if sig_tickers:
            sph = ", ".join([f":st{i}" for i in range(len(sig_tickers))])
            stp = {f"st{i}": t for i, t in enumerate(sig_tickers)}
            trade_rows = conn.execute(text(f"""
                SELECT ticker, DATE(sell_date) AS sell_date, return_pct
                FROM trade_log
                WHERE buy_date >= :start AND ticker IN ({sph})
            """), {"start": (start_date - timedelta(days=7)).isoformat(), **stp}).fetchall()
        else:
            trade_rows = []

        all_tickers = list({r[0] for r in user_rows} | {r[0] for r in signal_rows})
        ph = ", ".join([f":t{i}" for i in range(len(all_tickers))])
        tp = {f"t{i}": t for i, t in enumerate(all_tickers)}
        # Extra buffer before window for lots bought just before start_date
        cutoff_buf = (start_date - timedelta(days=60)).isoformat()

        price_rows = conn.execute(text(f"""
            SELECT ticker, DATE(date) AS d, close
            FROM daily_prices
            WHERE ticker IN ({ph}) AND date >= :cutoff
            ORDER BY ticker, date
        """), {**tp, "cutoff": cutoff_buf}).fetchall()

    price_map = defaultdict(list)
    for ticker, d_str, close in price_rows:
        try:
            d_obj = _date.fromisoformat(str(d_str)[:10])
            price_map[ticker].append((d_obj, float(close)))
        except Exception:
            pass

    def get_price_at(ticker, target):
        for d_obj, close in reversed(price_map.get(ticker, [])):
            if d_obj <= target:
                return close
        return None

    trade_map = {}
    for ticker, sell_d_str, return_pct in trade_rows:
        try:
            trade_map[ticker] = (_date.fromisoformat(str(sell_d_str)[:10]), float(return_pct))
        except Exception:
            pass

    vesign_positions = []
    for ticker, sig_d_str, entry_price in signal_rows:
        try:
            vesign_positions.append((_date.fromisoformat(str(sig_d_str)[:10]), ticker, float(entry_price)))
        except Exception:
            pass

    # Parse lots with buy dates
    user_lots = []
    for ticker, qty, buy_price, buy_date_str in user_rows:
        if qty is None or buy_price is None:
            continue
        try:
            buy_d = _date.fromisoformat(str(buy_date_str)[:10])
        except Exception:
            continue
        # Base price: price at week0 for pre-existing lots, actual buy_price for lots entered within window
        if buy_d <= weeks[0]:
            base_p = get_price_at(ticker, weeks[0])
            if base_p is None:
                base_p = float(buy_price)  # fallback
        else:
            base_p = float(buy_price)
        user_lots.append((ticker, float(qty), buy_d, base_p))

    result = []
    for week_date in weeks:
        # Only include lots purchased by this week; use each lot's own base price
        total_val = 0.0
        total_base = 0.0
        for ticker, qty, buy_d, base_p in user_lots:
            if buy_d > week_date:
                continue  # not yet purchased
            p = get_price_at(ticker, week_date)
            if p is not None:
                total_val += qty * p
                total_base += qty * base_p
        port_yield = round((total_val / total_base - 1) * 100, 2) if total_base > 0 else None

        # Vesign equal-weight: avg return of all active positions
        active_returns = []
        for sig_date, ticker, entry_price in vesign_positions:
            if sig_date > week_date:
                continue
            if ticker in trade_map:
                sell_d, rp = trade_map[ticker]
                if sell_d <= week_date:
                    active_returns.append(rp * 100)
                    continue
            p = get_price_at(ticker, week_date)
            if p is not None:
                active_returns.append((p / entry_price - 1) * 100)
        vesign_yield = round(sum(active_returns) / len(active_returns), 2) if active_returns else None

        result.append({"week": week_date.isoformat(), "portfolio": port_yield, "vesign": vesign_yield})

    return result


# --- Pipeline ---------------------------------------------------------------

@protected.post("/api/pipeline/run")
def run_pipeline():
    global _pipeline_proc, _pipeline_log_file

    if _pipeline_proc is not None and _pipeline_proc.poll() is None:
        raise HTTPException(status_code=409, detail="Pipeline already running")

    log_path = tempfile.mktemp(suffix="_pipeline.log")
    log_f = open(log_path, "w", buffering=1)
    _pipeline_proc = subprocess.Popen(
        [sys.executable, "-c",
         "import sys, os; sys.path.insert(0, os.getcwd()); "
         "from production.run_daily import run_daily; run_daily()"],
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=_APP_ROOT,
        text=True,
    )
    _pipeline_log_file = log_path
    return {"status": "started"}


@protected.get("/api/pipeline/status")
def pipeline_status():
    global _pipeline_proc, _pipeline_log_file

    if _pipeline_proc is None:
        return {"status": "idle"}

    ret = _pipeline_proc.poll()
    log_tail = ""
    if _pipeline_log_file and os.path.exists(_pipeline_log_file):
        with open(_pipeline_log_file) as f:
            content = f.read()
        log_tail = "\n".join(content.strip().splitlines()[-10:])

    if ret is None:
        return {"status": "running", "log": log_tail}

    return {"status": "success" if ret == 0 else "error", "exit_code": ret, "log": log_tail}


app.include_router(protected)

# ---------------------------------------------------------------------------
# SPA static file serving (production)
# Serve React build from FastAPI so only one process is needed.
# API routes defined above take precedence; this catches everything else.
# Only active when frontend/dist exists (i.e. after `npm run build`).
# ---------------------------------------------------------------------------

_DIST = os.path.join(_APP_ROOT, "frontend", "dist")

if os.path.isdir(_DIST):
    _ASSETS = os.path.join(_DIST, "assets")
    if os.path.isdir(_ASSETS):
        app.mount("/assets", StaticFiles(directory=_ASSETS), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def serve_spa(full_path: str):
        # Serve any existing static file (e.g. vite.svg, favicon.ico)
        candidate = os.path.join(_DIST, full_path)
        if full_path and os.path.isfile(candidate):
            return FileResponse(candidate)
        return FileResponse(os.path.join(_DIST, "index.html"))
