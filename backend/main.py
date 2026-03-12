import math
import os
import re
import smtplib
import subprocess
import sys
import tempfile
from email.mime.text import MIMEText
from datetime import datetime, time as dt_time, UTC, date
from typing import Optional

import pandas as pd
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


def _send_access_request_email(requester_email: str, message: str):
    admin_email  = os.getenv("ADMIN_EMAIL")
    smtp_host    = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port    = int(os.getenv("SMTP_PORT", "587"))
    smtp_user    = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    if not all([admin_email, smtp_user, smtp_password]):
        return  # not configured — skip silently
    body = f"New access request on Vesign:\n\nEmail: {requester_email}\nMessage: {message or '(none)'}"
    msg = MIMEText(body)
    msg["Subject"] = f"[Vesign] Access request from {requester_email}"
    msg["From"] = smtp_user
    msg["To"] = admin_email
    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=8) as srv:
            srv.starttls()
            srv.login(smtp_user, smtp_password)
            srv.send_message(msg)
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
                name TEXT NOT NULL UNIQUE
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


_init_tables()

# ---------------------------------------------------------------------------
# Market helpers
# ---------------------------------------------------------------------------

def market_is_open() -> bool:
    et = pytz.timezone("US/Eastern")
    now = datetime.now(UTC).astimezone(et)
    return now.weekday() < 5 and dt_time(9, 30) <= now.time() <= dt_time(16, 0)


def tase_is_open() -> bool:
    """TASE is open Sunday–Thursday, 09:59–17:29 IST (Asia/Jerusalem)."""
    il = pytz.timezone("Asia/Jerusalem")
    now = datetime.now(UTC).astimezone(il)
    dow = now.weekday()  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 6=Sun
    is_tase_day = dow <= 3 or dow == 6
    return is_tase_day and dt_time(9, 59) <= now.time() <= dt_time(17, 29)


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
        return {"is_open": tase_is_open()}
    return {"is_open": market_is_open()}


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
                   s.signal, c.company, c.logo_url, c.industry, c.description, c.description_short, h.score AS health_score, h.reason AS health_reason,
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
                   COALESCE(ae.target_mean_price, s.target_mean_price) AS target_mean_price, COALESCE(ae.target_low_price, s.target_low_price) AS target_low_price, COALESCE(ae.target_high_price, s.target_high_price) AS target_high_price,
                   s.prediction_score,
                   s.signal, c.company, c.logo_url, c.industry, c.description, c.description_short, h.score AS health_score, h.reason AS health_reason,
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
    """BUY→SELL success rate aggregated per company over the last N months."""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT s.ticker, s.date, s.signal, s.close,
                   c.company, c.logo_url, f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
            WHERE s.signal IN ('BUY', 'SELL')
            AND DATE(s.date) >= DATE('now', '-{months} months')
            ORDER BY s.ticker, s.date
        """), conn)

    df["date"] = pd.to_datetime(df["date"])

    rows = []
    for ticker, grp in df.groupby("ticker", sort=False):
        meta = grp.iloc[0]
        trades = []
        open_trade = None
        for _, row in grp.iterrows():
            if row["signal"] == "BUY" and open_trade is None:
                open_trade = row
            elif row["signal"] == "SELL" and open_trade is not None:
                ret = (row["close"] - open_trade["close"]) / open_trade["close"]
                trades.append({
                    "return_pct": ret * 100,
                    "days_held":  (row["date"] - open_trade["date"]).days,
                    "win":        ret > 0,
                })
                open_trade = None

        if not trades:
            continue

        total = len(trades)
        wins  = sum(t["win"] for t in trades)
        rows.append({
            "ticker":         ticker,
            "company":        meta["company"],
            "logo_url":       meta["logo_url"],
            "market_cap":     int(meta["market_cap"]) if pd.notna(meta["market_cap"]) else None,
            "total_trades":   total,
            "wins":           wins,
            "success_rate":   round(wins / total * 100, 1),
            "avg_return_pct": round(sum(t["return_pct"] for t in trades) / total, 2),
            "avg_days_held":  round(sum(t["days_held"]  for t in trades) / total, 1),
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
        result_prices.update(fetch_live_prices(active))

    # market_open reflects the market relevant to the majority of requested tickers
    il_count = sum(1 for t in ticker_list if t.endswith('.TA'))
    market_open = il_open if il_count > len(ticker_list) / 2 else us_open
    return {"market_open": market_open, "prices": result_prices}


# --- Watchlists -------------------------------------------------------------

@protected.get("/api/watchlists")
def get_watchlists():
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT id, name FROM watchlist_lists ORDER BY id"), conn)
    return _records(df)


@protected.post("/api/watchlists", status_code=201)
def create_watchlist(body: WatchlistCreate):
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name cannot be empty")
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("INSERT OR IGNORE INTO watchlist_lists (name) VALUES (:name)"),
                {"name": name},
            )
        if result.rowcount == 0:
            raise HTTPException(status_code=409, detail=f"List '{name}' already exists")
        with engine.connect() as conn:
            row = pd.read_sql(
                text("SELECT id, name FROM watchlist_lists WHERE name = :name"),
                conn, params={"name": name}
            )
        return row.iloc[0].to_dict()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@protected.delete("/api/watchlists/{list_id}", status_code=204)
def delete_watchlist(list_id: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM watchlist WHERE list_id = :lid"), {"lid": list_id})
        conn.execute(text("DELETE FROM watchlist_lists WHERE id = :lid"), {"lid": list_id})


@protected.get("/api/watchlists/{list_id}/tickers")
def get_watchlist_tickers(list_id: int):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT id, ticker, note FROM watchlist WHERE list_id = :lid ORDER BY id"),
            conn, params={"lid": list_id}
        )
    return _records(df)


@protected.post("/api/watchlists/{list_id}/tickers", status_code=201)
def add_ticker(list_id: int, body: TickerAdd):
    ticker = body.ticker.strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    try:
        with engine.begin() as conn:
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
def update_ticker_note(list_id: int, ticker: str, body: NoteUpdate):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE watchlist SET note = :note WHERE list_id = :lid AND ticker = :ticker"),
            {"note": body.note, "lid": list_id, "ticker": ticker.upper()},
        )
    return {"ok": True}


@protected.delete("/api/watchlists/{list_id}/tickers/{ticker}", status_code=204)
def remove_ticker(list_id: int, ticker: str):
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM watchlist WHERE list_id = :lid AND ticker = :ticker"),
            {"lid": list_id, "ticker": ticker.upper()},
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
        conditions.append("DATE(tl.buy_date) >= :start")
        params["start"] = start
    if end:
        conditions.append("DATE(tl.buy_date) <= :end")
        params["end"] = end

    where = "WHERE " + " AND ".join(conditions)

    def _v(v):
        return None if (isinstance(v, float) and math.isnan(v)) else v

    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT tl.ticker, tl.buy_date, tl.buy_price, tl.sell_date, tl.sell_price, tl.return_pct,
                   c.company, c.logo_url, c.industry, c.description, c.description_short,
                   f.market_cap, h.score AS health_score, h.reason AS health_reason
            FROM trade_log tl
            LEFT JOIN companies c ON tl.ticker = c.ticker
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f
                ON tl.ticker = f.ticker
            LEFT JOIN company_health h ON tl.ticker = h.ticker
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
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                s.ticker,
                s.date  AS buy_date,
                s.close AS buy_price,
                cur.signal AS current_signal,
                lp.latest_close AS current_price,
                c.company, c.logo_url, c.industry,
                f.market_cap,
                h.score AS health_score
            FROM signals s
            INNER JOIN (
                SELECT ticker, MAX(date) AS last_buy_date
                FROM signals WHERE signal = 'BUY'
                GROUP BY ticker
            ) lb ON s.ticker = lb.ticker AND s.date = lb.last_buy_date
            LEFT JOIN (
                SELECT ticker, MAX(date) AS last_sell_date
                FROM signals WHERE signal = 'SELL'
                GROUP BY ticker
            ) ls ON s.ticker = ls.ticker
            INNER JOIN (
                SELECT s2.ticker, s2.signal
                FROM signals s2
                INNER JOIN (SELECT ticker, MAX(date) AS md FROM signals GROUP BY ticker) mx
                    ON s2.ticker = mx.ticker AND s2.date = mx.md
            ) cur ON s.ticker = cur.ticker
            LEFT JOIN companies c ON s.ticker = c.ticker
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f
                ON s.ticker = f.ticker
            LEFT JOIN company_health h ON s.ticker = h.ticker
            LEFT JOIN (
                SELECT p1.ticker, p1.close AS latest_close
                FROM daily_prices p1
                INNER JOIN (SELECT ticker, MAX(date) AS max_date FROM daily_prices GROUP BY ticker) p2
                    ON p1.ticker = p2.ticker AND p1.date = p2.max_date
            ) lp ON s.ticker = lp.ticker
            WHERE (ls.last_sell_date IS NULL OR lb.last_buy_date > ls.last_sell_date)
            AND cur.signal IN ('BUY', 'HOLD')
            AND COALESCE(c.market, 'US') = :market
            ORDER BY s.date DESC
        """), conn, params={"market": mkt})

    def _v(v):
        return None if (isinstance(v, float) and math.isnan(v)) else v

    result = []
    for _, row in df.iterrows():
        buy_price     = row["buy_price"]
        current_price = row["current_price"]
        unrealized    = round(float((current_price - buy_price) / buy_price * 100), 2) \
                        if buy_price and current_price and float(buy_price) > 0 else None
        buy_date = pd.to_datetime(row["buy_date"]).date().isoformat() if row["buy_date"] is not None else None
        days_held = (date.today() - pd.to_datetime(row["buy_date"]).date()).days if row["buy_date"] is not None else None
        mc = row["market_cap"]
        score = row["health_score"]
        result.append({
            "ticker":          row["ticker"],
            "company":         _v(row["company"]),
            "logo_url":        _v(row["logo_url"]),
            "industry":        _v(row["industry"]),
            "market_cap":      int(mc) if mc is not None and not (isinstance(mc, float) and math.isnan(mc)) else None,
            "buy_date":        buy_date,
            "buy_price":       round(float(buy_price), 2) if buy_price is not None else None,
            "current_price":   round(float(current_price), 2) if current_price is not None else None,
            "days_held":       days_held,
            "unrealized_pct":  unrealized,
            "current_signal":  _v(row["current_signal"]),
            "health_score":    int(score) if score is not None and not (isinstance(score, float) and math.isnan(score)) else None,
        })
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
