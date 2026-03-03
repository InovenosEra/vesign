import math
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, time as dt_time, UTC
from typing import Optional

import pandas as pd
import pytz
import yfinance as yf
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text, event as sa_event
from sqlalchemy.pool import NullPool

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


def _init_tables():
    with engine.begin() as conn:
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


_init_tables()

# ---------------------------------------------------------------------------
# Market helpers
# ---------------------------------------------------------------------------

def market_is_open() -> bool:
    et = pytz.timezone("US/Eastern")
    now = datetime.now(UTC).astimezone(et)
    return now.weekday() < 5 and dt_time(9, 30) <= now.time() <= dt_time(16, 0)


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
    prices: dict = {}
    try:
        query = tickers[0] if len(tickers) == 1 else tickers
        raw = yf.download(query, period="1d", interval="1m", progress=False,
                          auto_adjust=True, group_by="ticker" if len(tickers) > 1 else "column")

        if len(tickers) == 1:
            series = _extract_close_series(raw)
            prices[tickers[0]] = float(series.iloc[-1]) if not series.empty else None
        else:
            # group_by="ticker" → top-level keys are tickers
            for t in tickers:
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        # Could be (ticker, field) or (field, ticker) depending on yfinance version
                        top = raw.columns.get_level_values(0).unique()
                        if t in top:
                            series = raw[t]["Close"].dropna()
                        else:
                            # (field, ticker) layout — fall through to fallback
                            series = _extract_close_series(raw, t)
                    else:
                        series = pd.Series(dtype=float)
                    prices[t] = float(series.iloc[-1]) if not series.empty else None
                except Exception:
                    prices[t] = None
    except Exception:
        prices = {t: None for t in tickers}

    # Fallback: retry tickers that got None using a single daily bar
    missing = [t for t, v in prices.items() if v is None]
    if missing:
        try:
            fb_query = missing[0] if len(missing) == 1 else missing
            fb_raw = yf.download(fb_query, period="5d", interval="1d", progress=False, auto_adjust=True)
            for t in missing:
                try:
                    series = _extract_close_series(fb_raw, t if len(missing) > 1 else None)
                    prices[t] = float(series.iloc[-1]) if not series.empty else None
                except Exception:
                    prices[t] = None
        except Exception:
            pass

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
# Endpoints
# ===========================================================================

# --- Health -----------------------------------------------------------------

@app.get("/api/health")
def health():
    return {"status": "ok", "db_exists": os.path.exists(DB_PATH)}


# --- Market status ----------------------------------------------------------

@app.get("/api/market/status")
def market_status():
    return {"is_open": market_is_open()}


# --- Signals ----------------------------------------------------------------

_MARKET_CAP_JOIN = """
    LEFT JOIN (
        SELECT ticker, MAX(market_cap) AS market_cap
        FROM fundamentals GROUP BY ticker
    ) f ON s.ticker = f.ticker
"""

@app.get("/api/signals/today")
def signals_today(signal: Optional[str] = None):
    """Today's signals (latest date in DB). Optional ?signal=BUY|SELL|HOLD filter."""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT s.date, s.ticker, s.close, s.rsi, s.fair_value_upside,
                   s.target_mean_price, s.target_low_price, s.target_high_price,
                   s.signal, c.company, c.logo_url,
                   f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
            WHERE DATE(s.date) = (SELECT DATE(MAX(date)) FROM signals)
        """), conn)

    if signal:
        df = df[df["signal"] == signal.upper()]

    return _records(df)


_SORTABLE = {"date", "ticker", "company", "close", "rsi", "fair_value_upside", "signal", "target_mean_price", "market_cap"}
_TICKER_RE = re.compile(r'^[A-Z0-9.\-]{1,10}$')
_SORT_COL_SQL = {
    "company":    "c.company",
    "market_cap": "f.market_cap",
}


@app.get("/api/signals")
def signals(
    signal: Optional[str] = None,
    search: Optional[str] = None,
    months: int = Query(default=12, ge=1, le=120),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=10, le=500),
):
    """Signals for the last N months with server-side sort and pagination."""
    _key = sort_by if sort_by in _SORTABLE else "date"
    sort_col = _SORT_COL_SQL.get(_key, f"s.{_key}")
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

    conditions = [f"DATE(s.date) >= DATE('now', '-{months} months')"]
    params: dict = {}

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
            SELECT s.date, s.ticker, s.close, s.rsi, s.fair_value_upside,
                   s.target_mean_price, s.target_low_price, s.target_high_price,
                   s.signal, c.company, c.logo_url,
                   f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
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


@app.get("/api/signals/by-tickers")
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
            SELECT s.ticker, c.company, c.logo_url,
                   s.close, s.signal, s.rsi,
                   s.fair_value_upside, s.target_mean_price,
                   f.market_cap
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

@app.get("/api/signals/success-rate")
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

@app.get("/api/prices/history")
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


@app.get("/api/prices/live")
def live_prices(tickers: str = Query(..., description="Comma-separated ticker symbols")):
    """Fetch real-time prices. Returns empty prices dict when market is closed."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="No tickers provided")

    if not market_is_open():
        return {"market_open": False, "prices": {t: None for t in ticker_list}}

    prices = fetch_live_prices(ticker_list)
    return {"market_open": True, "prices": prices}


# --- Watchlists -------------------------------------------------------------

@app.get("/api/watchlists")
def get_watchlists():
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT id, name FROM watchlist_lists ORDER BY id"), conn)
    return _records(df)


@app.post("/api/watchlists", status_code=201)
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


@app.delete("/api/watchlists/{list_id}", status_code=204)
def delete_watchlist(list_id: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM watchlist WHERE list_id = :lid"), {"lid": list_id})
        conn.execute(text("DELETE FROM watchlist_lists WHERE id = :lid"), {"lid": list_id})


@app.get("/api/watchlists/{list_id}/tickers")
def get_watchlist_tickers(list_id: int):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT id, ticker, note FROM watchlist WHERE list_id = :lid ORDER BY id"),
            conn, params={"lid": list_id}
        )
    return _records(df)


@app.post("/api/watchlists/{list_id}/tickers", status_code=201)
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


@app.patch("/api/watchlists/{list_id}/tickers/{ticker}")
def update_ticker_note(list_id: int, ticker: str, body: NoteUpdate):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE watchlist SET note = :note WHERE list_id = :lid AND ticker = :ticker"),
            {"note": body.note, "lid": list_id, "ticker": ticker.upper()},
        )
    return {"ok": True}


@app.delete("/api/watchlists/{list_id}/tickers/{ticker}", status_code=204)
def remove_ticker(list_id: int, ticker: str):
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM watchlist WHERE list_id = :lid AND ticker = :ticker"),
            {"lid": list_id, "ticker": ticker.upper()},
        )


# --- Historical trades ------------------------------------------------------

@app.get("/api/trades")
def historical_trades(
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """BUY→SELL trade pairs within the given date range."""
    conditions = []
    params: dict = {}
    if start:
        conditions.append("DATE(s.date) >= :start")
        params["start"] = start
    if end:
        conditions.append("DATE(s.date) <= :end")
        params["end"] = end

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT s.date, s.ticker, s.signal, s.close, c.company, c.logo_url,
                   f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
            {where}
            ORDER BY s.ticker, s.date
        """), conn, params=params)

    df["date"] = pd.to_datetime(df["date"])

    # Build individual trade pairs, then group by ticker
    ticker_trades: dict = {}
    for ticker, grp in df.groupby("ticker"):
        open_trade = None
        pairs = []
        for _, row in grp.iterrows():
            if row["signal"] == "BUY" and open_trade is None:
                open_trade = row
            elif row["signal"] == "SELL" and open_trade is not None:
                ret = (row["close"] - open_trade["close"]) / open_trade["close"]
                pairs.append({
                    "buy_date":   open_trade["date"].date().isoformat(),
                    "sell_date":  row["date"].date().isoformat(),
                    "buy_price":  round(float(open_trade["close"]), 2),
                    "sell_price": round(float(row["close"]), 2),
                    "return_pct": round(float(ret * 100), 2),
                    "days_held":  int((row["date"] - open_trade["date"]).days),
                    "result":     "Win" if ret > 0 else "Loss",
                })
                open_trade = None
        if not pairs:
            continue
        mc = grp.iloc[0].get("market_cap")
        wins = sum(1 for p in pairs if p["result"] == "Win")
        avg_ret = sum(p["return_pct"] for p in pairs) / len(pairs)
        avg_days = sum(p["days_held"] for p in pairs) / len(pairs)
        ticker_trades[ticker] = {
            "ticker":       ticker,
            "company":      grp.iloc[0]["company"],
            "logo_url":     grp.iloc[0]["logo_url"],
            "market_cap":   int(mc) if mc is not None and not (isinstance(mc, float) and math.isnan(mc)) else None,
            "trade_count":  len(pairs),
            "win_count":    wins,
            "avg_return":   round(avg_ret, 2),
            "avg_days":     round(avg_days, 1),
            "trades":       pairs,   # full list of pairs for the chart
        }

    # Enrich each ticker with organic_yield: (last_close - first_close) / first_close
    # over the selected date range, using daily_prices
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
                SELECT b.ticker,
                       d1.close AS first_close,
                       d2.close AS last_close
                FROM bounds b
                JOIN daily_prices d1 ON d1.ticker = b.ticker AND date(d1.date) = b.fd
                JOIN daily_prices d2 ON d2.ticker = b.ticker AND date(d2.date) = b.ld
            """), conn, params=p)
        for _, org_row in df_org.iterrows():
            t = org_row["ticker"]
            fc, lc = org_row["first_close"], org_row["last_close"]
            if t in ticker_trades and fc and float(fc) > 0:
                ticker_trades[t]["organic_yield"] = round(
                    float((lc - fc) / fc * 100), 2
                )

    return list(ticker_trades.values())


# --- Pipeline ---------------------------------------------------------------

@app.post("/api/pipeline/run")
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


@app.get("/api/pipeline/status")
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
