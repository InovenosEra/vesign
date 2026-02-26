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
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text, event as sa_event
from sqlalchemy.pool import NullPool

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_APP_ROOT, "vesign.db")

app = FastAPI(title="Vesign Trading API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
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


def fetch_live_prices(tickers: list[str]) -> dict:
    try:
        query = tickers[0] if len(tickers) == 1 else tickers
        raw = yf.download(query, period="1d", interval="1m", progress=False, auto_adjust=True)
        close = raw.get("Close", pd.DataFrame() if len(tickers) > 1 else pd.Series(dtype=float))
        prices: dict = {}
        if len(tickers) == 1:
            if isinstance(close, pd.Series):
                series = close.dropna()
            elif isinstance(close, pd.DataFrame) and not close.empty:
                series = close.iloc[:, 0].dropna()
            else:
                series = pd.Series(dtype=float)
            prices[tickers[0]] = float(series.iloc[-1]) if not series.empty else None
        else:
            for t in tickers:
                try:
                    series = close[t].dropna()
                    prices[t] = float(series.iloc[-1]) if not series.empty else None
                except Exception:
                    prices[t] = None
    except Exception:
        prices = {t: None for t in tickers}
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
    return df.where(pd.notna(df), None).to_dict(orient="records")


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
                   s.target_mean_price, s.signal, c.company, c.logo_url,
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
                   s.target_mean_price, s.signal, c.company, c.logo_url,
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
            SELECT s.date, s.ticker, s.signal, s.close, c.company, c.logo_url
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {where}
            ORDER BY s.ticker, s.date
        """), conn, params=params)

    df["date"] = pd.to_datetime(df["date"])

    trades = []
    for ticker, grp in df.groupby("ticker"):
        open_trade = None
        for _, row in grp.iterrows():
            if row["signal"] == "BUY" and open_trade is None:
                open_trade = row
            elif row["signal"] == "SELL" and open_trade is not None:
                ret = (row["close"] - open_trade["close"]) / open_trade["close"]
                trades.append({
                    "company":    open_trade["company"],
                    "logo_url":   open_trade["logo_url"],
                    "ticker":     ticker,
                    "buy_date":   open_trade["date"].date().isoformat(),
                    "sell_date":  row["date"].date().isoformat(),
                    "buy_price":  round(float(open_trade["close"]), 2),
                    "sell_price": round(float(row["close"]), 2),
                    "return_pct": round(float(ret * 100), 2),
                    "days_held":  int((row["date"] - open_trade["date"]).days),
                    "result":     "Win" if ret > 0 else "Loss",
                })
                open_trade = None

    return trades


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
