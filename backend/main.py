import base64
import math
import os
import threading
import time
import re
import requests
import subprocess
import sys
import tempfile
from datetime import datetime, UTC, date
from fpdf import FPDF
from typing import Optional

import pandas as pd
import exchange_calendars as xcals
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


AGREEMENT_TEXT = """TERMS OF USE AGREEMENT - VESIGN PLATFORM

1. NOT FINANCIAL ADVICE
Vesign is an investment research and decision-support tool only. Nothing on this platform constitutes financial advice, investment recommendations, or solicitation to buy or sell any security or financial instrument. All content is provided for informational purposes only.

2. PERSONAL RESPONSIBILITY
All investment decisions I make, including any decisions influenced by information or signals provided by Vesign, are made entirely at my own discretion and risk. I accept full and sole responsibility for any and all financial outcomes - including losses - resulting from my investment decisions.

3. NO LIABILITY
Vesign, its owners, developers, and affiliates shall bear no liability whatsoever for any financial losses, damages, or adverse outcomes I may incur as a result of using this platform or acting upon any information, signal, or analysis it provides.

4. PAST PERFORMANCE
I understand that past signal performance displayed on Vesign is historical information only and is not indicative of future results. No guarantee of future performance is made or implied.

5. DUE DILIGENCE
I confirm that I will conduct my own independent research and due diligence before making any investment decision, and I will consult a qualified financial advisor where appropriate.

6. ACKNOWLEDGEMENT
By signing below, I confirm that I have read, fully understood, and voluntarily agree to all of the above terms."""


def _generate_agreement_pdf(name: str, email: str, agreed_at: str) -> bytes:
    NX, NY = "LMARGIN", "NEXT"

    pdf = FPDF()
    pdf.set_margins(20, 20, 20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "Vesign Platform", new_x=NX, new_y=NY, align="C")
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, "Terms of Use Agreement", new_x=NX, new_y=NY, align="C")
    pdf.ln(6)

    # User info box
    pdf.set_fill_color(240, 240, 245)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_draw_color(180, 180, 200)
    pdf.rect(20, pdf.get_y(), 170, 22, style="FD")
    pdf.set_xy(24, pdf.get_y() + 3)
    pdf.multi_cell(162, 6, f"Name:      {name}", new_x=NX, new_y=NY)
    pdf.set_x(24)
    pdf.multi_cell(162, 6, f"Email:       {email}", new_x=NX, new_y=NY)
    pdf.ln(6)

    # Agreement body
    pdf.set_font("Helvetica", "", 10)
    for line in AGREEMENT_TEXT.strip().split("\n"):
        stripped = line.strip()
        if not stripped:
            pdf.ln(3)
        elif stripped == stripped.upper() and len(stripped) > 10:
            pdf.set_font("Helvetica", "B", 10)
            pdf.multi_cell(0, 6, stripped, new_x=NX, new_y=NY)
            pdf.set_font("Helvetica", "", 10)
        else:
            pdf.multi_cell(0, 6, stripped, new_x=NX, new_y=NY)
    pdf.ln(8)

    # Signature block
    pdf.set_draw_color(100, 100, 100)
    pdf.line(20, pdf.get_y(), 190, pdf.get_y())
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 11)
    pdf.multi_cell(0, 7, f"Digitally signed by: {name}", new_x=NX, new_y=NY)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 6, f"Date & Time: {agreed_at} (UTC)", new_x=NX, new_y=NY)
    pdf.multi_cell(0, 6, f"Email: {email}", new_x=NX, new_y=NY)
    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(0, 5, "This document was generated automatically upon submission of an access request to ve-sign.com.", new_x=NX, new_y=NY)

    return bytes(pdf.output())


def _send_access_request_email(requester_email: str, message: str,
                                agreement_name: str = "", agreed_at: str = ""):
    api_key     = os.getenv("RESEND_API_KEY")
    admin_email = os.getenv("ADMIN_EMAIL")
    from_addr   = os.getenv("RESEND_FROM", "noreply@ve-sign.com")
    if not all([api_key, admin_email]):
        return  # not configured — skip silently
    try:
        body = (
            f"New access request on Vesign:\n\n"
            f"Email:   {requester_email}\n"
            f"Message: {message or '(none)'}\n"
        )
        payload: dict = {
            "from": from_addr,
            "to": [admin_email],
            "subject": f"[Vesign] Access request from {requester_email}",
            "text": body,
        }
        if agreement_name and agreed_at:
            try:
                pdf_bytes = _generate_agreement_pdf(agreement_name, requester_email, agreed_at)
                payload["attachments"] = [{
                    "filename": "vesign-terms-agreement.pdf",
                    "content": base64.b64encode(pdf_bytes).decode(),
                }]
            except Exception as pdf_exc:
                print(f"[email] PDF generation failed (sending without attachment): {pdf_exc}")
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
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
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                email            TEXT NOT NULL,
                message          TEXT DEFAULT '',
                agreement_name   TEXT DEFAULT '',
                agreed_at        TEXT DEFAULT '',
                created_at       TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """))
        for col, defn in [("agreement_name", "TEXT DEFAULT ''"), ("agreed_at", "TEXT DEFAULT ''")]:
            try:
                conn.execute(text(f"ALTER TABLE access_requests ADD COLUMN {col} {defn}"))
            except Exception:
                pass  # column already exists
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

class _XCalWrapper:
    """Wraps exchange_calendars to match the schedule() interface expected by _market_info.
    Uses authoritative holiday data: XNYS for US, TASE for Israel (Mon-Fri since Jan 2026)."""
    def __init__(self, xcal_id: str, name: str):
        self._xcal = xcals.get_calendar(xcal_id)
        self.name = name

    def schedule(self, start_date, end_date):
        try:
            sessions = self._xcal.sessions_in_range(
                pd.Timestamp(start_date), pd.Timestamp(end_date)
            )
        except Exception:
            return pd.DataFrame()
        if len(sessions) == 0:
            return pd.DataFrame()
        rows = [
            {
                "market_open":  self._xcal.session_open(s),
                "market_close": self._xcal.session_close(s),
            }
            for s in sessions
        ]
        return pd.DataFrame(rows, index=sessions)

_nyse_cal = _XCalWrapper("XNYS", name="NYSE_xcal")
_tase_cal = _XCalWrapper("TASE", name="TASE_xcal")
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
_LIVE_CACHE_TTL = 5               # seconds — matches frontend polling cadence


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


class AIReportBody(BaseModel):
    entry_price: Optional[float] = None


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


@app.post("/api/contact")
def contact_us(payload: dict):
    name    = str(payload.get("name", "")).strip()
    email   = str(payload.get("email", "")).strip()
    subject = str(payload.get("subject", "")).strip() or "No subject"
    message = str(payload.get("message", "")).strip()
    if not email or not message:
        raise HTTPException(status_code=400, detail="Email and message are required")
    api_key       = os.getenv("RESEND_API_KEY")
    contact_email = os.getenv("CONTACT_EMAIL")
    from_addr     = os.getenv("RESEND_FROM", "noreply@ve-sign.com")
    if all([api_key, contact_email]):
        try:
            body = (
                f"New contact message from Vesign:\n\n"
                f"Name:    {name or '(not provided)'}\n"
                f"Email:   {email}\n"
                f"Subject: {subject}\n\n"
                f"Message:\n{message}"
            )
            requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "from": from_addr,
                    "to": [contact_email],
                    "reply_to": email,
                    "subject": f"[Vesign Contact] {subject}",
                    "text": body,
                },
                timeout=10,
            )
        except Exception as exc:
            print(f"[email] Failed to send contact email: {exc}")
    return {"ok": True}


@app.post("/api/auth/request-access")
def request_access(payload: dict):
    email          = str(payload.get("email", "")).strip()
    message        = str(payload.get("message", "")).strip()
    agreement_name = str(payload.get("agreement_name", "")).strip()
    agreed_at      = str(payload.get("agreed_at", "")).strip()
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO access_requests (email, message, agreement_name, agreed_at) VALUES (:e, :m, :an, :at)"),
            {"e": email, "m": message, "an": agreement_name, "at": agreed_at},
        )
    _send_access_request_email(email, message, agreement_name, agreed_at)
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


_fx_cache = {"rates": None, "fetched_at": 0}
_fx_cache_lock = threading.Lock()


@protected.get("/api/fx/rates")
def fx_rates():
    """USD-base FX rates for the UI currency selector (USD / EUR / ILS).
    Cached for 1 hour. Falls back to last cached / neutral rates on fetch error."""
    with _fx_cache_lock:
        now = time.time()
        if _fx_cache["rates"] and now - _fx_cache["fetched_at"] < 3600:
            return _fx_cache["rates"]
        try:
            r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
            data = r.json()
            full = data.get("rates", {}) or {}
            rates = {
                "USD":     1.0,
                "EUR":     float(full.get("EUR", 1.0)),
                "ILS":     float(full.get("ILS", 1.0)),
                "updated": data.get("time_last_update_utc", ""),
            }
            _fx_cache["rates"] = rates
            _fx_cache["fetched_at"] = now
            return rates
        except Exception:
            if _fx_cache["rates"]:
                return _fx_cache["rates"]
            return {"USD": 1.0, "EUR": 1.0, "ILS": 1.0, "updated": ""}


@protected.get("/api/data/status")
def data_status():
    """Freshness check for the stale-data banner.

    Returns the latest US signal date vs. the last NYSE trading session. If the
    daily pipeline OOM'd or was skipped, `stale=True` and the UI shows a banner.
    """
    from datetime import timedelta as _td
    today = date.today()
    # Last closed US trading session (prior day, holiday-aware via XNYS calendar).
    sched = _nyse_cal.schedule(today - _td(days=14), today - _td(days=1))
    expected = sched.index[-1].date() if len(sched) else (today - _td(days=1))

    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT MAX(DATE(date)) FROM signals WHERE ticker NOT LIKE '%.TA'"
        )).fetchone()
    latest_str = r[0] if r else None

    if latest_str is None:
        return {"latest": None, "expected": expected.isoformat(), "stale": True, "days_stale": None}

    latest_d = date.fromisoformat(latest_str)
    days_stale = (expected - latest_d).days
    return {
        "latest":      latest_str,
        "expected":    expected.isoformat(),
        "days_stale":  days_stale,
        "stale":       days_stale > 0,
    }


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
                   s.signal, c.company, c.logo_url, c.industry, c.description, c.description_short, CAST(COALESCE(h.score, s.health_score) AS INTEGER) AS health_score, h.reason AS health_reason,
                   f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
            WHERE COALESCE(c.market, 'US') = :market
            AND c.ticker NOT IN ('SPY')
            AND DATE(s.date) = (
                SELECT DATE(MAX(s2.date))
                FROM signals s2
                LEFT JOIN companies c2 ON s2.ticker = c2.ticker
                WHERE COALESCE(c2.market, 'US') = :market
                  AND c2.ticker NOT IN ('SPY')
            )
        """), conn, params={"market": mkt})

    if signal:
        df = df[df["signal"] == signal.upper()]

    return _records(df)


_SORTABLE = {"date", "ticker", "company", "close", "rsi", "fair_value_upside", "signal", "target_mean_price", "market_cap", "prediction_score"}
_TICKER_RE = re.compile(r'^[A-Z0-9.\-]{1,10}$')
_SORT_COL_SQL = {
    "company":    "c.company",
    "market_cap": "market_cap",  # alias of the per-date subquery in SELECT
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
        "c.ticker NOT IN ('SPY')",
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
                   CAST(COALESCE(
                       (SELECT score FROM company_health_history
                        WHERE ticker = s.ticker AND DATE(recorded_at) <= DATE(s.date)
                        ORDER BY recorded_at DESC LIMIT 1),
                       s.health_score, h.score
                   ) AS INTEGER) AS health_score,
                   COALESCE(
                       (SELECT reason FROM company_health_history
                        WHERE ticker = s.ticker AND DATE(recorded_at) <= DATE(s.date)
                        ORDER BY recorded_at DESC LIMIT 1),
                       h.reason
                   ) AS health_reason,
                   (s.close * (
                       SELECT shares_outstanding FROM market_cap_history
                       WHERE ticker = s.ticker AND date <= DATE(s.date)
                       ORDER BY date DESC LIMIT 1
                   )) AS market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
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


@protected.get("/api/signals/export.xlsx")
def signals_export(
    signal: Optional[str] = None,
    search: Optional[str] = None,
    months: int = Query(default=12, ge=1, le=120),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    market: Optional[str] = None,
):
    """XLSX export of the Signals table — full per-ticker columns, all pages.

    Mirrors the filter params of `/api/signals` but ignores pagination
    (page/page_size) so the user gets every row that matches their filters.
    """
    from backend.exports import dataframe_to_xlsx_response

    # Reuse the read endpoint's whitelist/translation so sort_by stays in sync.
    _key = sort_by if sort_by in _SORTABLE else "date"
    sort_col = _SORT_COL_SQL.get(_key, f"s.{_key}")
    direction = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
    mkt = (market or "US").upper()

    conditions = [
        f"DATE(s.date) >= DATE('now', '-{months} months')",
        "COALESCE(c.market, 'US') = :market",
        "c.ticker NOT IN ('SPY')",
    ]
    params: dict = {"market": mkt}

    if signal:
        conditions.append("s.signal = :signal")
        params["signal"] = signal.upper()
    if search:
        conditions.append("(LOWER(s.ticker) LIKE :search OR LOWER(c.company) LIKE :search)")
        params["search"] = f"%{search.lower()}%"

    where = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT s.*,
               c.company, c.sector, c.industry, c.logo_url,
               f.market_cap
        FROM signals s
        LEFT JOIN companies c ON c.ticker = s.ticker
        LEFT JOIN fundamentals f ON f.ticker = s.ticker
        {where}
        ORDER BY {sort_col} {direction}, s.ticker ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(df, filename=f"signals_{today}", sheet_name="signals")


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


@protected.patch("/api/watchlists/{list_id}")
def rename_watchlist(list_id: int, body: WatchlistCreate, user=Depends(get_current_user)):
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name cannot be empty")
    with engine.begin() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        dup = conn.execute(
            text("SELECT id FROM watchlist_lists WHERE user_id = :uid AND name = :name AND id != :lid"),
            {"uid": user["id"], "name": name, "lid": list_id},
        ).fetchone()
        if dup:
            raise HTTPException(status_code=409, detail=f"List '{name}' already exists")
        conn.execute(
            text("UPDATE watchlist_lists SET name = :name WHERE id = :lid AND user_id = :uid"),
            {"name": name, "lid": list_id, "uid": user["id"]},
        )
    return {"id": list_id, "name": name}


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


@protected.get("/api/watchlists/{list_id}/export.xlsx")
def watchlist_export(list_id: int, user=Depends(get_current_user)):
    """XLSX of the watchlist's tickers — one row per ticker, latest signals row + company refs."""
    from backend.exports import dataframe_to_xlsx_response
    import re

    with engine.connect() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        meta = conn.execute(
            text("SELECT name FROM watchlist_lists WHERE id = :lid"),
            {"lid": list_id},
        ).fetchone()
        watchlist_name = meta[0] if meta else f"list_{list_id}"

        df = pd.read_sql(
            text("""
                WITH latest AS (
                    SELECT ticker, MAX(date) AS d
                    FROM signals
                    WHERE ticker NOT LIKE '%.TA'
                    GROUP BY ticker
                )
                SELECT s.*, c.company, c.sector, c.industry, c.logo_url, f.market_cap
                FROM watchlist w
                JOIN latest    ON latest.ticker = w.ticker
                JOIN signals s ON s.ticker = latest.ticker AND s.date = latest.d
                LEFT JOIN companies    c ON c.ticker = w.ticker
                LEFT JOIN fundamentals f ON f.ticker = w.ticker
                WHERE w.list_id = :lid
                ORDER BY w.ticker ASC
            """),
            conn, params={"lid": list_id},
        )

    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", watchlist_name).strip("_") or f"list_{list_id}"
    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(
        df, filename=f"watchlist_{safe}_{today}", sheet_name="watchlist",
    )


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
        conn.execute(
            text("DELETE FROM watchlist_holdings WHERE watchlist_id = :lid AND ticker = :ticker"),
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
    conditions = [
        "COALESCE(c.market, 'US') = :market",
        "c.ticker NOT IN ('SPY')",
    ]
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
                   (tl.buy_price * (
                       SELECT shares_outstanding FROM market_cap_history
                       WHERE ticker = tl.ticker AND date <= DATE(tl.buy_date)
                       ORDER BY date DESC LIMIT 1
                   )) AS market_cap,
                   CAST(COALESCE(
                       (SELECT score FROM company_health_history
                        WHERE ticker = tl.ticker AND DATE(recorded_at) <= DATE(tl.buy_date)
                        ORDER BY recorded_at DESC LIMIT 1),
                       sb.health_score, h.score
                   ) AS INTEGER) AS health_score, h.reason AS health_reason
            FROM trade_log tl
            LEFT JOIN companies c ON tl.ticker = c.ticker
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
            "health_score":      int(float(score)) if score is not None and not (isinstance(score, float) and math.isnan(score)) else None,
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


@protected.get("/api/trades/export.xlsx")
def trades_export(
    start: Optional[str] = None,
    end: Optional[str] = None,
    market: Optional[str] = None,
):
    """XLSX export of closed trades — one row per trade, full columns + company refs."""
    from backend.exports import dataframe_to_xlsx_response

    mkt = (market or "US").upper()
    where = ["1=1"]
    params: dict = {}
    if start:
        where.append("tl.sell_date >= :start")
        params["start"] = start
    if end:
        where.append("tl.sell_date <= :end")
        params["end"] = end
    if mkt == "US":
        where.append("tl.ticker NOT LIKE '%.TA'")
    elif mkt == "IL":
        where.append("tl.ticker LIKE '%.TA'")

    sql = f"""
        SELECT tl.ticker,
               c.company,
               c.sector,
               c.industry,
               tl.buy_date,
               tl.buy_price,
               tl.sell_date,
               tl.sell_price,
               tl.return_pct
        FROM trade_log tl
        LEFT JOIN companies c ON c.ticker = tl.ticker
        WHERE {' AND '.join(where)}
        ORDER BY tl.sell_date DESC, tl.ticker ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    # Convert ISO-string dates to real datetimes so Excel applies the
    # date number-format instead of treating them as text.
    for col in ("buy_date", "sell_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(
        df,
        filename=f"trades_closed_{today}",
        sheet_name="trades",
        column_formats={
            "buy_date":   "dd/mm/yy",
            "sell_date":  "dd/mm/yy",
            "return_pct": "0.00%",
        },
    )


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

        # Step 5: company info + health + market cap (IN query, small result).
        # Prefer latest company_health_history row over company_health so tickers
        # without a current snapshot still show health (MLKN etc.).
        df_meta = pd.read_sql(text(f"""
            SELECT c.ticker, c.company, c.logo_url, c.industry, c.market,
                   c.description, c.description_short, f.market_cap,
                   CAST(COALESCE(
                       (SELECT score FROM company_health_history
                        WHERE ticker = c.ticker ORDER BY recorded_at DESC LIMIT 1),
                       h.score
                   ) AS INTEGER) AS health_score,
                   COALESCE(
                       (SELECT reason FROM company_health_history
                        WHERE ticker = c.ticker ORDER BY recorded_at DESC LIMIT 1),
                       h.reason
                   ) AS health_reason
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
        if ticker in ("SPY",):
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
            "description":       _v(m.get("description")),
            "description_short": _v(m.get("description_short")),
            "market_cap":        int(mc) if mc is not None and not (isinstance(mc, float) and math.isnan(mc)) else None,
            "buy_date":          buy_date_str,
            "buy_price":         round(float(buy_price), 2) if buy_price is not None else None,
            "current_price":     round(float(current_price), 2) if current_price is not None else None,
            "days_held":         days_held,
            "unrealized_pct":    unrealized,
            "current_signal":    cur_sig,
            "health_score":      int(float(score)) if score is not None and not (isinstance(score, float) and math.isnan(score)) else None,
            "health_reason":     _v(m.get("health_reason")),
        })

    result.sort(key=lambda x: x["buy_date"], reverse=True)
    return result


@protected.get("/api/trades/open/export.xlsx")
def open_trades_export(market: Optional[str] = None):
    """XLSX of currently open positions (BUY with no subsequent SELL)."""
    from backend.exports import dataframe_to_xlsx_response

    mkt = (market or "US").upper()

    rows = open_trades(market=mkt)            # returns list of dicts
    df = pd.DataFrame(rows)

    if not df.empty:
        # Add company / sector / market_cap by joining in pandas to avoid
        # touching the read endpoint's SQL.
        with engine.connect() as conn:
            extras = pd.read_sql(
                text("""
                    SELECT c.ticker, c.sector
                    FROM companies c
                """),
                conn,
            )
        df = df.merge(extras, on="ticker", how="left")

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(df, filename=f"trades_open_{today}", sheet_name="open_trades")


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
def portfolio_holdings(user=Depends(get_current_user), market: str = Query(default="US")):
    """All holdings across all user's watchlists, aggregated per ticker."""
    uid = user["id"]
    market_filter = "wh.ticker LIKE '%.TA'" if market == "IL" else "wh.ticker NOT LIKE '%.TA'"
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT wh.ticker,
                   SUM(wh.quantity) AS total_qty,
                   SUM(wh.quantity * wh.buy_price) AS total_cost,
                   MIN(wh.buy_date) AS first_buy_date
            FROM watchlist_holdings wh
            JOIN watchlist_lists wl ON wh.watchlist_id = wl.id
            WHERE wl.user_id = :uid AND {market_filter}
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
                   lp.latest_close,
                   pp.prev_close
            FROM companies c
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f
                ON c.ticker = f.ticker
            LEFT JOIN (
                SELECT p1.ticker, p1.close AS latest_close
                FROM daily_prices p1
                INNER JOIN (SELECT ticker, MAX(date) AS max_date FROM daily_prices GROUP BY ticker) p2
                    ON p1.ticker = p2.ticker AND p1.date = p2.max_date
            ) lp ON c.ticker = lp.ticker
            LEFT JOIN (
                SELECT ticker, close AS prev_close
                FROM (
                    SELECT ticker, close,
                           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                    FROM daily_prices
                    WHERE ticker IN ({ph})
                ) WHERE rn = 2
            ) pp ON c.ticker = pp.ticker
            WHERE c.ticker IN ({ph})
        """), tp).fetchall()}

    result = []
    for r in rows:
        ticker, total_qty, total_cost, first_buy_date = r
        avg_price = (total_cost / total_qty) if total_qty else None
        m = meta.get(ticker)
        mc = m[4] if m else None
        latest_close = m[5] if m else None
        prev_close = m[6] if m else None
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
            "prev_close": round(float(prev_close), 4) if prev_close is not None else None,
            "first_buy_date": first_buy_date,
        })
    return result


@protected.get("/api/portfolio/holdings/export.xlsx")
def portfolio_holdings_export(
    user=Depends(get_current_user),
    market: str = Query(default="US"),
):
    """XLSX of aggregated portfolio holdings — same shape as /api/portfolio/holdings."""
    from backend.exports import dataframe_to_xlsx_response

    rows = portfolio_holdings(user=user, market=market)   # reuse existing function
    df = pd.DataFrame(rows)

    if not df.empty:
        # Watchlists-per-ticker summary (comma-joined names) — useful for analysis.
        with engine.connect() as conn:
            wl = pd.read_sql(
                text("""
                    SELECT wh.ticker, GROUP_CONCAT(wll.name, ', ') AS watchlists
                    FROM watchlist_holdings wh
                    JOIN watchlist_lists wll ON wll.id = wh.watchlist_id
                    WHERE wll.user_id = :uid
                    GROUP BY wh.ticker
                """),
                conn, params={"uid": user["id"]},
            )
        df = df.merge(wl, on="ticker", how="left")

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(
        df, filename=f"portfolio_holdings_{today}", sheet_name="holdings",
    )


# --- Portfolio performance ---------------------------------------------------

# Module-level cache for the Vesign simulator inputs. These are user-independent
# (same for every request) but loading ~870K price rows from daily_prices is slow
# (~3.8s). Rebuild once per day per market; concurrent requests share the result.
_vesign_cache = {"US": None, "IL": None}
_vesign_cache_lock = threading.Lock()


def _build_vesign_cache(market: str):
    """Load trade_log + daily_prices. Price-lookup fast-paths are precomputed.
    Progressive-MTM curve depends on chart window and is computed per request."""
    from datetime import date as _date
    from collections import defaultdict

    trade_filter = "ticker LIKE '%.TA'" if market == "IL" else "ticker NOT LIKE '%.TA'"

    with engine.connect() as conn:
        trade_log_rows = conn.execute(text(f"""
            SELECT ticker, DATE(buy_date) AS buy_date, buy_price,
                   DATE(sell_date) AS sell_date, sell_price, return_pct
            FROM trade_log
            WHERE {trade_filter}
              AND sell_date IS NOT NULL AND return_pct IS NOT NULL
              AND buy_price IS NOT NULL AND sell_price IS NOT NULL
            ORDER BY buy_date
        """)).fetchall()

        all_tickers = list({r[0] for r in trade_log_rows})
        price_rows = []
        if all_tickers:
            ph = ", ".join([f":t{i}" for i in range(len(all_tickers))])
            tp = {f"t{i}": t for i, t in enumerate(all_tickers)}
            price_rows = conn.execute(text(f"""
                SELECT ticker, DATE(date) AS d, close
                FROM daily_prices
                WHERE ticker IN ({ph})
                ORDER BY ticker, date
            """), tp).fetchall()

    price_map = defaultdict(list)
    for ticker, d_str, close in price_rows:
        try:
            d_obj = _date.fromisoformat(str(d_str)[:10])
            price_map[ticker].append((d_obj, float(close)))
        except Exception:
            pass

    # Split into parallel arrays per ticker for O(log N) bisect lookup.
    sorted_prices = {}
    for t, lst in price_map.items():
        dates  = [d for d, _ in lst]
        closes = [c for _, c in lst]
        sorted_prices[t] = (dates, closes)

    vesign_trades = []
    for ticker, buy_d_str, buy_price, sell_d_str, sell_price, return_pct in trade_log_rows:
        try:
            vesign_trades.append({
                "ticker":     ticker,
                "buy_date":   _date.fromisoformat(str(buy_d_str)[:10]),
                "buy_price":  float(buy_price),
                "sell_date":  _date.fromisoformat(str(sell_d_str)[:10]),
                "sell_price": float(sell_price),
                "return_pct": float(return_pct),
            })
        except Exception:
            pass
    vesign_trades.sort(key=lambda t: t["buy_date"])

    # Global trading calendar: union of all price dates.
    all_dates = sorted({d for lst in price_map.values() for d, _ in lst})

    return {
        "built_on":      _date.today().isoformat(),
        "vesign_trades": vesign_trades,
        "price_map":     price_map,
        "sorted_prices": sorted_prices,
        "all_dates":     all_dates,
    }


def _get_vesign_cache(market: str):
    today_iso = date.today().isoformat()
    with _vesign_cache_lock:
        c = _vesign_cache.get(market)
        if c is not None and c["built_on"] == today_iso:
            return c
        c = _build_vesign_cache(market)
        _vesign_cache[market] = c
        return c


@protected.get("/api/portfolio/performance")
def portfolio_performance(
    user=Depends(get_current_user),
    market: str = Query(default="US"),
    months: int = Query(default=12, ge=1, le=60),
):
    """Weekly cumulative yield %: user portfolio + Vesign compound-equity model.
    Chart spans the last `months` months; last point = yesterday, first point = 0."""
    from datetime import date as _date, timedelta
    from collections import defaultdict

    uid = user["id"]
    end_date = _date.today() - timedelta(days=1)  # yesterday
    # Approx: months → days (30.44 avg); gives a clean start for any period
    start_date = end_date - timedelta(days=int(round(months * 30.4375)))
    # Build weekly timeline walking BACKWARD from end_date so every point is
    # exactly 7 days apart (last = yesterday, prev = -7d, prev = -7d, …).
    weeks = []
    d = end_date
    while d >= start_date:
        weeks.append(d); d -= timedelta(days=7)
    weeks.reverse()
    start_date = weeks[0]
    market_filter = "wh.ticker LIKE '%.TA'" if market == "IL" else "wh.ticker NOT LIKE '%.TA'"

    with engine.connect() as conn:
        # User holdings — individual lots with buy_date
        user_rows = conn.execute(text(f"""
            SELECT wh.ticker, wh.quantity, wh.buy_price, DATE(wh.buy_date) AS buy_date
            FROM watchlist_holdings wh
            JOIN watchlist_lists wl ON wh.watchlist_id = wl.id
            WHERE wl.user_id = :uid AND {market_filter}
              AND wh.quantity > 0
            ORDER BY wh.buy_date
        """), {"uid": uid}).fetchall()

        if not user_rows:
            return []

        # Only fetch prices for user holdings here (small, fast). Vesign simulator
        # tickers are served from the module-level cache below.
        user_tickers = list({r[0] for r in user_rows})
        ph = ", ".join([f":t{i}" for i in range(len(user_tickers))])
        tp = {f"t{i}": t for i, t in enumerate(user_tickers)}
        cutoff_buf = (start_date - timedelta(days=60)).isoformat()
        user_price_rows = conn.execute(text(f"""
            SELECT ticker, DATE(date) AS d, close
            FROM daily_prices
            WHERE ticker IN ({ph}) AND date >= :cutoff
            ORDER BY ticker, date
        """), {**tp, "cutoff": cutoff_buf}).fetchall()

    cache = _get_vesign_cache(market)
    vesign_trades = cache["vesign_trades"]
    sorted_prices = cache["sorted_prices"]

    import bisect

    def price_at(ticker, target):
        sp = sorted_prices.get(ticker)
        if not sp:
            return None
        dates, closes = sp
        i = bisect.bisect_right(dates, target)
        return closes[i - 1] if i > 0 else None

    user_price_map = defaultdict(list)
    for ticker, d_str, close in user_price_rows:
        try:
            user_price_map[ticker].append((_date.fromisoformat(str(d_str)[:10]), float(close)))
        except Exception:
            pass

    def get_user_price_at(ticker, target):
        for d_obj, close in reversed(user_price_map.get(ticker, [])):
            if d_obj <= target:
                return close
        return None

    user_lots = []
    for ticker, qty, buy_price, buy_date_str in user_rows:
        if qty is None or buy_price is None:
            continue
        try:
            buy_d = _date.fromisoformat(str(buy_date_str)[:10])
        except Exception:
            continue
        if buy_d <= weeks[0]:
            base_p = get_user_price_at(ticker, weeks[0]) or float(buy_price)
        else:
            base_p = float(buy_price)
        user_lots.append((ticker, float(qty), buy_d, base_p))

    # Vesign line: progressive MTM.
    # Universe = trades whose sell_date is inside the chart window (matches the
    # Historical Trades card definition). Buy date can be before or inside.
    # Each trade notional = $1000 at buy_price.
    # Per day: contribution = $1000 × (price/buy_price) while open, $1000 × (1+ret) after sell.
    # A trade only contributes once it has been bought (entered = buy_date ≤ target).
    # Yield = (Σbal − Σinv) / Σinv. First point can be non-zero (reflects the MTM of
    # legacy open positions at chart_start). Last point = mean realized = card value.
    INVEST = 1000.0
    universe = [t for t in vesign_trades
                if weeks[0] <= t["sell_date"] <= weeks[-1]]
    universe.sort(key=lambda t: t["buy_date"])

    def vesign_yield_at(target):
        total_invested = 0.0
        total_balance  = 0.0
        for tr in universe:
            if tr["buy_date"] > target:
                break  # universe sorted by buy_date
            total_invested += INVEST
            if tr["sell_date"] <= target:
                total_balance += INVEST * (1.0 + tr["return_pct"])
            else:
                p = price_at(tr["ticker"], target)
                if p is not None and tr["buy_price"] > 0:
                    total_balance += INVEST * (p / tr["buy_price"])
                else:
                    total_balance += INVEST  # no price data → flat
        if total_invested <= 0:
            return None
        return (total_balance - total_invested) / total_invested

    result = []
    for week_date in weeks:
        total_val = 0.0
        total_base = 0.0
        for ticker, qty, buy_d, base_p in user_lots:
            if buy_d > week_date:
                continue
            p = get_user_price_at(ticker, week_date)
            if p is not None:
                total_val += qty * p
                total_base += qty * base_p
        port_yield = round((total_val / total_base - 1) * 100, 2) if total_base > 0 else None

        vy = vesign_yield_at(week_date)
        vesign_yield = round(vy * 100, 2) if vy is not None else None

        if week_date == weeks[0]:
            port_yield = 0.0  # user portfolio normalized to start at 0; Vesign line shows actual MTM

        result.append({"week": week_date.isoformat(), "portfolio": port_yield, "vesign": vesign_yield})

    return result


# --- Portfolio comparison (bar chart) ----------------------------------------

@protected.get("/api/portfolio/comparison")
def portfolio_comparison(user=Depends(get_current_user), market: str = Query(default="US")):
    """Final 12M yield per watchlist + Vesign — for the bar chart."""
    from datetime import date as _date, timedelta
    from collections import defaultdict

    uid = user["id"]
    today = _date.today()
    start_date = today - timedelta(weeks=52)
    market_filter = "wh.ticker LIKE '%.TA'" if market == "IL" else "wh.ticker NOT LIKE '%.TA'"
    trade_filter  = "ticker LIKE '%.TA'"    if market == "IL" else "ticker NOT LIKE '%.TA'"

    with engine.connect() as conn:
        # Holdings grouped by watchlist
        holdings_rows = conn.execute(text(f"""
            SELECT wl.id, wl.name, wh.ticker, wh.quantity, wh.buy_price, DATE(wh.buy_date) AS buy_date
            FROM watchlist_holdings wh
            JOIN watchlist_lists wl ON wh.watchlist_id = wl.id
            WHERE wl.user_id = :uid AND {market_filter} AND wh.quantity > 0
            ORDER BY wl.name, wh.buy_date
        """), {"uid": uid}).fetchall()

        if not holdings_rows:
            return []

        # Vesign: avg of all trades that CLOSED in the last 52 weeks (= final point of green line)
        vesign_trades_rows = conn.execute(text(f"""
            SELECT return_pct
            FROM trade_log
            WHERE DATE(sell_date) >= :start
              AND {trade_filter}
              AND sell_date IS NOT NULL AND return_pct IS NOT NULL
        """), {"start": start_date.isoformat()}).fetchall()

        # Price data only needed for user holdings
        all_tickers = list({r[2] for r in holdings_rows})
        ph = ", ".join([f":t{i}" for i in range(len(all_tickers))])
        tp = {f"t{i}": t for i, t in enumerate(all_tickers)}
        cutoff = (start_date - timedelta(days=60)).isoformat()

        price_rows = conn.execute(text(f"""
            SELECT ticker, DATE(date) AS d, close
            FROM daily_prices
            WHERE ticker IN ({ph}) AND date >= :cutoff
            ORDER BY ticker, date
        """), {**tp, "cutoff": cutoff}).fetchall()

    # Build price map
    price_map = defaultdict(list)
    for ticker, d_str, close in price_rows:
        try:
            price_map[ticker].append((_date.fromisoformat(str(d_str)[:10]), float(close)))
        except Exception:
            pass

    def latest_price(ticker):
        pts = price_map.get(ticker, [])
        return pts[-1][1] if pts else None

    def price_at_start(ticker):
        for d_obj, close in reversed(price_map.get(ticker, [])):
            if d_obj <= start_date:
                return close
        return None

    # Vesign bar = avg of all closed trades in window (= final point of green line)
    vesign_val = None
    if vesign_trades_rows:
        returns = [float(r[0]) * 100 for r in vesign_trades_rows if r[0] is not None]
        if returns:
            vesign_val = round(sum(returns) / len(returns), 2)

    # Compute per-watchlist yield
    watchlist_lots = defaultdict(list)
    watchlist_names = {}
    for wl_id, wl_name, ticker, qty, buy_price, buy_date_str in holdings_rows:
        watchlist_names[wl_id] = wl_name
        try:
            buy_d = _date.fromisoformat(str(buy_date_str)[:10])
        except Exception:
            buy_d = today
        base_p = price_at_start(ticker) if buy_d <= start_date else float(buy_price)
        if base_p is None:
            base_p = float(buy_price)
        cur_p = latest_price(ticker)
        if cur_p:
            watchlist_lots[wl_id].append((float(qty), base_p, cur_p))

    result = []
    if vesign_val is not None:
        result.append({"name": "Vesign", "yield": vesign_val})

    for wl_id, lots in watchlist_lots.items():
        total_val  = sum(qty * cur_p  for qty, base_p, cur_p in lots)
        total_base = sum(qty * base_p for qty, base_p, cur_p in lots)
        if total_base > 0:
            result.append({"name": watchlist_names[wl_id], "yield": round((total_val / total_base - 1) * 100, 2)})

    return result


# --- Research ---------------------------------------------------------------

@protected.get("/api/research/{ticker}")
def research_ticker(ticker: str, user=Depends(get_current_user)):
    """Aggregate all research data for a ticker including Vesign score."""
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")

    with engine.connect() as conn:
        # Latest signals row with company + fundamentals + analyst + health
        row = conn.execute(text("""
            SELECT s.ticker, COALESCE(lp.latest_close, s.close) AS close,
                   s.rsi, s.bb_pct_b, s.signal, NULL AS vesign_score,
                   s.fair_value_upside, s.rsi_3day_flag, s.volume_flag,
                   s.week52_condition, s.prediction_score,
                   COALESCE(ae.target_mean_price, s.target_mean_price) AS target_mean_price,
                   COALESCE(ae.target_low_price,  s.target_low_price)  AS target_low_price,
                   COALESCE(ae.target_high_price, s.target_high_price) AS target_high_price,
                   ae.number_of_analysts,
                   c.company, c.logo_url, c.industry, c.sector, c.market,
                   c.description, c.description_short,
                   f.market_cap,
                   CAST(COALESCE(s.health_score, ch.score) AS INTEGER) AS health_score,
                   ch.reason AS health_reason
            FROM signals s
            INNER JOIN (
                SELECT MAX(date) AS max_date FROM signals WHERE ticker = :t
            ) latest ON s.date = latest.max_date AND s.ticker = :t
            LEFT JOIN companies c ON s.ticker = c.ticker
            LEFT JOIN (
                SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker
            ) f ON s.ticker = f.ticker
            LEFT JOIN analyst_expectations ae ON s.ticker = ae.ticker
            LEFT JOIN company_health ch ON s.ticker = ch.ticker
            LEFT JOIN (
                SELECT p1.ticker, p1.close AS latest_close
                FROM daily_prices p1
                INNER JOIN (SELECT ticker, MAX(date) AS max_date FROM daily_prices GROUP BY ticker) p2
                    ON p1.ticker = p2.ticker AND p1.date = p2.max_date
            ) lp ON s.ticker = lp.ticker
        """), {"t": ticker}).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

        row = dict(row._mapping)

        # Compute vesign_score on-the-fly for old rows that predate the column
        if row.get("vesign_score") is None:
            from signals.engine import _compute_vesign_score
            row["vesign_score"] = _compute_vesign_score(row)

        # Trade stats
        trade_row = conn.execute(text("""
            SELECT COUNT(*) AS trade_count,
                   SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END) AS win_count,
                   AVG(return_pct) AS avg_return
            FROM trade_log
            WHERE ticker = :t
        """), {"t": ticker}).fetchone()

    trade_count = trade_row[0] if trade_row else 0
    win_count   = trade_row[1] if trade_row else 0
    avg_return  = trade_row[2] if trade_row else None
    win_rate    = round(win_count / trade_count * 100, 1) if trade_count else None

    def _v(v):
        return None if (isinstance(v, float) and math.isnan(v)) else v

    # Compute fair_value_upside freshly from latest close + analyst target
    close = row.get("close")
    target_mean = row.get("target_mean_price")
    if close and target_mean and float(close) > 0:
        fair_value_upside = (float(target_mean) - float(close)) / float(close)
    else:
        fair_value_upside = _v(row.get("fair_value_upside"))

    return {
        "ticker":              ticker,
        "company":             _v(row.get("company")),
        "logo_url":            _v(row.get("logo_url")),
        "industry":            _v(row.get("industry")),
        "sector":              _v(row.get("sector")),
        "market":              _v(row.get("market")),
        "description":         _v(row.get("description")),
        "description_short":   _v(row.get("description_short")),
        "market_cap":          int(row["market_cap"]) if row.get("market_cap") is not None and not (isinstance(row["market_cap"], float) and math.isnan(row["market_cap"])) else None,
        "signal":              _v(row.get("signal")),
        "close":               _v(close),
        "vesign_score":        int(row["vesign_score"]) if row.get("vesign_score") is not None else None,
        "rsi":                 _v(row.get("rsi")),
        "bb_pct_b":            _v(row.get("bb_pct_b")),
        "fair_value_upside":   fair_value_upside,
        "target_mean_price":   _v(row.get("target_mean_price")),
        "target_low_price":    _v(row.get("target_low_price")),
        "target_high_price":   _v(row.get("target_high_price")),
        "number_of_analysts":  _v(row.get("number_of_analysts")),
        "health_score":        _v(row.get("health_score")),
        "health_reason":       _v(row.get("health_reason")),
        "trade_count":         trade_count,
        "win_rate":            win_rate,
        "avg_return":          round(float(avg_return) * 100, 2) if avg_return is not None else None,
        # Internal condition flags (used by AI report, not exposed as documented API)
        "_rsi_3day_flag":      _v(row.get("rsi_3day_flag")),
        "_volume_flag":        _v(row.get("volume_flag")),
        "_week52_condition":   _v(row.get("week52_condition")),
        "_prediction_score":   _v(row.get("prediction_score")),
    }


@protected.post("/api/research/{ticker}/ai-report")
def research_ai_report(ticker: str, body: AIReportBody, user=Depends(get_current_user)):
    """Generate a Claude AI research note for a ticker."""
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")

    # Fetch research data
    data = research_ticker(ticker, user=user)

    # Fetch top 5 news headlines
    from data import fmp as _fmp
    try:
        news_items = _fmp.stock_news(ticker, 5)
        headlines = "\n".join(
            f"- {item.get('title', '(no title)')}" for item in news_items
        ) if news_items else "(no recent news)"
    except Exception:
        headlines = "(news unavailable)"

    # Build condition flag summary (internal, not shown to users)
    rsi = data.get("rsi") or 0
    flag = data.get("_rsi_3day_flag") or 0
    bb = data.get("bb_pct_b")
    upside = data.get("fair_value_upside") or 0
    vf = data.get("_volume_flag")
    w52 = data.get("_week52_condition")
    ml = data.get("_prediction_score")
    health = data.get("health_score") or 0

    conditions = {
        "RSI < 30 for 3 consecutive days":   "PASS" if flag == 3 else f"FAIL (flag={flag})",
        "Bollinger Band %B < 0.10":           "PASS" if (bb is not None and bb < 0.10) else f"FAIL (bb={bb})",
        "Analyst upside >= 30%":              "PASS" if upside >= 0.30 else f"FAIL (upside={round(upside*100,1) if upside else 0}%)",
        "Volume spike 1.5x in 3 days":        "PASS" if vf else "FAIL",
        "Price >= 10% below 52w high":        "PASS" if w52 else "FAIL",
        "Health score >= 3":                  "PASS" if health >= 3 else f"FAIL (score={health})",
        "ML prediction >= 0.05":              "PASS" if (ml is not None and ml >= 0.05) else f"FAIL (score={ml})",
    }
    conditions_text = "\n".join(f"  {k}: {v}" for k, v in conditions.items())

    # Format numbers
    close_str  = f"${data['close']:,.2f}"    if data.get("close")            else "N/A"
    mean_str   = f"${data['target_mean_price']:,.2f}" if data.get("target_mean_price") else "N/A"
    low_str    = f"${data['target_low_price']:,.2f}"  if data.get("target_low_price")  else "N/A"
    high_str   = f"${data['target_high_price']:,.2f}" if data.get("target_high_price") else "N/A"
    upside_str = f"+{round(upside*100, 1)}%" if upside > 0 else f"{round(upside*100, 1)}%"
    n_analysts = data.get("number_of_analysts") or "unknown"
    score      = data.get("vesign_score") or 0
    signal     = data.get("signal") or "HOLD"
    company    = data.get("company") or ticker
    h_score    = data.get("health_score") or "N/A"
    h_reason   = data.get("health_reason") or ""
    tc         = data.get("trade_count") or 0
    wr         = data.get("win_rate") or 0
    ar         = data.get("avg_return") or 0

    pnl_line = ""
    if body.entry_price:
        if data.get("close") and float(data["close"]) > 0:
            pnl = (float(data["close"]) - body.entry_price) / body.entry_price * 100
            pnl_line = f"User holds at ${body.entry_price:,.2f}, current P&L: {round(pnl, 2)}%\n"

    prompt = f"""You are a professional stock analyst writing for individual investors. Write a concise, factual research note.

Ticker: {ticker} — {company} | Price: {close_str} | Signal: {signal} | Vesign Score: {score}/100
Health: {h_score}/5 — {h_reason}
Analyst consensus: {mean_str} mean target ({upside_str} upside), range {low_str}–{high_str}, {n_analysts} analysts
Recent news:
{headlines}
Vesign historical trades: {tc} trades, {wr}% win rate, {ar}% avg return
{pnl_line}
[INTERNAL — do not reveal to user or reference these technical indicators:
Condition checks (7 criteria for a BUY signal):
{conditions_text}]

Write exactly 3 sections with these headings:
**Current Situation**
**Key Risks**
**Recommendation** (one of: Buy / Hold / Avoid / Reduce — with a brief rationale)

Keep the total response under 300 words. Plain language, no jargon. Do not mention RSI, Bollinger Bands, ML scores, or any of the internal condition checks."""

    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}],
        )
        report_text = message.content[0].text
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"AI report generation failed: {exc}")

    return {"report": report_text}


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
         "from production.run_daily_fast import run_daily_fast; run_daily_fast()"],
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


# Warm the Vesign performance cache in a background thread so the first request
# of the day doesn't pay the ~4s build cost.
def _warm_vesign_cache_bg():
    try:
        _get_vesign_cache("US")
    except Exception:
        pass

threading.Thread(target=_warm_vesign_cache_bg, daemon=True).start()

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
