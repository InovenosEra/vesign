import base64
import bisect
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
from typing import Literal, Optional

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
from sqlalchemy import bindparam, create_engine, text, event as sa_event
from sqlalchemy.pool import NullPool
from backend.auth import get_current_user
from backend.yield_calcs import avg_cost_dollar_weighted, Lot, simulate_bank_hand

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


# ---------------------------------------------------------------------------
# Self-hosted company logos (served at /logos/{TICKER}.png)
# ---------------------------------------------------------------------------
_LOGO_DIR = os.path.join(_APP_ROOT, "static", "logos")
if os.path.isdir(_LOGO_DIR):
    app.mount("/logos", StaticFiles(directory=_LOGO_DIR), name="logos")


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
    Uses XNYS for US holiday/session data."""
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


from datetime import timedelta as _td

_PRE_OFFSET  = _td(hours=-5, minutes=-30)  # pre_open  = regular_open  - 5.5h
_POST_OFFSET = _td(hours=4)                # post_close = regular_close + 4h


def _session_boundaries(session_date):
    """Returns (pre_open, regular_open, regular_close, post_close) as tz-aware UTC datetimes
    for a given NYSE session date. Returns None for non-session days."""
    xcal = _nyse_cal._xcal
    try:
        if not xcal.is_session(session_date):
            return None
    except Exception:
        return None
    regular_open  = xcal.session_open(session_date).to_pydatetime()
    regular_close = xcal.session_close(session_date).to_pydatetime()
    return (
        regular_open + _PRE_OFFSET,
        regular_open,
        regular_close,
        regular_close + _POST_OFFSET,
    )


def _phase_info(now_utc=None) -> dict:
    """Returns {phase, next_event_utc, next_event_name,
                next_regular_event_utc, next_regular_event_name} for NYSE.

    phase in {'idle','pre','regular','post'}.
    next_event_name in {'pre_open','regular_open','regular_close','post_close'}.

    next_regular_event_* always points to the next REGULAR-hours boundary
    (regular_open or regular_close), independent of pre/post-market sessions.
    The frontend uses this for the "Opens in / Closes in" countdown so the
    countdown always references regular trading hours.

    `now_utc` defaults to current UTC time; pass explicit value for testing."""
    if now_utc is None:
        now_utc = datetime.now(UTC)
    today = now_utc.date()

    bounds = _session_boundaries(pd.Timestamp(today))
    if bounds is not None:
        pre_open, reg_open, reg_close, post_close = bounds
        if now_utc < pre_open:
            # idle before today's pre-market — next regular open is today's reg_open
            return _add_regular(_idle_until_today_pre(pre_open), reg_open, "regular_open")
        if pre_open <= now_utc < reg_open:
            return _add_regular({"phase": "pre", "next_event_name": "regular_open",
                                  "next_event_utc": reg_open.isoformat()},
                                 reg_open, "regular_open")
        if reg_open <= now_utc < reg_close:
            return _add_regular({"phase": "regular", "next_event_name": "regular_close",
                                  "next_event_utc": reg_close.isoformat()},
                                 reg_close, "regular_close")
        if reg_close <= now_utc < post_close:
            # post-market: next regular event is the NEXT session's regular_open
            return _add_next_regular_open(
                {"phase": "post", "next_event_name": "post_close",
                 "next_event_utc": post_close.isoformat()},
                today)
        # past today's post_close: idle until next session
        return _add_next_regular_open(_idle_until_next_pre(today), today)

    # Not a session day (weekend / holiday): idle until next session
    return _add_next_regular_open(_idle_until_next_pre(today), today)


def _add_regular(d: dict, regular_dt, name: str) -> dict:
    """Attach next_regular_event_* fields pointing to a known regular boundary."""
    d["next_regular_event_utc"] = regular_dt.isoformat()
    d["next_regular_event_name"] = name
    return d


def _add_next_regular_open(d: dict, today) -> dict:
    """Find the next session's regular_open after `today` and attach to dict."""
    for delta in range(1, 11):
        cand = today + _td(days=delta)
        b = _session_boundaries(pd.Timestamp(cand))
        if b is not None:
            d["next_regular_event_utc"] = b[1].isoformat()
            d["next_regular_event_name"] = "regular_open"
            return d
    d["next_regular_event_utc"] = None
    d["next_regular_event_name"] = "regular_open"
    return d


def _idle_until_today_pre(pre_open):
    return {"phase": "idle", "next_event_name": "pre_open",
            "next_event_utc": pre_open.isoformat()}


def _idle_until_next_pre(today):
    for delta in range(1, 11):
        cand = today + _td(days=delta)
        bounds = _session_boundaries(pd.Timestamp(cand))
        if bounds is not None:
            return {"phase": "idle", "next_event_name": "pre_open",
                    "next_event_utc": bounds[0].isoformat()}
    return {"phase": "idle", "next_event_name": "pre_open", "next_event_utc": None}


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
_live_price_cache_phase = None    # tracks which phase the cache was populated under
_LIVE_CACHE_TTL = 2               # seconds — slightly longer than 1s poll cadence


def fetch_live_prices(tickers: list[str]) -> dict:
    """Fetch latest prices via a single FMP batch call."""
    from data import fmp as _fmp

    if not tickers:
        return {}
    return _fmp.live_prices(tickers)


def fetch_aftermarket_trades(tickers: list[str]) -> dict:
    """Wraps data.fmp.aftermarket_trades for testability."""
    from data.fmp import aftermarket_trades as _amt
    if not tickers:
        return {}
    return _amt(tickers)


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
    lang: Optional[str] = None  # i18n locale: en/he/es/fr/de/it. Defaults to en.


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
    return _phase_info()


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

_signals_today_cache: dict[str, dict] = {}
_signals_today_cache_lock = threading.Lock()


def _build_signals_today(mkt: str) -> list[dict]:
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT s.date, s.ticker, COALESCE(lp.latest_close, s.close) AS close, s.rsi,
                   {_ANALYST_UPSIDE_SQL},
                   COALESCE(ae.target_mean_price, s.target_mean_price) AS target_mean_price, COALESCE(ae.target_low_price, s.target_low_price) AS target_low_price, COALESCE(ae.target_high_price, s.target_high_price) AS target_high_price,
                   s.prediction_score,
                   s.vqs,
                   s.signal, s.lot_seq, c.company, c.logo_url, c.industry, c.domain, c.description, c.description_short, CAST(COALESCE(h.score, s.health_score) AS INTEGER) AS health_score, h.reason AS health_reason,
                   f.market_cap
            FROM signals s
            LEFT JOIN companies c ON s.ticker = c.ticker
            {_MARKET_CAP_JOIN}
            WHERE COALESCE(c.market, 'US') = :market
            AND c.ticker NOT IN ('SPY', 'VOO')
            AND DATE(s.date) = (
                SELECT DATE(MAX(s2.date))
                FROM signals s2
                LEFT JOIN companies c2 ON s2.ticker = c2.ticker
                WHERE COALESCE(c2.market, 'US') = :market
                  AND c2.ticker NOT IN ('SPY', 'VOO')
            )
        """), conn, params={"market": mkt})
    return _records(df)


def _get_signals_today_cached(mkt: str) -> list[dict]:
    today_iso = datetime.now(UTC).date().isoformat()
    with _signals_today_cache_lock:
        c = _signals_today_cache.get(mkt)
        if c is not None and c["built_on"] == today_iso:
            return c["data"]
        data = _build_signals_today(mkt)
        _signals_today_cache[mkt] = {"built_on": today_iso, "data": data}
        return data


@protected.get("/api/signals/today")
def signals_today(signal: Optional[str] = None, market: Optional[str] = None):
    """Today's signals (latest date in DB). Optional ?signal=BUY|SELL|HOLD&market=US|IL filter."""
    mkt = (market or "US").upper()
    rows = _get_signals_today_cached(mkt)
    if signal:
        wanted = signal.upper()
        rows = [r for r in rows if r.get("signal") == wanted]
    return rows


_SORTABLE = {"date", "ticker", "company", "close", "rsi", "fair_value_upside", "signal", "target_mean_price", "market_cap", "prediction_score"}
_TICKER_RE = re.compile(r'^[A-Z0-9.\-]{1,10}$')
_ISO_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_SORT_COL_SQL = {
    "company":    "c.company",
    "market_cap": "market_cap",  # alias of the per-date subquery in SELECT
}


@protected.get("/api/signals")
def signals(
    signal: Optional[str] = None,
    search: Optional[str] = None,
    months: int = Query(default=12, ge=1, le=120),
    start: Optional[str] = None,
    end: Optional[str] = None,
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=10, le=500),
    market: Optional[str] = None,
):
    """Signals for the last N months with server-side sort and pagination.

    `start`/`end` (ISO YYYY-MM-DD) override the rolling-`months` window when
    provided. Either bound can be supplied independently.
    """
    _key = sort_by if sort_by in _SORTABLE else "date"
    sort_col = _SORT_COL_SQL.get(_key, f"s.{_key}")
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
    mkt = (market or "US").upper()

    start_v = start if start and _ISO_DATE_RE.match(start) else None
    end_v   = end   if end   and _ISO_DATE_RE.match(end)   else None

    conditions = [
        "COALESCE(c.market, 'US') = :market",
        "c.ticker NOT IN ('SPY', 'VOO')",
    ]
    if start_v:
        conditions.append("DATE(s.date) >= :start_date")
    else:
        conditions.append(f"DATE(s.date) >= DATE('now', '-{months} months')")
    if end_v:
        conditions.append("DATE(s.date) <= :end_date")
    params: dict = {"market": mkt}
    if start_v: params["start_date"] = start_v
    if end_v:   params["end_date"]   = end_v

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
                   s.target_mean_price, s.target_low_price, s.target_high_price,
                   s.prediction_score,
                   s.signal, s.lot_seq, c.company, c.logo_url, c.industry, c.domain, c.description, c.description_short,
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


# ---------------------------------------------------------------------------
# Common export normalization
# ---------------------------------------------------------------------------
# UI keeps raw values (market cap in dollars, dates as ISO strings). Excel/CSV
# exports show market cap in billions and render dates with this format.
_EXPORT_DATE_FMT = "dd/mm/yy"
_EXPORT_MCAP_FMT = "0.00"


def _prepare_export_df(df: pd.DataFrame, *date_cols: str) -> pd.DataFrame:
    """Mutates `df` in place: market_cap → billions, each date_col → datetime."""
    if "market_cap" in df.columns:
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce") / 1e9
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


@protected.get("/api/signals/export")
@protected.get("/api/signals/export.xlsx")  # legacy alias — defaults to xlsx
def signals_export(
    signal: Optional[str] = None,
    search: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    market: Optional[str] = None,
    format: str = Query(default="xlsx", description="xlsx | csv | zip"),
):
    """Export of the Signals table — full per-ticker columns, all history, in
    the chosen `format` (xlsx / csv / zip-of-csv).

    Mirrors the signal/search/sort filters of `/api/signals` (including the
    optional `start`/`end` date range) but ignores pagination so the user
    gets every row that matches. A hard LIMIT 200000 caps the worst case so
    the server doesn't spend forever streaming HOLDs nobody reads.
    """
    from backend.exports import dispatch_cursor_response

    EXPORT_ROW_LIMIT = 200_000

    _key = sort_by if sort_by in _SORTABLE else "date"
    sort_col = _SORT_COL_SQL.get(_key, f"s.{_key}")
    direction = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
    mkt = (market or "US").upper()

    start_v = start if start and _ISO_DATE_RE.match(start) else None
    end_v   = end   if end   and _ISO_DATE_RE.match(end)   else None

    conditions = [
        "COALESCE(c.market, 'US') = :market",
        "c.ticker NOT IN ('SPY', 'VOO')",
    ]
    params: dict = {"market": mkt}

    if start_v:
        conditions.append("DATE(s.date) >= :start_date")
        params["start_date"] = start_v
    if end_v:
        conditions.append("DATE(s.date) <= :end_date")
        params["end_date"] = end_v
    if signal:
        conditions.append("s.signal = :signal")
        params["signal"] = signal.upper()
    if search:
        conditions.append("(LOWER(s.ticker) LIKE :search OR LOWER(c.company) LIKE :search)")
        params["search"] = f"%{search.lower()}%"

    where = "WHERE " + " AND ".join(conditions)

    sql = text(f"""
        SELECT DATE(s.date) AS date,
               s.ticker,
               c.company, c.sector, c.industry,
               (f.market_cap / 1.0e9) AS "market_cap(b)",
               s.open, s.high, s.low, s.close,
               (s.volume / 1.0e6) AS "volume(m)",
               s.rsi, s.bb_high, s.bb_low, s.macd,
               s.rsi_factor, s.bb_factor, s.macd_factor, s.trend_factor,
               (s.volume_sma_20 / 1.0e6) AS "volume_sma_20(m)",
               s.volume_ratio,
               s.week52_high, s.pct_from_52w_high,
               s.target_mean_price, s.target_high_price, s.target_low_price,
               s.number_of_analysts,
               CAST(s.health_score AS REAL) AS health_score,
               s.prediction_score, s.fair_value_upside,
               s.bb_pct_b,
               s.vqs, s.signal, s.lot_seq, s.score, s.vesign_score,
               s.news_block_reason
        FROM signals s
        LEFT JOIN companies c ON c.ticker = s.ticker
        LEFT JOIN fundamentals f ON f.ticker = s.ticker
        {where}
        ORDER BY {sort_col} {direction}, s.ticker ASC
        LIMIT {EXPORT_ROW_LIMIT}
    """)

    # Excel number formats — see column spec on /api/signals/export.
    _SIGNED_PCT = '+0.00%;[Red](0.00%);-'
    column_formats = {
        "date":              _EXPORT_DATE_FMT,
        "market_cap(b)":     _EXPORT_MCAP_FMT,
        "volume(m)":         "0.00",
        "volume_sma_20(m)":  "0.00",
        "rsi":               "0.00",
        "bb_high":           "0.00",
        "bb_low":            "0.00",
        "macd":              "0.00",
        "rsi_factor":        "0.00",
        "bb_factor":         "0.00",
        "macd_factor":       "0.00",
        "trend_factor":      "0.00",
        "score":             "0.00",
        "volume_ratio":      "0.00%",
        "pct_from_52w_high": "0.00%",
        "target_mean_price": "0.0",
        "target_high_price": "0.0",
        "target_low_price":  "0.0",
        "health_score":      "0",
        "prediction_score":  _SIGNED_PCT,
        "fair_value_upside": _SIGNED_PCT,
        "bb_pct_b":          _SIGNED_PCT,
    }

    today = datetime.now(UTC).date().isoformat()
    with engine.connect() as conn:
        return dispatch_cursor_response(
            format, conn, sql, params,
            filename=f"signals_{today}", sheet_name="signals",
            column_formats=column_formats,
            date_columns=("date",),
            auto_size=True,
        )


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
            SELECT s.ticker, c.company, c.logo_url, c.industry, c.domain,
                   c.description, c.description_short,
                   COALESCE(lp.latest_close, s.close) AS close, s.signal, s.vqs, s.rsi,
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
            SELECT c.ticker, c.company, c.logo_url, c.industry, c.domain,
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
def signal_markers(ticker: str, months: int = Query(default=13, ge=1, le=144)):
    """Return all BUY/SELL signals for a ticker over the last N months (for chart overlay)."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT DATE(date) AS date, signal, lot_seq, close
            FROM signals
            WHERE ticker = :t
              AND signal IN ('BUY', 'SELL')
              AND DATE(date) >= DATE('now', :offset)
            ORDER BY date ASC
        """), conn, params={"t": ticker, "offset": f"-{months} months"})
    # Use _records so NaN lot_seq (HOLD/SELL rows) becomes JSON null — raw NaN
    # in the response breaks browser JSON.parse and silently empties the chart markers.
    return _records(df)


@protected.get("/api/signals/success-rate")
def signals_success_rate(months: int = Query(default=12, ge=1, le=120)):
    """BUY→SELL success rate from trade_log (US only) over the last N months."""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT tl.ticker, tl.buy_date, tl.sell_date,
                   tl.buy_price, tl.sell_price, tl.return_pct,
                   c.company, c.logo_url, c.domain, f.market_cap
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
    """Historical analyst targets (Low/Base/High) for a ticker, point-in-time.

    Returns ONLY the per-date historical values stored in signals (which the
    engine writes from analyst_targets_history with a 90-day staleness cutoff).
    No fallback to the current analyst_expectations snapshot — that would
    fabricate flat target lines on the chart for any ticker without historical
    coverage (e.g. newly-added tickers). Frontend forward-fills from the first
    real entry; dates before that get no target line, which is the correct
    point-in-time behavior per feedback_analyst_forward_fill.md.
    """
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    start_date = start or "2026-01-01"
    end_date   = end   or date.today().isoformat()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT DATE(s.date) AS date,
                   s.target_mean_price,
                   s.target_low_price,
                   s.target_high_price
            FROM signals s
            WHERE s.ticker = :ticker
              AND s.target_mean_price IS NOT NULL
              AND DATE(s.date) BETWEEN :start AND :end
            ORDER BY s.date ASC
        """), conn, params={"ticker": ticker, "start": start_date, "end": end_date})
    return _records(df)


@protected.get("/api/prices/live")
def live_prices(tickers: str = Query(..., description="Comma-separated ticker symbols")):
    """Fetch real-time prices, phase-aware (regular vs extended hours)."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="No tickers provided")

    info = _phase_info()
    phase = info["phase"]

    if phase == "idle":
        return {"phase": "idle", "prices": {t: None for t in ticker_list}}

    global _live_price_cache, _live_price_cache_ts, _live_price_cache_phase
    now = time.time()
    if _live_price_cache_phase != phase:
        # phase changed → flush cache to avoid stale prices from prior phase
        _live_price_cache.clear()
        _live_price_cache_phase = phase

    cached = {t: _live_price_cache[t] for t in ticker_list if t in _live_price_cache}
    stale  = [t for t in ticker_list if t not in _live_price_cache or now - _live_price_cache_ts > _LIVE_CACHE_TTL]
    if stale:
        if phase == "regular":
            fresh = fetch_live_prices(stale)
        else:  # 'pre' or 'post'
            fresh = fetch_aftermarket_trades(stale)
        _live_price_cache.update(fresh)
        _live_price_cache_ts = now
        cached.update(fresh)

    return {"phase": phase, "prices": cached}


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


@protected.get("/api/watchlists/{list_id}/export")
@protected.get("/api/watchlists/{list_id}/export.xlsx")  # legacy alias
def watchlist_export(
    list_id: int,
    format: str = Query(default="xlsx", description="xlsx | csv | zip"),
    user=Depends(get_current_user),
):
    """Export the watchlist's tickers (xlsx/csv/zip) — one row per ticker, latest signals row + company refs."""
    from backend.exports import dispatch_dataframe_response
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
                SELECT s.*, c.company, c.sector, c.industry, c.logo_url, c.domain, f.market_cap
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

    _prepare_export_df(df, "date", "last_update")

    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", watchlist_name).strip("_") or f"list_{list_id}"
    today = datetime.now(UTC).date().isoformat()
    return dispatch_dataframe_response(
        format, df, filename=f"watchlist_{safe}_{today}", sheet_name="watchlist",
        column_formats={
            "date":        _EXPORT_DATE_FMT,
            "last_update": _EXPORT_DATE_FMT,
            "market_cap":  _EXPORT_MCAP_FMT,
        },
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

_historical_trades_cache: dict[str, dict] = {}
_historical_trades_cache_lock = threading.Lock()


def _build_historical_trades(mkt: str, start, end, include_lots: bool) -> list:
    """BUY→SELL trade pairs from pre-built trade_log table."""
    conditions = [
        "COALESCE(c.market, 'US') = :market",
        "c.ticker NOT IN ('SPY', 'VOO')",
    ]
    params: dict = {"market": mkt}
    # Use string compare (not DATE()) so trade_log.sell_date can be filtered
    # without wrapping the column. End is converted to exclusive end+1 day so
    # rows whose stored timestamp is "YYYY-MM-DD 00:00:00.000000" still match.
    if start:
        conditions.append("tl.sell_date >= :start")
        params["start"] = start
    if end:
        from datetime import datetime as _dt, timedelta as _td
        conditions.append("tl.sell_date < :end_p1")
        params["end_p1"] = (_dt.strptime(end, "%Y-%m-%d") + _td(days=1)).strftime("%Y-%m-%d")

    where = "WHERE " + " AND ".join(conditions)

    def _v(v):
        return None if (isinstance(v, float) and math.isnan(v)) else v

    # Run all 3 read-only queries on one connection to avoid NullPool overhead
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT tl.ticker, tl.buy_date, tl.buy_price, tl.sell_date, tl.sell_price, tl.return_pct,
                   c.company, c.logo_url, c.industry, c.domain, c.description, c.description_short,
                   h.score AS health_fallback, h.reason AS health_reason,
                   sb.health_score AS health_signal
            FROM trade_log tl
            LEFT JOIN companies c ON tl.ticker = c.ticker
            LEFT JOIN company_health h ON tl.ticker = h.ticker
            -- String-equality JOIN uses idx_signals_unique_ticker_date for the
            -- full composite key (ticker, date). DATE()=DATE() degraded the
            -- planner to a ticker-only index probe (~200x slower for 5Y).
            LEFT JOIN signals sb ON sb.ticker = tl.ticker AND sb.date = tl.buy_date
            {where}
            ORDER BY tl.ticker, tl.buy_date
        """), conn, params=params)

        # --- Bulk as-of lookups to replace N+1 correlated subqueries ---------
        tickers_in_result = df["ticker"].unique().tolist()
        if tickers_in_result:
            ph = ",".join([f":t{i}" for i in range(len(tickers_in_result))])
            tp = {f"t{i}": t for i, t in enumerate(tickers_in_result)}

            mcap_rows = conn.execute(text(f"""
                SELECT ticker, date, shares_outstanding
                FROM market_cap_history
                WHERE ticker IN ({ph}) AND shares_outstanding IS NOT NULL
                ORDER BY ticker, date
            """), tp).fetchall()

            health_rows = conn.execute(text(f"""
                SELECT ticker, recorded_at, score
                FROM company_health_history
                WHERE ticker IN ({ph}) AND score IS NOT NULL
                ORDER BY ticker, recorded_at
            """), tp).fetchall()
        else:
            mcap_rows = []
            health_rows = []

    # Build per-ticker sorted (date_str, value) pairs for bisect lookups
    mcap_idx: dict[str, tuple] = {}
    for ticker, d, so in mcap_rows:
        entry = mcap_idx.setdefault(ticker, ([], []))
        entry[0].append(str(d)[:10])
        entry[1].append(so)

    health_idx: dict[str, tuple] = {}
    for ticker, ra, score in health_rows:
        entry = health_idx.setdefault(ticker, ([], []))
        entry[0].append(str(ra)[:10])
        entry[1].append(score)

    def _as_of(idx, ticker, buy_date_str):
        """Latest value at or before buy_date_str. Returns None if no history."""
        entry = idx.get(ticker)
        if not entry:
            return None
        dates, vals = entry
        pos = bisect.bisect_right(dates, buy_date_str)
        return vals[pos - 1] if pos > 0 else None

    # -------------------------------------------------------------------------

    def _num(v, decimals=2):
        """Return rounded float or None for NaN/None values."""
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return round(float(v), decimals)

    # Pre-compute date strings and days_held vectorised (avoids slow per-row pd.to_datetime)
    df["buy_date_s"]  = df["buy_date"].astype(str).str[:10]
    df["sell_date_s"] = df["sell_date"].apply(
        lambda v: str(v)[:10] if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
    )
    # days_held: vectorised date subtraction — far faster than row-by-row pd.to_datetime
    _buy_dt  = pd.to_datetime(df["buy_date_s"],  errors="coerce")
    _sell_dt = pd.to_datetime(df["sell_date_s"], errors="coerce")
    df["days_held_v"] = (_sell_dt - _buy_dt).dt.days.where(_sell_dt.notna()).astype(object)
    df.loc[df["days_held_v"].isna(), "days_held_v"] = None

    # Group by ticker
    ticker_trades: dict = {}
    for ticker, grp in df.groupby("ticker"):
        pairs = []
        for row in grp.itertuples(index=False):
            raw_ret    = row.return_pct
            ret        = _num(raw_ret * 100) if raw_ret is not None and not (isinstance(raw_ret, float) and math.isnan(raw_ret)) else None
            buy_price  = _num(row.buy_price)
            sell_price = _num(row.sell_price)
            buy_date   = row.buy_date_s
            sell_date  = row.sell_date_s
            days_held  = int(row.days_held_v) if row.days_held_v is not None else None
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

        # --- as-of market_cap and health_score using bisect ------------------
        # Use the most recent trade's buy_date as the representative as-of date
        # (same behaviour as the old grp.iloc[0] which took the first row)
        first_buy_date_iso = grp.iloc[0]["buy_date_s"]
        shares = _as_of(mcap_idx, ticker, first_buy_date_iso)
        first_buy_price = grp.iloc[0]["buy_price"]
        mc = (float(first_buy_price) * shares) if (shares is not None and first_buy_price is not None) else None

        hs_hist = _as_of(health_idx, ticker, first_buy_date_iso)
        signal_health = grp.iloc[0].get("health_signal")
        fallback_health = grp.iloc[0].get("health_fallback")
        if hs_hist is not None:
            score = hs_hist
        elif signal_health is not None and not (isinstance(signal_health, float) and math.isnan(signal_health)):
            score = int(signal_health)
        elif fallback_health is not None and not (isinstance(fallback_health, float) and math.isnan(fallback_health)):
            score = int(fallback_health)
        else:
            score = None
        # ---------------------------------------------------------------------
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
            "domain":            _v(grp.iloc[0]["domain"]),
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

    # Enrichment queries — all in one connection to avoid NullPool overhead
    if ticker_trades:
        tickers_list = list(ticker_trades.keys())
        with engine.connect() as conn_enrich:
            # Organic yield (only when date range given). Index-friendly form:
            # the previous version wrapped daily_prices.date in date() which
            # forced a per-ticker scan of every price row instead of using the
            # (ticker, date) index for a range probe. Removing the date() calls
            # lets the planner use idx_daily_prices_ticker_date for both the
            # WHERE filter and the inner JOINs (5-34x faster on cold path).
            if start and end:
                from datetime import datetime as _dt, timedelta as _td
                _start_dt = _dt.strptime(start, "%Y-%m-%d")
                _end_dt   = _dt.strptime(end,   "%Y-%m-%d")
                lower    = (_start_dt - _td(days=30)).strftime("%Y-%m-%d")
                start_p1 = (_start_dt + _td(days=1)).strftime("%Y-%m-%d")
                end_p1   = (_end_dt   + _td(days=1)).strftime("%Y-%m-%d")
                placeholders = ", ".join([f":t{i}" for i in range(len(tickers_list))])
                p_org = {f"t{i}": t for i, t in enumerate(tickers_list)}
                p_org["lower"]    = lower     # start - 30d (lookback for non-trading start)
                p_org["start_p1"] = start_p1  # exclusive upper for first_close
                p_org["end_p1"]   = end_p1    # exclusive upper for last_close + WHERE
                df_org = pd.read_sql(text(f"""
                    WITH bounds AS (
                        SELECT ticker,
                               MAX(CASE WHEN date < :start_p1 THEN date END) AS fd,
                               MAX(CASE WHEN date < :end_p1   THEN date END) AS ld
                        FROM daily_prices
                        WHERE date >= :lower
                          AND date <  :end_p1
                          AND ticker IN ({placeholders})
                        GROUP BY ticker
                    )
                    SELECT b.ticker, d1.close AS first_close, d2.close AS last_close
                    FROM bounds b
                    JOIN daily_prices d1 ON d1.ticker = b.ticker AND d1.date = b.fd
                    JOIN daily_prices d2 ON d2.ticker = b.ticker AND d2.date = b.ld
                """), conn_enrich, params=p_org)
                for _, org_row in df_org.iterrows():
                    t = org_row["ticker"]
                    fc, lc = org_row["first_close"], org_row["last_close"]
                    if t in ticker_trades and fc and float(fc) > 0:
                        ticker_trades[t]["organic_yield"] = round(float((lc - fc) / fc * 100), 2)

            # Current signal
            ph = ", ".join([f":h{i}" for i in range(len(tickers_list))])
            hp = {f"h{i}": t for i, t in enumerate(tickers_list)}
            df_curr = pd.read_sql(text(f"""
                SELECT s.ticker, s.signal AS current_signal
                FROM signals s
                JOIN (SELECT ticker, MAX(date) AS md FROM signals
                      WHERE ticker IN ({ph}) GROUP BY ticker) l
                  ON s.ticker = l.ticker AND s.date = l.md
            """), conn_enrich, params=hp)
            for _, sr in df_curr.iterrows():
                t = sr["ticker"]
                if t in ticker_trades:
                    ticker_trades[t]["current_signal"] = sr["current_signal"]

            # DCA lots (only when include_lots=1)
            if include_lots:
                ph_l = ", ".join([f":l{i}" for i in range(len(tickers_list))])
                lp = {f"l{i}": t for i, t in enumerate(tickers_list)}
                lots_df = pd.read_sql(text(f"""
                    SELECT ticker, buy_date, sell_date, lot_seq, lot_date, lot_price
                    FROM trade_lots
                    WHERE ticker IN ({ph_l})
                    ORDER BY ticker, buy_date, sell_date, lot_seq
                """), conn_enrich, params=lp)
                lots_by_key: dict = {}
                for _, lr in lots_df.iterrows():
                    key = (lr["ticker"], str(lr["buy_date"])[:10], str(lr["sell_date"])[:10])
                    lots_by_key.setdefault(key, []).append({
                        "seq":   int(lr["lot_seq"]),
                        "date":  str(lr["lot_date"])[:10],
                        "price": round(float(lr["lot_price"]), 4),
                    })
                for tk_data in ticker_trades.values():
                    for pair in tk_data["trades"]:
                        if pair["result"] == "Open" or not pair["buy_date"] or not pair["sell_date"]:
                            continue
                        lots = lots_by_key.get(
                            (tk_data["ticker"], pair["buy_date"], pair["sell_date"]), []
                        )
                        if lots:
                            pair["lots"]     = lots
                            pair["n_lots"]   = len(lots)
                            pair["avg_cost"] = round(avg_cost_dollar_weighted(l["price"] for l in lots), 4)

    return list(ticker_trades.values())


def _get_historical_trades_cached(mkt: str, start, end, include_lots: bool) -> list:
    cache_key = f"{mkt}|{start or ''}|{end or ''}|{int(bool(include_lots))}"
    today_iso = date.today().isoformat()
    with _historical_trades_cache_lock:
        c = _historical_trades_cache.get(cache_key)
        if c is not None and c["built_on"] == today_iso:
            return c["data"]
        data = _build_historical_trades(mkt, start, end, include_lots)
        _historical_trades_cache[cache_key] = {"built_on": today_iso, "data": data}
        return data


@protected.get("/api/trades")
def historical_trades(
    start: Optional[str] = None,
    end: Optional[str] = None,
    market: Optional[str] = None,
    include_lots: Optional[int] = 0,
):
    """BUY→SELL trade pairs from pre-built trade_log table."""
    mkt = (market or "US").upper()
    return _get_historical_trades_cached(mkt, start, end, bool(include_lots))


@protected.get("/api/trades/export")
@protected.get("/api/trades/export.xlsx")  # legacy alias
def trades_export(
    start: Optional[str] = None,
    end: Optional[str] = None,
    market: Optional[str] = None,
    format: str = Query(default="xlsx", description="xlsx | csv | zip"),
):
    """Export of closed trades (xlsx/csv/zip) — one row per trade, full columns + company refs."""
    from backend.exports import dispatch_dataframe_response

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
               f.market_cap,
               tl.buy_date,
               tl.buy_price,
               tl.sell_date,
               tl.sell_price,
               tl.return_pct,
               s.open, s.high, s.low, s.close, s.volume,
               s.rsi, s.bb_high, s.bb_low, s.macd,
               s.rsi_factor, s.bb_factor, s.macd_factor, s.trend_factor,
               s.volume_sma_20, s.volume_ratio,
               s.week52_high, s.pct_from_52w_high,
               s.target_mean_price, s.target_high_price, s.target_low_price,
               s.number_of_analysts,
               s.health_score, s.prediction_score, s.fair_value_upside,
               p.pred_5d, p.pred_20d,
               s.bb_pct_b,
               s.vqs, s.signal, s.score, s.vesign_score,
               tlots.n_lots, tlots.avg_cost,
               tl_lot.lot_seq, tl_lot.lot_date, tl_lot.lot_price,
               CASE
                 WHEN s.rsi_3day_flag = 3 AND s.bb_condition = 1 AND s.analyst_condition = 1
                      AND s.volume_flag = 1 AND s.week52_condition = 1
                      AND s.health_condition = 1 AND s.ml_condition = 1
                      AND s.vqs = 9 THEN 'V1+V2'
                 WHEN s.rsi_3day_flag = 3 AND s.bb_condition = 1 AND s.analyst_condition = 1
                      AND s.volume_flag = 1 AND s.week52_condition = 1
                      AND s.health_condition = 1 AND s.ml_condition = 1 THEN 'V1'
                 WHEN s.vqs = 9 THEN 'V2'
                 ELSE NULL
               END AS buy_path
        FROM trade_log tl
        LEFT JOIN companies c ON c.ticker = tl.ticker
        LEFT JOIN fundamentals f ON f.ticker = tl.ticker
        LEFT JOIN signals s
          ON s.ticker = tl.ticker AND DATE(s.date) = DATE(tl.buy_date)
        LEFT JOIN predictions p
          ON p.ticker = tl.ticker AND DATE(p.date) = DATE(tl.buy_date)
        LEFT JOIN (
            SELECT ticker, DATE(buy_date) AS bd, DATE(sell_date) AS sd,
                   COUNT(*) AS n_lots,
                   COUNT(*) * 1.0 / NULLIF(SUM(1.0 / NULLIF(lot_price, 0)), 0) AS avg_cost
            FROM trade_lots
            GROUP BY ticker, DATE(buy_date), DATE(sell_date)
        ) tlots ON tlots.ticker = tl.ticker
                AND tlots.bd   = DATE(tl.buy_date)
                AND tlots.sd   = DATE(tl.sell_date)
        -- Per-lot expansion: multi-lot trades produce one row per lot.
        -- Trades absent from trade_lots stay as a single row with NULL lot cols.
        LEFT JOIN trade_lots tl_lot
               ON tl_lot.ticker = tl.ticker
              AND DATE(tl_lot.buy_date)  = DATE(tl.buy_date)
              AND DATE(tl_lot.sell_date) = DATE(tl.sell_date)
        WHERE {' AND '.join(where)}
        ORDER BY tl.sell_date DESC, tl.ticker ASC, tl_lot.lot_seq ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    # Market cap → billions, ISO-string dates → real datetimes for Excel.
    _prepare_export_df(df, "buy_date", "sell_date", "lot_date")

    # DCA return: yield against avg_cost (vs return_pct which is vs first lot only)
    if "avg_cost" in df.columns and "sell_price" in df.columns:
        df["dca_return_pct"] = (df["sell_price"] - df["avg_cost"]) / df["avg_cost"]

    # Per-lot return: yield of this specific lot's entry price against the sell
    if "lot_price" in df.columns and "sell_price" in df.columns:
        df["lot_return_pct"] = (df["sell_price"] - df["lot_price"]) / df["lot_price"]

    today = datetime.now(UTC).date().isoformat()
    return dispatch_dataframe_response(
        format, df,
        filename=f"trades_closed_{today}",
        sheet_name="trades",
        column_formats={
            "buy_date":       _EXPORT_DATE_FMT,
            "sell_date":      _EXPORT_DATE_FMT,
            "lot_date":       _EXPORT_DATE_FMT,
            "market_cap":     _EXPORT_MCAP_FMT,
            "return_pct":     "0.00%",
            "dca_return_pct": "0.00%",
            "lot_return_pct": "0.00%",
        },
    )


# In-memory cache for /api/trades/open — the underlying CTE scans the signals
# table 3× and takes ~16s on prod data. The result only changes when the daily
# pipeline rewrites signals, so a once-per-day rebuild per market is correct.
_open_trades_cache: dict[str, dict] = {}
_open_trades_cache_lock = threading.Lock()


# Strict ≤90%-of-previous-lot DCA rule, applied live for currently-open positions.
# Mirrors the trade_lots table-builder (closed trades) but operates on
# (buy_date → today] instead of (buy_date → sell_date].
_DCA_V1 = ("(rsi_3day_flag = 3 AND bb_condition = 1 AND analyst_condition = 1 "
           "AND volume_flag = 1 AND week52_condition = 1 AND health_condition = 1 "
           "AND ml_condition = 1)")
_DCA_V2 = "(vqs = 9)"


def _compute_open_trade_lots(open_positions: list[dict]) -> dict[str, list[dict]]:
    """For each currently-open position, compute DCA lots from buy_date to today.
    Lot 1 = original BUY. Each subsequent re-fire counts only if it (a) passes
    V1 or V2 and (b) closes ≤ 90% of the last taken lot's price."""
    if not open_positions:
        return {}
    today_iso = date.today().isoformat()
    lots_by_ticker: dict[str, list[dict]] = {}
    # Under Path-B DCA the engine writes BUY (with lot_seq) for add-on lots
    # that already passed V1/V2 + the 90% rule. Older rows in the same window
    # may still be HOLD. We include both and exclude SELL; the 90% rule below
    # is the actual lot-eligibility check.
    sql = text(f"""
        SELECT date, close FROM signals
        WHERE ticker = :tk
          AND DATE(date) > DATE(:bd)
          AND DATE(date) <= DATE(:today)
          AND signal != 'SELL'
          AND ({_DCA_V1} OR {_DCA_V2})
          AND close IS NOT NULL
        ORDER BY date
    """)
    with engine.connect() as conn:
        for pos in open_positions:
            tk = pos["ticker"]
            bd = pos["buy_date"]
            bp = pos["buy_price"]
            if bp is None:
                continue
            lots = [{"seq": 1, "date": bd, "price": float(bp)}]
            last_price = float(bp)
            seq = 1
            for d, px in conn.execute(sql, {"tk": tk, "bd": bd, "today": today_iso}).fetchall():
                px = float(px)
                if px <= last_price * 0.90:
                    seq += 1
                    lots.append({"seq": seq, "date": str(d)[:10], "price": round(px, 4)})
                    last_price = px
            lots_by_ticker[tk] = lots
    return lots_by_ticker


def _build_open_trades(mkt: str, include_lots: bool = False) -> list[dict]:
    """Slow path — same body as the endpoint, kept here for the cache builder."""

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

        # ML prediction fields at the buy date — pred_5d, pred_20d, prediction_score (blended)
        buy_predictions = {r[0]: (r[1], r[2], r[3]) for r in conn.execute(text(f"""
            WITH pairs(t, d) AS (VALUES {buy_vals})
            SELECT p.ticker, p.pred_5d, p.pred_20d, p.prediction_score
            FROM predictions p
            JOIN pairs ON p.ticker = pairs.t AND p.date = pairs.d
        """)).fetchall()}

        # Which strategy path triggered the BUY: V1 / V2 / V1+V2 / NULL.
        # Computed from the gate columns the engine wrote at scoring time.
        buy_paths = {r[0]: r[1] for r in conn.execute(text(f"""
            WITH pairs(t, d) AS (VALUES {buy_vals})
            SELECT s.ticker,
                   CASE
                     WHEN s.rsi_3day_flag = 3 AND s.bb_condition = 1 AND s.analyst_condition = 1
                          AND s.volume_flag = 1 AND s.week52_condition = 1
                          AND s.health_condition = 1 AND s.ml_condition = 1
                          AND s.vqs = 9 THEN 'V1+V2'
                     WHEN s.rsi_3day_flag = 3 AND s.bb_condition = 1 AND s.analyst_condition = 1
                          AND s.volume_flag = 1 AND s.week52_condition = 1
                          AND s.health_condition = 1 AND s.ml_condition = 1 THEN 'V1'
                     WHEN s.vqs = 9 THEN 'V2'
                     ELSE NULL
                   END AS buy_path
            FROM signals s
            JOIN pairs ON s.ticker = pairs.t AND s.date = pairs.d
        """)).fetchall()}

        # Step 4: latest price per ticker (IN query, ticker_date index)
        latest_prices = {r[0]: r[1] for r in conn.execute(text(f"""
            SELECT dp.ticker, dp.close AS current_price
            FROM daily_prices dp
            JOIN (SELECT ticker, MAX(date) AS md FROM daily_prices WHERE ticker IN ({ph}) GROUP BY ticker) lp
                ON dp.ticker = lp.ticker AND dp.date = lp.md
        """), tp).fetchall()}

        # Step 5: company info + health + market cap (IN query, small result).
        # Prefer latest company_health_history row over company_health so tickers
        # without a current snapshot still show health (MLKN etc.).
        df_meta = pd.read_sql(text(f"""
            SELECT c.ticker, c.company, c.logo_url, c.industry, c.market, c.domain,
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
        if ticker in ("SPY", "VOO"):
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
        p5, p20, pscore = buy_predictions.get(ticker, (None, None, None))
        result.append({
            "ticker":            ticker,
            "company":           _v(m.get("company")),
            "logo_url":          _v(m.get("logo_url")),
            "industry":          _v(m.get("industry")),
            "domain":            _v(m.get("domain")),
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
            "pred_5d":           _v(float(p5))     if p5     is not None else None,
            "pred_20d":          _v(float(p20))    if p20    is not None else None,
            "prediction_score":  _v(float(pscore)) if pscore is not None else None,
            "buy_path":          buy_paths.get(ticker),
        })

    result.sort(key=lambda x: x["buy_date"], reverse=True)

    if include_lots:
        lots_by_ticker = _compute_open_trade_lots(result)
        for r in result:
            lots = lots_by_ticker.get(r["ticker"], [])
            if lots:
                r["lots"]     = lots
                r["n_lots"]   = len(lots)
                r["avg_cost"] = round(avg_cost_dollar_weighted(l["price"] for l in lots), 4)

    return result


def _get_open_trades_cached(mkt: str, include_lots: bool = False) -> list[dict]:
    today_iso = date.today().isoformat()
    cache_key = f"{mkt}:lots" if include_lots else mkt
    with _open_trades_cache_lock:
        c = _open_trades_cache.get(cache_key)
        if c is not None and c["built_on"] == today_iso:
            return c["data"]
        data = _build_open_trades(mkt, include_lots=include_lots)
        _open_trades_cache[cache_key] = {"built_on": today_iso, "data": data}
        return data


@protected.get("/api/trades/open")
def open_trades(market: Optional[str] = None, include_lots: Optional[int] = 0):
    """Tickers with a BUY signal and no SELL since — currently open positions."""
    return _get_open_trades_cached((market or "US").upper(), bool(include_lots))


@protected.get("/api/trades/open/export")
@protected.get("/api/trades/open/export.xlsx")  # legacy alias
def open_trades_export(
    market: Optional[str] = None,
    format: str = Query(default="xlsx", description="xlsx | csv | zip"),
):
    """Export of currently open positions (BUY with no subsequent SELL) — xlsx/csv/zip."""
    from backend.exports import dispatch_dataframe_response

    mkt = (market or "US").upper()

    rows = open_trades(market=mkt, include_lots=1)   # include lots so export has DCA columns
    df = pd.DataFrame(rows)

    if not df.empty:
        # DCA-aware unrealized yield (vs avg_cost rather than first lot's buy_price)
        if "avg_cost" in df.columns and "current_price" in df.columns:
            df["dca_unrealized_pct"] = (
                (df["current_price"] - df["avg_cost"]) / df["avg_cost"] * 100
            )
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

        # Per-lot expansion: multi-lot positions produce one row per lot.
        # Positions without a lots array (rare — buy_price was missing) keep a
        # single row with NULL lot columns.
        if "lots" in df.columns:
            df["lots"] = df["lots"].apply(
                lambda x: x if isinstance(x, list) and x else [{"seq": None, "date": None, "price": None}]
            )
            df = df.explode("lots", ignore_index=True)
            df["lot_seq"]   = df["lots"].apply(lambda d: d.get("seq")   if isinstance(d, dict) else None)
            df["lot_date"]  = df["lots"].apply(lambda d: d.get("date")  if isinstance(d, dict) else None)
            df["lot_price"] = df["lots"].apply(lambda d: d.get("price") if isinstance(d, dict) else None)
            df = df.drop(columns=["lots"])
            # Per-lot unrealized return vs that lot's entry price
            if "current_price" in df.columns:
                df["lot_unrealized_pct"] = (
                    (df["current_price"] - df["lot_price"]) / df["lot_price"] * 100
                )

        _prepare_export_df(df, "buy_date", "lot_date")

    today = datetime.now(UTC).date().isoformat()
    return dispatch_dataframe_response(
        format, df,
        filename=f"trades_open_{today}",
        sheet_name="open_trades",
        column_formats={
            "buy_date":   _EXPORT_DATE_FMT,
            "lot_date":   _EXPORT_DATE_FMT,
            "market_cap": _EXPORT_MCAP_FMT,
        },
    )


# --- News & analyst endpoints -----------------------------------------------

@protected.get("/api/news")
def stock_news_endpoint(
    ticker: str = Query(...),
    limit: int = Query(default=5, ge=1, le=20),
    lang: Optional[str] = Query(default=None),
):
    """Recent news headlines for a ticker. Titles are translated to `lang`
    (en/he/es/fr/de/it) when provided; non-en is cached after first request.
    Sites, URLs, dates are NOT translated."""
    from data import fmp as _fmp
    from backend.translation import translate_batch as _tx_batch
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    items = _fmp.stock_news(ticker, limit) or []
    if items and lang and lang.lower().split("-")[0] != "en":
        titles = [item.get("title", "") for item in items]
        translated = _tx_batch(titles, lang)
        for item, t in zip(items, translated):
            if t:
                item["title"] = t
    return items


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
            SELECT c.ticker, c.company, c.logo_url, c.industry, c.domain,
                   f.market_cap,
                   lp.latest_close,
                   pp.prev_close
            FROM companies c
            LEFT JOIN (SELECT ticker, MAX(market_cap) AS market_cap FROM fundamentals GROUP BY ticker) f
                ON c.ticker = f.ticker
            LEFT JOIN (
                SELECT p1.ticker, p1.close AS latest_close, p2.max_date
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
        # Column order: ticker, company, logo_url, industry, domain, market_cap, latest_close, prev_close
        mc = m[5] if m else None
        latest_close = m[6] if m else None
        prev_close = m[7] if m else None
        result.append({
            "ticker": ticker,
            "company": m[1] if m else None,
            "logo_url": m[2] if m else None,
            "industry": m[3] if m else None,
            "domain": m[4] if m else None,
            "market_cap": int(mc) if mc is not None and not (isinstance(mc, float) and math.isnan(mc)) else None,
            "total_qty": total_qty,
            "total_cost": round(total_cost, 2) if total_cost is not None else None,
            "avg_price": round(avg_price, 4) if avg_price is not None else None,
            "latest_close": round(float(latest_close), 4) if latest_close is not None else None,
            "prev_close": round(float(prev_close), 4) if prev_close is not None else None,
            "first_buy_date": first_buy_date,
        })
    return result


@protected.get("/api/portfolio/holdings/export")
@protected.get("/api/portfolio/holdings/export.xlsx")  # legacy alias
def portfolio_holdings_export(
    user=Depends(get_current_user),
    market: str = Query(default="US"),
    format: str = Query(default="xlsx", description="xlsx | csv | zip"),
):
    """Export of aggregated portfolio holdings (xlsx/csv/zip) — same shape as /api/portfolio/holdings."""
    from backend.exports import dispatch_dataframe_response

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

        _prepare_export_df(df, "first_buy_date")

    today = datetime.now(UTC).date().isoformat()
    return dispatch_dataframe_response(
        format, df, filename=f"portfolio_holdings_{today}", sheet_name="holdings",
        column_formats={
            "first_buy_date": _EXPORT_DATE_FMT,
            "market_cap":     _EXPORT_MCAP_FMT,
        },
    )


# --- Portfolio performance ---------------------------------------------------

# Module-level cache for the Vesign simulator inputs. These are user-independent
# (same for every request) but loading ~870K price rows from daily_prices is slow
# (~3.8s). Rebuild once per day per market; concurrent requests share the result.
_vesign_cache = {"US": None, "IL": None}
_vesign_cache_lock = threading.Lock()


def _build_vesign_cache(market: str):
    """Load every closed-trade lot + daily_prices for those tickers. Used by
    /api/portfolio/performance and /api/portfolio/comparison to drive a
    bank/hand compounding simulation. Lots missing from trade_lots fall back
    to a synthetic single lot built from trade_log.buy_date/buy_price."""
    from datetime import date as _date
    from collections import defaultdict

    trade_filter = "tl.ticker LIKE '%.TA'" if market == "IL" else "tl.ticker NOT LIKE '%.TA'"

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT tl.ticker,
                   DATE(tl.buy_date)  AS trade_buy_date,
                   tl.buy_price,
                   DATE(tl.sell_date) AS sell_date,
                   tl.sell_price,
                   DATE(l.lot_date)   AS lot_date,
                   l.lot_price,
                   l.lot_seq
            FROM trade_log tl
            LEFT JOIN trade_lots l
              ON l.ticker    = tl.ticker
             AND l.buy_date  = tl.buy_date
             AND l.sell_date = tl.sell_date
            LEFT JOIN companies c ON tl.ticker = c.ticker
            WHERE COALESCE(c.market, 'US') = (CASE WHEN :mkt = 'IL' THEN 'IL' ELSE 'US' END)
              AND tl.ticker NOT IN ('SPY', 'VOO')
              AND {trade_filter}
              AND tl.sell_date IS NOT NULL AND tl.return_pct IS NOT NULL
              AND tl.buy_price IS NOT NULL AND tl.sell_price IS NOT NULL
        """), {"mkt": market}).fetchall()

    vesign_lots: list[Lot] = []
    tickers: set[str] = set()
    for ticker, trade_buy_d, buy_p, sell_d, sell_p, lot_d, lot_p, _seq in rows:
        try:
            sell_price = float(sell_p)
            if lot_p is not None and float(lot_p) > 0:
                lot_price = float(lot_p)
                lot_date  = _date.fromisoformat(str(lot_d)[:10])
            else:
                lot_price = float(buy_p)
                lot_date  = _date.fromisoformat(str(trade_buy_d)[:10])
            vesign_lots.append(Lot(
                ticker=ticker,
                buy_date=lot_date,
                sell_date=_date.fromisoformat(str(sell_d)[:10]),
                lot_price=lot_price,
                sell_price=sell_price,
            ))
            tickers.add(ticker)
        except Exception:
            continue

    # Prefetch daily prices for MTM of open lots at intermediate eval dates
    price_rows = []
    if tickers:
        ph = ", ".join([f":t{i}" for i in range(len(tickers))])
        tp = {f"t{i}": t for i, t in enumerate(tickers)}
        with engine.connect() as conn:
            price_rows = conn.execute(text(f"""
                SELECT ticker, DATE(date) AS d, close
                FROM daily_prices
                WHERE ticker IN ({ph})
                ORDER BY ticker, date
            """), tp).fetchall()

    price_map: dict[str, list[tuple[_date, float]]] = defaultdict(list)
    for ticker, d_str, close in price_rows:
        try:
            price_map[ticker].append((_date.fromisoformat(str(d_str)[:10]), float(close)))
        except Exception:
            pass

    import bisect
    sorted_prices: dict[str, tuple[list[_date], list[float]]] = {}
    for t, lst in price_map.items():
        dates  = [d for d, _ in lst]
        closes = [c for _, c in lst]
        sorted_prices[t] = (dates, closes)

    def price_at(ticker: str, target: _date) -> Optional[float]:
        sp = sorted_prices.get(ticker)
        if not sp:
            return None
        dates, closes = sp
        i = bisect.bisect_right(dates, target)
        return closes[i - 1] if i > 0 else None

    return {
        "built_on":   _date.today().isoformat(),
        "lots":       vesign_lots,
        "price_at":   price_at,
        "all_dates":  sorted({d for lst in price_map.values() for d, _ in lst}),
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

    # Vesign line: bank/hand compounding simulation.
    # Universe = lots whose parent trade's sell_date is inside the chart window
    # (same filter as the Historical Trades card). At each week the yield is
    # the running yield: (equity / bank_drawn_so_far) − 1. The denominator must
    # be the cumulative draw up to that week — using the global peak would make
    # early-window weeks look wildly negative because future draws haven't
    # happened yet. At the last week bank_drawn_so_far == peak_bank, so the
    # final point matches the comparison-bar value by construction.
    window_lots = [lot for lot in cache["lots"]
                   if weeks[0] <= lot.sell_date <= weeks[-1]]
    sim = simulate_bank_hand(window_lots, cache["price_at"], weeks)
    points_by_week = {d: (eq, bd) for d, eq, bd in sim.equity_curve}

    def vesign_yield_at(target):
        pt = points_by_week.get(target)
        if pt is None:
            return None
        eq, bd = pt
        if bd <= 0:
            return None
        return (eq / bd) - 1

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

    # Vesign bar = bank/hand compounded yield over the same window
    cache = _get_vesign_cache(market)
    window_lots = [lot for lot in cache["lots"]
                   if start_date <= lot.sell_date <= today]
    sim = simulate_bank_hand(window_lots, cache["price_at"], [today])
    if sim.peak_bank > 0 and sim.equity_curve:
        vesign_val = round(((sim.equity_curve[-1][1] / sim.peak_bank) - 1) * 100, 2)
    else:
        vesign_val = None

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
def research_ticker(
    ticker: str,
    lang: Optional[str] = Query(default=None),
    user=Depends(get_current_user),
):
    """Aggregate all research data for a ticker including Vesign score.

    Optional `lang` (i18n locale: en/he/es/fr/de/it) translates user-facing
    free-text fields (currently `health_reason`). Numbers and codes are kept
    in their original form. Defaults to English (passthrough)."""
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")

    with engine.connect() as conn:
        # Latest signals row with company + fundamentals + analyst + health
        row = conn.execute(text("""
            SELECT s.ticker, COALESCE(lp.latest_close, s.close) AS close,
                   s.rsi, s.bb_pct_b, s.signal, s.vqs, NULL AS vesign_score,
                   s.fair_value_upside, s.rsi_3day_flag, s.volume_flag,
                   s.week52_condition, s.prediction_score,
                   COALESCE(ae.target_mean_price, s.target_mean_price) AS target_mean_price,
                   COALESCE(ae.target_low_price,  s.target_low_price)  AS target_low_price,
                   COALESCE(ae.target_high_price, s.target_high_price) AS target_high_price,
                   ae.number_of_analysts,
                   c.company, c.logo_url, c.industry, c.sector, c.market, c.domain,
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

    # Translate the only free-text field that benefits from localization.
    # Other fields are numbers, codes, or IDs that should not be translated.
    from backend.translation import translate as _tx
    health_reason_translated = _tx(_v(row.get("health_reason")) or "", lang)

    return {
        "ticker":              ticker,
        "company":             _v(row.get("company")),
        "logo_url":            _v(row.get("logo_url")),
        "industry":            _v(row.get("industry")),
        "sector":              _v(row.get("sector")),
        "market":              _v(row.get("market")),
        "domain":              _v(row.get("domain")),
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
        "health_reason":       health_reason_translated or None,
        "trade_count":         trade_count,
        "win_rate":            win_rate,
        "avg_return":          round(float(avg_return) * 100, 2) if avg_return is not None else None,
        # ML score is a public field used by the modal "ML Score" row.
        "prediction_score":    _v(row.get("prediction_score")),
        # Internal condition flags (used by AI report, not exposed as documented API)
        "_rsi_3day_flag":      _v(row.get("rsi_3day_flag")),
        "_volume_flag":        _v(row.get("volume_flag")),
        "_week52_condition":   _v(row.get("week52_condition")),
    }


@protected.post("/api/research/{ticker}/ai-report")
def research_ai_report(ticker: str, body: AIReportBody, user=Depends(get_current_user)):
    """Generate a Claude AI research note for a ticker."""
    ticker = ticker.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")

    # Fetch research data (pass lang explicitly — research_ticker's `lang`
    # parameter has a FastAPI Query default that only resolves through the
    # HTTP layer, not when called directly as a Python function).
    data = research_ticker(ticker, lang=body.lang, user=user)

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
    ml = data.get("prediction_score")
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
**Recommendation** — the recommendation verb MUST match the Vesign signal:
  - Signal "BUY"  → start with "Buy."
  - Signal "SELL" → start with "Reduce."
  - Signal "HOLD" → start with "Hold."
After the verb, give 2-3 sentences of rationale. You may flag concerns or
nuances in the rationale, but DO NOT contradict the headline verb.

Keep the total response under 300 words. Plain language, no jargon. Do not mention RSI, Bollinger Bands, ML scores, or any of the internal condition checks."""

    # Language directive — appended last so Claude follows it for the whole response.
    LANG_NAMES = {"en": "English", "he": "Hebrew", "es": "Spanish",
                  "fr": "French", "de": "German", "it": "Italian"}
    lang_code = (body.lang or "en").lower()
    lang_name = LANG_NAMES.get(lang_code, "English")
    if lang_code != "en":
        prompt += (
            f"\n\nIMPORTANT: Respond entirely in {lang_name}. "
            f"Translate the section headings (Current Situation, Key Risks, Recommendation) "
            f"and the recommendation verb (Buy/Hold/Avoid/Reduce) to {lang_name} as well. "
            f"Keep the ticker symbol, currency symbols ($), numbers, and percentages in their "
            f"original form. Use proper {lang_name} punctuation."
        )

    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        # max_tokens=1800 covers a ~300-word response even in Hebrew/Russian/etc.
        # where each character takes 2-3x more BPE tokens than Latin scripts.
        # English typically uses ~450 tokens, well under the cap.
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1800,
            messages=[{"role": "user", "content": prompt}],
        )
        report_text = message.content[0].text
        if message.stop_reason == "max_tokens":
            report_text += "\n\n[…]"  # explicit ellipsis if Claude STILL hit the cap
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


# Warm the Vesign performance cache in a background thread so the first request
# of the day doesn't pay the ~4s build cost.
def _warm_vesign_cache_bg():
    try:
        _get_vesign_cache("US")
    except Exception:
        pass

threading.Thread(target=_warm_vesign_cache_bg, daemon=True).start()


# Warm the open-trades cache too — its CTE takes ~16s on cold cache and is the
# slowest user-facing endpoint without warming.
def _warm_open_trades_cache_bg():
    try:
        _get_open_trades_cached("US", include_lots=False)
        _get_open_trades_cached("US", include_lots=True)
    except Exception:
        pass

threading.Thread(target=_warm_open_trades_cache_bg, daemon=True).start()


def _chip_months_ago(d: date, n: int) -> date:
    """today - n calendar months, mirroring JS Date.setMonth() in TradesPage —
    overflow days roll forward (May 31 - 3mo = Mar 3, not Feb 28). Matching JS
    is what keeps the warmer's cache keys aligned with the frontend's chip URLs."""
    from calendar import monthrange
    m_total = d.month - 1 - n
    new_year = d.year + m_total // 12
    new_month = m_total % 12 + 1
    last_day = monthrange(new_year, new_month)[1]
    if d.day <= last_day:
        return d.replace(year=new_year, month=new_month)
    excess = d.day - last_day
    next_month = new_month + 1 if new_month < 12 else 1
    next_year = new_year + 1 if new_month == 12 else new_year
    return date(next_year, next_month, excess)


def _warm_historical_trades_cache_bg():
    """Warm every chip the UI exposes — 3M / 6M / YTD / 1Y / 2Y / 3Y / 5Y +
    all-time — for both include_lots variants. Without this, first click on
    any chip other than 1Y/all-time pays a 3–9 s cold rebuild."""
    try:
        today = date.today()
        end = today.isoformat()
        starts = [_chip_months_ago(today, m).isoformat() for m in (3, 6, 12, 24, 36, 60)]
        starts.append(f"{today.year}-01-01")  # YTD
        for include_lots in (False, True):
            for s in starts:
                _get_historical_trades_cached("US", s, end, include_lots)
            _get_historical_trades_cached("US", None, None, include_lots)  # all time
    except Exception:
        pass

threading.Thread(target=_warm_historical_trades_cache_bg, daemon=True).start()


def _warm_signals_today_cache_bg():
    try:
        _get_signals_today_cached("US")
    except Exception:
        pass

threading.Thread(target=_warm_signals_today_cache_bg, daemon=True).start()

# ---------------------------------------------------------------------------
# Spotlight — daily "watch this" ticker for the Market page.
# Computes the engine's best non-BUY non-SELL near-miss on demand. No schema
# changes; see docs/superpowers/specs/2026-05-23-daily-spotlight-design.md
# ---------------------------------------------------------------------------

_spotlight_cache: dict[str, dict] = {}
_spotlight_cache_lock = threading.Lock()


def _build_spotlight_today() -> dict | None:
    """Compute today's Spotlight ticker, or None if no qualifying row exists.

    Ranking: highest V1 gates met, tiebreak by vqs DESC, prediction_score DESC,
    ticker ASC (deterministic). Excludes today's BUY and SELL tickers so the
    Spotlight never collides with the canonical signals. US-only.
    """
    sql = text("""
        SELECT
            s.date AS signal_date,
            s.ticker, c.company, c.domain,
            s.close, p.pred_5d, s.prediction_score, s.vqs,
            s.rsi_3day_flag, s.bb_condition, s.analyst_condition,
            s.volume_flag, s.week52_condition, s.health_condition, s.ml_condition,
            prev.close AS prev_close,
            (CASE WHEN s.rsi_3day_flag = 3 THEN 1 ELSE 0 END
             + COALESCE(s.bb_condition, 0)
             + COALESCE(s.analyst_condition, 0)
             + COALESCE(s.volume_flag, 0)
             + COALESCE(s.week52_condition, 0)
             + COALESCE(s.health_condition, 0)
             + COALESCE(s.ml_condition, 0)) AS gates_met
        FROM signals s
        LEFT JOIN companies c ON c.ticker = s.ticker
        LEFT JOIN predictions p
          ON p.ticker = s.ticker
          AND p.date = s.date
        LEFT JOIN signals prev
          ON prev.ticker = s.ticker
          AND prev.date = (
            SELECT MAX(date) FROM signals
            WHERE ticker = s.ticker AND date < s.date
          )
        WHERE s.date = (SELECT MAX(date) FROM signals)
          AND s.signal NOT IN ('BUY', 'SELL')
          AND COALESCE(c.market, 'US') = 'US'
        ORDER BY gates_met DESC,
                 s.vqs DESC,
                 s.prediction_score DESC,
                 s.ticker ASC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().fetchone()
    if row is None:
        return None

    close = row["close"]
    prev_close = row["prev_close"]
    day_change_pct = (
        round((close - prev_close) / prev_close * 100, 4)
        if prev_close not in (None, 0) and close is not None
        else None
    )

    gate_labels = {
        "rsi_3day_flag":    "RSI<30 for 3 consecutive days",
        "bb_condition":     "Bollinger oversold",
        "analyst_condition":"Analyst target upside",
        "volume_flag":      "Volume confirmation",
        "week52_condition": "Near 52-week low",
        "health_condition": "Company health pass",
        "ml_condition":     "ML model positive",
    }
    reasons = []
    for gate, label in gate_labels.items():
        if gate == "rsi_3day_flag":
            val = row["rsi_3day_flag"]
            met = val == 3
            reasons.append({
                "gate": gate, "met": met, "label": label,
                "value": val, "needed": 3,
            })
        else:
            met = bool(row[gate]) if row[gate] is not None else False
            reasons.append({"gate": gate, "met": met, "label": label})

    signal_date = row["signal_date"]
    date_str = str(signal_date).split(" ")[0] if signal_date else None

    return {
        "date": date_str,
        "ticker": row["ticker"],
        "company": row["company"],
        "domain": row["domain"],
        "close": close,
        "day_change_pct": day_change_pct,
        "gates_met": int(row["gates_met"]),
        "gates_total": 7,
        "vqs": int(row["vqs"]) if row["vqs"] is not None else 0,
        "ml_pred_5d": row["pred_5d"],
        "reasons": reasons,
    }


def _get_spotlight_today_cached() -> dict | None:
    # Always fetch MAX(date) so the cache auto-invalidates on the next pipeline write.
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MAX(date) FROM signals")).fetchone()
    max_date = row[0] if row else None
    if max_date is None:
        return None
    today_iso = datetime.now(UTC).date().isoformat()
    key = f"{today_iso}|{max_date}"
    with _spotlight_cache_lock:
        c = _spotlight_cache.get("today")
        if c is not None and c["key"] == key:
            return c["data"]
        data = _build_spotlight_today()
        _spotlight_cache["today"] = {"key": key, "data": data}
        return data


@protected.get("/api/spotlight/today")
def spotlight_today():
    """Today's Spotlight ticker — engine's best non-BUY/non-SELL near-miss.

    Requires authentication. Returns 200 + null body when no signals exist for today.
    """
    return _get_spotlight_today_cached()


# ---------------------------------------------------------------------------
# Market page — Phase 1 endpoints
# See docs/superpowers/specs/2026-05-23-market-page-live-data-design.md
# ---------------------------------------------------------------------------

_INDICES_TICKERS = ["SPY", "QQQ", "DIA", "IWM"]  # VIX is sourced separately

_market_cache: dict[str, dict] = {}
_market_cache_lock = threading.Lock()
_MARKET_TTL_SECONDS = 60


def _build_market_indices() -> dict:
    """Return {indices: [...]} for the 5 headline cards.

    SPY/QQQ/DIA/IWM read from daily_prices; VIX from the vix table (yfinance path).
    Each entry: {ticker, close, change_pct, sparkline} where sparkline is up to
    the last 30 closes oldest→newest. close=None when the ticker has no data.
    """
    out = []
    with engine.connect() as conn:
        for ticker in _INDICES_TICKERS:
            rows = conn.execute(
                text(
                    "SELECT close FROM daily_prices "
                    "WHERE ticker = :t ORDER BY date DESC LIMIT 30"
                ),
                {"t": ticker},
            ).fetchall()
            closes = [r[0] for r in rows][::-1]  # oldest → newest
            out.append(_index_entry(ticker, closes))

        vix_rows = conn.execute(
            text("SELECT close FROM vix ORDER BY date DESC LIMIT 30")
        ).fetchall()
        vix_closes = [r[0] for r in vix_rows][::-1]
        out.append(_index_entry("VIX", vix_closes))

    return {"indices": out}


def _index_entry(ticker: str, closes: list[float]) -> dict:
    if not closes:
        return {"ticker": ticker, "close": None, "change_pct": None, "sparkline": []}
    close = closes[-1]
    change_pct = None
    if len(closes) >= 2 and closes[-2] not in (None, 0) and close is not None:
        change_pct = round((close - closes[-2]) / closes[-2] * 100, 4)
    return {
        "ticker": ticker,
        "close": close,
        "change_pct": change_pct,
        "sparkline": closes,
    }


def _get_market_indices_cached() -> dict:
    now = time.time()
    with _market_cache_lock:
        c = _market_cache.get("indices")
        if c is not None and now - c["t"] < _MARKET_TTL_SECONDS:
            return c["data"]
        data = _build_market_indices()
        _market_cache["indices"] = {"t": now, "data": data}
        return data


@protected.get("/api/market/indices")
def market_indices():
    """5 headline indices (SPY/QQQ/DIA/IWM/VIX) with 30-day sparkline."""
    return _get_market_indices_cached()


_MOVERS_EXCLUDE = ("SPY", "VOO")  # ETFs/funds shouldn't crowd the top movers panel


def _build_market_movers(mover_type: str, limit: int) -> dict:
    """Top N US tickers ranked by 1-day change % (gainers/losers) or volume (active)."""
    sort_clause = {
        "gainers": "change_pct DESC NULLS LAST",
        "losers":  "change_pct ASC NULLS LAST",
        "active":  "today.volume DESC NULLS LAST",
    }[mover_type]
    sql = text(f"""
        WITH bounds AS (SELECT MAX(date) AS today FROM daily_prices),
        prev_bounds AS (
            SELECT MAX(dp.date) AS prev
            FROM daily_prices dp, bounds b
            WHERE dp.date < b.today
        )
        SELECT
            today.ticker, c.company, today.close, today.volume,
            CASE
              WHEN prev.close IS NULL OR prev.close = 0 THEN NULL
              ELSE ROUND((today.close - prev.close) / prev.close * 100.0, 4)
            END AS change_pct
        FROM daily_prices today
        JOIN bounds b ON today.date = b.today
        LEFT JOIN daily_prices prev
          ON prev.ticker = today.ticker
         AND prev.date = (SELECT prev FROM prev_bounds)
        JOIN companies c ON c.ticker = today.ticker
        WHERE COALESCE(c.market, 'US') = 'US'
          AND today.ticker NOT IN :exclude
        ORDER BY {sort_clause}
        LIMIT :limit
    """).bindparams(bindparam("exclude", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(sql, {"exclude": list(_MOVERS_EXCLUDE), "limit": limit}).mappings().all()
    return {
        "movers": [
            {
                "ticker": r["ticker"],
                "company": r["company"],
                "close": r["close"],
                "volume": r["volume"],
                "change_pct": r["change_pct"],
            }
            for r in rows
        ]
    }


def _get_market_movers_cached(mover_type: str, limit: int) -> dict:
    key = f"movers:{mover_type}:{limit}"
    now = time.time()
    with _market_cache_lock:
        c = _market_cache.get(key)
        if c is not None and now - c["t"] < _MARKET_TTL_SECONDS:
            return c["data"]
        data = _build_market_movers(mover_type, limit)
        _market_cache[key] = {"t": now, "data": data}
        return data


@protected.get("/api/market/movers")
def market_movers(
    type: Literal["gainers", "losers", "active"] = "gainers",
    limit: int = 5,
):
    """Top US gainers / losers / most-active tickers vs. prior trading day."""
    return _get_market_movers_cached(type, limit)


def _build_market_breadth() -> dict:
    """Market internals across US tickers: advance/decline, 52w hi/lo, %>50d MA."""
    sql = text("""
        WITH bounds AS (SELECT MAX(date) AS today FROM daily_prices),
        prev_bounds AS (
            SELECT MAX(dp.date) AS prev
            FROM daily_prices dp, bounds b
            WHERE dp.date < b.today
        ),
        us_tickers AS (
            SELECT ticker FROM companies WHERE COALESCE(market, 'US') = 'US'
        ),
        today_px AS (
            SELECT dp.ticker, dp.close
            FROM daily_prices dp, bounds b
            WHERE dp.date = b.today AND dp.ticker IN (SELECT ticker FROM us_tickers)
        ),
        prev_px AS (
            SELECT dp.ticker, dp.close
            FROM daily_prices dp, prev_bounds pb
            WHERE dp.date = pb.prev AND dp.ticker IN (SELECT ticker FROM us_tickers)
        ),
        year_window AS (
            SELECT dp.ticker, MAX(dp.close) AS hi52, MIN(dp.close) AS lo52
            FROM daily_prices dp, bounds b
            WHERE dp.ticker IN (SELECT ticker FROM us_tickers)
              AND dp.date > date(b.today, '-365 days')
            GROUP BY dp.ticker
        ),
        last50 AS (
            SELECT ticker, close,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM daily_prices
            WHERE ticker IN (SELECT ticker FROM us_tickers)
        ),
        ma50 AS (
            SELECT ticker, AVG(close) AS ma
            FROM last50
            WHERE rn <= 50
            GROUP BY ticker
        )
        SELECT
            SUM(CASE WHEN p.close IS NOT NULL AND t.close > p.close THEN 1 ELSE 0 END) AS advancers,
            SUM(CASE WHEN p.close IS NOT NULL AND t.close < p.close THEN 1 ELSE 0 END) AS decliners,
            SUM(CASE WHEN yw.hi52 IS NOT NULL AND t.close >= yw.hi52 THEN 1 ELSE 0 END) AS week52_highs,
            SUM(CASE WHEN yw.lo52 IS NOT NULL AND t.close <= yw.lo52 THEN 1 ELSE 0 END) AS week52_lows,
            SUM(CASE WHEN ma50.ma IS NOT NULL AND t.close > ma50.ma THEN 1.0 ELSE 0.0 END)
              / NULLIF(SUM(CASE WHEN ma50.ma IS NOT NULL THEN 1.0 ELSE 0.0 END), 0)
              AS above_50d_ma_pct
        FROM today_px t
        LEFT JOIN prev_px p ON p.ticker = t.ticker
        LEFT JOIN year_window yw ON yw.ticker = t.ticker
        LEFT JOIN ma50 ON ma50.ticker = t.ticker
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().fetchone()
    if row is None:
        return {"advancers": 0, "decliners": 0, "week52_highs": 0,
                "week52_lows": 0, "above_50d_ma_pct": None}
    return {
        "advancers": int(row["advancers"] or 0),
        "decliners": int(row["decliners"] or 0),
        "week52_highs": int(row["week52_highs"] or 0),
        "week52_lows": int(row["week52_lows"] or 0),
        "above_50d_ma_pct": round(row["above_50d_ma_pct"], 6)
            if row["above_50d_ma_pct"] is not None else None,
    }


def _get_market_breadth_cached() -> dict:
    now = time.time()
    with _market_cache_lock:
        c = _market_cache.get("breadth")
        if c is not None and now - c["t"] < _MARKET_TTL_SECONDS:
            return c["data"]
        data = _build_market_breadth()
        _market_cache["breadth"] = {"t": now, "data": data}
        return data


@protected.get("/api/market/breadth")
def market_breadth():
    """US-wide breadth snapshot: advancers, decliners, 52w hi/lo, %>50d MA."""
    return _get_market_breadth_cached()


def _build_market_sectors() -> dict:
    """Per-sector market-cap-weighted % change + top-3 movers by absolute change."""
    sql = text("""
        WITH bounds AS (SELECT MAX(date) AS today FROM daily_prices),
        prev_bounds AS (
            SELECT MAX(dp.date) AS prev
            FROM daily_prices dp, bounds b WHERE dp.date < b.today
        ),
        ticker_change AS (
            SELECT
                c.ticker, c.sector, f.market_cap,
                ((t.close - p.close) / p.close * 100.0) AS change_pct
            FROM companies c
            JOIN daily_prices t
              ON t.ticker = c.ticker AND t.date = (SELECT today FROM bounds)
            JOIN daily_prices p
              ON p.ticker = c.ticker AND p.date = (SELECT prev FROM prev_bounds)
            JOIN fundamentals f ON f.ticker = c.ticker
            WHERE COALESCE(c.market, 'US') = 'US'
              AND c.sector IS NOT NULL
              AND f.market_cap IS NOT NULL
              AND f.market_cap > 0
              AND p.close IS NOT NULL AND p.close <> 0
        )
        SELECT ticker, sector, market_cap, change_pct
        FROM ticker_change
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    by_sector: dict[str, list[dict]] = {}
    for r in rows:
        by_sector.setdefault(r["sector"], []).append({
            "ticker": r["ticker"],
            "market_cap": r["market_cap"],
            "change_pct": r["change_pct"],
        })

    sectors_out = []
    for sector_name, items in by_sector.items():
        total_mc = sum(x["market_cap"] for x in items)
        weighted = sum(x["market_cap"] * x["change_pct"] for x in items) / total_mc
        top_movers = sorted(items, key=lambda x: abs(x["change_pct"]), reverse=True)[:3]
        sectors_out.append({
            "sector": sector_name,
            "change_pct": round(weighted, 4),
            "top_movers": [
                {"ticker": m["ticker"], "change_pct": round(m["change_pct"], 4)}
                for m in top_movers
            ],
        })

    sectors_out.sort(key=lambda s: s["change_pct"], reverse=True)
    return {"sectors": sectors_out}


def _get_market_sectors_cached() -> dict:
    now = time.time()
    with _market_cache_lock:
        c = _market_cache.get("sectors")
        if c is not None and now - c["t"] < _MARKET_TTL_SECONDS:
            return c["data"]
        data = _build_market_sectors()
        _market_cache["sectors"] = {"t": now, "data": data}
        return data


@protected.get("/api/market/sectors")
def market_sectors():
    """US sectors: market-cap-weighted % change + top-3 movers by absolute %."""
    return _get_market_sectors_cached()


_TAPE_TICKERS = [
    "SPY", "QQQ", "DIA", "IWM", "VIX",
    "NVDA", "MSFT", "AAPL", "META", "TSLA",
    "AMZN", "GOOGL", "MU", "PM", "MTD", "JPM",
]


def _build_market_tape() -> dict:
    """One-roundtrip payload for the 32px tape ticker: 16 tickers × {close, change_pct}."""
    out = []
    with engine.connect() as conn:
        for ticker in _TAPE_TICKERS:
            if ticker == "VIX":
                rows = conn.execute(
                    text("SELECT close FROM vix ORDER BY date DESC LIMIT 2")
                ).fetchall()
            else:
                rows = conn.execute(
                    text("SELECT close FROM daily_prices "
                         "WHERE ticker = :t ORDER BY date DESC LIMIT 2"),
                    {"t": ticker},
                ).fetchall()
            closes = [r[0] for r in rows]  # newest, prev
            close = closes[0] if closes else None
            change_pct = None
            if len(closes) >= 2 and closes[1] not in (None, 0) and close is not None:
                change_pct = round((close - closes[1]) / closes[1] * 100, 4)
            out.append({"ticker": ticker, "close": close, "change_pct": change_pct})
    return {"tape": out}


def _get_market_tape_cached() -> dict:
    now = time.time()
    with _market_cache_lock:
        c = _market_cache.get("tape")
        if c is not None and now - c["t"] < _MARKET_TTL_SECONDS:
            return c["data"]
        data = _build_market_tape()
        _market_cache["tape"] = {"t": now, "data": data}
        return data


@protected.get("/api/market/tape")
def market_tape():
    """16-ticker tape strip — single roundtrip for the looped marquee at the top of /market."""
    return _get_market_tape_cached()


_ANALYST_CACHE_TTL_SECONDS = 5 * 60  # spec §2: news + analyst use a 5-min cache


def _build_market_analyst_changes(days: int, limit: int) -> dict:
    """Top US tickers ranked by |Δ target_mean_price| over the last `days` days.

    Classification:
      INITIATE  — ticker has today's row but none on the comparison date
      RAISE-TP  — target_mean_price went up
      LOWER-TP  — target_mean_price went down
    No-change tickers are excluded (per "top changes" framing). UPGRADE /
    DOWNGRADE are not produced — analyst_targets_history has no rating data,
    only consensus TP.
    """
    sql = text("""
        WITH latest AS (
            SELECT MAX(date) AS d FROM analyst_targets_history
        ),
        today AS (
            SELECT h.ticker, h.target_mean_price, h.source, h.date
            FROM analyst_targets_history h, latest l
            WHERE h.date = l.d AND h.target_mean_price IS NOT NULL
        ),
        prev AS (
            SELECT h.ticker, h.target_mean_price
            FROM analyst_targets_history h, latest l
            WHERE h.date = date(l.d, '-' || :days || ' days')
              AND h.target_mean_price IS NOT NULL
        )
        SELECT
            t.ticker, c.company, t.target_mean_price AS tp_now,
            p.target_mean_price AS tp_prev, t.source, t.date AS as_of
        FROM today t
        LEFT JOIN companies c ON c.ticker = t.ticker
        LEFT JOIN prev p ON p.ticker = t.ticker
        WHERE COALESCE(c.market, 'US') = 'US'
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"days": days}).mappings().all()

    classified = []
    for r in rows:
        tp_now = r["tp_now"]
        tp_prev = r["tp_prev"]
        if tp_prev is None:
            classified.append({
                "ticker": r["ticker"],
                "company": r["company"],
                "kind": "INITIATE",
                "target_mean_price": tp_now,
                "prev_target_mean_price": None,
                "change_pct": None,
                "abs_change_pct": 0.0,  # initiates float to the end of the sort
                "date": r["as_of"],
                "source": r["source"],
            })
            continue
        if tp_prev == 0 or tp_now == tp_prev:
            continue  # excluded: flat or undefined
        change_pct = (tp_now - tp_prev) / tp_prev * 100.0
        classified.append({
            "ticker": r["ticker"],
            "company": r["company"],
            "kind": "RAISE-TP" if change_pct > 0 else "LOWER-TP",
            "target_mean_price": tp_now,
            "prev_target_mean_price": tp_prev,
            "change_pct": round(change_pct, 4),
            "abs_change_pct": abs(change_pct),
            "date": r["as_of"],
            "source": r["source"],
        })

    classified.sort(key=lambda x: x["abs_change_pct"], reverse=True)
    for x in classified:
        del x["abs_change_pct"]
    return {"changes": classified[:limit]}


def _get_market_analyst_changes_cached(days: int, limit: int) -> dict:
    key = f"analyst-changes:{days}:{limit}"
    now = time.time()
    with _market_cache_lock:
        c = _market_cache.get(key)
        if c is not None and now - c["t"] < _ANALYST_CACHE_TTL_SECONDS:
            return c["data"]
        data = _build_market_analyst_changes(days, limit)
        _market_cache[key] = {"t": now, "data": data}
        return data


@protected.get("/api/market/analyst-changes/top")
def market_analyst_changes_top(days: int = 1, limit: int = 5):
    """Top |Δ TP| moves over the trailing `days` days, ranked, classified."""
    return _get_market_analyst_changes_cached(days, limit)


_CROSS_TICKERS = [
    ("DX-Y.NYB", "USD Index"),
    ("^TNX",     "10Y Yield"),
    ("GC=F",     "Gold"),
    ("CL=F",     "Crude Oil"),
    ("BTC-USD",  "Bitcoin"),
    ("EURUSD=X", "EUR / USD"),
]


def _fetch_cross_quotes() -> dict | None:
    """Pull current + prior close for the cross-market tickers from yfinance.

    Returns {ticker: {price, prev_close}} on success, or None on hard failure
    (the caller then falls back to the last cached value with stale=true).
    FMP does not cover these symbols (currency/yield/futures), so there is no
    second-source fallback inside this helper.
    """
    try:
        tickers = [t for t, _ in _CROSS_TICKERS]
        raw = yf.download(
            " ".join(tickers), period="5d", auto_adjust=False, progress=False
        )
        if raw is None or raw.empty:
            return None
    except Exception:
        return None
    out: dict = {}
    for ticker, _ in _CROSS_TICKERS:
        try:
            closes = _extract_close_series(raw, ticker)
            if len(closes) >= 2:
                out[ticker] = {"price": float(closes.iloc[-1]),
                               "prev_close": float(closes.iloc[-2])}
            elif len(closes) == 1:
                out[ticker] = {"price": float(closes.iloc[-1]), "prev_close": None}
        except Exception:
            continue
    return out or None


def _build_market_cross() -> dict:
    """Compose the cross-market strip. On fetch failure, fall back to cached + stale."""
    raw = _fetch_cross_quotes()
    if raw:
        rows = []
        for ticker, label in _CROSS_TICKERS:
            q = raw.get(ticker)
            if not q:
                continue
            price = q.get("price")
            prev = q.get("prev_close")
            change_pct = None
            if price is not None and prev not in (None, 0):
                change_pct = round((price - prev) / prev * 100, 4)
            rows.append({
                "ticker": ticker, "label": label,
                "price": price, "change_pct": change_pct, "stale": False,
            })
        return {"cross": rows}

    # Hard failure: serve last fresh response with stale=true.
    cached = _market_cache.get("cross_last_good")
    if cached is None:
        return {"cross": []}
    return {"cross": [{**r, "stale": True} for r in cached["data"]["cross"]]}


def _get_market_cross_cached() -> dict:
    now = time.time()
    with _market_cache_lock:
        c = _market_cache.get("cross")
        if c is not None and now - c["t"] < _MARKET_TTL_SECONDS:
            return c["data"]
        data = _build_market_cross()
        _market_cache["cross"] = {"t": now, "data": data}
        # Preserve the last fresh (non-stale) snapshot for future fallback use.
        if data["cross"] and not any(r.get("stale") for r in data["cross"]):
            _market_cache["cross_last_good"] = {"t": now, "data": data}
        return data


@protected.get("/api/market/cross")
def market_cross():
    """USD/10Y/Gold/Oil/BTC/EURUSD strip via yfinance; stale=true on fetch failure."""
    return _get_market_cross_cached()


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
