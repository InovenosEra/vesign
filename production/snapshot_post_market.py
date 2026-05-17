"""Daily post-market snapshot. Chained after the morning pipeline (07:00 IDT
Mon-Sat) via crontab — see /tmp/crontab.bak_2026-05-17 on the prod server for
the previous standalone schedule.

Fetches FMP's /aftermarket-trade for every ticker in yesterday's signals
and writes the result to extended_hours_prices, which the UI consumes
via COALESCE(extended_close, close).

History: This used to run at 03:05 IDT (cron `5 3 * * *`) directly after the
NYSE post-market window ended. But the pipeline that writes signals for the
just-closed session runs at 07:00 IDT — 4 hours LATER. So at 03:05 IDT the
signals didn't exist yet and snapshot logged "No tickers found, skipping"
silently every day from 2026-05-14 (when it was first scheduled) through
2026-05-17 (when this comment was added). The cron was changed to chain
snapshot after pipeline in the same cron line so signals exist by the time
snapshot needs them."""

import os
import sqlite3
import logging
from datetime import datetime, timezone, timedelta

import exchange_calendars as xcals
import pandas as pd

from data.fmp import aftermarket_trades

log = logging.getLogger("snapshot_post_market")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

DB_PATH = os.environ.get("VESIGN_DB_PATH", "vesign.db")
_POST_OFFSET = timedelta(hours=4)


def now_utc():
    return datetime.now(timezone.utc)


def _most_recent_session_and_post_close():
    """Return (session_date, post_close_utc) for the most recently ended NYSE session,
    or (None, None) if none in the last week."""
    nyse = xcals.get_calendar("XNYS")
    today = now_utc().date()
    for delta in range(0, 8):
        cand = today - timedelta(days=delta)
        ts = pd.Timestamp(cand)
        try:
            if not nyse.is_session(ts):
                continue
        except Exception:
            continue
        post_close = nyse.session_close(ts).to_pydatetime() + _POST_OFFSET
        if post_close <= now_utc():
            return cand, post_close
    return None, None


def _load_universe(session_date):
    """All tickers in `signals` for the given session date — superset of
    open positions, holdings, and surfaced UI rows.

    Note: signals.date is stored as 'YYYY-MM-DD HH:MM:SS.ffffff'. We use DATE()
    to coerce both sides to plain dates before comparison."""
    # Re-read the env var on each call so tests can override DB_PATH via monkeypatch
    db_path = os.environ.get("VESIGN_DB_PATH", DB_PATH)
    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT DISTINCT ticker FROM signals WHERE DATE(date) = ?",
        (session_date.isoformat(),)
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def fetch_extended(tickers):
    """Indirection seam for tests; production calls FMP."""
    return aftermarket_trades(tickers)


def _upsert(session_date, prices):
    """Upsert non-None prices into extended_hours_prices."""
    db_path = os.environ.get("VESIGN_DB_PATH", DB_PATH)
    con = sqlite3.connect(db_path)
    try:
        con.executemany(
            """INSERT INTO extended_hours_prices (date, ticker, extended_close)
               VALUES (?, ?, ?)
               ON CONFLICT(date, ticker) DO UPDATE SET
                 extended_close = excluded.extended_close,
                 updated_at     = CURRENT_TIMESTAMP""",
            [(session_date.isoformat(), t, p) for t, p in prices.items() if p is not None]
        )
        con.commit()
    finally:
        con.close()


def snapshot_post_market():
    session, post_close = _most_recent_session_and_post_close()
    if session is None:
        log.info("No recent NYSE session found, skipping.")
        return

    age = now_utc() - post_close
    if age < timedelta(0):
        log.info("Post-close not yet reached, skipping.")
        return
    if age > timedelta(hours=6):
        log.info("Most-recent post_close is %s ago — too stale, skipping.", age)
        return

    tickers = _load_universe(session)
    if not tickers:
        log.warning("No tickers found in signals for %s, skipping.", session)
        return

    log.info("Snapshotting %d tickers for session %s", len(tickers), session)
    prices = fetch_extended(tickers)
    have = sum(1 for v in prices.values() if v is not None)
    log.info("Got extended prices for %d/%d tickers", have, len(tickers))

    _upsert(session, prices)
    log.info("Snapshot complete for %s.", session)


if __name__ == "__main__":
    snapshot_post_market()
