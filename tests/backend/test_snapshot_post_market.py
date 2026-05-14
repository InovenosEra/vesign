"""Tests for production.snapshot_post_market — daily post-market lock job."""
from datetime import datetime, timezone, timedelta
import sqlite3
import tempfile
import os
import pytest


@pytest.fixture
def temp_db(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE extended_hours_prices (
          date TEXT NOT NULL,
          ticker TEXT NOT NULL,
          extended_close REAL NOT NULL,
          source TEXT DEFAULT 'fmp_aftermarket',
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (date, ticker)
        );
        CREATE TABLE signals (
          date TEXT, ticker TEXT, close REAL
        );
        INSERT INTO signals VALUES
          ('2026-05-13 00:00:00.000000', 'AAPL', 298.0),
          ('2026-05-13 00:00:00.000000', 'MSFT', 404.0),
          ('2026-05-13 00:00:00.000000', 'DIME', 9.5);
    """)
    con.commit()
    con.close()
    monkeypatch.setenv("VESIGN_DB_PATH", path)
    yield path
    os.unlink(path)


def test_writes_only_non_none_prices(temp_db, monkeypatch):
    from production import snapshot_post_market as snap

    fake_session = datetime(2026, 5, 13).date()
    fake_post_close = datetime(2026, 5, 14, 0, 0, tzinfo=timezone.utc)
    fake_now = fake_post_close + timedelta(minutes=10)

    monkeypatch.setattr(snap, "_most_recent_session_and_post_close",
                        lambda: (fake_session, fake_post_close))
    monkeypatch.setattr(snap, "now_utc", lambda: fake_now)
    monkeypatch.setattr(snap, "fetch_extended",
                        lambda tickers: {"AAPL": 297.5, "MSFT": 405.1, "DIME": None})

    snap.snapshot_post_market()

    con = sqlite3.connect(temp_db)
    rows = con.execute("SELECT ticker, extended_close FROM extended_hours_prices "
                       "WHERE date=? ORDER BY ticker", ("2026-05-13",)).fetchall()
    con.close()
    assert rows == [("AAPL", 297.5), ("MSFT", 405.1)]  # DIME skipped


def test_skips_when_no_fresh_session(temp_db, monkeypatch):
    from production import snapshot_post_market as snap

    fake_session = datetime(2026, 5, 13).date()
    fake_post_close = datetime(2026, 5, 14, 0, 0, tzinfo=timezone.utc)
    # 10 hours after post_close — too old
    fake_now = fake_post_close + timedelta(hours=10)

    monkeypatch.setattr(snap, "_most_recent_session_and_post_close",
                        lambda: (fake_session, fake_post_close))
    monkeypatch.setattr(snap, "now_utc", lambda: fake_now)
    called = []
    monkeypatch.setattr(snap, "fetch_extended",
                        lambda tickers: (called.append(tickers), {})[1])

    snap.snapshot_post_market()
    assert called == []  # never reached FMP


def test_idempotent_upsert(temp_db, monkeypatch):
    from production import snapshot_post_market as snap

    fake_session = datetime(2026, 5, 13).date()
    fake_post_close = datetime(2026, 5, 14, 0, 0, tzinfo=timezone.utc)
    fake_now = fake_post_close + timedelta(minutes=10)

    monkeypatch.setattr(snap, "_most_recent_session_and_post_close",
                        lambda: (fake_session, fake_post_close))
    monkeypatch.setattr(snap, "now_utc", lambda: fake_now)
    monkeypatch.setattr(snap, "fetch_extended",
                        lambda tickers: {"AAPL": 297.5, "MSFT": 405.1, "DIME": None})

    snap.snapshot_post_market()
    snap.snapshot_post_market()  # should not duplicate

    con = sqlite3.connect(temp_db)
    n = con.execute("SELECT COUNT(*) FROM extended_hours_prices WHERE date=?",
                    ("2026-05-13",)).fetchone()[0]
    con.close()
    assert n == 2
