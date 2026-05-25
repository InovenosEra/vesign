"""Tests for GET /api/market/tape — single-roundtrip payload for the 32px ticker tape.

The tape is the top-N US stocks by market cap, ETFs excluded, ordered cap-descending.
"""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def tape_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_tape.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE daily_prices (
                date DATETIME, ticker TEXT,
                open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                volume BIGINT
            )
        """))
        conn.execute(text("""
            CREATE TABLE vix (date DATETIME, close FLOAT)
        """))
        conn.execute(text("""
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT,
                industry TEXT, description TEXT, description_short TEXT,
                logo_url TEXT, domain TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE fundamentals (
                ticker TEXT, market_cap FLOAT, pe_ttm FLOAT, eps_ttm FLOAT,
                revenue_ttm FLOAT, revenue_growth FLOAT, gross_margin FLOAT,
                op_margin FLOAT, net_margin FLOAT, roe FLOAT, de_ratio FLOAT,
                fundamentals_updated TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE company_health_history (
                ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT
            )
        """))
    temp_engine.dispose()

    import importlib
    import backend.main as bm
    importlib.reload(bm)
    yield bm, TestClient(bm.app)

    for fname in os.listdir(tmpdir):
        try:
            os.remove(os.path.join(tmpdir, fname))
        except OSError:
            pass
    try:
        os.rmdir(tmpdir)
    except OSError:
        pass


def _insert_two_day(bm, *, ticker, prev, today):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for d, c in [("2026-05-21 00:00:00", prev), ("2026-05-22 00:00:00", today)]:
            conn.execute(text("""
                INSERT INTO daily_prices (date, ticker, open, high, low, close, volume)
                VALUES (:d, :t, :c, :c, :c, :c, 0)
            """), {"d": d, "t": ticker, "c": c})


def _insert_company(bm, *, ticker, company=None, sector="Information Technology",
                    market="US", market_cap=None):
    from sqlalchemy import text
    company = company or ticker
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO companies (ticker, company, sector, market)
            VALUES (:t, :c, :s, :m)
        """), {"t": ticker, "c": company, "s": sector, "m": market})
        if market_cap is not None:
            conn.execute(text("""
                INSERT INTO fundamentals (ticker, market_cap) VALUES (:t, :mc)
            """), {"t": ticker, "mc": market_cap})


def test_tape_returns_top_stocks_ordered_by_market_cap(tape_app):
    bm, client = tape_app
    # Three stocks, descending cap order is MID > BIG > SMALL by mcap value below.
    _insert_company(bm, ticker="BIG", market_cap=2_000e9)
    _insert_company(bm, ticker="MID", market_cap=5_000e9)   # largest
    _insert_company(bm, ticker="SMALL", market_cap=100e9)
    _insert_two_day(bm, ticker="MID", prev=100.0, today=102.0)   # +2.0
    _insert_two_day(bm, ticker="BIG", prev=300.0, today=297.0)   # -1.0
    _insert_two_day(bm, ticker="SMALL", prev=50.0, today=50.5)   # +1.0

    items = client.get("/api/market/tape").json()["tape"]
    assert [r["ticker"] for r in items] == ["MID", "BIG", "SMALL"]  # cap desc

    by = {r["ticker"]: r for r in items}
    assert by["MID"]["close"] == pytest.approx(102.0)
    assert by["MID"]["change_pct"] == pytest.approx(2.0, abs=1e-3)
    assert by["BIG"]["change_pct"] == pytest.approx(-1.0, abs=1e-3)


def test_etfs_are_excluded_even_if_high_market_cap(tape_app):
    bm, client = tape_app
    _insert_company(bm, ticker="SPY", sector="ETF", market_cap=9_000e9)  # huge, but ETF
    _insert_company(bm, ticker="NVDA", sector="Information Technology", market_cap=5_000e9)
    _insert_two_day(bm, ticker="SPY", prev=740.0, today=745.0)
    _insert_two_day(bm, ticker="NVDA", prev=100.0, today=102.0)

    tickers = [r["ticker"] for r in client.get("/api/market/tape").json()["tape"]]
    assert "SPY" not in tickers
    assert tickers == ["NVDA"]


def test_tape_caps_at_twenty_tickers(tape_app):
    bm, client = tape_app
    for i in range(25):
        t = f"T{i:02d}"
        _insert_company(bm, ticker=t, market_cap=(25 - i) * 1e9)  # T00 largest
        _insert_two_day(bm, ticker=t, prev=10.0, today=11.0)

    items = client.get("/api/market/tape").json()["tape"]
    assert len(items) == 20
    assert items[0]["ticker"] == "T00"   # largest cap first
    assert items[-1]["ticker"] == "T19"  # 20th largest; T20..T24 dropped


def test_stock_without_prices_is_excluded(tape_app):
    # Anchored to the last close: a top-cap ticker with no daily_prices row is
    # excluded (not shown with a null/stale price); the next company fills in.
    bm, client = tape_app
    _insert_company(bm, ticker="NODATA", market_cap=5_000e9)  # top cap, no prices
    _insert_company(bm, ticker="HASDATA", market_cap=1_000e9)
    _insert_two_day(bm, ticker="HASDATA", prev=100.0, today=105.0)

    tickers = [r["ticker"] for r in client.get("/api/market/tape").json()["tape"]]
    assert "NODATA" not in tickers
    assert tickers == ["HASDATA"]
    assert {r["ticker"]: r for r in client.get("/api/market/tape").json()["tape"]}["HASDATA"]["close"] == pytest.approx(105.0)


def test_stale_ticker_excluded_from_tape(tape_app):
    # A higher-cap ticker whose latest row is OLDER than the global last close is
    # excluded — it must never show a stale price — and the fresh ticker is kept.
    bm, client = tape_app
    _insert_company(bm, ticker="STALE", market_cap=9_000e9)  # bigger cap, but stale
    _insert_company(bm, ticker="FRESH", market_cap=1_000e9)
    _insert_two_day(bm, ticker="FRESH", prev=100.0, today=101.0)  # defines last close = 05-22
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for d, c in [("2026-05-19 00:00:00", 50.0), ("2026-05-20 00:00:00", 51.0)]:
            conn.execute(text("""INSERT INTO daily_prices (date, ticker, open, high, low, close, volume)
                                 VALUES (:d, :t, :c, :c, :c, :c, 0)"""), {"d": d, "t": "STALE", "c": c})

    tickers = [r["ticker"] for r in client.get("/api/market/tape").json()["tape"]]
    assert "STALE" not in tickers
    assert tickers == ["FRESH"]


def test_dual_class_shares_collapse_to_higher_cap(tape_app):
    bm, client = tape_app
    # Alphabet's two share classes share a base name; only the larger should show.
    _insert_company(bm, ticker="GOOGL", company="Alphabet Inc. (Class A)",
                    sector="Communication Services", market_cap=4_700e9)
    _insert_company(bm, ticker="GOOG", company="Alphabet Inc. (Class C)",
                    sector="Communication Services", market_cap=4_650e9)
    _insert_company(bm, ticker="AAPL", company="Apple Inc.", market_cap=4_000e9)
    for t in ("GOOGL", "GOOG", "AAPL"):
        _insert_two_day(bm, ticker=t, prev=100.0, today=101.0)

    tickers = [r["ticker"] for r in client.get("/api/market/tape").json()["tape"]]
    assert "GOOGL" in tickers       # higher-cap class kept
    assert "GOOG" not in tickers    # lower-cap class dropped
    assert tickers == ["GOOGL", "AAPL"]


def test_dual_class_drop_lets_next_company_fill_the_slot(tape_app):
    bm, client = tape_app
    # Limit is 20. With a collapsed dual-class pair, a 21st distinct company fills in.
    _insert_company(bm, ticker="GOOGL", company="Alphabet Inc. (Class A)",
                    market_cap=100_000e9)
    _insert_company(bm, ticker="GOOG", company="Alphabet Inc. (Class C)",
                    market_cap=99_000e9)
    for i in range(20):  # 20 more distinct companies, all smaller than Alphabet
        t = f"T{i:02d}"
        _insert_company(bm, ticker=t, company=f"Company {i}", market_cap=(20 - i) * 1e9)
        _insert_two_day(bm, ticker=t, prev=10.0, today=11.0)
    _insert_two_day(bm, ticker="GOOGL", prev=10.0, today=11.0)
    _insert_two_day(bm, ticker="GOOG", prev=10.0, today=11.0)

    tickers = [r["ticker"] for r in client.get("/api/market/tape").json()["tape"]]
    assert len(tickers) == 20
    assert "GOOG" not in tickers
    assert tickers[0] == "GOOGL"
    # Collapsing GOOG into GOOGL frees a slot, so T18 makes it in (without dedup,
    # GOOGL+GOOG would consume two slots and push T18 out). T19 is the 21st
    # distinct company and is still beyond the limit.
    assert "T18" in tickers
    assert "T19" not in tickers


def test_stocks_without_market_cap_are_excluded(tape_app):
    bm, client = tape_app
    _insert_company(bm, ticker="RANKED", market_cap=1_000e9)
    _insert_company(bm, ticker="NOCAP", market_cap=None)  # no fundamentals row
    _insert_two_day(bm, ticker="RANKED", prev=100.0, today=101.0)
    _insert_two_day(bm, ticker="NOCAP", prev=100.0, today=101.0)

    tickers = [r["ticker"] for r in client.get("/api/market/tape").json()["tape"]]
    assert tickers == ["RANKED"]
