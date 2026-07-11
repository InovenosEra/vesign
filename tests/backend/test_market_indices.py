"""Tests for GET /api/market/indices — the 5 headline-index cards.

Real index levels (^GSPC/^NDX/^DJI/^RUT) come from the index_prices table;
VIX comes from the vix table. Both are populated from yfinance by the pipeline.
"""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient

INDEXES = ["^GSPC", "^NDX", "^DJI", "^RUT"]


@pytest.fixture
def indices_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_indices.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("CREATE TABLE index_prices (date DATETIME, ticker TEXT, close FLOAT)"))
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE vix (date DATETIME, close FLOAT)"))
        conn.execute(text("""
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY, company TEXT, domain TEXT,
                market TEXT, logo_url TEXT, industry TEXT,
                description TEXT, description_short TEXT
            )
        """))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
    temp_engine.dispose()

    import importlib
    import backend.main as bm
    importlib.reload(bm)
    # The indices builder fetches the intraday sparkline via _fetch_yf_intraday in
    # addition to the live quote. Stub it so real yfinance intraday data can't leak
    # into tests that drive _fetch_yf_quotes; tests opt into live data via that mock.
    with patch.object(bm, "_fetch_yf_intraday", return_value={}):
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


@pytest.fixture
def indices_app_no_optional_tables():
    """Same as indices_app but WITHOUT index_prices/vix -- simulates a
    prod-synced DB predating those tables (both are populated by a pipeline/
    overlay script that never runs in production), which is what caused the
    500 this fixture guards against."""
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_indices_no_opt.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("""
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY, company TEXT, domain TEXT,
                market TEXT, logo_url TEXT, industry TEXT,
                description TEXT, description_short TEXT
            )
        """))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
    temp_engine.dispose()

    import importlib
    import backend.main as bm
    importlib.reload(bm)
    with patch.object(bm, "_fetch_yf_intraday", return_value={}):
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


def test_returns_200_with_empty_shape_when_index_prices_and_vix_absent(indices_app_no_optional_tables):
    bm, client = indices_app_no_optional_tables
    with patch.object(bm, "_fetch_yf_quotes", return_value={}):
        resp = client.get("/api/market/indices")
    assert resp.status_code == 200
    body = resp.json()
    by = {row["ticker"]: row for row in body["indices"]}
    assert set(by.keys()) == {"^GSPC", "^NDX", "^DJI", "^RUT", "VIX"}
    for row in body["indices"]:
        assert row["close"] is None
        assert row["change_pct"] is None
        assert row["sparkline"] == []


def test_live_quotes_still_served_when_index_prices_and_vix_absent(indices_app_no_optional_tables):
    """The degrade path must still overlay a live quote when one is available --
    absent tables mean no sparkline history, not a fully blank card."""
    bm, client = indices_app_no_optional_tables
    live = {"^GSPC": {"price": 7150.0, "prev_close": 7100.0},
            "^NDX": {"price": 1.0, "prev_close": 1.0}, "^DJI": {"price": 1.0, "prev_close": 1.0},
            "^RUT": {"price": 1.0, "prev_close": 1.0}, "^VIX": {"price": 18.5, "prev_close": 18.0}}
    with patch.object(bm, "_fetch_yf_quotes", return_value=live):
        resp = client.get("/api/market/indices")
    assert resp.status_code == 200
    by = {row["ticker"]: row for row in resp.json()["indices"]}
    assert by["^GSPC"]["close"] == 7150.0
    assert by["^GSPC"]["sparkline"] == [7150.0]  # no table history, just the live point
    assert by["VIX"]["close"] == 18.5


def _insert_price(bm, *, date, ticker, close):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO index_prices (date, ticker, close) VALUES (:date, :ticker, :close)"),
                     dict(date=date, ticker=ticker, close=close))


def _insert_vix(bm, *, date, close):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO vix (date, close) VALUES (:date, :close)"), dict(date=date, close=close))


def test_returns_five_indices_with_close_and_change(indices_app):
    bm, client = indices_app
    for ticker, prev, today in [
        ("^GSPC", 7440.0, 7470.0),  # +0.4032%
        ("^NDX", 29600.0, 29400.0),  # -0.6757%
        ("^DJI", 50400.0, 50526.0),  # +0.25%
        ("^RUT", 2840.0, 2868.4),    # +1.0%
    ]:
        _insert_price(bm, date="2026-05-21 00:00:00", ticker=ticker, close=prev)
        _insert_price(bm, date="2026-05-22 00:00:00", ticker=ticker, close=today)
    _insert_vix(bm, date="2026-05-21 00:00:00", close=17.00)
    _insert_vix(bm, date="2026-05-22 00:00:00", close=16.70)  # -1.7647%

    from unittest.mock import patch
    with patch.object(bm, "_fetch_yf_quotes", return_value={}):
        body = client.get("/api/market/indices").json()
    by = {row["ticker"]: row for row in body["indices"]}
    assert set(by.keys()) == {"^GSPC", "^NDX", "^DJI", "^RUT", "VIX"}
    assert by["^GSPC"]["close"] == pytest.approx(7470.0)
    assert by["^GSPC"]["change_pct"] == pytest.approx(0.4032, abs=1e-3)
    assert by["^NDX"]["change_pct"] == pytest.approx(-0.6757, abs=1e-3)
    assert by["VIX"]["close"] == pytest.approx(16.70)
    assert by["VIX"]["change_pct"] == pytest.approx(-1.7647, abs=1e-3)


def test_sparkline_is_chronological_and_capped_at_30(indices_app):
    bm, client = indices_app
    for i in range(40):
        date = f"2026-04-{i+1:02d} 00:00:00" if i < 30 else f"2026-05-{i-29:02d} 00:00:00"
        _insert_price(bm, date=date, ticker="^GSPC", close=100.0 + i)

    from unittest.mock import patch
    with patch.object(bm, "_fetch_yf_quotes", return_value={}):
        spy = next(r for r in client.get("/api/market/indices").json()["indices"] if r["ticker"] == "^GSPC")
    spark = spy["sparkline"]
    assert len(spark) == 30
    assert spark == sorted(spark)
    assert spark[-1] == pytest.approx(139.0)
    assert spy["close"] == pytest.approx(139.0)


def test_missing_ticker_returns_null_close(indices_app):
    bm, client = indices_app
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="^GSPC", close=7470.0)
    from unittest.mock import patch
    with patch.object(bm, "_fetch_yf_quotes", return_value={}):
        body = client.get("/api/market/indices").json()
    by = {row["ticker"]: row for row in body["indices"]}
    assert set(by.keys()) == {"^GSPC", "^NDX", "^DJI", "^RUT", "VIX"}
    assert by["^NDX"]["close"] is None
    assert by["^NDX"]["change_pct"] is None
    assert by["^NDX"]["sparkline"] == []
    assert by["VIX"]["close"] is None


def test_change_pct_null_with_single_day(indices_app):
    bm, client = indices_app
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="^GSPC", close=7470.0)
    from unittest.mock import patch
    with patch.object(bm, "_fetch_yf_quotes", return_value={}):
        spy = next(r for r in client.get("/api/market/indices").json()["indices"] if r["ticker"] == "^GSPC")
    assert spy["close"] == pytest.approx(7470.0)
    assert spy["change_pct"] is None
    assert spy["sparkline"] == [7470.0]


def test_indices_latest_from_live_yf(indices_app):
    bm, client = indices_app
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for d, c in [("2026-05-21 00:00:00", 7000.0), ("2026-05-22 00:00:00", 7100.0)]:
            conn.execute(text("INSERT INTO index_prices (date,ticker,close) VALUES (:d,'^GSPC',:c)"), {"d": d, "c": c})
        conn.execute(text("INSERT INTO vix (date, close) VALUES ('2026-05-22 00:00:00', 18.0)"))
    live = {"^GSPC": {"price": 7150.0, "prev_close": 7100.0},
            "^NDX": {"price": 1.0, "prev_close": 1.0}, "^DJI": {"price": 1.0, "prev_close": 1.0},
            "^RUT": {"price": 1.0, "prev_close": 1.0}, "^VIX": {"price": 18.5, "prev_close": 18.0}}
    from unittest.mock import patch
    with patch.object(bm, "_fetch_yf_quotes", return_value=live):
        idx = client.get("/api/market/indices").json()["indices"]
    gspc = next(i for i in idx if i["ticker"] == "^GSPC")
    assert gspc["close"] == 7150.0                       # live, not the 7100 table close
    assert round(gspc["change_pct"], 4) == round((7150 - 7100) / 7100 * 100, 4)
    assert len(gspc["sparkline"]) >= 2                   # sparkline still from table
    assert gspc["sparkline"][-1] == 7150.0               # last point replaced with live price


def test_indices_hold_last_good_on_empty_fetch(indices_app):
    """Regression: a transient EMPTY yfinance fetch must not drop the live overlay.
    Before the per-ticker last-good cache, an empty fetch flipped every card to its
    last STORED daily close + that completed day's move (e.g. 7100 / Fri-vs-Thu)
    for a few seconds, then snapped back to live. It must hold the last good live
    quote instead."""
    bm = indices_app[0]
    from unittest.mock import patch
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for d, c in [("2026-05-21 00:00:00", 7000.0), ("2026-05-22 00:00:00", 7100.0)]:
            conn.execute(text("INSERT INTO index_prices (date,ticker,close) VALUES (:d,'^GSPC',:c)"), {"d": d, "c": c})
        conn.execute(text("INSERT INTO vix (date, close) VALUES ('2026-05-22 00:00:00', 18.0)"))
    live = {"^GSPC": {"price": 7150.0, "prev_close": 7100.0},
            "^NDX": {"price": 1.0, "prev_close": 1.0}, "^DJI": {"price": 1.0, "prev_close": 1.0},
            "^RUT": {"price": 1.0, "prev_close": 1.0}, "^VIX": {"price": 18.5, "prev_close": 18.0}}
    # 1) a good fetch populates last-good
    with patch.object(bm, "_fetch_yf_quotes", return_value=live):
        bm._build_market_indices()
    # 2) a transient EMPTY fetch must still serve the live value, not the 7100 close
    with patch.object(bm, "_fetch_yf_quotes", return_value={}):
        out = bm._build_market_indices()
    g = next(i for i in out["indices"] if i["ticker"] == "^GSPC")
    assert g["close"] == 7150.0                                   # held live, not stored 7100
    assert round(g["change_pct"], 4) == round((7150 - 7100) / 7100 * 100, 4)


def test_sparkline_final_segment_matches_change_sign_on_fresh_day(indices_app):
    """Regression: on a fresh trading day the latest STORED close IS prev_close.
    The live point must anchor on that close (not the day before it) so the final
    drawn segment rises on an up day. Previously the code dropped prev_close,
    making the line fall from the day-before's higher close even when change% > 0."""
    bm, client = indices_app
    from sqlalchemy import text
    # day-before close is HIGHER than today's live price; prev (latest stored) is lower.
    with bm.engine.begin() as conn:
        for d, c in [("2026-06-04 00:00:00", 7584.31),   # closes[-2] (higher)
                     ("2026-06-05 00:00:00", 7383.74)]:   # closes[-1] == prev_close
            conn.execute(text("INSERT INTO index_prices (date,ticker,close) VALUES (:d,'^GSPC',:c)"), {"d": d, "c": c})
        conn.execute(text("INSERT INTO vix (date, close) VALUES ('2026-06-05 00:00:00', 18.0)"))
    live = {"^GSPC": {"price": 7429.20, "prev_close": 7383.74},  # up vs Friday
            "^NDX": {"price": 1.0, "prev_close": 1.0}, "^DJI": {"price": 1.0, "prev_close": 1.0},
            "^RUT": {"price": 1.0, "prev_close": 1.0}, "^VIX": {"price": 18.5, "prev_close": 18.0}}
    from unittest.mock import patch
    with patch.object(bm, "_fetch_yf_quotes", return_value=live):
        idx = client.get("/api/market/indices").json()["indices"]
    g = next(i for i in idx if i["ticker"] == "^GSPC")
    sp = g["sparkline"]
    assert g["change_pct"] > 0                            # up day
    assert sp[-2] == 7383.74                              # prev_close anchors the live point
    assert sp[-1] == 7429.20
    assert sp[-1] > sp[-2]                                # final segment rises, matching change%
