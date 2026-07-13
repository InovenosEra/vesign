"""Tests for backend._phase_info — NYSE phase state machine."""
from datetime import datetime, timezone

# Imported lazily inside tests so backend env vars don't load at import time
def _pi(now_utc):
    from backend.main import _phase_info
    return _phase_info(now_utc)


# NYSE on 2026-05-14 (Thursday, regular session):
#   pre_open      = 08:00 UTC (04:00 ET)
#   regular_open  = 13:30 UTC (09:30 ET)
#   regular_close = 20:00 UTC (16:00 ET)
#   post_close    = 24:00 UTC (20:00 ET)

def test_regular_session_midday():
    out = _pi(datetime(2026, 5, 14, 17, 0, tzinfo=timezone.utc))  # 13:00 ET
    assert out["phase"] == "regular"
    assert out["next_event_name"] == "regular_close"
    assert out["next_event_utc"] == "2026-05-14T20:00:00+00:00"
    # next_regular_event should be the regular_close
    assert out["next_regular_event_name"] == "regular_close"
    assert out["next_regular_event_utc"] == "2026-05-14T20:00:00+00:00"

def test_pre_market():
    out = _pi(datetime(2026, 5, 14, 10, 0, tzinfo=timezone.utc))  # 06:00 ET
    assert out["phase"] == "pre"
    assert out["next_event_name"] == "regular_open"
    assert out["next_event_utc"] == "2026-05-14T13:30:00+00:00"
    # next_regular_event should be today's regular_open
    assert out["next_regular_event_name"] == "regular_open"
    assert out["next_regular_event_utc"] == "2026-05-14T13:30:00+00:00"

def test_post_market():
    out = _pi(datetime(2026, 5, 14, 22, 0, tzinfo=timezone.utc))  # 18:00 ET
    assert out["phase"] == "post"
    assert out["next_event_name"] == "post_close"
    assert out["next_event_utc"] == "2026-05-15T00:00:00+00:00"
    # next_regular_event must be NEXT trading day's regular_open (Friday 5/15)
    assert out["next_regular_event_name"] == "regular_open"
    assert out["next_regular_event_utc"].startswith("2026-05-15T13:30:00")

def test_idle_overnight_next_regular_event_is_today_open():
    """Idle BEFORE today's pre-market — next regular event is today's regular_open."""
    out = _pi(datetime(2026, 5, 14, 3, 0, tzinfo=timezone.utc))
    assert out["phase"] == "idle"
    assert out["next_regular_event_name"] == "regular_open"
    assert out["next_regular_event_utc"] == "2026-05-14T13:30:00+00:00"

def test_idle_weekend_next_regular_event_is_monday_open():
    """Idle Saturday — next regular event is Monday's regular_open."""
    out = _pi(datetime(2026, 5, 16, 12, 0, tzinfo=timezone.utc))
    assert out["phase"] == "idle"
    assert out["next_regular_event_name"] == "regular_open"
    assert out["next_regular_event_utc"].startswith("2026-05-18T13:30:00")

def test_idle_overnight_before_next_pre():
    # 03:00 UTC Thu 2026-05-14 → already past Wed's post_close (24:00 UTC = 2026-05-14T00:00:00),
    # before Thu's pre_open (2026-05-14T08:00:00).
    out = _pi(datetime(2026, 5, 14, 3, 0, tzinfo=timezone.utc))
    assert out["phase"] == "idle"
    assert out["next_event_name"] == "pre_open"
    assert out["next_event_utc"] == "2026-05-14T08:00:00+00:00"

def test_weekend_saturday():
    out = _pi(datetime(2026, 5, 16, 12, 0, tzinfo=timezone.utc))  # Sat
    assert out["phase"] == "idle"
    # Next event is Monday's pre_open
    assert out["next_event_name"] == "pre_open"
    assert out["next_event_utc"].startswith("2026-05-18T08:00:00")

def test_boundary_exactly_at_pre_open():
    # At exactly pre_open UTC, we are in pre-market (inclusive lower bound).
    out = _pi(datetime(2026, 5, 14, 8, 0, tzinfo=timezone.utc))
    assert out["phase"] == "pre"

def test_boundary_exactly_at_regular_open():
    out = _pi(datetime(2026, 5, 14, 13, 30, tzinfo=timezone.utc))
    assert out["phase"] == "regular"


def test_market_status_endpoint_returns_phase(monkeypatch):
    """Integration test: /api/market/status should return phase + next_event_name."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main

    # Bypass auth for this test
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}

    # Force _phase_info to return a known shape regardless of real wall clock
    monkeypatch.setattr(backend_main, "_phase_info",
        lambda now_utc=None: {
            "phase": "regular",
            "next_event_name": "regular_close",
            "next_event_utc": "2026-05-14T20:00:00+00:00",
        })

    client = TestClient(backend_main.app)
    r = client.get("/api/market/status")
    assert r.status_code == 200
    body = r.json()
    assert body["phase"] == "regular"
    assert body["next_event_name"] == "regular_close"
    assert body["next_event_utc"] == "2026-05-14T20:00:00+00:00"

    # Cleanup auth override
    backend_main.app.dependency_overrides.clear()


def test_prices_live_returns_phase_field(monkeypatch):
    """Idle phase: endpoint returns phase='idle' with all prices None."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}
    backend_main._page_live_cache.clear()
    backend_main._page_live_cache_ts.clear()

    monkeypatch.setattr(backend_main, "_phase_info",
        lambda now_utc=None: {"phase": "idle", "next_event_name": "pre_open",
                              "next_event_utc": "2026-05-14T08:00:00+00:00"})

    client = TestClient(backend_main.app)
    r = client.get("/api/prices/live?tickers=AAPL,MSFT")
    body = r.json()
    assert body["phase"] == "idle"
    assert body["prices"] == {"AAPL": None, "MSFT": None}

    backend_main.app.dependency_overrides.clear()


def test_prices_live_reads_bounded_last_trade(monkeypatch):
    """The endpoint fetches a real last-trade price for the requested (bounded)
    ticker list directly — not the whole-universe bid/ask-mid snapshot."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}
    backend_main._page_live_cache.clear()
    backend_main._page_live_cache_ts.clear()

    monkeypatch.setattr(backend_main, "_phase_info", lambda now_utc=None: {"phase": "regular"})
    monkeypatch.setattr(backend_main, "fetch_live_prices",
        lambda tickers: {"AAPL": 298.99, "MSFT": 410.0})

    client = TestClient(backend_main.app)
    body = client.get("/api/prices/live?tickers=AAPL").json()
    assert body["phase"] == "regular"
    assert body["prices"]["AAPL"] == 298.99

    backend_main.app.dependency_overrides.clear()


def test_prices_live_uses_aftermarket_trades_in_pre_post(monkeypatch):
    """Pre/post phase uses the real last aftermarket trade, not a bid/ask mid."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}
    backend_main._page_live_cache.clear()
    backend_main._page_live_cache_ts.clear()

    monkeypatch.setattr(backend_main, "_phase_info", lambda now_utc=None: {"phase": "pre"})
    monkeypatch.setattr(backend_main, "fetch_aftermarket_trades",
        lambda tickers: {"AAPL": 297.55})

    client = TestClient(backend_main.app)
    body = client.get("/api/prices/live?tickers=AAPL").json()
    assert body["phase"] == "pre"
    assert body["prices"]["AAPL"] == 297.55

    backend_main.app.dependency_overrides.clear()


def test_prices_live_filters_to_requested_tickers(monkeypatch):
    """Only the requested tickers are returned; one missing from the fetch → None."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}
    backend_main._page_live_cache.clear()
    backend_main._page_live_cache_ts.clear()

    monkeypatch.setattr(backend_main, "_phase_info", lambda now_utc=None: {"phase": "regular"})
    monkeypatch.setattr(backend_main, "fetch_live_prices",
        lambda tickers: {"AAPL": 100.0, "MSFT": 200.0})

    client = TestClient(backend_main.app)
    body = client.get("/api/prices/live?tickers=AAPL,GOOG").json()
    assert body["prices"] == {"AAPL": 100.0, "GOOG": None}
    assert "MSFT" not in body["prices"]

    backend_main.app.dependency_overrides.clear()
