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

    monkeypatch.setattr(backend_main, "_phase_info",
        lambda now_utc=None: {"phase": "idle", "next_event_name": "pre_open",
                              "next_event_utc": "2026-05-14T08:00:00+00:00"})

    client = TestClient(backend_main.app)
    r = client.get("/api/prices/live?tickers=AAPL,MSFT")
    body = r.json()
    assert body["phase"] == "idle"
    assert body["prices"] == {"AAPL": None, "MSFT": None}

    backend_main.app.dependency_overrides.clear()


def test_prices_live_uses_aftermarket_in_pre_phase(monkeypatch):
    """Pre phase: endpoint fetches via fetch_aftermarket_trades, not fetch_live_prices."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}

    monkeypatch.setattr(backend_main, "_phase_info",
        lambda now_utc=None: {"phase": "pre", "next_event_name": "regular_open",
                              "next_event_utc": "2026-05-14T13:30:00+00:00"})
    monkeypatch.setattr(backend_main, "fetch_aftermarket_trades",
        lambda tickers: {"AAPL": 297.55})
    # Clear cache so the test doesn't pick up stale values from prior calls
    backend_main._live_price_cache.clear()
    backend_main._live_price_cache_phase = None

    client = TestClient(backend_main.app)
    r = client.get("/api/prices/live?tickers=AAPL")
    body = r.json()
    assert body["phase"] == "pre"
    assert body["prices"]["AAPL"] == 297.55

    backend_main.app.dependency_overrides.clear()


def test_prices_live_uses_quote_in_regular_phase(monkeypatch):
    """Regular phase: endpoint fetches via fetch_live_prices."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}

    monkeypatch.setattr(backend_main, "_phase_info",
        lambda now_utc=None: {"phase": "regular", "next_event_name": "regular_close",
                              "next_event_utc": "2026-05-14T20:00:00+00:00"})
    monkeypatch.setattr(backend_main, "fetch_live_prices",
        lambda tickers: {"AAPL": 298.99})
    backend_main._live_price_cache.clear()
    backend_main._live_price_cache_phase = None

    client = TestClient(backend_main.app)
    r = client.get("/api/prices/live?tickers=AAPL")
    body = r.json()
    assert body["phase"] == "regular"
    assert body["prices"]["AAPL"] == 298.99

    backend_main.app.dependency_overrides.clear()


def test_prices_live_flushes_cache_on_phase_change(monkeypatch):
    """When phase changes between consecutive calls, cache should flush so stale prices don't leak."""
    from fastapi.testclient import TestClient
    from backend import main as backend_main
    backend_main.app.dependency_overrides[backend_main.get_current_user] = lambda: {"id": "test"}

    # Reset cache before test
    backend_main._live_price_cache.clear()
    backend_main._live_price_cache_phase = None

    # First call: regular phase
    monkeypatch.setattr(backend_main, "_phase_info",
        lambda now_utc=None: {"phase": "regular", "next_event_name": "regular_close",
                              "next_event_utc": "2026-05-14T20:00:00+00:00"})
    monkeypatch.setattr(backend_main, "fetch_live_prices",
        lambda tickers: {"AAPL": 100.0})
    client = TestClient(backend_main.app)
    r1 = client.get("/api/prices/live?tickers=AAPL").json()
    assert r1["prices"]["AAPL"] == 100.0

    # Second call: post phase — aftermarket should be called even though AAPL is in cache
    monkeypatch.setattr(backend_main, "_phase_info",
        lambda now_utc=None: {"phase": "post", "next_event_name": "post_close",
                              "next_event_utc": "2026-05-15T00:00:00+00:00"})
    monkeypatch.setattr(backend_main, "fetch_aftermarket_trades",
        lambda tickers: {"AAPL": 200.0})
    r2 = client.get("/api/prices/live?tickers=AAPL").json()
    assert r2["phase"] == "post"
    assert r2["prices"]["AAPL"] == 200.0, "Cache should flush on phase change"

    backend_main.app.dependency_overrides.clear()
