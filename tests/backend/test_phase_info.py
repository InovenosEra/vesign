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

def test_pre_market():
    out = _pi(datetime(2026, 5, 14, 10, 0, tzinfo=timezone.utc))  # 06:00 ET
    assert out["phase"] == "pre"
    assert out["next_event_name"] == "regular_open"
    assert out["next_event_utc"] == "2026-05-14T13:30:00+00:00"

def test_post_market():
    out = _pi(datetime(2026, 5, 14, 22, 0, tzinfo=timezone.utc))  # 18:00 ET
    assert out["phase"] == "post"
    assert out["next_event_name"] == "post_close"
    assert out["next_event_utc"] == "2026-05-15T00:00:00+00:00"

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
