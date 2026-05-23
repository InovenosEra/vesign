"""Tests for GET /api/market/news/top — recent market news headlines."""
import os
import tempfile
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def news_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_news.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT,
                industry TEXT, description TEXT, description_short TEXT,
                logo_url TEXT, domain TEXT
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


def _ago(minutes: int) -> str:
    """ISO timestamp `minutes` minutes ago in UTC, with a 'Z' suffix."""
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes))\
        .strftime("%Y-%m-%dT%H:%M:%SZ")


def test_returns_headlines_with_age_in_minutes(news_app):
    bm, client = news_app
    fixture = [
        {"title": "Fed signals rate cut",       "site": "Reuters",   "url": "u1",
         "symbol": "",     "publishedDate": _ago(5)},
        {"title": "NVDA hits all-time high",    "site": "Bloomberg", "url": "u2",
         "symbol": "NVDA", "publishedDate": _ago(12)},
        {"title": "Oil drops on demand fears",  "site": "WSJ",       "url": "u3",
         "symbol": "",     "publishedDate": _ago(45)},
    ]
    with patch.object(bm, "_fetch_top_news", return_value=fixture):
        body = client.get("/api/market/news/top?limit=5").json()

    rows = body["news"]
    assert len(rows) == 3
    titles = [r["title"] for r in rows]
    assert "Fed signals rate cut" in titles
    fed = next(r for r in rows if r["title"] == "Fed signals rate cut")
    assert fed["source"] == "Reuters"
    assert fed["url"] == "u1"
    # Age window: 4–6 minutes (allows for clock skew during the test run)
    assert 4 <= fed["age_minutes"] <= 6
    # Ticker carries through when present
    nvda = next(r for r in rows if r["title"] == "NVDA hits all-time high")
    assert nvda["ticker"] == "NVDA"
    # Articles without a symbol come back with ticker=None (UI hides the chip)
    assert fed["ticker"] is None


def test_limit_clips_results(news_app):
    bm, client = news_app
    fixture = [{"title": f"News {i}", "site": "X", "url": f"u{i}",
                "symbol": "", "publishedDate": _ago(i)} for i in range(20)]
    with patch.object(bm, "_fetch_top_news", return_value=fixture[:3]) as m:
        body = client.get("/api/market/news/top?limit=3").json()
    # The fetcher should have been called with the requested limit.
    m.assert_called_once_with(3)
    assert len(body["news"]) == 3


def test_empty_when_fetch_returns_none(news_app):
    bm, client = news_app
    with patch.object(bm, "_fetch_top_news", return_value=None):
        body = client.get("/api/market/news/top").json()
    assert body == {"news": []}
