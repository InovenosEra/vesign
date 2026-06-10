"""Unit tests for the BUY-signal explainer module (data/explanations.py)."""
import os
import json
import tempfile
import importlib
import pytest


@pytest.fixture
def mod():
    """Point the module's engine at a seeded temp DB, return the reloaded module."""
    saved_db = os.environ.get("DB_PATH")
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "expl.db")

    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}")
    with eng.begin() as conn:
        conn.execute(text("""CREATE TABLE signals (date TEXT, ticker TEXT, close REAL,
            prediction_score REAL, vqs INTEGER, signal TEXT, health_score INTEGER,
            target_mean_price REAL)"""))
        conn.execute(text("""CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT)"""))
        conn.execute(text("""CREATE TABLE fundamentals (ticker TEXT, market_cap REAL,
            pe_ttm REAL, gross_margin REAL, op_margin REAL, net_margin REAL,
            roe REAL, de_ratio REAL)"""))
        # AAPL: full data + analyst target above close
        conn.execute(text("""INSERT INTO companies VALUES ('AAPL','Apple Inc.')"""))
        conn.execute(text("""INSERT INTO signals VALUES
            ('2026-05-26 00:00:00','AAPL',100.0,0.30,9,'BUY',4,120.0)"""))
        conn.execute(text("""INSERT INTO fundamentals VALUES
            ('AAPL',3e12,28.0,0.44,0.30,0.25,0.15,1.5)"""))
        # NEW: V2 VQS=9 BUY with NO analyst target and NO fundamentals row
        conn.execute(text("""INSERT INTO companies VALUES ('NEW','Newco')"""))
        conn.execute(text("""INSERT INTO signals VALUES
            ('2026-05-26 00:00:00','NEW',50.0,0.41,9,'BUY',3,NULL)"""))
    eng.dispose()

    import data.loaders as loaders; importlib.reload(loaders)
    import data.explanations as expl; importlib.reload(expl)
    # Make news deterministic + offline for every test by default
    expl.fmp.stock_news = lambda ticker, limit=3: []
    yield expl

    if saved_db is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = saved_db
    import shutil; shutil.rmtree(tmpdir, ignore_errors=True)


def test_ensure_table_creates_signal_explanations(mod):
    mod._ensure_table()
    from sqlalchemy import text
    from data.loaders import engine
    with engine.begin() as conn:
        names = {r[0] for r in conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table'"))}
    assert "signal_explanations" in names


def test_assemble_evidence_full(mod):
    ev = mod.assemble_evidence("AAPL", "2026-05-26")
    assert ev["ticker"] == "AAPL"
    assert ev["company"] == "Apple Inc."
    assert ev["action"] == "BUY"
    assert ev["close"] == 100.0
    assert ev["ml_score"] == 0.30
    assert ev["strong_buy"] is True            # vqs == 9
    assert ev["health_score"] == 4
    assert ev["fundamentals"]["pe_ttm"] == 28.0
    assert ev["fundamentals"]["roe"] == 0.15
    assert ev["analyst_upside_pct"] == 20.0     # (120-100)/100*100
    assert ev["news"] == []                      # stubbed offline


def test_assemble_evidence_omits_nulls(mod):
    # NEW has no analyst target and no fundamentals row
    ev = mod.assemble_evidence("NEW", "2026-05-26")
    assert ev["ticker"] == "NEW"
    assert "analyst_upside_pct" not in ev        # null omitted, not guessed
    assert ev["fundamentals"] == {}              # no row → empty, not None


def test_assemble_evidence_news_titles(mod):
    mod.fmp.stock_news = lambda ticker, limit=3: [
        {"title": "Apple beats earnings"}, {"title": "New product launch"}, {"title": None}]
    ev = mod.assemble_evidence("AAPL", "2026-05-26")
    assert ev["news"] == ["Apple beats earnings", "New product launch"]


def test_assemble_evidence_unknown_returns_none(mod):
    assert mod.assemble_evidence("ZZZZ", "2026-05-26") is None


class _FakeBlock:
    type = "text"
    def __init__(self, txt): self.text = txt

class _FakeResp:
    def __init__(self, txt): self.content = [_FakeBlock(txt)]

class _FakeClient:
    """Records the create() kwargs and returns a canned JSON body."""
    def __init__(self, body):
        self._body = body
        self.calls = []
        self.messages = self
    def create(self, **kw):
        self.calls.append(kw)
        return _FakeResp(self._body)


def test_generate_explanation_returns_validated_dict(mod):
    body = json.dumps({
        "headline": "Strong fundamentals, cheap vs peers",
        "strengths": ["High ROE", "Analyst upside 20%", "Healthy balance sheet"],
        "risks": ["Elevated P/E"],
        "key_numbers": [{"label": "P/E", "value": "28.0"}],
    })
    client = _FakeClient(body)
    out = mod.generate_explanation({"ticker": "AAPL"}, client=client)
    assert out["headline"].startswith("Strong fundamentals")
    assert len(out["strengths"]) == 3
    assert out["key_numbers"][0]["label"] == "P/E"
    kw = client.calls[0]
    assert kw["model"] == mod.MODEL
    assert kw["output_config"]["format"]["type"] == "json_schema"


def test_generate_explanation_trims_overlong_arrays(mod):
    body = json.dumps({
        "headline": "h",
        "strengths": ["a", "b", "c", "d", "e"],
        "risks": ["x", "y", "z"],
        "key_numbers": [{"label": str(i), "value": str(i)} for i in range(8)],
    })
    out = mod.generate_explanation({"ticker": "AAPL"}, client=_FakeClient(body))
    assert len(out["strengths"]) == 3
    assert len(out["risks"]) == 2
    assert len(out["key_numbers"]) == 4


def _canned_client():
    return _FakeClient(json.dumps({
        "headline": "h", "strengths": ["a"], "risks": ["x"],
        "key_numbers": [{"label": "P/E", "value": "28.0"}],
    }))


def test_get_or_create_miss_generates_and_caches(mod):
    client = _canned_client()
    out = mod.get_or_create("AAPL", "2026-05-26", client=client)
    assert out["headline"] == "h"
    assert len(client.calls) == 1
    from sqlalchemy import text
    from data.loaders import engine
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT payload, model FROM signal_explanations "
            "WHERE ticker='AAPL' AND signal_date='2026-05-26'")).fetchone()
    assert row is not None
    assert json.loads(row[0])["headline"] == "h"
    assert row[1] == mod.MODEL


def test_get_or_create_hit_does_not_call_model(mod):
    first = _canned_client()
    mod.get_or_create("AAPL", "2026-05-26", client=first)
    second = _canned_client()
    out = mod.get_or_create("AAPL", "2026-05-26", client=second)
    assert out["headline"] == "h"
    assert len(second.calls) == 0          # served from cache


def test_get_or_create_unknown_signal_returns_none(mod):
    client = _canned_client()
    assert mod.get_or_create("ZZZZ", "2026-05-26", client=client) is None
    assert len(client.calls) == 0
