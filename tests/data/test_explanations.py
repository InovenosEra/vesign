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
