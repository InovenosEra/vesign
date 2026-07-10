"""Tests that the one-time holdings migration copies watchlist_holdings rows
(resolving user_id via watchlist_lists) into the new user-scoped `holdings`
table, then drops the old table."""
import os
import tempfile
import pytest
from sqlalchemy import create_engine, text


@pytest.fixture
def migration_db():
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "t.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)
    eng = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with eng.begin() as c:
        c.execute(text("CREATE TABLE watchlist_lists (id INTEGER PRIMARY KEY, user_id TEXT, name TEXT)"))
        c.execute(text("CREATE TABLE watchlist_holdings (id INTEGER PRIMARY KEY, watchlist_id INTEGER, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        c.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, industry TEXT, logo_url TEXT, domain TEXT)"))
        c.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        c.execute(text("INSERT INTO watchlist_lists VALUES (1,'user-a','Mine'), (2,'user-b','Theirs')"))
        c.execute(text("INSERT INTO watchlist_holdings (watchlist_id,ticker,quantity,buy_price,buy_date) VALUES "
                        "(1,'AAPL',10,100.0,'2026-01-01'), (2,'MSFT',3,300.0,'2026-02-01')"))
    eng.dispose()
    yield db_path
    for f in os.listdir(tmp):
        try: os.remove(os.path.join(tmp, f))
        except OSError: pass
    os.rmdir(tmp)


def test_migration_copies_rows_with_resolved_user_id_and_drops_old_table(migration_db):
    import importlib, backend.main as bm
    importlib.reload(bm)  # runs _init_tables(), including the migration

    eng = create_engine(f"sqlite:///{migration_db}")
    with eng.connect() as conn:
        rows = conn.execute(text("SELECT user_id, ticker, quantity, buy_price, buy_date FROM holdings ORDER BY ticker")).fetchall()
        table_exists = conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='watchlist_holdings'"
        )).fetchone()
    eng.dispose()

    assert [tuple(r) for r in rows] == [
        ("user-a", "AAPL", 10.0, 100.0, "2026-01-01"),
        ("user-b", "MSFT", 3.0, 300.0, "2026-02-01"),
    ]
    assert table_exists is None


@pytest.fixture
def migration_db_with_orphan():
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "t.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)
    eng = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with eng.begin() as c:
        c.execute(text("CREATE TABLE watchlist_lists (id INTEGER PRIMARY KEY, user_id TEXT, name TEXT)"))
        c.execute(text("CREATE TABLE watchlist_holdings (id INTEGER PRIMARY KEY, watchlist_id INTEGER, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        c.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, industry TEXT, logo_url TEXT, domain TEXT)"))
        c.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        c.execute(text("INSERT INTO watchlist_lists VALUES (1,'user-a','Mine')"))
        c.execute(text("INSERT INTO watchlist_holdings (watchlist_id,ticker,quantity,buy_price,buy_date) VALUES "
                        "(1,'AAPL',10,100.0,'2026-01-01'), (999,'ORPHAN',5,50.0,'2026-03-01')"))
    eng.dispose()
    yield db_path
    for f in os.listdir(tmp):
        try: os.remove(os.path.join(tmp, f))
        except OSError: pass
    os.rmdir(tmp)


def test_migration_logs_and_skips_orphaned_watchlist_holdings_rows(migration_db_with_orphan, capsys):
    """A watchlist_holdings row whose watchlist_id has no matching watchlist_lists
    row (can happen from before a since-fixed delete_watchlist cascade bug) must
    be skipped from the new holdings table AND logged, not silently dropped."""
    import importlib, backend.main as bm
    importlib.reload(bm)  # runs _init_tables(), including the migration

    captured = capsys.readouterr()

    eng = create_engine(f"sqlite:///{migration_db_with_orphan}")
    with eng.connect() as conn:
        rows = conn.execute(text("SELECT user_id, ticker, quantity, buy_price, buy_date FROM holdings ORDER BY ticker")).fetchall()
    eng.dispose()

    tickers = [r[1] for r in rows]
    assert "ORPHAN" not in tickers
    assert tickers == ["AAPL"]
    assert "ORPHAN" in captured.out
    assert "999" in captured.out


def test_migration_does_not_duplicate_if_source_table_reappears(migration_db):
    """Reproduces the real-world bug: production still runs pre-migration code,
    so its watchlist_holdings table is never dropped there. A local dev DB sync
    (vesign-daily-sync.sh) can pull a fresh, un-migrated watchlist_holdings
    snapshot back into a local DB that already completed this migration once.
    The next app restart must not re-copy those rows into holdings a second
    time — this is what actually doubled a user's local Holdings total."""
    import importlib, backend.main as bm
    importlib.reload(bm)  # first run: migrates + drops watchlist_holdings

    # Simulate a DB sync reintroducing the (still un-migrated, from prod's
    # point of view) watchlist_holdings table with the same rows as before.
    eng = create_engine(f"sqlite:///{migration_db}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE watchlist_holdings (id INTEGER PRIMARY KEY, watchlist_id INTEGER, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        c.execute(text("INSERT INTO watchlist_holdings (watchlist_id,ticker,quantity,buy_price,buy_date) VALUES "
                        "(1,'AAPL',10,100.0,'2026-01-01'), (2,'MSFT',3,300.0,'2026-02-01')"))
    eng.dispose()

    importlib.reload(bm)  # second run: must not duplicate already-migrated rows

    eng = create_engine(f"sqlite:///{migration_db}")
    with eng.connect() as conn:
        rows = conn.execute(text("SELECT user_id, ticker, quantity, buy_price, buy_date FROM holdings ORDER BY ticker")).fetchall()
    eng.dispose()

    assert [tuple(r) for r in rows] == [
        ("user-a", "AAPL", 10.0, 100.0, "2026-01-01"),
        ("user-b", "MSFT", 3.0, 300.0, "2026-02-01"),
    ]
