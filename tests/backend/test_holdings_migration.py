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
