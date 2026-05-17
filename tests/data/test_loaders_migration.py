"""Tests for data.loaders._ensure_analyst_source_column — idempotent ALTER TABLE."""
from sqlalchemy import create_engine, text


def _make_tables(eng):
    """Create the legacy schema (no source column) for testing migration."""
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE analyst_targets_history (
                date TEXT, ticker TEXT, target_mean_price REAL,
                target_high_price REAL, target_low_price REAL,
                number_of_analysts REAL
            )
        """))
        conn.execute(text("""
            CREATE TABLE analyst_expectations (
                ticker TEXT PRIMARY KEY, target_mean_price REAL,
                target_high_price REAL, target_low_price REAL,
                number_of_analysts REAL, last_update TEXT
            )
        """))


def test_adds_source_column_on_first_run(monkeypatch):
    eng = create_engine("sqlite://")  # in-memory
    _make_tables(eng)

    # Point the helper at our in-memory engine
    import data.loaders
    monkeypatch.setattr(data.loaders, "engine", eng)
    data.loaders._ensure_analyst_source_column()

    for tbl in ("analyst_targets_history", "analyst_expectations"):
        cols = {r[1] for r in eng.connect().execute(text(f"PRAGMA table_info({tbl})"))}
        assert "source" in cols, f"{tbl} missing source column after migration"


def test_idempotent_no_duplicate_alter(monkeypatch):
    eng = create_engine("sqlite://")
    _make_tables(eng)
    import data.loaders
    monkeypatch.setattr(data.loaders, "engine", eng)

    data.loaders._ensure_analyst_source_column()
    # Second call must not raise
    data.loaders._ensure_analyst_source_column()

    cols = {r[1] for r in eng.connect().execute(text("PRAGMA table_info(analyst_expectations)"))}
    assert "source" in cols


def test_handles_missing_tables_gracefully(monkeypatch):
    """Verify OperationalError is caught when tables don't exist (fresh DB).

    Module load runs the migration before market_data.py has had a chance to
    create the tables. The function must not raise.
    """
    from sqlalchemy import create_engine
    eng = create_engine("sqlite://")  # empty in-memory, no tables
    import data.loaders
    monkeypatch.setattr(data.loaders, "engine", eng)

    # Should not raise even though tables don't exist
    data.loaders._ensure_analyst_source_column()
