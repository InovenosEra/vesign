from sqlalchemy import create_engine, text, inspect, event as sa_event
import pandas as pd
import yaml
import os

# -------------------------
# Load DB config
# -------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(BASE_DIR, "config", "settings.yaml")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# DB path resolution honors both overrides: VESIGN_DB (a db name or absolute
# path, used by the tier rebuild/verify scripts) and DB_PATH (an explicit full
# path). Falls back to the configured name. Either env var unset → prior behavior.
DB_NAME = os.environ.get("VESIGN_DB") or config["database"]["name"]
DB_PATH = os.environ.get("DB_PATH") or (DB_NAME if os.path.isabs(DB_NAME) else os.path.join(BASE_DIR, DB_NAME))

engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)


@sa_event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, _):
    """Match backend/main.py's WAL + busy_timeout settings on every connection.
    Without this, scripts importing `from data.loaders import engine` (e.g.
    production backfills, signals/engine.py, backtesting/engine.py) wrote in
    DELETE journal mode while WAL files from concurrent vesign sessions sat on
    disk — the mismatch caused a "database disk image is malformed" corruption
    during the 2026-05-16 trailing-stop backfill at date 2020-04-14."""
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=30000")
    cur.close()

# -------------------------
# Data functions
# -------------------------

def load_prices(days=None):
    if days is None:
        return pd.read_sql("SELECT * FROM daily_prices", engine)
    # Load only last N trading days per ticker (enough for rolling windows)
    sql = f"""
        SELECT * FROM daily_prices
        WHERE date >= (
            SELECT date FROM (
                SELECT DISTINCT date FROM daily_prices
                ORDER BY date DESC LIMIT {int(days)}
            ) ORDER BY date ASC LIMIT 1
        )
        ORDER BY ticker, date
    """
    return pd.read_sql(sql, engine)


def save_features(df: pd.DataFrame):
    inspector = inspect(engine)
    table_exists = "features" in inspector.get_table_names()

    if table_exists:
        min_date = df["date"].min()
        max_date = df["date"].max()

        with engine.begin() as conn:
            conn.execute(
                text("""
                    DELETE FROM features
                    WHERE date BETWEEN :min_date AND :max_date
                """),
                {"min_date": min_date, "max_date": max_date}
            )

    df.to_sql("features", engine, if_exists="append", index=False)


def _ensure_analyst_source_column():
    """Idempotently add `source` column to analyst_targets_history and
    analyst_expectations. Safe to call on every uvicorn startup.

    Legacy rows (pre-migration) get source=NULL. Interpretation: unknown
    provenance / assume FMP since that was our only source before this column
    was added. New rows must always set source explicitly to 'yfinance',
    'fmp', or 'none'.
    """
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError
    try:
        with engine.begin() as conn:
            for tbl in ("analyst_targets_history", "analyst_expectations"):
                cols = {r[1] for r in conn.execute(text(f"PRAGMA table_info({tbl})"))}
                if "source" not in cols:
                    conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN source TEXT"))
    except OperationalError:
        # Tables don't exist yet; will be created lazily by data/market_data.py
        pass


def _ensure_analyst_targets_history_unique_index():
    """Idempotently ensure UNIQUE(ticker, date) on analyst_targets_history.

    Without this, INSERT OR REPLACE in snapshot_analyst_targets behaves as
    plain INSERT (no conflict to detect → duplicates accumulate if the daily
    pipeline ever runs twice in a day). The `should_run("analyst_update", 24)`
    throttle has prevented dupes in practice, but the schema-level constraint
    is the proper safety net.
    """
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError
    try:
        with engine.begin() as conn:
            indexes = {r[0] for r in conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='analyst_targets_history'"
            ))}
            if "idx_ath_unique" not in indexes:
                conn.execute(text(
                    "CREATE UNIQUE INDEX idx_ath_unique ON analyst_targets_history(ticker, date)"
                ))
            if "idx_ath" in indexes:
                # Old non-unique index is redundant once the unique one exists
                conn.execute(text("DROP INDEX idx_ath"))
    except OperationalError:
        # Table doesn't exist yet or duplicates would block the unique index;
        # will be retried next startup once the data is consistent
        pass


def _ensure_fundamentals_columns():
    """Idempotently add the TTM fundamentals columns to the `fundamentals`
    table (which historically held only ticker + market_cap). Safe to call on
    every uvicorn startup. Legacy rows get NULL until the backfill/pipeline
    populates them. See data/fundamentals.py for the writer.
    """
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError
    cols_to_add = (
        "pe_ttm", "eps_ttm", "revenue_ttm", "revenue_growth",
        "gross_margin", "op_margin", "net_margin", "roe", "de_ratio",
        "fundamentals_updated",  # ISO date of last refresh — drives staleness repair
    )
    try:
        with engine.begin() as conn:
            cols = {r[1] for r in conn.execute(text("PRAGMA table_info(fundamentals)"))}
            for c in cols_to_add:
                if c not in cols:
                    typ = "TEXT" if c == "fundamentals_updated" else "FLOAT"
                    conn.execute(text(f"ALTER TABLE fundamentals ADD COLUMN {c} {typ}"))
    except OperationalError:
        # Table doesn't exist yet; created lazily elsewhere
        pass


# Run migrations on module load
_ensure_analyst_source_column()
_ensure_fundamentals_columns()
_ensure_analyst_targets_history_unique_index()
