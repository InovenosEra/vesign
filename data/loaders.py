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

DB_NAME = config["database"]["name"]
DB_PATH = os.path.join(BASE_DIR, DB_NAME)

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
