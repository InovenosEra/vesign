from sqlalchemy import create_engine, text, inspect
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

engine = create_engine(f"sqlite:///{DB_NAME}", echo=False)

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
