from sqlalchemy import create_engine, text, inspect
import pandas as pd

engine = create_engine("sqlite:///vesign.db")


def load_prices():
    return pd.read_sql("SELECT * FROM daily_prices", engine)


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
