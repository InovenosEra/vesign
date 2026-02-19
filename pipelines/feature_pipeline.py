from sqlalchemy import text, inspect
import pandas as pd
from database.db_connection import engine
from features.technical_indicators import add_indicators


def run_feature_pipeline():

    print("Running feature pipeline...")

    prices = pd.read_sql("SELECT * FROM daily_prices", engine)

    frames = []

    for ticker, df in prices.groupby("ticker"):
        df = df.sort_values("date")
        df = add_indicators(df)
        frames.append(df)

    final = pd.concat(frames)

    # ensure uniqueness before writing
    final.drop_duplicates(subset=["ticker", "date"], inplace=True)

    # detect whether features table already exists
    inspector = inspect(engine)
    table_exists = "features" in inspector.get_table_names()

    # delete existing rows for overlapping dates only if table exists
    if table_exists:
        min_date = final["date"].min()
        max_date = final["date"].max()

        with engine.begin() as conn:
            conn.execute(
                text("""
                    DELETE FROM features
                    WHERE date BETWEEN :min_date AND :max_date
                """),
                {"min_date": min_date, "max_date": max_date}
            )

    # append refreshed rows (creates table automatically on first run)
    final.to_sql("features", engine, if_exists="append", index=False)

    print("Features generated successfully")
