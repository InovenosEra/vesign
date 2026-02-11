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

    final.to_sql("features", engine, if_exists="replace", index=False)

    print("Features generated successfully")
