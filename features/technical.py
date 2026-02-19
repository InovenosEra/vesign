import pandas as pd
from features.technical_indicators import add_indicators


def compute_features(prices_df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for ticker, df in prices_df.groupby("ticker"):
        df = df.sort_values("date")
        df = add_indicators(df)
        frames.append(df)

    final = pd.concat(frames)
    final.drop_duplicates(subset=["ticker", "date"], inplace=True)

    return final
