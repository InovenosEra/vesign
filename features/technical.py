import gc
import pandas as pd
from sqlalchemy import text
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


def compute_and_save_features_chunked(engine, days: int = 280, chunk_size: int = 50):
    """Memory-efficient feature computation: processes tickers in small chunks
    and writes directly to DB, avoiding the peak memory of concatenating all frames.

    Uses `days` trading days of price history (need 252 for 52-week high rolling window).
    """
    with engine.connect() as conn:
        cutoff = conn.execute(text("""
            SELECT date FROM (
                SELECT DISTINCT date FROM daily_prices ORDER BY date DESC LIMIT :days
            ) ORDER BY date ASC LIMIT 1
        """), {"days": days}).scalar()

        tickers = [r[0] for r in conn.execute(
            text("SELECT DISTINCT ticker FROM daily_prices WHERE date >= :c ORDER BY ticker"),
            {"c": cutoff}
        ).fetchall()]

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM features WHERE date >= :c"), {"c": cutoff})

    total = len(tickers)
    for i in range(0, total, chunk_size):
        chunk = tickers[i:i + chunk_size]
        placeholders = ",".join([f":t{j}" for j in range(len(chunk))])
        params = {f"t{j}": t for j, t in enumerate(chunk)}
        params["cutoff"] = cutoff
        prices_chunk = pd.read_sql(
            f"SELECT * FROM daily_prices WHERE date >= :cutoff"
            f" AND ticker IN ({placeholders}) ORDER BY ticker, date",
            engine,
            params=params,
        )
        frames = []
        for ticker, df in prices_chunk.groupby("ticker"):
            df = df.sort_values("date")
            frames.append(add_indicators(df))
        if frames:
            features_chunk = pd.concat(frames)
            features_chunk.drop_duplicates(subset=["ticker", "date"], inplace=True)
            features_chunk.to_sql("features", engine, if_exists="append", index=False)
            del features_chunk
        del prices_chunk, frames
        gc.collect()
