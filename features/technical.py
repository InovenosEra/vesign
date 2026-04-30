import gc
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
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


def compute_and_save_features_chunked(engine, days: int = 280, chunk_size: int = 50, warmup: int = 252):
    """Memory-efficient feature computation: processes tickers in small chunks
    and writes directly to DB, avoiding the peak memory of concatenating all frames.

    Loads `days + warmup` trading days of prices but only writes the most recent
    `days` of features. The warmup is required because rolling indicators (notably
    `week52_high = close.rolling(252).max()`) produce NaN until they have 252
    days of history. Without it, every recompute overwrote the cached
    week52_high with NULL for the first ~251 of the `days` rows, ratcheting
    NULLs across history and silently letting BUY signals through the
    "pass through if NULL" 52-week gate.
    """
    with engine.connect() as conn:
        write_cutoff = conn.execute(text("""
            SELECT date FROM (
                SELECT DISTINCT date FROM daily_prices ORDER BY date DESC LIMIT :days
            ) ORDER BY date ASC LIMIT 1
        """), {"days": days}).scalar()

        # Load `days + warmup` of prices so rolling(252) is fully warm before
        # the first row we'll keep.
        load_cutoff = conn.execute(text("""
            SELECT date FROM (
                SELECT DISTINCT date FROM daily_prices ORDER BY date DESC LIMIT :n
            ) ORDER BY date ASC LIMIT 1
        """), {"n": days + warmup}).scalar()

        tickers = [r[0] for r in conn.execute(
            text("SELECT DISTINCT ticker FROM daily_prices WHERE date >= :c ORDER BY ticker"),
            {"c": load_cutoff}
        ).fetchall()]

    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM features WHERE date >= :c"), {"c": write_cutoff})
    except OperationalError:
        # Table doesn't exist yet — fresh DB rebuild. First to_sql will create it.
        pass

    total = len(tickers)
    for i in range(0, total, chunk_size):
        chunk = tickers[i:i + chunk_size]
        placeholders = ",".join([f":t{j}" for j in range(len(chunk))])
        params = {f"t{j}": t for j, t in enumerate(chunk)}
        params["cutoff"] = load_cutoff
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
            # Only write the rows we promised — drop the warmup tail.
            features_chunk = features_chunk[features_chunk["date"] >= write_cutoff]
            features_chunk.to_sql("features", engine, if_exists="append", index=False)
            del features_chunk
        del prices_chunk, frames
        gc.collect()
