"""Repair NULL flag columns on existing signals rows.

Some prior backfills wrote only a subset of columns to the signals table, leaving
rsi_3day_flag, volume_flag, week52_condition, health_condition, ml_condition, and
vesign_score as NULL. The signal (BUY/SELL/HOLD) itself is correct — only the
per-gate diagnostic columns are missing. This script recomputes them from the
features table and UPDATEs the existing rows in place (no DELETE/INSERT).

Chunked by year to keep memory bounded on the 1 GB server. Safe to re-run.
"""
import os, sys, yaml
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.loaders import engine
from sqlalchemy import text
from signals.engine import _compute_vesign_score


def _load_config():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(BASE_DIR, "config", "settings.yaml")) as f:
        return yaml.safe_load(f)


def _clean_int(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return int(v)


def _clean_bool(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return int(bool(v))


def _process_year(year: int, cfg: dict) -> int:
    """Repair all NULL-flag signal rows whose date falls in the given year."""
    volume_threshold = cfg.get("volume_ratio_threshold", 1.2)
    pct_52w_min      = cfg.get("pct_from_52w_high_min", 0.10)
    ml_threshold     = cfg.get("ml_score_min", 0.015)
    health_min       = cfg.get("health_score_min", 3)
    health_min_il    = cfg.get("health_score_min_il", 2)

    # Look back 10 days before the year so the 3-day rolling RSI flag is correct
    # for early-January rows.
    window_start = f"{year - 1}-12-15"
    window_end   = f"{year + 1}-01-01"

    feat = pd.read_sql(
        text(
            "SELECT date, ticker, rsi, volume_ratio, pct_from_52w_high, "
            "close, bb_high, bb_low "
            "FROM features "
            "WHERE date >= :s AND date < :e "
            "ORDER BY ticker, date"
        ),
        engine,
        params={"s": window_start, "e": window_end},
    )
    if feat.empty:
        return 0

    feat["date"] = pd.to_datetime(feat["date"])
    feat["rsi_below_30"] = feat["rsi"] < 30
    feat["rsi_3day_flag"] = (
        feat.groupby("ticker")["rsi_below_30"]
            .rolling(3, min_periods=3).sum().reset_index(level=0, drop=True)
    )
    feat["volume_flag"] = (
        feat.groupby("ticker")["volume_ratio"]
            .rolling(3, min_periods=1).max().reset_index(level=0, drop=True)
        >= volume_threshold
    )
    feat["week52_condition"] = (
        (feat["pct_from_52w_high"] <= -pct_52w_min) | feat["pct_from_52w_high"].isna()
    )
    bb_range = feat["bb_high"] - feat["bb_low"]
    feat["bb_pct_b"] = (feat["close"] - feat["bb_low"]) / bb_range.where(bb_range != 0, other=float("nan"))

    # Only keep rows whose date is IN the target year AND which currently have NULL flags.
    # Join signals to pick up health_score and prediction_score (not in features).
    sig = pd.read_sql(
        text(
            "SELECT date, ticker, health_score, prediction_score, "
            "target_mean_price, fair_value_upside "
            "FROM signals "
            "WHERE date >= :s AND date < :e "
            "AND (rsi_3day_flag IS NULL OR volume_flag IS NULL "
            "     OR week52_condition IS NULL OR health_condition IS NULL "
            "     OR ml_condition IS NULL OR vesign_score IS NULL)"
        ),
        engine,
        params={"s": f"{year}-01-01", "e": f"{year + 1}-01-01"},
    )
    if sig.empty:
        del feat
        return 0

    sig["date"] = pd.to_datetime(sig["date"])
    df = feat.merge(sig, on=["ticker", "date"], how="inner")
    del feat

    is_tase = df["ticker"].str.endswith(".TA")
    df["health_condition"] = (
        (is_tase  & ((df["health_score"] >= health_min_il) | df["health_score"].isna()))
        | (~is_tase & ((df["health_score"] >= health_min)    | df["health_score"].isna()))
    )
    df["ml_condition"] = (
        (df["prediction_score"] >= ml_threshold)
        | is_tase
        | df["prediction_score"].isna()
    )
    df["vesign_score"] = df.apply(_compute_vesign_score, axis=1)

    updates = [{
        "date":           r["date"].strftime("%Y-%m-%d %H:%M:%S.%f"),
        "ticker":         r["ticker"],
        "rsi_3day_flag":  _clean_int(r.get("rsi_3day_flag")),
        "volume_flag":    _clean_bool(r.get("volume_flag")),
        "week52_cond":    _clean_bool(r.get("week52_condition")),
        "health_cond":    _clean_bool(r.get("health_condition")),
        "ml_cond":        _clean_bool(r.get("ml_condition")),
        "vesign_score":   _clean_int(r.get("vesign_score")),
    } for _, r in df.iterrows()]

    sql = text("""
        UPDATE signals
           SET rsi_3day_flag    = :rsi_3day_flag,
               volume_flag      = :volume_flag,
               week52_condition = :week52_cond,
               health_condition = :health_cond,
               ml_condition     = :ml_cond,
               vesign_score     = :vesign_score
         WHERE ticker = :ticker AND date = :date
    """)
    CHUNK = 5000
    with engine.begin() as conn:
        for i in range(0, len(updates), CHUNK):
            conn.execute(sql, updates[i:i + CHUNK])

    return len(updates)


def repair():
    cfg = _load_config()
    total_before = pd.read_sql(
        """
        SELECT COUNT(*) AS n, MIN(DATE(date)) AS mn, MAX(DATE(date)) AS mx
        FROM signals
        WHERE rsi_3day_flag IS NULL OR volume_flag IS NULL
           OR week52_condition IS NULL OR health_condition IS NULL
           OR ml_condition IS NULL OR vesign_score IS NULL
        """,
        engine,
    ).iloc[0]
    n_before = int(total_before["n"])
    if n_before == 0:
        print("Nothing to repair.")
        return
    y_start = int(total_before["mn"][:4])
    y_end   = int(total_before["mx"][:4])
    print(f"Rows needing repair: {n_before:,}   span: {total_before['mn']} → {total_before['mx']}")

    done = 0
    for year in range(y_start, y_end + 1):
        print(f"\n[{year}] loading…", flush=True)
        n = _process_year(year, cfg)
        done += n
        print(f"[{year}] updated {n:,} rows  (cumulative {done:,}/{n_before:,})", flush=True)

    remaining = pd.read_sql(
        """
        SELECT COUNT(*) AS n FROM signals
         WHERE rsi_3day_flag IS NULL OR volume_flag IS NULL
            OR week52_condition IS NULL OR health_condition IS NULL
            OR ml_condition IS NULL OR vesign_score IS NULL
        """,
        engine,
    )["n"][0]
    print(f"\nDone. Rows still with at least one NULL flag: {int(remaining):,}")


if __name__ == "__main__":
    repair()
