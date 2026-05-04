import os
import yaml
import pandas as pd
from data.loaders import engine
from sqlalchemy import inspect


def _get_vix_multiplier(config):
    """Return a position-size multiplier based on the latest VIX close."""
    vix_cfg = config.get("vix", {})

    try:
        inspector = inspect(engine)
        if "vix" not in inspector.get_table_names():
            return 1.0
        row = pd.read_sql("SELECT close FROM vix ORDER BY date DESC LIMIT 1", engine)
        if row.empty:
            return 1.0
        vix = float(row["close"].iloc[0])
    except Exception:
        return 1.0

    low_t     = vix_cfg.get("low_threshold", 15)
    high_t    = vix_cfg.get("high_threshold", 25)
    extreme_t = vix_cfg.get("extreme_threshold", 35)

    if vix > extreme_t:
        return vix_cfg.get("extreme_multiplier", 2.0)
    elif vix > high_t:
        return vix_cfg.get("high_multiplier", 1.5)
    elif vix >= low_t:
        return 1.0
    else:
        return vix_cfg.get("low_multiplier", 0.8)


def run_allocator():

    print("Running portfolio allocator...")

    # ---------- Load config ----------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "config", "settings.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    vix_multiplier = _get_vix_multiplier(config)
    position_cap = config.get("vix", {}).get("position_cap", 0.15)
    print(f"VIX multiplier: {vix_multiplier:.2f}, position cap: {position_cap:.0%}")

    ranked = pd.read_sql("SELECT * FROM daily_ranked", engine)
    companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)

    buys = ranked[ranked["signal"] == "BUY"].merge(companies, on="ticker")

    if buys.empty:
        print("No BUY signals today")
        return

    # Bucket NaN/empty sectors as "Unknown" so the per-sector loop doesn't end
    # up with len(sector_df)==0 → ZeroDivisionError. Without this, any BUY
    # whose company row has a NULL sector (typical for newly-added tickers
    # before company_profile populates) crashes the entire allocator.
    buys["sector"] = (
        buys["sector"]
            .where(buys["sector"].notna() & (buys["sector"].astype(str).str.strip() != ""), "Unknown")
    )

    # Equal capital per sector
    sectors = buys["sector"].unique()
    sector_weight = 1 / len(sectors)

    allocations = []

    for sector in sectors:
        sector_df = buys[buys["sector"] == sector].copy()

        if "prediction_score" in sector_df.columns:
            sector_df["alloc_score"] = (
                sector_df["prediction_score"]
                .fillna(sector_df["score"])
                .clip(lower=0)
            )
        else:
            sector_df["alloc_score"] = sector_df["score"]

        total_score = sector_df["alloc_score"].sum()

        if total_score == 0:
            sector_df["allocation_pct"] = sector_weight / len(sector_df)
        else:
            sector_df["allocation_pct"] = (
                (sector_df["alloc_score"] / total_score) * sector_weight
            )

        # Apply VIX multiplier then cap per position
        sector_df["allocation_pct"] = (
            sector_df["allocation_pct"] * vix_multiplier
        ).clip(upper=position_cap)

        allocations.append(sector_df)

    portfolio = pd.concat(allocations)

    portfolio.to_sql("daily_portfolio", engine, if_exists="replace", index=False)

    print("Sector-balanced allocation completed")
