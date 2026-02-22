import pandas as pd
from data.loaders import engine


def run_allocator():

    print("Running portfolio allocator...")

    ranked = pd.read_sql("SELECT * FROM daily_ranked", engine)
    companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)

    buys = ranked[ranked["signal"] == "BUY"].merge(companies, on="ticker")

    if buys.empty:
        print("No BUY signals today")
        return

    # Equal capital per sector
    sectors = buys["sector"].unique()
    sector_weight = 1 / len(sectors)

    allocations = []

    for sector in sectors:
        sector_df = buys[buys["sector"] == sector].copy()

        # Use ML prediction_score for within-sector position sizing.
        # Clip at 0 so weights stay positive (negative predictions → 0 weight).
        # Fall back to RSI-based score if predictions are unavailable.
        # If every stock in the sector clips to 0, allocate equally.
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

        allocations.append(sector_df)

    portfolio = pd.concat(allocations)

    portfolio.to_sql("daily_portfolio", engine, if_exists="replace", index=False)

    print("Sector-balanced allocation completed")
