import pandas as pd
from database.db_connection import engine


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

        total_score = sector_df["score"].sum()

        sector_df["allocation_pct"] = (
            (sector_df["score"] / total_score) * sector_weight
        )

        allocations.append(sector_df)

    portfolio = pd.concat(allocations)

    portfolio.to_sql("daily_portfolio", engine, if_exists="replace", index=False)

    print("Sector-balanced allocation completed")
