"""
IL-only expansion script — safe to run on the server.
- Fetches TA-125 + SME60 tickers from Wikipedia
- INSERT OR IGNORE into companies (never touches existing rows)
- Backfills 3 years of price data ONLY for new IL tickers
- Re-scores health ONLY for all TASE tickers
Run: venv/bin/python scripts/add_il_tickers.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from data.loaders import engine
from utils.universe_loader import _fetch_ta_index

UTC = timezone.utc

# ── Step 1: fetch new IL tickers ─────────────────────────────────────────────
new_frames = []
for name, url in [
    ("TA-125",  "https://en.wikipedia.org/wiki/TA-125_Index"),
    ("TA-SME60","https://en.wikipedia.org/wiki/TA-SME60"),
]:
    try:
        df = _fetch_ta_index(url)
        print(f"Fetched {len(df)} tickers from {name}")
        new_frames.append(df)
    except Exception as e:
        print(f"WARNING: Could not fetch {name}: {e}")

if not new_frames:
    print("No new tickers fetched — nothing to do.")
    sys.exit(0)

new_il = pd.concat(new_frames, ignore_index=True).drop_duplicates(subset=["ticker"])
print(f"Total new IL candidates: {len(new_il)}")

# ── Step 2: INSERT OR IGNORE new tickers only ────────────────────────────────
with engine.connect() as conn:
    existing = set(pd.read_sql("SELECT ticker FROM companies WHERE market='IL'", conn)["ticker"])

added = []
with engine.begin() as conn:
    for _, row in new_il.iterrows():
        if row["ticker"] in existing:
            continue
        conn.execute(text("""
            INSERT OR IGNORE INTO companies (ticker, company, sector, market, logo_url)
            VALUES (:ticker, :company, :sector, :market, :logo_url)
        """), {
            "ticker":   row["ticker"],
            "company":  row.get("company", row["ticker"]),
            "sector":   row.get("sector", ""),
            "market":   "IL",
            "logo_url": row.get("logo_url", ""),
        })
        added.append(row["ticker"])

print(f"Added {len(added)} new IL tickers: {added[:20]}{'...' if len(added)>20 else ''}")

if added:
    # ── Step 3: backfill prices for new tickers only ─────────────────────────
    print(f"\nBackfilling 3 years of prices for {len(added)} new tickers…")
    from data.market_data import _download_and_save
    today      = datetime.now(UTC).date()
    start_date = today - timedelta(days=365 * 3)
    _download_and_save(added, start_date, today)
    print("Price backfill complete.")
else:
    print("No new tickers — skipping price backfill.")

# ── Step 4: re-score health for ALL TASE tickers ────────────────────────────
# Delete existing TASE health scores so update_company_health() treats them all as pending.
# US scores are untouched (they're fresh / < 7 days old and won't be re-queued).
print("\nClearing existing TASE health scores to force rescore…")
with engine.begin() as conn:
    result = conn.execute(text("DELETE FROM company_health WHERE ticker LIKE '%.TA'"))
    print(f"Cleared {result.rowcount} TASE health scores.")

print("Running health scoring (TASE tickers will be picked up as pending)…")
from data.market_data import update_company_health
update_company_health()
print("Health scoring complete.")

print("\nAll done. Run the signals engine next if needed.")
