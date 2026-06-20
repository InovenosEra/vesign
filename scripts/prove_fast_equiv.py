"""Prove fast_signal_pass() == the real run_scoring per-date path, on a
clean-start window (2018: empty open-positions at the very first date).

Operates on whatever VESIGN_DB points at (MUST be a disposable copy):
  1. fast_signal_pass() over the whole DB (minutes) -> snapshot 2018 labels
  2. run_scoring(target_date=d, open_positions) for each 2018 date, threading
     open_positions from EMPTY (the real/full path) -> snapshot 2018 labels
  3. diff signal/tier/lot_seq for every 2018 (ticker,date)

Exit 0 + "EQUIVALENT" if identical; non-zero + mismatch sample otherwise.

Run:  VESIGN_DB=/tmp/equiv.db venv/bin/python scripts/prove_fast_equiv.py
"""
import os, sys, time
import pandas as pd

assert os.environ.get("VESIGN_DB"), "Set VESIGN_DB to a disposable DB copy."

from data.loaders import engine
from signals.engine import run_scoring
from production.backfill_trailing_stop_dca import _next_open_positions
from production.fast_rebuild_tiers import fast_signal_pass

# Clean-start window is always 2018 (empty positions at the first date).
# Compare span = 2018 .. END_YEAR; the full path runs continuously across it so
# that later years (e.g. 2022) carry correct open-position state from prior years.
END_YEAR = int(os.environ.get("PROVE_END_YEAR", "2018"))
START_YEAR = 2018


def _snapshot():
    return pd.read_sql(
        f"SELECT ticker, DATE(date) AS d, signal, tier, lot_seq FROM signals "
        f"WHERE CAST(strftime('%Y', date) AS INT) BETWEEN {START_YEAR} AND {END_YEAR} "
        f"ORDER BY ticker, d", engine)


# ---- 1. FAST path ---------------------------------------------------------
print("FAST pass over whole DB...", flush=True)
t0 = time.time()
fast_signal_pass()
fast = _snapshot()
print(f"  fast done in {time.time()-t0:.0f}s; {len(fast):,} rows in {START_YEAR}-{END_YEAR}", flush=True)

# ---- 2. FULL path (real run_scoring, empty start at 2018, continuous) ------
dates = pd.read_sql(
    f"SELECT DISTINCT DATE(date) AS d FROM features "
    f"WHERE CAST(strftime('%Y', date) AS INT) BETWEEN {START_YEAR} AND {END_YEAR} "
    f"ORDER BY d", engine)["d"].tolist()
print(f"FULL path: {len(dates)} dates {START_YEAR}-{END_YEAR} via run_scoring (slow)...", flush=True)
op = {}
t0 = time.time()
for i, d in enumerate(dates, 1):
    run_scoring(target_date=d, open_positions=op)
    op = _next_open_positions(op, d)
    if i % 25 == 0:
        el = time.time() - t0
        print(f"  [{i}/{len(dates)}] {d} elapsed={el/60:.1f}m "
              f"eta={el/i*(len(dates)-i)/60:.1f}m open={len(op)}", flush=True)
full = _snapshot()
print(f"  full done in {(time.time()-t0)/60:.1f}m; {len(full):,} rows in {START_YEAR}-{END_YEAR}", flush=True)

# ---- 3. DIFF --------------------------------------------------------------
m = full.merge(fast, on=["ticker", "d"], how="outer",
               suffixes=("_full", "_fast"), indicator=True)
only = m[m["_merge"] != "both"]
# normalize NaN tier/lot_seq for HOLD/SELL so NaN==NaN compares equal
for c in ["tier", "lot_seq"]:
    for s in ["_full", "_fast"]:
        m[c+s] = m[c+s].astype("Int64")
mism = m[(m["_merge"] == "both") & (
    (m.signal_full != m.signal_fast) |
    (m.tier_full != m.tier_fast) |
    (m.lot_seq_full != m.lot_seq_fast))]

print("\n================ EQUIVALENCE RESULT ================", flush=True)
print(f"{START_YEAR}-{END_YEAR} rows: full={len(full):,} fast={len(fast):,}")
print(f"row-set mismatches (present in one only): {len(only)}")
print(f"label mismatches (signal/tier/lot_seq): {len(mism)}")
# per-year mismatch breakdown so volatile years (e.g. 2022) are isolated
if len(mism):
    mism = mism.copy(); mism["yr"] = mism["d"].str[:4]
    print("  mismatches by year:", mism["yr"].value_counts().sort_index().to_dict())
both = m[m["_merge"] == "both"].copy(); both["yr"] = both["d"].str[:4]
print("  rows compared by year:", both["yr"].value_counts().sort_index().to_dict())
buys_full = (full.signal == "BUY").sum()
buys_fast = (fast.signal == "BUY").sum()
print(f"BUY count: full={buys_full} fast={buys_fast}")
print(f"tier dist full: {full[full.signal=='BUY'].tier.value_counts().to_dict()}")
print(f"tier dist fast: {fast[fast.signal=='BUY'].tier.value_counts().to_dict()}")

if len(only) == 0 and len(mism) == 0:
    print(f"\nEQUIVALENT ✅  fast == full on {START_YEAR}-{END_YEAR}")
    sys.exit(0)
else:
    print("\nNOT EQUIVALENT ❌  sample mismatches:")
    print(mism.head(20).to_string())
    print(only.head(10).to_string())
    sys.exit(1)
