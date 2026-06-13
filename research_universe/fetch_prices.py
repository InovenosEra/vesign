"""STEP 2 — Pull daily price history for every symbol in the universe master.

Per symbol, TWO /stable calls (merged on date):
  • historical-price-eod/full            → raw open/high/low/close/volume
  • historical-price-eod/dividend-adjusted → adjClose (split+dividend adjusted)
(The single-call adjClose the brief assumed isn't available on /stable's `full`
endpoint — it returns change/vwap, not adjClose — so we merge the two.)

from=2019-01-01. Delisted symbols are TRUNCATED to date <= delistedDate (guards
against symbol-reuse contamination). RESUMABLE: one parquet per symbol under
prices/; existing files are skipped, so an interrupted run just re-runs. A
failed symbol is retried once, then logged to failures.csv and skipped (no
file written → a later re-run retries it).

Writes ONLY under research_universe/. Touches nothing in the live system.
"""
import os
import sys
import csv
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.fmp import _get  # noqa: E402  (pure HTTP client; key + 429 backoff)

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV = os.path.join(OUT_DIR, "universe_master.csv")
PRICE_DIR = os.path.join(OUT_DIR, "prices")
FAIL_CSV = os.path.join(OUT_DIR, "failures.csv")
START = "2019-01-01"
TODAY = pd.Timestamp.today().strftime("%Y-%m-%d")

WORKERS = 8            # well under FMP Premium's ~750/min with 2 calls/symbol
CALL_GAP = 0.04        # small per-call spacing; _get also backs off on 429

os.makedirs(PRICE_DIR, exist_ok=True)
_print_lock = threading.Lock()
_fail_lock = threading.Lock()


def _safe(sym: str) -> str:
    return sym.replace("/", "_").replace("\\", "_")


def _fetch_full(sym: str):
    time.sleep(CALL_GAP)
    return _get("historical-price-eod/full", {"symbol": sym, "from": START, "to": TODAY})


def _fetch_adj(sym: str):
    time.sleep(CALL_GAP)
    return _get("historical-price-eod/dividend-adjusted", {"symbol": sym, "from": START, "to": TODAY})


def process(row: dict) -> tuple[str, str, int, bool]:
    """Returns (symbol, outcome, n_rows, truncated). outcome ∈ ok|empty|error."""
    sym = row["symbol"]
    path = os.path.join(PRICE_DIR, _safe(sym) + ".parquet")
    if os.path.exists(path):
        return sym, "skip", -1, False

    delisted_date = (row.get("delistedDate") or "").strip()

    def attempt():
        full = _fetch_full(sym)
        if not isinstance(full, list) or not full:
            return None
        df = pd.DataFrame(full)
        keep = [c for c in ("date", "open", "high", "low", "close", "volume") if c in df.columns]
        df = df[keep].copy()
        # adjClose from the dividend-adjusted series (best-effort; OHLCV stands alone)
        adj = _fetch_adj(sym)
        if isinstance(adj, list) and adj:
            adf = pd.DataFrame(adj)[["date", "adjClose"]].rename(columns={"adjClose": "adjclose"})
            df = df.merge(adf, on="date", how="left")
        else:
            df["adjclose"] = pd.NA
        return df

    df = None
    err = None
    for _try in range(2):  # initial + one retry
        try:
            df = attempt()
            if df is not None:
                break
        except Exception as e:  # noqa: BLE001
            err = str(e)[:120]
        time.sleep(0.5)

    if df is None or df.empty:
        with _fail_lock:
            with open(FAIL_CSV, "a", newline="") as f:
                csv.writer(f).writerow([sym, row.get("status"), err or "empty"])
        return sym, "error" if err else "empty", 0, False

    df.insert(0, "symbol", sym)
    truncated = False
    if delisted_date:
        before = len(df)
        df = df[df["date"] <= delisted_date]
        truncated = len(df) < before
    df = df[["symbol", "date", "open", "high", "low", "close", "adjclose", "volume"]]
    df = df.sort_values("date").reset_index(drop=True)
    df.to_parquet(path, index=False)
    return sym, "ok", len(df), truncated


def main():
    with open(MASTER_CSV) as f:
        master = list(csv.DictReader(f))
    total = len(master)
    if not os.path.exists(FAIL_CSV):
        with open(FAIL_CSV, "w", newline="") as f:
            csv.writer(f).writerow(["symbol", "status", "reason"])

    done = ok = empty = error = skip = trunc = 0
    rows_total = 0
    print(f"STEP 2 — fetching prices for {total} symbols (from {START}; resumable)")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process, r): r["symbol"] for r in master}
        for fut in as_completed(futs):
            sym, outcome, n, truncated = fut.result()
            done += 1
            if outcome == "ok":
                ok += 1; rows_total += n
                if truncated:
                    trunc += 1
            elif outcome == "skip":
                skip += 1
            elif outcome == "empty":
                empty += 1
            else:
                error += 1
            if done % 250 == 0 or done == total:
                el = time.time() - t0
                with _print_lock:
                    print(f"  {done}/{total} | ok={ok} skip={skip} empty={empty} "
                          f"err={error} trunc={trunc} | rows={rows_total:,} | {el:.0f}s")

    print(f"\nDONE. ok={ok} skipped={skip} empty={empty} error={error} "
          f"truncated={trunc} | new rows={rows_total:,}")
    print(f"failures logged to {FAIL_CSV}")


if __name__ == "__main__":
    main()
