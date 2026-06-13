"""STEP 1 — Build a survivorship-bias-free Nasdaq common-stock ticker master.

Active  : /stable/company-screener?exchange=NASDAQ&isActivelyTrading=true,
          common stock only (isEtf=False AND isFund=False).
Delisted: ALL pages of /stable/delisted-companies (sort is NOT monotonic — never
          stop early), keep NASDAQ-variant rows with delistedDate >= 2020-01-01,
          drop non-common-stock by symbol pattern (warrants W/WW, units U,
          rights R, foreign tickers containing a dot).

Writes ONLY research_universe/universe_master.csv. Touches nothing in the live
system. Reuses data.fmp._get for the API key + retry/backoff (no code changes).
"""
import os
import sys
import csv
import time

# Reuse the project's FMP key + retry helper WITHOUT importing anything that
# writes to the DB. data.fmp is a pure HTTP client (loads .env on import).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.fmp import _get  # noqa: E402

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV = os.path.join(OUT_DIR, "universe_master.csv")

DELISTED_FLOOR = "2020-01-01"   # universe membership: traded at any point since 2020


def is_nasdaq(exch: str) -> bool:
    return "NASDAQ" in (exch or "").upper()


def is_common_stock_symbol(sym: str) -> tuple[bool, str]:
    """Heuristic exclusion of non-common-stock delisted symbols by pattern.
    Returns (keep, drop_reason). Applied to DELISTED rows only (the screener
    carries proper isEtf/isFund flags for active names)."""
    if "." in sym:
        return False, "foreign(dot)"
    if sym.endswith("WW") or sym.endswith("W"):
        return False, "warrant"
    if sym.endswith("U"):
        return False, "unit"
    if sym.endswith("R"):
        return False, "right"
    return True, ""


def fetch_active() -> list[dict]:
    """Active NASDAQ common stocks via the screener. The probe showed
    isActivelyTrading=true returns ~8.7k (< the 10k cap), complete in one call;
    we still guard against a full cap by widening if needed."""
    rows = _get("company-screener", {
        "exchange": "NASDAQ", "isActivelyTrading": "true", "limit": 10000,
    }) or []
    if len(rows) >= 10000:
        print("  WARNING: screener hit the 10k cap — active list may be truncated.")
    out = []
    for r in rows:
        if not is_nasdaq(r.get("exchange") or r.get("exchangeShortName")):
            continue
        if r.get("isEtf") or r.get("isFund"):
            continue
        out.append({
            "symbol": r["symbol"],
            "status": "active",
            "ipoDate": "",                       # not provided by screener
            "delistedDate": "",
        })
    return out


def fetch_delisted() -> tuple[list[dict], dict]:
    """Paginate the ENTIRE delisted dataset (sort not monotonic → no early stop).
    Returns (kept_rows, drop_stats)."""
    kept = []
    drops = {"not_nasdaq": 0, "before_2020": 0, "foreign(dot)": 0,
             "warrant": 0, "unit": 0, "right": 0}
    page = 0
    empty_streak = 0
    total_seen = 0
    while True:
        body = _get("delisted-companies", {"page": page})
        if not isinstance(body, list) or not body:
            empty_streak += 1
            # One empty page = end (dataset ends ~page 94). Double-check once.
            if empty_streak >= 2:
                break
            page += 1
            continue
        empty_streak = 0
        total_seen += len(body)
        for r in body:
            sym = r.get("symbol") or ""
            if not is_nasdaq(r.get("exchange")):
                drops["not_nasdaq"] += 1
                continue
            dd = r.get("delistedDate") or ""
            if dd < DELISTED_FLOOR:
                drops["before_2020"] += 1
                continue
            keep, reason = is_common_stock_symbol(sym)
            if not keep:
                drops[reason] += 1
                continue
            kept.append({
                "symbol": sym,
                "status": "delisted",
                "ipoDate": r.get("ipoDate") or "",
                "delistedDate": dd,
            })
        page += 1
        if page > 200:  # hard safety stop (dataset is ~95 pages)
            print("  WARNING: pagination exceeded 200 pages — stopping.")
            break
        time.sleep(0.05)
    drops["_pages"] = page
    drops["_rows_seen"] = total_seen
    return kept, drops


def main():
    print("STEP 1 — building Nasdaq common-stock universe master")
    print("Fetching ACTIVE (screener)…")
    active = fetch_active()
    print(f"  active common stocks: {len(active)}")

    print("Fetching DELISTED (full pagination)…")
    delisted, drops = fetch_delisted()
    print(f"  delisted pages walked: {drops['_pages']}  rows seen: {drops['_rows_seen']}")
    print(f"  delisted NASDAQ common kept (>= {DELISTED_FLOOR}): {len(delisted)}")
    print(f"  drop breakdown: " + ", ".join(
        f"{k}={v}" for k, v in drops.items() if not k.startswith("_")))

    # Union + dedupe. If a symbol appears in both (reused ticker), prefer 'active'.
    by_sym: dict[str, dict] = {}
    for r in delisted:
        by_sym[r["symbol"]] = r
    for r in active:
        by_sym[r["symbol"]] = r          # active overrides a same-symbol delisted row
    master = sorted(by_sym.values(), key=lambda r: r["symbol"])

    with open(MASTER_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "status", "ipoDate", "delistedDate"])
        w.writeheader()
        w.writerows(master)

    n_active = sum(1 for r in master if r["status"] == "active")
    n_delisted = sum(1 for r in master if r["status"] == "delisted")
    print(f"\nMASTER written: {MASTER_CSV}")
    print(f"  total unique: {len(master)}  (active={n_active}, delisted={n_delisted})")


if __name__ == "__main__":
    main()
