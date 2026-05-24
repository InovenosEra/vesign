#!/usr/bin/env python
"""One-time backfill of TTM fundamentals for all US tickers.

Pulls ratios_ttm / key_metrics_ttm / financial_growth / income_statement from
FMP for each US company and UPSERTs the 9 fundamentals fields. Resumable:
re-running skips tickers that already have pe_ttm populated (use --force to
refetch). FMP's client already backs off on 429s.

    venv/bin/python -m production.backfill_fundamentals            # resume
    venv/bin/python -m production.backfill_fundamentals --force    # refetch all
    venv/bin/python -m production.backfill_fundamentals --limit 20 # smoke test
"""
import argparse
import sys
import time
from datetime import date

from sqlalchemy import text

from data import fundamentals
from data.loaders import engine


def _us_tickers(skip_done: bool):
    """US tickers from companies (authoritative universe), excluding ETFs.
    When skip_done, drop tickers that already have pe_ttm populated."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ticker FROM companies "
            "WHERE market = 'US' AND (sector IS NULL OR sector != 'ETF') "
            "ORDER BY ticker"
        )).fetchall()
        tickers = [r[0] for r in rows]
        if skip_done:
            done = {r[0] for r in conn.execute(text(
                "SELECT ticker FROM fundamentals WHERE pe_ttm IS NOT NULL"
            )).fetchall()}
            tickers = [t for t in tickers if t not in done]
    return tickers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="refetch even if already populated")
    ap.add_argument("--limit", type=int, default=None, help="cap number of tickers (testing)")
    args = ap.parse_args()

    tickers = _us_tickers(skip_done=not args.force)
    if args.limit:
        tickers = tickers[: args.limit]
    total = len(tickers)
    today = date.today().isoformat()
    print(f"Backfilling fundamentals for {total} tickers "
          f"({'forced' if args.force else 'resume — skipping populated'})", flush=True)

    populated = empty = errors = 0
    t0 = time.time()
    try:
        for i, tk in enumerate(tickers, 1):
            try:
                fund = fundamentals.fetch_fundamentals(tk)
                fundamentals.store_fundamentals(tk, fund, engine, updated=today)
                if fund.get("pe_ttm") is not None or fund.get("net_margin") is not None:
                    populated += 1
                else:
                    empty += 1
            except Exception as e:  # noqa: BLE001 — keep going, report at end
                errors += 1
                print(f"  ! {tk}: {e}", flush=True)
            if i % 50 == 0 or i == total:
                rate = i / max(1e-9, time.time() - t0)
                print(f"  {i}/{total}  populated={populated} empty={empty} "
                      f"err={errors}  ({rate:.1f}/s)", flush=True)
    except KeyboardInterrupt:
        print("\nInterrupted — progress saved; re-run to resume.", flush=True)
        sys.exit(130)

    print(f"Done. populated={populated} empty={empty} errors={errors} "
          f"in {time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
