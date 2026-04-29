"""Bulk-download company logos to static/logos/{TICKER}.png and update the DB.

Usage:
  venv/bin/python -m production.download_logos                # all tickers
  venv/bin/python -m production.download_logos --missing-only # only those without a file

Replaces production/repair_broken_logos.py.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.loaders import engine
from data.logo_sources import resolve

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGO_DIR = os.path.join(_APP_ROOT, "static", "logos")


def _save_atomic(path: str, data: bytes) -> None:
    """Write data to path atomically (temp file + rename)."""
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _logo_path(ticker: str) -> str:
    return os.path.join(LOGO_DIR, f"{ticker}.png")


def download_one(ticker: str, website: Optional[str]) -> tuple[str, Optional[str]]:
    """Resolve and save a logo for one ticker. Return (ticker, source_used or None)."""
    data, src = resolve(ticker, website)
    if data is None:
        return ticker, None
    _save_atomic(_logo_path(ticker), data)
    return ticker, src


def download_all(missing_only: bool = False, max_workers: int = 20) -> dict:
    """Download logos for every ticker (or only those without an on-disk file).

    Returns {'downloaded': N, 'failed': M, 'failed_tickers': [...], 'sources': {src: count}}.
    Also updates companies.logo_url in the DB to '/logos/{T}.png' on success, NULL on failure.
    """
    os.makedirs(LOGO_DIR, exist_ok=True)
    df = pd.read_sql("SELECT ticker, website FROM companies", engine)

    if missing_only:
        df = df[~df["ticker"].apply(lambda t: os.path.exists(_logo_path(t)))]
        print(f"Missing-only mode: {len(df)} tickers without an on-disk logo")
    else:
        print(f"Full mode: {len(df)} tickers")

    if df.empty:
        return {"downloaded": 0, "failed": 0, "failed_tickers": [], "sources": {}}

    succeeded: list[str] = []
    failed: list[str] = []
    sources: dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(download_one, row["ticker"], row.get("website")): row["ticker"]
            for _, row in df.iterrows()
        }
        for i, fut in enumerate(as_completed(futures), 1):
            ticker = futures[fut]
            try:
                _, src = fut.result()
            except Exception as exc:
                print(f"  [{ticker}] error: {exc}")
                failed.append(ticker)
                continue
            if src is None:
                failed.append(ticker)
            else:
                succeeded.append(ticker)
                sources[src] = sources.get(src, 0) + 1
            if i % 200 == 0:
                print(f"  {i}/{len(df)} processed, {len(succeeded)} ok, {len(failed)} failed")

    # ── Update DB rows ───────────────────────────────────────────────────────
    with engine.begin() as conn:
        for t in succeeded:
            conn.execute(
                text("UPDATE companies SET logo_url = :u WHERE ticker = :t"),
                {"u": f"/logos/{t}.png", "t": t},
            )
        for t in failed:
            conn.execute(
                text("UPDATE companies SET logo_url = NULL WHERE ticker = :t"),
                {"t": t},
            )

    print(f"\nDone: {len(succeeded)} downloaded, {len(failed)} failed")
    print(f"Sources: {sources}")
    if failed:
        print(f"Failed tickers (first 20): {failed[:20]}")

    return {
        "downloaded": len(succeeded),
        "failed": len(failed),
        "failed_tickers": failed,
        "sources": sources,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--missing-only", action="store_true",
                    help="Only download tickers whose PNG file does not exist on disk")
    ap.add_argument("--workers", type=int, default=20)
    args = ap.parse_args()
    download_all(missing_only=args.missing_only, max_workers=args.workers)


if __name__ == "__main__":
    main()
