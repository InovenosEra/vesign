"""Find tickers with broken logo URLs (404/timeout) and replace with logo.dev.

Runs HEAD checks in parallel, updates companies.logo_url in place.
Can be invoked one-off or added to the daily pipeline.
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from sqlalchemy import text

from data.loaders import engine

LOGO_DEV_TOKEN = "pk_X-1ZO13GSgeOoUrIuJ6GMQ"  # public demo token
UA = {"User-Agent": "Mozilla/5.0"}


def _check_logo(row) -> tuple[str, bool]:
    """Return (ticker, is_broken). Broken = 4xx/5xx or timeout."""
    ticker = row["ticker"]
    url = row["logo_url"]
    if not url:
        return (ticker, True)
    try:
        # HEAD is faster but some CDNs reject it; fall back to GET with small timeout
        r = requests.head(url, timeout=5, allow_redirects=True, headers=UA)
        if r.status_code >= 400:
            return (ticker, True)
        ct = r.headers.get("content-type", "").lower()
        # Accept any image/* or empty (some CDNs lie on HEAD)
        if ct.startswith("image/") or not ct:
            return (ticker, False)
        # Not an image → broken
        return (ticker, True)
    except Exception:
        return (ticker, True)


def repair_logos():
    companies = pd.read_sql("SELECT ticker, logo_url FROM companies", engine)
    print(f"Checking {len(companies)} logos…")

    broken = []
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = [ex.submit(_check_logo, row) for _, row in companies.iterrows()]
        for i, fut in enumerate(as_completed(futures), 1):
            ticker, is_broken = fut.result()
            if is_broken:
                broken.append(ticker)
            if i % 200 == 0:
                print(f"  {i}/{len(companies)} checked, {len(broken)} broken so far")

    print(f"\nFound {len(broken)} broken logos")
    if not broken:
        return

    # Replace with logo.dev ticker-based URL
    new_url = "https://img.logo.dev/ticker/{}?token=" + LOGO_DEV_TOKEN
    with engine.begin() as conn:
        for ticker in broken:
            conn.execute(text(
                "UPDATE companies SET logo_url = :url WHERE ticker = :t"
            ), {"url": new_url.format(ticker), "t": ticker})
    print(f"Updated {len(broken)} rows to logo.dev fallback")

    # Quick sample verification
    print("\nVerifying 5 random updates:")
    import random
    for t in random.sample(broken, min(5, len(broken))):
        try:
            r = requests.get(new_url.format(t), timeout=5, allow_redirects=True, headers=UA)
            ct = r.headers.get("content-type", "")
            print(f"  {t:6s}: HTTP {r.status_code}, {ct[:20]:<20}, {len(r.content)}B")
        except Exception as e:
            print(f"  {t}: ERR {e}")


if __name__ == "__main__":
    repair_logos()
