# Self-Hosted Logos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace FMP's broken logo CDN with self-hosted PNGs served from `/logos/{TICKER}.png`, downloaded once via a 4-source fallback chain.

**Architecture:** Files live at `static/logos/{TICKER}.png` (gitignored). FastAPI mounts the directory at `/logos`. `companies.logo_url` becomes a relative path. A bulk downloader tries Parqet → logo.dev → Google favicons → manual overrides until each ticker has a valid PNG ≥ 1 KB. The daily pipeline re-runs the downloader in `--missing-only` mode for new tickers.

**Tech Stack:** Python 3, FastAPI, SQLAlchemy, requests, ThreadPoolExecutor, SQLite. The repo has no pytest setup — verification uses standalone smoke scripts and curl checks against the running server.

**Spec:** `docs/superpowers/specs/2026-04-29-self-hosted-logos-design.md`

---

## File Map

- **Create:** `static/logos/.gitkeep` — keeps the (otherwise gitignored) directory in version control.
- **Create:** `data/logo_sources.py` — fallback-chain resolver, pure HTTP, no DB writes, no disk writes.
- **Create:** `production/download_logos.py` — bulk downloader CLI; uses `data/logo_sources.py`.
- **Modify:** `.gitignore` — add `static/logos/*` and `!static/logos/.gitkeep`.
- **Modify:** `backend/main.py` — mount `/logos` static directory.
- **Modify:** `frontend/vite.config.js` — proxy `/logos` to `:8000` for dev.
- **Modify:** `utils/logo_overrides.py` — keep `LOGO_OVERRIDES` dict, drop `apply_logo_overrides()`.
- **Modify:** `utils/universe_loader.py:196` — drop `apply_logo_overrides(engine)` call.
- **Modify:** `data/market_data.py:483-487` — drop the inline `LOGO_OVERRIDES` re-application loop.
- **Modify:** `production/run_daily.py` — add `_download_missing_logos()` repair pass.
- **Delete:** `production/repair_broken_logos.py` — replaced by `download_logos.py`.

---

## Task 1: Storage scaffolding

**Files:**
- Create: `static/logos/.gitkeep`
- Modify: `.gitignore`

- [ ] **Step 1: Create the directory and keep file**

```bash
mkdir -p static/logos
touch static/logos/.gitkeep
```

- [ ] **Step 2: Append rules to `.gitignore`**

Append these three lines to the bottom of `.gitignore` (creates the file if missing):

```
# Self-hosted logos — actual PNGs are downloaded at deploy time, not committed
static/logos/*
!static/logos/.gitkeep
```

- [ ] **Step 3: Verify gitignore behavior**

Run:
```bash
touch static/logos/AAPL.png
git status --short static/logos/
rm static/logos/AAPL.png
```

Expected: empty output (the `.gitkeep` is already tracked, the test PNG is ignored). If `AAPL.png` shows up, the gitignore lines are wrong.

- [ ] **Step 4: Commit**

```bash
git add .gitignore static/logos/.gitkeep
git commit -m "feat: scaffold static/logos/ for self-hosted company logos"
```

---

## Task 2: Logo source resolver

**Files:**
- Create: `data/logo_sources.py`

- [ ] **Step 1: Write the module**

Create `data/logo_sources.py` with the full content below:

```python
"""Resolve a usable PNG for a given ticker by trying multiple sources in order.

Pure HTTP — no filesystem writes, no DB writes. Caller is responsible for
persistence. Each source returns either bytes (the image) or None.
"""
from __future__ import annotations

from typing import Callable, Optional
from urllib.parse import urlparse

import requests

from utils.logo_overrides import LOGO_OVERRIDES

UA = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 8
MIN_BYTES = 1024
LOGO_DEV_TOKEN = "pk_X-1ZO13GSgeOoUrIuJ6GMQ"


def _fetch(url: str) -> Optional[bytes]:
    """GET url, return bytes if response is a valid image >= MIN_BYTES, else None."""
    try:
        r = requests.get(url, timeout=TIMEOUT, headers=UA, allow_redirects=True)
    except Exception:
        return None
    if r.status_code != 200:
        return None
    ct = r.headers.get("content-type", "").lower()
    if not ct.startswith("image/"):
        return None
    if len(r.content) < MIN_BYTES:
        return None
    return r.content


def _domain_of(website: Optional[str]) -> Optional[str]:
    if not website:
        return None
    if "://" not in website:
        website = "https://" + website
    host = urlparse(website).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


# ── Source functions ────────────────────────────────────────────────────────

def from_override(ticker: str, _website: Optional[str]) -> Optional[bytes]:
    url = LOGO_OVERRIDES.get(ticker)
    return _fetch(url) if url else None


def from_parqet(ticker: str, _website: Optional[str]) -> Optional[bytes]:
    return _fetch(f"https://assets.parqet.com/logos/symbol/{ticker}?format=png")


def from_logo_dev(ticker: str, _website: Optional[str]) -> Optional[bytes]:
    return _fetch(f"https://img.logo.dev/ticker/{ticker}?token={LOGO_DEV_TOKEN}")


def from_google_favicon(_ticker: str, website: Optional[str]) -> Optional[bytes]:
    domain = _domain_of(website)
    if not domain:
        return None
    return _fetch(f"https://www.google.com/s2/favicons?domain={domain}&sz=128")


SOURCES: list[Callable[[str, Optional[str]], Optional[bytes]]] = [
    from_override,
    from_parqet,
    from_logo_dev,
    from_google_favicon,
]


def resolve(ticker: str, website: Optional[str] = None) -> tuple[Optional[bytes], Optional[str]]:
    """Try each source in order. Return (bytes, source_name) on first hit, else (None, None)."""
    for src in SOURCES:
        data = src(ticker, website)
        if data is not None:
            return data, src.__name__
    return None, None
```

- [ ] **Step 2: Smoke-test the resolver against live sources**

Run in a python REPL or `venv/bin/python -c`:

```bash
venv/bin/python -c "
from data.logo_sources import resolve
for tk, web in [('AAPL', 'https://www.apple.com'),
                ('MSFT', 'https://www.microsoft.com'),
                ('NOT-A-REAL-TICKER-XYZ', 'https://example.invalid'),
                ('PENG', None)]:  # PENG is in LOGO_OVERRIDES
    data, src = resolve(tk, web)
    print(f'{tk:25s} {src or \"NONE\":>20s}  bytes={len(data) if data else 0}')
"
```

Expected output (real numbers will vary):
- `AAPL` → `from_parqet` (or `from_override` if added later)
- `MSFT` → `from_parqet`
- `NOT-A-REAL-TICKER-XYZ` → `from_google_favicon` (Google's default globe icon, still passes size check) — or `NONE` if Google rejects the invalid domain
- `PENG` → `from_override`

If any popular ticker returns `NONE`, the source order is wrong or thresholds are too strict — debug before continuing.

- [ ] **Step 3: Commit**

```bash
git add data/logo_sources.py
git commit -m "feat: data/logo_sources — fallback chain for resolving ticker logos"
```

---

## Task 3: Bulk downloader

**Files:**
- Create: `production/download_logos.py`

- [ ] **Step 1: Write the module**

Create `production/download_logos.py` with this content:

```python
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
```

- [ ] **Step 2: Smoke-test on 5 tickers (without DB writes)**

Run:
```bash
venv/bin/python -c "
from production.download_logos import download_one, _logo_path
import os
for tk, web in [('AAPL', 'https://apple.com'), ('MSFT', 'https://microsoft.com'),
                ('NVDA', None), ('BRK-B', None), ('PENG', None)]:
    t, src = download_one(tk, web)
    p = _logo_path(t)
    sz = os.path.getsize(p) if os.path.exists(p) else 0
    print(f'{t:7s} src={src or \"NONE\":>20s}  file_size={sz}B')
"
```

Expected: 5 files appear in `static/logos/`, each > 1024 bytes. Open one to visually confirm it's a real logo:

```bash
open static/logos/AAPL.png   # macOS Preview
```

If a file looks wrong (corrupted, blank, default-globe), debug `data/logo_sources.py` before continuing.

- [ ] **Step 3: Clean up smoke-test files**

```bash
rm -f static/logos/AAPL.png static/logos/MSFT.png static/logos/NVDA.png static/logos/BRK-B.png static/logos/PENG.png
```

(They'll be re-downloaded properly during the full backfill in Task 10.)

- [ ] **Step 4: Commit**

```bash
git add production/download_logos.py
git commit -m "feat: production/download_logos — bulk fetch + DB update for self-hosted logos"
```

---

## Task 4: Mount `/logos` in the backend

**Files:**
- Modify: `backend/main.py` (after line 74, before `AGREEMENT_TEXT`)

- [ ] **Step 1: Add the static mount**

Find the block around `backend/main.py:74` (right after `_ensure_indexes()`). Insert this:

```python
# ---------------------------------------------------------------------------
# Self-hosted company logos (served at /logos/{TICKER}.png)
# ---------------------------------------------------------------------------
_LOGO_DIR = os.path.join(_APP_ROOT, "static", "logos")
if os.path.isdir(_LOGO_DIR):
    app.mount("/logos", StaticFiles(directory=_LOGO_DIR), name="logos")
```

`StaticFiles` is already imported on line 23 — no new import needed.

**Important:** This mount MUST be defined BEFORE the SPA catch-all at line 2429 (`@app.get("/{full_path:path}")`). FastAPI evaluates routes in registration order, and the catch-all would otherwise swallow `/logos/*`. The placement after `_ensure_indexes()` (line 74) puts it well before line 2429, so the order is correct.

- [ ] **Step 2: Drop a test PNG and verify it serves**

```bash
# Make sure the dev server is running: venv/bin/uvicorn backend.main:app --reload --port 8000
# In another terminal:
curl -s -o static/logos/_TEST.png -L "https://assets.parqet.com/logos/symbol/AAPL?format=png"
curl -sI -o /dev/null -w "HTTP %{http_code} ct=%{content_type} bytes=%{size_download}\n" \
  "http://localhost:8000/logos/_TEST.png"
rm static/logos/_TEST.png
```

Expected: `HTTP 200 ct=image/png bytes=<some-positive-number>`.
If 404: mount didn't load (restart uvicorn or check file path).

- [ ] **Step 3: Commit**

```bash
git add backend/main.py
git commit -m "feat: mount /logos static directory for self-hosted company logos"
```

---

## Task 5: Vite dev proxy

**Files:**
- Modify: `frontend/vite.config.js`

- [ ] **Step 1: Add `/logos` to the proxy table**

Replace the entire `proxy:` block in `frontend/vite.config.js` with:

```js
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/logos': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
```

- [ ] **Step 2: Commit**

```bash
git add frontend/vite.config.js
git commit -m "feat(frontend): proxy /logos to backend in vite dev server"
```

---

## Task 6: Drop `apply_logo_overrides` callers

**Files:**
- Modify: `utils/logo_overrides.py`
- Modify: `utils/universe_loader.py:5,196`
- Modify: `data/market_data.py:19,483-487`

- [ ] **Step 1: Trim `utils/logo_overrides.py` to data only**

Replace the entire content of `utils/logo_overrides.py` with:

```python
"""Manual logo URL overrides for tickers where the primary CDN returns junk.

These URLs are tried FIRST by `data.logo_sources.from_override()` during the
bulk-download phase. The downloader fetches the bytes from these URLs, saves
them to static/logos/, and updates companies.logo_url to /logos/{T}.png — so
no caller needs to apply this dict to the DB anymore.
"""

LOGO_OVERRIDES = {
    "PENG": "https://cdn.prod.website-files.com/6764579f0a24e5a0083f25bb/67bb88245ce879aaca499ddb_schema--penguin-logo.jpg",
    "HWKN": "https://www.hawkinsinc.com/wp-content/uploads/2025/10/Hawkins-logo-300-x-300.jpg",
    "GTM":  "https://img.logo.dev/zoominfo.com?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "AAMI": "https://img.logo.dev/ticker/AAMI?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "FLG":  "https://img.logo.dev/ticker/FLG?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "VSNT": "https://img.logo.dev/ticker/VSNT?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "OPLN": "https://img.logo.dev/ticker/OPLN?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "HTO":  "https://img.logo.dev/ticker/HTO?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
}
```

The `apply_logo_overrides()` function is removed entirely.

- [ ] **Step 2: Drop the call site in `utils/universe_loader.py`**

In `utils/universe_loader.py`:

Line 5 — remove the import:
```python
from utils.logo_overrides import apply_logo_overrides
```
(Delete that line entirely.)

Line 196 — remove the call:
```python
    apply_logo_overrides(engine)
```
(Delete that line entirely.)

- [ ] **Step 3: Drop the inline loop in `data/market_data.py`**

In `data/market_data.py`:

Line 19 — remove the import:
```python
from utils.logo_overrides import LOGO_OVERRIDES
```
(Delete that line entirely.)

Lines 483–487 — remove the comment + loop:
```python
    # Re-apply custom logo overrides (FMP placeholder images would otherwise overwrite them)
    with engine.begin() as conn:
        for ticker, url in LOGO_OVERRIDES.items():
            conn.execute(text("UPDATE companies SET logo_url = :url WHERE ticker = :t"),
                         {"url": url, "t": ticker})
```
(Delete those 5 lines entirely.)

- [ ] **Step 4: Verify no remaining references**

```bash
grep -rn "apply_logo_overrides" --include="*.py" .
grep -rn "from utils.logo_overrides import LOGO_OVERRIDES" --include="*.py" .
```

Expected:
- First grep: no results.
- Second grep: only `data/logo_sources.py` (which still uses `LOGO_OVERRIDES`).

If anything else shows up, it's a missed call site — handle it.

- [ ] **Step 5: Commit**

```bash
git add utils/logo_overrides.py utils/universe_loader.py data/market_data.py
git commit -m "refactor: drop apply_logo_overrides — overrides are now consumed by logo_sources during download"
```

---

## Task 7: Daily-pipeline integration

**Files:**
- Modify: `production/run_daily.py`

- [ ] **Step 1: Add the repair function**

In `production/run_daily.py`, add this function near the other `_repair_*` functions (e.g. after `_repair_analyst_targets()` near line ~165):

```python
def _download_missing_logos():
    """Fetch logo PNGs for any tickers that don't have an on-disk file yet."""
    from production.download_logos import download_all
    download_all(missing_only=True)
```

- [ ] **Step 2: Wire it into `run_daily()`**

Find lines 369–371 (the `# ── Remaining self-healing repairs ──` block) and add the new call at the bottom:

```python
    # ── Remaining self-healing repairs ───────────────────────────────────────
    _repair_market_caps()
    _repair_analyst_targets()
    _download_missing_logos()
```

- [ ] **Step 3: Verify import works**

```bash
venv/bin/python -c "from production.run_daily import _download_missing_logos; print('ok')"
```

Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add production/run_daily.py
git commit -m "feat: daily pipeline auto-fetches logos for new tickers"
```

---

## Task 8: Delete the old repair script

**Files:**
- Delete: `production/repair_broken_logos.py`

- [ ] **Step 1: Confirm no callers**

```bash
grep -rn "repair_broken_logos\|repair_logos" --include="*.py" --include="*.md" .
```

Expected: only matches in the design doc and this plan (markdown). If a `.py` file still imports it, fix that file before deleting.

- [ ] **Step 2: Delete the file**

```bash
git rm production/repair_broken_logos.py
```

- [ ] **Step 3: Commit**

```bash
git commit -m "chore: remove repair_broken_logos — replaced by download_logos"
```

---

## Task 9: Local end-to-end smoke test

No new files. Verifies Tasks 1–7 work together against the local SQLite DB.

- [ ] **Step 1: Run the downloader on 5 representative tickers**

Force a small subset by writing a one-shot script. Run:

```bash
venv/bin/python -c "
from production.download_logos import download_one, _logo_path
import os, sqlite3
conn = sqlite3.connect('vesign.db')
for tk in ['AAPL', 'MSFT', 'BRK-B', 'PENG', 'GOOGL']:
    web = conn.execute('SELECT website FROM companies WHERE ticker=?', (tk,)).fetchone()
    web = web[0] if web else None
    t, src = download_one(tk, web)
    p = _logo_path(t)
    sz = os.path.getsize(p) if os.path.exists(p) else 0
    print(f'{t:7s} src={src or \"NONE\":>20s} bytes={sz}')
    if src:
        conn.execute('UPDATE companies SET logo_url=? WHERE ticker=?',
                     (f'/logos/{t}.png', t))
conn.commit()
conn.close()
"
```

Expected: 5 lines, each with a non-NONE source and bytes > 1024.

- [ ] **Step 2: Restart uvicorn and curl the new mount**

```bash
# If uvicorn is already running with --reload, the mount auto-reloads.
# Otherwise:
#   venv/bin/uvicorn backend.main:app --reload --port 8000
curl -sI -o /dev/null -w "AAPL: HTTP %{http_code} ct=%{content_type} bytes=%{size_download}\n" \
  "http://localhost:8000/logos/AAPL.png"
curl -sI -o /dev/null -w "BRK-B: HTTP %{http_code} ct=%{content_type} bytes=%{size_download}\n" \
  "http://localhost:8000/logos/BRK-B.png"
```

Expected: both `HTTP 200 ct=image/png bytes=<positive>`.

- [ ] **Step 3: Spot-check the API still works**

```bash
curl -s "http://localhost:8000/api/signals/today" | head -c 1500
```

Expected: JSON response. Look for one of `AAPL`, `MSFT`, `GOOGL` (whichever is in today's signals) and confirm `logo_url` is `/logos/{TICKER}.png`. If it still shows `financialmodelingprep.com/...`, that ticker wasn't in the smoke-test set — it'll be fixed by the full backfill in Task 10.

- [ ] **Step 4: Spot-check the frontend**

Open `http://localhost:3000` in a browser. The 5 tickers from Step 1 should now have proper logos in any list (Signals, Trades, Watchlist). Other tickers will still appear broken until Task 10.

- [ ] **No commit** — this task only verified things work; no code changed.

---

## Task 10: Production deploy + full backfill

This task runs against the live server. Snapshot the DB before starting.

- [ ] **Step 1: Push all commits**

```bash
git push origin main
```

- [ ] **Step 2: Deploy code on the server**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && git pull && cd frontend && npm run build && systemctl restart vesign'
```

Expected: `git pull` shows the new commits; `npm run build` finishes without errors; `systemctl restart vesign` returns silently.

- [ ] **Step 3: Snapshot the `companies` table on the server (rollback safety)**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && cp vesign.db /root/vesign.db.before-logos-$(date +%Y%m%d)'
```

Expected: silent. Verify with `ssh ... ls -la /root/vesign.db.before-logos-*`.

- [ ] **Step 4: Run the full bulk download on the server**

This may take 5–15 minutes for 1,638 tickers.

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && venv/bin/python -m production.download_logos 2>&1 | tail -40'
```

Expected ending: `Done: ~1638 downloaded, <small_number> failed` and a `Sources: {...}` summary. If failure count > 50, investigate before declaring success — likely a regression in `data/logo_sources.py`.

- [ ] **Step 5: Spot-check files on disk**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'ls /opt/vesign/static/logos/ | wc -l; du -sh /opt/vesign/static/logos/'
```

Expected: count ≈ 1638 (minus whatever failed); size 10–30 MB.

- [ ] **Step 6: Verify the live site**

Open https://ve-sign.com/ in a browser. Logos should render across:
- Signals page table
- Trades page (open + closed)
- Portfolio page holdings
- Watchlist page
- Global search dropdown
- Signal modal (when you click a row)

Test in DevTools Network tab: at least one `/logos/{T}.png` request should return `200 OK image/png`. Hard-refresh (Cmd-Shift-R) if cached.

- [ ] **Step 7: Tail logs for any 404s**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'journalctl -u vesign -n 100 --no-pager | grep -i "logo\|/logos"'
```

Expected: minimal output. Any `/logos/*.png` 404s mean the file failed to download — capture the ticker list and decide whether to add a manual override or accept the placeholder.

- [ ] **Step 8: Tomorrow morning verification (after the 7am pipeline)**

The next day, after the cron pipeline runs, verify the daily auto-fetch step worked:

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'tail -50 /var/log/vesign-pipeline.log | grep -i "missing-only\|download_logos\|missing"'
```

Expected: a line like `Missing-only mode: 0 tickers without an on-disk logo` (no new tickers that day) or `Missing-only mode: N tickers...` followed by `Done:` on success.

- [ ] **No commit** — this task is deployment + verification only.

---

## Self-Review

**1. Spec coverage:**

| Spec section | Plan task |
|---|---|
| Storage at `static/logos/{T}.png` + gitignore | Task 1 |
| FastAPI `/logos` mount | Task 4 |
| Vite dev proxy | Task 5 |
| Source fallback chain (4 sources) | Task 2 |
| Bulk downloader CLI | Task 3 |
| Daily pipeline `--missing-only` | Task 7 |
| `companies.logo_url = '/logos/{T}.png'` | Task 3 (inside `download_all`) |
| Drop `apply_logo_overrides` callers | Task 6 |
| Delete `repair_broken_logos.py` | Task 8 |
| Validation: image/* + ≥ 1 KB | Task 2 (`_fetch`) |
| Atomic save (temp + rename) | Task 3 (`_save_atomic`) |

All spec requirements covered.

**2. Placeholder scan:** No "TBD", "TODO", or "implement later" anywhere. All code blocks are complete.

**3. Type consistency:**
- `resolve()` returns `tuple[Optional[bytes], Optional[str]]` — used consistently in `download_one`.
- `download_one()` returns `tuple[str, Optional[str]]` (ticker, source) — matched in `download_all`.
- `LOGO_OVERRIDES` is `dict[str, str]` in both `utils/logo_overrides.py` and `data/logo_sources.py`.
- `_logo_path(ticker)` always uses uppercase as stored in DB.

No mismatches.
