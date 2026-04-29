# Self-Hosted Company Logos — Design

**Date:** 2026-04-29
**Status:** approved (pending spec review)

## Problem

FMP deprecated their logo endpoints. Both URL patterns we relied on are dead:

- DB-stored URLs (`financialmodelingprep.com/image-stock/{TICKER}.png`) → HTTP 502 (CDN decommissioned).
- The host advertised in FMP's profile `image` field (`images.financialmodelingprep.com`) is unreachable from any IP we tested.
- Authenticated FMP image routes return: *"Legacy Endpoint... only available for legacy users who have valid subscriptions prior August 31, 2025."*

All 1,638 US tickers in `companies.logo_url` point at a broken CDN. Logos are invisible across SignalsPage, TradesPage, PortfolioPage, WatchlistPage, GlobalSearch, and SignalModal.

## Goal

Self-hosted logo files served from our own server. Downloaded once from external CDNs, then permanent. Zero runtime dependency on third-party logo hosts after backfill.

100% coverage target — fall back through multiple sources until every ticker has a local file.

## Non-goals

- High-DPI / retina logo variants (single PNG per ticker is enough).
- Logo CMS / admin UI for editing logos (manual override file already exists for this).
- Migrating TASE tickers (site is US-only — see project memory).

## Architecture

### Storage

```
/opt/vesign/static/logos/{TICKER}.png   # production
static/logos/{TICKER}.png               # local dev
```

- One PNG per ticker, file name is the uppercase ticker exactly as stored in `companies.ticker`.
- Tickers with `-` (e.g. `BRK-B`) preserve the dash: `BRK-B.png`.
- Add `static/logos/` to `.gitignore` — binaries don't belong in git. Dev machines either rsync from the server (`rsync -av root@…:/opt/vesign/static/logos/ static/logos/`) or run the downloader locally.

### Serving

In `backend/main.py`, mount static directory:

```python
from fastapi.staticfiles import StaticFiles
app.mount("/logos", StaticFiles(directory="static/logos"), name="logos")
```

- Files served at `https://ve-sign.com/logos/{TICKER}.png` in prod.
- In dev, Vite proxies `/logos/*` to `:8000` (already configured for `/api/*`; we'll add `/logos` to the proxy list).

### Database

`companies.logo_url` becomes a relative path: `/logos/{TICKER}.png` for tickers with a local file, `NULL` for tickers we couldn't fetch from any source (frontend already renders the placeholder div in this case).

No schema change required — column already accepts arbitrary strings.

### Source Fallback Chain

For each ticker, try sources in order, accept the first response that is a valid PNG ≥ 1 KB (filters out placeholder/error stub images):

1. **Manual override** — `utils/logo_overrides.py::LOGO_OVERRIDES` dict (8 entries today). Source URL fetched and stored.
2. **Parqet CDN** — `https://assets.parqet.com/logos/symbol/{TICKER}?format=png`. Tested 100% on 20 popular US tickers; expected near-100% overall.
3. **logo.dev** — `https://img.logo.dev/ticker/{TICKER}?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ` (token already in repo, used by `repair_broken_logos.py`).
4. **Google favicon from website** — `https://www.google.com/s2/favicons?domain={DOMAIN}&sz=128`, where `{DOMAIN}` is parsed from `companies.website`. Always returns *something* (default globe icon for unknown domains, but those still pass our size filter — acceptable, vastly better than no logo).

### Validation

A candidate file is accepted iff:
- HTTP 200,
- Content-Type starts with `image/`,
- Body size ≥ 1024 bytes (filters tiny placeholder responses).

Downloads are saved to a temp file and atomically renamed into `static/logos/` only after validation passes.

## Components

### `production/download_logos.py` (new)

One-shot bulk downloader. Replaces `production/repair_broken_logos.py` (delete that file).

```python
def download_logo(ticker: str, website: str | None) -> bool:
    """Try fallback chain, save to static/logos/{ticker}.png, return success."""

def download_all(tickers: list[str] | None = None) -> dict:
    """Download logos for all tickers (or a subset). Parallel via ThreadPoolExecutor.
    Returns {'downloaded': N, 'failed': M, 'failed_tickers': [...]}."""

def update_db_paths(engine):
    """Set companies.logo_url = '/logos/{T}.png' for every ticker with a local file,
    NULL for those without."""
```

CLI: `venv/bin/python -m production.download_logos` (downloads all). Optional `--missing-only` flag for daily-pipeline use (skips tickers that already have a file on disk).

### `production/run_daily.py` (modified)

Add a step at the end of the pipeline:

```python
from production.download_logos import download_all, update_db_paths
download_all(missing_only=True)   # only fetches new tickers + retries previous failures
update_db_paths(engine)
```

Adds ~0 sec on most days (no new tickers); ~30 sec when the universe changes.

### `backend/main.py` (modified)

- Add the `StaticFiles` mount for `/logos`.
- No endpoint logic changes — existing queries already SELECT `c.logo_url`, which now contains a relative path.

### `frontend/vite.config.js` (modified)

Add `/logos` to the proxy table so `npm run dev` (port 3000) can fetch from the backend (port 8000):

```js
proxy: {
  '/api':   'http://localhost:8000',
  '/logos': 'http://localhost:8000',  // NEW
}
```

### `utils/logo_overrides.py` (modified)

`apply_logo_overrides()` becomes a no-op — its work is now subsumed by the fallback chain in `download_logos.py`. Keep `LOGO_OVERRIDES` dict as the source of truth for override URLs; remove the DB-update logic. Callers of `apply_logo_overrides()` in `utils/universe_loader.py` and `data/market_data.py` get cleaned up.

### `production/repair_broken_logos.py` (deleted)

Replaced by `download_logos.py` which always downloads to local storage.

## Data Flow

**One-time backfill (run once after deploy):**

```
download_logos.py
  → for each ticker in companies:
    → try LOGO_OVERRIDES[T] → Parqet → logo.dev → Google favicon
    → first valid response (≥ 1 KB image) wins
    → save to static/logos/{T}.png
    → companies.logo_url = '/logos/{T}.png'
  → tickers that fail all 4 sources: companies.logo_url = NULL
```

**Daily (already-running pipeline picks up new tickers):**

```
run_daily.py … (existing steps)
  → download_logos.download_all(missing_only=True)
  → tickers without an on-disk file go through the same fallback chain
```

**Runtime (unchanged):**

```
Frontend GET /api/signals
  → row.logo_url = '/logos/AAPL.png'
  → <img src='/logos/AAPL.png'>
  → browser fetches https://ve-sign.com/logos/AAPL.png
  → FastAPI StaticFiles streams from disk
```

## Error Handling

- Per-ticker fetch failures during backfill are isolated (one bad ticker doesn't kill the run).
- All 4 sources failing for a ticker → log a warning, set `logo_url = NULL`, frontend placeholder renders.
- A future re-run of `download_logos.py --missing-only` will retry any tickers without a file.
- If `static/logos/{T}.png` exists but DB still has the old FMP URL: `update_db_paths()` rewrites the column.

## Testing

- Manually run `download_logos.py` on a subset of 5–10 tickers locally first; verify all 4 fallbacks fire (force a Parqet failure by overriding the URL to a 404 in test code).
- Visual check on 3 pages (Signals, Trades, Portfolio) that logos render after deploy.
- Verify SignalModal placeholder still works for any tickers we couldn't fetch.

## Rollout

1. Build downloader + serving locally; smoke-test with 10 tickers.
2. Deploy code to server (`git pull && systemctl restart vesign`).
3. SSH to server, run `venv/bin/python -m production.download_logos` (one-shot, expect ~5–10 min for 1,638 tickers with parallel fetches).
4. Verify a few rows: `sqlite3 vesign.db "SELECT ticker, logo_url FROM companies LIMIT 10"` — should show `/logos/AAPL.png` etc.
5. Visit ve-sign.com, confirm logos render.
6. Cleanup: delete `production/repair_broken_logos.py`.

## Open Questions

None — all major choices locked in via brainstorm.
