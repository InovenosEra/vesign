# XLSX Table Exports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add five "Download as XLSX" buttons (one per table) that emit XLSX files containing the full set of DB columns per ticker, respecting the user's current filters and ignoring frontend pagination.

**Architecture:** Backend FastAPI endpoints generate `.xlsx` via `openpyxl` and return them as `StreamingResponse` attachments. The frontend uses a small `<DownloadXLSXButton>` that performs an authed `fetch`, gets a blob, and triggers a download via an injected `<a>` tag. Filter state is forwarded as query params identical to each table's existing read endpoint.

**Tech Stack:** FastAPI · pandas · openpyxl · React · Clerk (auth)

**Project conventions** (read these once before starting):
- All new endpoints attach to the existing `protected` router defined at `backend/main.py:523` (`protected = APIRouter(dependencies=[Depends(get_current_user)])`).
- The router is mounted at `app.include_router(protected)` (`backend/main.py:2185`); no extra wiring needed.
- Authed frontend fetches use `authHeaders()` from `frontend/src/api.js` (Clerk Bearer token).
- The site is **US-only**. Filter `WHERE ticker NOT LIKE '%.TA'` whenever pulling tickers; do not introduce TASE handling.
- The project has **no pytest infrastructure**. Each task verifies via a small smoke script (`scripts/smoke_<topic>.py`) and curl-based HTTP checks against a locally-running uvicorn — not pytest.
- Local dev: `venv/bin/uvicorn backend.main:app --port 8000`. Frontend: `cd frontend && npm run dev` (port 3000). Production: ve-sign.com (after deploy task).
- All UI verification at the end is on the running app — start uvicorn + Vite, click each button, open the XLSX in a viewer.

---

## File structure

| File | Purpose | New / Modified |
| --- | --- | --- |
| `requirements.txt` | Add `openpyxl==3.1.5` | Modified |
| `backend/exports.py` | XLSX builder helpers (DataFrame → StreamingResponse) | New |
| `backend/main.py` | 5 new `@protected.get` endpoints, all near the existing signals/trades/watchlists/portfolio routes | Modified |
| `frontend/src/components/DownloadXLSXButton.jsx` | Reusable button: authed fetch → blob → download | New |
| `frontend/src/pages/SignalsPage.jsx` | Mount one button | Modified |
| `frontend/src/pages/TradesPage.jsx` | Mount two buttons (Historical + Open) | Modified |
| `frontend/src/pages/WatchlistPage.jsx` | Mount one button (current watchlist) | Modified |
| `frontend/src/pages/PortfolioPage.jsx` | Mount one button | Modified |
| `scripts/smoke_exports.py` | Smoke tests for `backend/exports.py` and the endpoints | New |

---

## Task 1: Add `openpyxl` and create the XLSX helper module

**Files:**
- Modify: `requirements.txt`
- Create: `backend/exports.py`
- Create: `scripts/smoke_exports.py`

- [ ] **Step 1: Write the failing smoke test**

Create `scripts/smoke_exports.py`:

```python
"""Smoke tests for backend.exports. Run with: venv/bin/python scripts/smoke_exports.py"""
import io
import pandas as pd
from openpyxl import load_workbook

from backend.exports import dataframe_to_xlsx_response


def test_basic_workbook():
    df = pd.DataFrame([
        {"ticker": "AAPL", "close": 220.0, "rsi": 55.1},
        {"ticker": "MSFT", "close": 410.0, "rsi": 48.0},
    ])
    resp = dataframe_to_xlsx_response(df, filename="signals_2026-04-27", sheet_name="signals")

    assert resp.media_type == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert 'attachment; filename="signals_2026-04-27.xlsx"' in resp.headers["content-disposition"]

    body = b"".join(resp.body_iterator) if hasattr(resp, "body_iterator") else resp.body
    wb = load_workbook(io.BytesIO(body))
    assert wb.sheetnames == ["signals"]
    ws = wb["signals"]
    assert [c.value for c in ws[1]] == ["ticker", "close", "rsi"]
    assert ws.cell(row=2, column=1).value == "AAPL"
    assert ws.cell(row=3, column=2).value == 410.0
    print("test_basic_workbook PASS")


def test_empty_dataframe_still_emits_header():
    df = pd.DataFrame(columns=["ticker", "close"])
    resp = dataframe_to_xlsx_response(df, filename="empty", sheet_name="signals")
    body = b"".join(resp.body_iterator) if hasattr(resp, "body_iterator") else resp.body
    wb = load_workbook(io.BytesIO(body))
    ws = wb["signals"]
    assert [c.value for c in ws[1]] == ["ticker", "close"]
    assert ws.max_row == 1   # header only
    print("test_empty_dataframe_still_emits_header PASS")


if __name__ == "__main__":
    test_basic_workbook()
    test_empty_dataframe_still_emits_header()
    print("\nAll exports.py smoke tests passed.")
```

- [ ] **Step 2: Run the smoke test and confirm it fails**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `ModuleNotFoundError: No module named 'backend.exports'` (or `'openpyxl'`).

- [ ] **Step 3: Add the dep**

Add the line `openpyxl==3.1.5` to `requirements.txt`, then install:

```bash
venv/bin/pip install openpyxl==3.1.5
```

- [ ] **Step 4: Implement `backend/exports.py`**

Create `backend/exports.py`:

```python
"""XLSX export helpers — DataFrame → FastAPI StreamingResponse."""
from __future__ import annotations

import io
from typing import Optional

import pandas as pd
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

XLSX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


def _write_dataframe_to_workbook(df: pd.DataFrame, sheet_name: str) -> Workbook:
    wb = Workbook(write_only=False)
    ws = wb.active
    ws.title = sheet_name[:31]  # Excel sheet-name limit

    columns = list(df.columns)
    ws.append(columns)

    # Pandas timestamps and NaN don't serialize cleanly — coerce here.
    for row in df.itertuples(index=False, name=None):
        ws.append([_cell_value(v) for v in row])

    # Make headers bold and freeze the header row so users can scroll.
    for col_idx, _ in enumerate(columns, start=1):
        ws.cell(row=1, column=col_idx).font = ws.cell(row=1, column=col_idx).font.copy(bold=True)
    ws.freeze_panes = "A2"

    # Auto-size columns based on the header length (cheap heuristic that
    # avoids walking every row for large exports).
    for col_idx, col_name in enumerate(columns, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = max(12, len(str(col_name)) + 2)

    return wb


def _cell_value(v):
    """Coerce values that openpyxl can't serialize directly."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def dataframe_to_xlsx_response(
    df: pd.DataFrame,
    filename: str,
    sheet_name: str = "Sheet1",
) -> StreamingResponse:
    """Build an XLSX from `df` and return it as a download attachment.

    `filename` should NOT include the .xlsx extension — it's added here.
    Empty DataFrames produce a header-only workbook (caller's intent: "no rows
    matched my filters" should not be a hard error).
    """
    wb = _write_dataframe_to_workbook(df, sheet_name=sheet_name)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    safe_name = filename.replace('"', "").replace("\\", "")
    headers = {
        "content-disposition": f'attachment; filename="{safe_name}.xlsx"',
    }
    return StreamingResponse(buf, media_type=XLSX_MEDIA_TYPE, headers=headers)
```

- [ ] **Step 5: Re-run the smoke test**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: prints `test_basic_workbook PASS` then `test_empty_dataframe_still_emits_header PASS` then `All exports.py smoke tests passed.`

- [ ] **Step 6: Commit**

```bash
git add requirements.txt backend/exports.py scripts/smoke_exports.py
git commit -m "feat: add openpyxl-backed XLSX export helper"
```

---

## Task 2: `/api/signals/export.xlsx` endpoint

**Files:**
- Modify: `backend/main.py` (add new route immediately AFTER `signals_today` at ~line 637 or AFTER `signals` at ~line 677 — placement is fine either way; pick the spot just below the existing `signals` paginated endpoint)
- Modify: `scripts/smoke_exports.py` (add HTTP test)

- [ ] **Step 1: Add the failing HTTP smoke test**

Append to `scripts/smoke_exports.py` (and update the `__main__` block to call it):

```python
import os, subprocess, time, urllib.request, urllib.parse, json
from openpyxl import load_workbook


def _start_uvicorn():
    """Boot uvicorn in the background for HTTP smoke tests."""
    p = subprocess.Popen(
        ["venv/bin/uvicorn", "backend.main:app", "--port", "8765"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    # Wait for /api/health to respond
    for _ in range(60):
        try:
            with urllib.request.urlopen("http://127.0.0.1:8765/api/health", timeout=1) as r:
                if r.status == 200:
                    return p
        except Exception:
            time.sleep(0.5)
    p.terminate()
    raise RuntimeError("uvicorn did not start in 30s")


def _http_get(path: str, token: str | None = None) -> tuple[int, bytes, dict]:
    req = urllib.request.Request("http://127.0.0.1:8765" + path)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)


def test_signals_export_requires_auth():
    status, body, _ = _http_get("/api/signals/export.xlsx")
    assert status == 401, f"expected 401 without token, got {status}: {body[:200]}"
    print("test_signals_export_requires_auth PASS")
```

(Note: the auth-required check is the only HTTP test we can run without a real Clerk token. The full happy-path smoke is verified manually in the deploy task using the user's logged-in browser.)

In `__main__`, add:

```python
    proc = _start_uvicorn()
    try:
        test_signals_export_requires_auth()
    finally:
        proc.terminate()
        proc.wait(timeout=5)
```

- [ ] **Step 2: Run the smoke test, confirm it fails on the new check**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: workbook tests still PASS, then `AssertionError: expected 401 without token, got 404` (the route doesn't exist yet).

- [ ] **Step 3: Implement the endpoint**

In `backend/main.py`, just below the existing `signals(...)` function (around the `@protected.get("/api/signals/by-tickers")` route, so the new export sits with its sibling), add:

```python
@protected.get("/api/signals/export.xlsx")
def signals_export(
    signal: Optional[str] = None,
    search: Optional[str] = None,
    months: int = Query(default=12, ge=1, le=120),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
):
    """XLSX export of the Signals table — full per-ticker columns, all pages.

    Mirrors the filter params of `/api/signals` but ignores pagination
    (page/page_size) so the user gets every row that matches their filters.
    """
    from backend.exports import dataframe_to_xlsx_response

    sort_col = sort_by if sort_by in {
        "date", "ticker", "close", "rsi", "vesign_score", "score", "signal", "prediction_score",
    } else "date"
    sort_dir_sql = "ASC" if str(sort_dir).lower() == "asc" else "DESC"

    where = ["s.date >= DATE('now', :months)", "s.ticker NOT LIKE '%.TA'"]
    params: dict = {"months": f"-{months} months"}

    if signal:
        where.append("s.signal = :signal")
        params["signal"] = signal.upper()
    if search:
        where.append("(s.ticker LIKE :q OR c.company LIKE :q)")
        params["q"] = f"%{search}%"

    sql = f"""
        SELECT s.*,
               c.company, c.sector, c.industry, c.logo_url,
               f.market_cap
        FROM signals s
        LEFT JOIN companies c ON c.ticker = s.ticker
        LEFT JOIN fundamentals f ON f.ticker = s.ticker
        WHERE {' AND '.join(where)}
        ORDER BY s.{sort_col} {sort_dir_sql}, s.ticker ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(df, filename=f"signals_{today}", sheet_name="signals")
```

(`Optional`, `Query`, `text`, `pd`, `engine`, `datetime`, `UTC` are already imported at the top of `main.py`. If you see import errors run `grep -n '^from\\|^import' backend/main.py | head -30` and add what's missing — do NOT add the imports speculatively.)

- [ ] **Step 4: Re-run the smoke test**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: all PASS lines including `test_signals_export_requires_auth PASS`.

- [ ] **Step 5: Manual auth check**

Start uvicorn and curl with no token:

```bash
venv/bin/uvicorn backend.main:app --port 8000 &
sleep 2
curl -i http://127.0.0.1:8000/api/signals/export.xlsx
kill %1 2>/dev/null
```

Expected: `HTTP/1.1 401 Unauthorized` with `{"detail": "Not authenticated"}`.

- [ ] **Step 6: Commit**

```bash
git add backend/main.py scripts/smoke_exports.py
git commit -m "feat: signals XLSX export endpoint"
```

---

## Task 3: `/api/trades/export.xlsx` endpoint (closed trades)

**Files:**
- Modify: `backend/main.py` (insert directly below the existing `historical_trades` at ~line 1192)

- [ ] **Step 1: Add the failing auth smoke test**

Append to `scripts/smoke_exports.py`:

```python
def test_trades_export_requires_auth():
    status, body, _ = _http_get("/api/trades/export.xlsx")
    assert status == 401, f"expected 401, got {status}: {body[:200]}"
    print("test_trades_export_requires_auth PASS")
```

Add the call to `__main__` between the existing `test_signals_export_requires_auth()` and the `finally:`.

- [ ] **Step 2: Run, confirm it fails**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `expected 401, got 404`.

- [ ] **Step 3: Implement the endpoint**

In `backend/main.py`, immediately below the existing `historical_trades(...)` function, add:

```python
@protected.get("/api/trades/export.xlsx")
def trades_export(
    start: Optional[str] = None,
    end: Optional[str] = None,
    market: Optional[str] = None,
):
    """XLSX export of closed trades — one row per trade, full columns + company refs."""
    from backend.exports import dataframe_to_xlsx_response

    mkt = (market or "US").upper()
    where = ["1=1"]
    params: dict = {}
    if start:
        where.append("tl.sell_date >= :start")
        params["start"] = start
    if end:
        where.append("tl.sell_date <= :end")
        params["end"] = end
    if mkt == "US":
        where.append("tl.ticker NOT LIKE '%.TA'")
    elif mkt == "IL":
        where.append("tl.ticker LIKE '%.TA'")

    sql = f"""
        SELECT tl.*,
               c.company, c.sector, c.industry, c.logo_url,
               f.market_cap
        FROM trade_log tl
        LEFT JOIN companies c    ON c.ticker = tl.ticker
        LEFT JOIN fundamentals f ON f.ticker = tl.ticker
        WHERE {' AND '.join(where)}
        ORDER BY tl.sell_date DESC, tl.ticker ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(df, filename=f"trades_closed_{today}", sheet_name="trades")
```

- [ ] **Step 4: Re-run smoke**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `test_trades_export_requires_auth PASS`.

- [ ] **Step 5: Commit**

```bash
git add backend/main.py scripts/smoke_exports.py
git commit -m "feat: closed trades XLSX export endpoint"
```

---

## Task 4: `/api/trades/open/export.xlsx` endpoint

**Files:**
- Modify: `backend/main.py` (insert directly below the existing `open_trades` at ~line 1352)

- [ ] **Step 1: Add the failing auth smoke test**

Append to `scripts/smoke_exports.py`:

```python
def test_open_trades_export_requires_auth():
    status, body, _ = _http_get("/api/trades/open/export.xlsx")
    assert status == 401, f"expected 401, got {status}: {body[:200]}"
    print("test_open_trades_export_requires_auth PASS")
```

Add to `__main__`.

- [ ] **Step 2: Run, confirm fails**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `expected 401, got 404`.

- [ ] **Step 3: Read the existing `open_trades` SQL so the export matches it**

Look at `backend/main.py:1352-1450` (approximately). The endpoint computes `last_close`, `days_held`, `live_price`, `unrealized_yield_pct`. The export should produce the **same shape** plus the company refs.

- [ ] **Step 4: Implement the endpoint**

In `backend/main.py`, immediately below the existing `open_trades(...)` function, add (replace the LIVE_PRICE_FN call to whatever the function in main.py is named — search for `live_prices` cache helper near line 380):

```python
@protected.get("/api/trades/open/export.xlsx")
def open_trades_export(market: Optional[str] = None):
    """XLSX of currently open positions (BUY with no subsequent SELL)."""
    from backend.exports import dataframe_to_xlsx_response

    mkt = (market or "US").upper()
    market_filter = (
        "tl.ticker NOT LIKE '%.TA'" if mkt == "US"
        else "tl.ticker LIKE '%.TA'" if mkt == "IL"
        else "1=1"
    )

    # Reuse the same join the existing open_trades route uses. If `open_trades`
    # already builds a list of dicts in Python, the simplest path is to call
    # that function and wrap its output as a DataFrame:
    rows = open_trades(market=mkt)            # returns list of dicts
    df = pd.DataFrame(rows)

    if not df.empty:
        # Add company / sector / market_cap columns by joining in pandas to
        # avoid touching the read endpoint's SQL.
        with engine.connect() as conn:
            extras = pd.read_sql(
                text("""
                    SELECT c.ticker, c.company, c.sector, c.industry, c.logo_url,
                           f.market_cap
                    FROM companies c
                    LEFT JOIN fundamentals f ON f.ticker = c.ticker
                """),
                conn,
            )
        df = df.merge(extras, on="ticker", how="left")

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(df, filename=f"trades_open_{today}", sheet_name="open_trades")
```

If `open_trades` returns a `JSONResponse` (not a list of dicts), instead replicate its body in this new function — copy the SQL block from `open_trades` verbatim, build a DataFrame with the same columns, then add the join above.

- [ ] **Step 5: Re-run smoke**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `test_open_trades_export_requires_auth PASS`.

- [ ] **Step 6: Commit**

```bash
git add backend/main.py scripts/smoke_exports.py
git commit -m "feat: open trades XLSX export endpoint"
```

---

## Task 5: `/api/watchlists/{list_id}/export.xlsx` endpoint

**Files:**
- Modify: `backend/main.py` (insert below `get_watchlist_tickers` at ~line 1095, but above the POST/PATCH/DELETE routes — keep GET routes together)

- [ ] **Step 1: Add the failing auth smoke test**

Append to `scripts/smoke_exports.py`:

```python
def test_watchlist_export_requires_auth():
    status, body, _ = _http_get("/api/watchlists/1/export.xlsx")
    assert status == 401, f"expected 401, got {status}: {body[:200]}"
    print("test_watchlist_export_requires_auth PASS")
```

Add to `__main__`.

- [ ] **Step 2: Run, confirm fails**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `expected 401, got 404`.

- [ ] **Step 3: Implement the endpoint**

In `backend/main.py`, immediately below `get_watchlist_tickers(...)`, add:

```python
@protected.get("/api/watchlists/{list_id}/export.xlsx")
def watchlist_export(list_id: int, user=Depends(get_current_user)):
    """XLSX of the watchlist's tickers — one row per ticker, latest signals row + company refs."""
    from backend.exports import dataframe_to_xlsx_response
    import re

    with engine.connect() as conn:
        _assert_owns_list(conn, list_id, user["id"])
        meta = conn.execute(
            text("SELECT name FROM watchlist_lists WHERE id = :lid"),
            {"lid": list_id},
        ).fetchone()
        watchlist_name = meta[0] if meta else f"list_{list_id}"

        df = pd.read_sql(
            text("""
                WITH latest AS (
                    SELECT ticker, MAX(date) AS d
                    FROM signals
                    WHERE ticker NOT LIKE '%.TA'
                    GROUP BY ticker
                )
                SELECT s.*, c.company, c.sector, c.industry, c.logo_url, f.market_cap
                FROM watchlist w
                JOIN latest    ON latest.ticker = w.ticker
                JOIN signals s ON s.ticker = latest.ticker AND s.date = latest.d
                LEFT JOIN companies    c ON c.ticker = w.ticker
                LEFT JOIN fundamentals f ON f.ticker = w.ticker
                WHERE w.list_id = :lid
                ORDER BY w.ticker ASC
            """),
            conn, params={"lid": list_id},
        )

    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", watchlist_name).strip("_") or f"list_{list_id}"
    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(
        df, filename=f"watchlist_{safe}_{today}", sheet_name="watchlist",
    )
```

- [ ] **Step 4: Re-run smoke**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `test_watchlist_export_requires_auth PASS`.

- [ ] **Step 5: Commit**

```bash
git add backend/main.py scripts/smoke_exports.py
git commit -m "feat: watchlist XLSX export endpoint"
```

---

## Task 6: `/api/portfolio/holdings/export.xlsx` endpoint

**Files:**
- Modify: `backend/main.py` (insert below `portfolio_holdings` at ~line 1512)

- [ ] **Step 1: Add the failing auth smoke test**

Append to `scripts/smoke_exports.py`:

```python
def test_portfolio_export_requires_auth():
    status, body, _ = _http_get("/api/portfolio/holdings/export.xlsx")
    assert status == 401, f"expected 401, got {status}: {body[:200]}"
    print("test_portfolio_export_requires_auth PASS")
```

Add to `__main__`.

- [ ] **Step 2: Run, confirm fails**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `expected 401, got 404`.

- [ ] **Step 3: Read the existing `portfolio_holdings` body**

`backend/main.py:1512-1680` (approximately). Take the same aggregation SQL it uses; do not invent new logic.

- [ ] **Step 4: Implement the endpoint**

In `backend/main.py`, immediately below `portfolio_holdings(...)`, add:

```python
@protected.get("/api/portfolio/holdings/export.xlsx")
def portfolio_holdings_export(
    user=Depends(get_current_user),
    market: str = Query(default="US"),
):
    """XLSX of aggregated portfolio holdings — same shape as /api/portfolio/holdings."""
    from backend.exports import dataframe_to_xlsx_response

    rows = portfolio_holdings(user=user, market=market)   # reuse existing function
    df = pd.DataFrame(rows)

    if not df.empty:
        # Watchlists-per-ticker summary (comma-joined names) — useful for analysis.
        with engine.connect() as conn:
            wl = pd.read_sql(
                text("""
                    SELECT wh.ticker, GROUP_CONCAT(wll.name, ', ') AS watchlists
                    FROM watchlist_holdings wh
                    JOIN watchlist_lists wll ON wll.id = wh.watchlist_id
                    WHERE wll.user_id = :uid
                    GROUP BY wh.ticker
                """),
                conn, params={"uid": user["id"]},
            )
        df = df.merge(wl, on="ticker", how="left")

    today = datetime.now(UTC).date().isoformat()
    return dataframe_to_xlsx_response(
        df, filename=f"portfolio_holdings_{today}", sheet_name="holdings",
    )
```

- [ ] **Step 5: Re-run smoke**

```bash
venv/bin/python scripts/smoke_exports.py
```

Expected: `test_portfolio_export_requires_auth PASS`.

- [ ] **Step 6: Commit**

```bash
git add backend/main.py scripts/smoke_exports.py
git commit -m "feat: portfolio holdings XLSX export endpoint"
```

---

## Task 7: Frontend `<DownloadXLSXButton>` component

**Files:**
- Create: `frontend/src/components/DownloadXLSXButton.jsx`

- [ ] **Step 1: Implement the component**

Create `frontend/src/components/DownloadXLSXButton.jsx`:

```jsx
import { useState } from 'react'

/** Authed XLSX download. Performs an authenticated fetch (Clerk Bearer token
 *  from `frontend/src/api.js` token getter), reads the response as a blob,
 *  and triggers a native browser download via an injected <a download>. */
export default function DownloadXLSXButton({ url, filenameFallback = 'export', label = 'Download XLSX' }) {
  const [busy, setBusy] = useState(false)
  const [err,  setErr]  = useState(null)

  async function handleClick() {
    if (busy) return
    setBusy(true); setErr(null)
    try {
      const token = await window.Clerk?.session?.getToken()
      const res = await fetch(url, {
        headers: {
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'ngrok-skip-browser-warning': 'true',
        },
        cache: 'no-store',
      })
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)

      // Prefer the filename the server told us; fall back to the prop.
      const cd = res.headers.get('content-disposition') || ''
      const m = /filename="([^"]+)"/.exec(cd)
      const downloadName = m ? m[1] : `${filenameFallback}.xlsx`

      const blob = await res.blob()
      const blobUrl = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = blobUrl
      a.download = downloadName
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(blobUrl)
    } catch (e) {
      setErr(e.message || 'download failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <button
      onClick={handleClick}
      disabled={busy}
      title={err ? `Failed: ${err}` : label}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        padding: '6px 10px',
        borderRadius: 6,
        border: '1px solid #d0d7de',
        background: busy ? '#f3f4f6' : '#ffffff',
        color: '#1f2937',
        cursor: busy ? 'wait' : 'pointer',
        fontSize: 13,
      }}
    >
      <span aria-hidden="true">⬇</span>
      <span>{busy ? 'Preparing…' : (err ? 'Retry XLSX' : 'XLSX')}</span>
    </button>
  )
}
```

- [ ] **Step 2: Manual smoke check the component compiles**

In a separate terminal:

```bash
cd frontend && npm run build
```

Expected: build completes with no JSX/TS errors. (`vite build` is fine even if it warns about unused chunks — only red errors block.)

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/DownloadXLSXButton.jsx
git commit -m "feat: DownloadXLSXButton component (authed blob download)"
```

---

## Task 8: Mount the button on SignalsPage

**Files:**
- Modify: `frontend/src/pages/SignalsPage.jsx`

- [ ] **Step 1: Read the current page to find where the filter/header bar is**

```bash
grep -n "filters\|Filters\|signal=\|search=\|months=" frontend/src/pages/SignalsPage.jsx | head -20
```

Identify the JSX region that already holds the filter controls / page title. The button sits beside that.

- [ ] **Step 2: Wire it up**

At the top of `SignalsPage.jsx`, add:

```jsx
import DownloadXLSXButton from '../components/DownloadXLSXButton'
```

Inside the JSX, alongside the filter bar (or directly to the right of the page title), add:

```jsx
<DownloadXLSXButton
  url={`/api/signals/export.xlsx?${new URLSearchParams({
    ...(signal     ? { signal }     : {}),
    ...(search     ? { search }     : {}),
    ...(months     ? { months }     : {}),
    ...(sortBy     ? { sort_by: sortBy }       : {}),
    ...(sortDir    ? { sort_dir: sortDir }     : {}),
  }).toString()}`}
  filenameFallback="signals"
/>
```

(Substitute the actual state-variable names the page uses — find them via the grep in Step 1. The pattern is: include the param iff its state is truthy.)

- [ ] **Step 3: Verify visually**

Start the dev stack:

```bash
venv/bin/uvicorn backend.main:app --port 8000 &
(cd frontend && npm run dev) &
```

Open http://localhost:3000/, log in, navigate to the Signals page. Confirm:
- Button is visible next to the filter bar.
- Clicking it produces a `signals_<YYYY-MM-DD>.xlsx` download.
- Open the file: column count matches the spec (38 signals columns + 5 company refs ≈ 43 columns).

Then:

```bash
kill %1 %2 2>/dev/null
```

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/SignalsPage.jsx
git commit -m "feat: SignalsPage XLSX download button"
```

---

## Task 9: Mount two buttons on TradesPage

**Files:**
- Modify: `frontend/src/pages/TradesPage.jsx`

- [ ] **Step 1: Read the page for date-range / market state**

```bash
grep -n "start\|end\|market\|Historical\|Open Trades" frontend/src/pages/TradesPage.jsx | head -25
```

Identify the section header for **Historical Trades** and for **Open Trades**, and locate the state variables holding the date range.

- [ ] **Step 2: Wire both buttons**

Add the import:

```jsx
import DownloadXLSXButton from '../components/DownloadXLSXButton'
```

Beside the **Historical Trades** section header:

```jsx
<DownloadXLSXButton
  url={`/api/trades/export.xlsx?${new URLSearchParams({
    ...(start  ? { start }  : {}),
    ...(end    ? { end }    : {}),
    ...(market ? { market } : {}),
  }).toString()}`}
  filenameFallback="trades_closed"
/>
```

Beside the **Open Trades** section header:

```jsx
<DownloadXLSXButton
  url={`/api/trades/open/export.xlsx${market ? `?market=${encodeURIComponent(market)}` : ''}`}
  filenameFallback="trades_open"
/>
```

- [ ] **Step 3: Verify visually**

Restart dev stack (Task 8 Step 3 commands), navigate to Trades. Confirm both buttons download distinct files (`trades_closed_*.xlsx`, `trades_open_*.xlsx`). Apply a date filter, re-download, confirm row count drops to match the visible table.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/TradesPage.jsx
git commit -m "feat: TradesPage XLSX download buttons (closed + open)"
```

---

## Task 10: Mount the button on WatchlistPage

**Files:**
- Modify: `frontend/src/pages/WatchlistPage.jsx`

- [ ] **Step 1: Find the currently-selected watchlist id**

```bash
grep -n "selectedListId\|currentList\|activeList\|list_id" frontend/src/pages/WatchlistPage.jsx | head -10
```

- [ ] **Step 2: Wire the button**

Add the import:

```jsx
import DownloadXLSXButton from '../components/DownloadXLSXButton'
```

Beside the watchlist tickers table header:

```jsx
{currentListId && (
  <DownloadXLSXButton
    url={`/api/watchlists/${currentListId}/export.xlsx`}
    filenameFallback="watchlist"
  />
)}
```

(Use the actual state variable name from Step 1 — `currentListId` is illustrative.)

- [ ] **Step 3: Verify visually**

Restart dev stack, navigate to a watchlist. Confirm the button is gated on a list being selected, downloads `watchlist_<name>_<date>.xlsx`, and row count = ticker count of the watchlist.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/WatchlistPage.jsx
git commit -m "feat: WatchlistPage XLSX download button"
```

---

## Task 11: Mount the button on PortfolioPage

**Files:**
- Modify: `frontend/src/pages/PortfolioPage.jsx`

- [ ] **Step 1: Wire the button**

Add the import:

```jsx
import DownloadXLSXButton from '../components/DownloadXLSXButton'
```

Beside the holdings table header:

```jsx
<DownloadXLSXButton
  url="/api/portfolio/holdings/export.xlsx"
  filenameFallback="portfolio_holdings"
/>
```

- [ ] **Step 2: Verify visually**

Restart dev stack, navigate to Portfolio. Confirm button is present, downloads `portfolio_holdings_<date>.xlsx`. Open the file: row count should match the holdings table; one extra `watchlists` column lists which watchlists each ticker belongs to.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/PortfolioPage.jsx
git commit -m "feat: PortfolioPage XLSX download button"
```

---

## Task 12: Deploy to production

**Files:** none (deploy step)

- [ ] **Step 1: Push all commits**

```bash
git push origin main
```

- [ ] **Step 2: Pull + install dep + rebuild on the server**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 "cd /opt/vesign && git pull && venv/bin/pip install openpyxl==3.1.5 && cd frontend && npm run build && systemctl restart vesign"
```

Expected output: `openpyxl-3.1.5` installed, `vite build` finishes, `systemctl restart` is silent.

- [ ] **Step 3: Verify endpoints respond on the server**

```bash
for p in /api/signals/export.xlsx /api/trades/export.xlsx /api/trades/open/export.xlsx /api/watchlists/1/export.xlsx /api/portfolio/holdings/export.xlsx; do
  printf "%s -> " "$p"
  curl -s -o /dev/null -w "%{http_code}\n" "https://ve-sign.com$p"
done
```

Expected: each prints `401` (unauthenticated calls — auth is correctly enforced).

- [ ] **Step 4: Manual end-to-end on ve-sign.com**

In a real browser, log in, then for each of the 5 buttons:
1. Click the button.
2. Confirm a `.xlsx` file downloads.
3. Open it; confirm sheet name matches the spec (`signals` / `trades` / `open_trades` / `watchlist` / `holdings`) and row count matches what was on screen (after filters).
4. Confirm the workbook has more columns than the on-screen table (this is the whole point of the feature — full DB columns).

- [ ] **Step 5: No commit needed** (deploy step makes no local changes).
