# Fix ML Lookahead Leak Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate ML lookahead bias by backfilling 2018-2019 training data, retraining the prediction model walk-forward (one model per calendar quarter, trained only on data strictly older than the prediction window), and rebuilding the `predictions`, `signals`, and `trade_log` tables so the historical track record reflects what the system would actually have produced in real time.

**Architecture:** Five-phase change executed against the production droplet (134.209.82.105, `/opt/vesign/`). Phase 1 backfills 2018-2019 prices/VIX/features/forward_returns so each future quarterly cutoff has a 730-day warmup window of training data. Phase 2 introduces `models/walk_forward.py` — quarterly retrain + per-period prediction with a `model_cutoff` audit column on the predictions row. Phase 3 atomically swaps the contaminated `predictions` table for the rebuilt one after passing a no-leak verification. Phase 4 cascades the rebuild downstream (signals → trade_log) using the engine's existing `target_date` mode. Phase 5 wires periodic retraining into `production/run_daily.py` so the leak cannot recur.

All work runs on the **production droplet** (server-first rule). Each phase ends in a verification step that must pass before moving on. The `vesign.service` is stopped during heavy rebuild steps to avoid OOM (4 GB RAM + 2 GB swap).

**Tech Stack:** Python 3, pandas, scikit-learn / XGBoost, SQLite (3.4 GB DB), SQLAlchemy, FMP API (Premium tier), yfinance (VIX only), pytest 9.x.

---

## File Structure

**New files (in `/opt/vesign/`, mirrored in local repo):**

| Path | Responsibility |
|---|---|
| `scripts/backfill_prices_2018_2019.py` | One-shot loader: fetch 2018-01-01 → 2019-12-31 prices for current US universe via FMP, append to `daily_prices` |
| `scripts/backfill_vix_2018_2019.py` | One-shot loader: fetch 2018-2019 `^VIX` via yfinance, append to `vix` |
| `scripts/recompute_features_full.py` | Recompute `features` table from 2018-01-01 onwards (full rebuild, chunked by ticker) |
| `scripts/recompute_forward_returns_full.py` | Re-run existing `compute_forward_returns()` so labels include 2018-2019 |
| `models/walk_forward.py` | Quarterly retrain + period-bounded prediction; produces `(date, ticker, pred_5d, pred_20d, prediction_score, model_cutoff)` rows |
| `scripts/rebuild_predictions_walk.py` | Wipe → repopulate `predictions_walk` staging table by walking quarterly cutoffs from 2020-01-02 to today |
| `scripts/verify_no_leak.py` | Assert `model_cutoff <= date` for every prediction row; fail loudly otherwise |
| `scripts/swap_predictions_table.py` | Atomic SWAP: `predictions` ↔ `predictions_walk` inside a transaction |
| `scripts/rebuild_signals_from_date.py` | Wipe `signals` from 2020-01-02 and re-run `run_scoring(target_date=d)` chronologically |
| `tests/models/test_walk_forward.py` | pytest: cutoff math, no-leak invariants, model file path discipline |

**Modified files:**

| Path | Change |
|---|---|
| `models/train.py:50-58` | Add docstring example showing `train_end_date` walk-forward usage; no behavior change |
| `production/run_daily.py:397` | Call `walk_forward.maybe_retrain_for_today()` before `run_prediction_engine()` so a new pickle is saved at each quarter boundary |
| `CLAUDE.md` (repo root) | Document walk-forward architecture so future me / contributors don't reintroduce the leak |
| `tests/__init__.py` (touch only if missing) | Ensure pytest discovers `tests/models/` |

**Schema change:** `predictions` gains one column — `model_cutoff TEXT NOT NULL`. Backward-compat: existing reads (`SELECT pred_5d, pred_20d, prediction_score FROM predictions`) keep working.

---

## Pre-flight (before any task)

- [ ] **Pre-flight 1: SSH into prod and confirm environment**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  '/opt/vesign/venv/bin/python -c "import xgboost, pandas, sqlalchemy, sklearn, ta, yfinance; print(\"deps ok\")"'
```

Expected: `deps ok`

- [ ] **Pre-flight 2: Snapshot the production DB**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'systemctl stop vesign && cp /opt/vesign/vesign.db /opt/vesign/vesign.db.pre_walkforward_$(date +%Y%m%d) && systemctl start vesign && ls -lh /opt/vesign/vesign.db*'
```

Expected: two files, both ~3.4 GB. Keeps a known-good rollback.

- [ ] **Pre-flight 3: Capture baseline track-record numbers**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3
c = sqlite3.connect(\"/opt/vesign/vesign.db\")
n, w, avg = c.execute(\"\"\"
SELECT COUNT(*), SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END), AVG(return_pct)
FROM trade_log WHERE buy_date>=\"2021-01-04\" AND sell_date IS NOT NULL
\"\"\").fetchone()
print(f\"baseline trade_log 2021+: n={n} wins={w} wr={w/n*100:.1f}% avg={avg*100:+.2f}%\")
"'
```

Save the output — this is the *contaminated* baseline we expect to drop after the rebuild.

---

## Phase 1: Backfill 2018-2019 training data

### Task 1.1: Backfill `daily_prices` for 2018-2019

**Files:**
- Create: `scripts/backfill_prices_2018_2019.py`

- [ ] **Step 1: Write the failing assertion**

Create `scripts/verify_2018_prices.py`:

```python
"""Asserts the 2018-2019 backfill is in place."""
import sqlite3, sys
c = sqlite3.connect("/opt/vesign/vesign.db")
n_2018 = c.execute("SELECT COUNT(*) FROM daily_prices WHERE date >= '2018-01-01' AND date < '2019-01-01'").fetchone()[0]
n_2019 = c.execute("SELECT COUNT(*) FROM daily_prices WHERE date >= '2019-01-01' AND date < '2020-01-01'").fetchone()[0]
tickers_with_2018 = c.execute("SELECT COUNT(DISTINCT ticker) FROM daily_prices WHERE date >= '2018-01-01' AND date < '2019-01-01'").fetchone()[0]
print(f"2018 rows: {n_2018:,}  2019 rows: {n_2019:,}  tickers with 2018 history: {tickers_with_2018}")
# 252 trading days/year * ~1000 historical-living tickers ≈ 250k rows/year minimum
if n_2018 < 200_000 or n_2019 < 200_000 or tickers_with_2018 < 800:
    print("FAIL: 2018-2019 backfill incomplete")
    sys.exit(1)
print("OK")
```

- [ ] **Step 2: Run the assertion to confirm it fails**

```bash
scp -i ~/.ssh/id_vesign scripts/verify_2018_prices.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_2018_prices.py'
```

Expected: `FAIL: 2018-2019 backfill incomplete` (no 2018-2019 rows yet).

- [ ] **Step 3: Implement the backfill script**

Create `scripts/backfill_prices_2018_2019.py`:

```python
"""Fetch 2018-01-01 → 2019-12-31 prices for current US universe via FMP.

Idempotent: re-running skips tickers that already have 2018-01 rows.
Memory-conscious: appends per-ticker, no cross-ticker concat in pandas.
"""
import sys, time
import pandas as pd
from sqlalchemy import text
from data.loaders import engine
from data.fmp import historical_prices

START = "2018-01-01"
END = "2019-12-31"

def already_has_2018(ticker: str) -> bool:
    with engine.connect() as c:
        n = c.execute(
            text("SELECT COUNT(*) FROM daily_prices WHERE ticker=:t AND date < '2019-01-01' AND date >= '2018-01-01'"),
            {"t": ticker},
        ).scalar()
    return n > 0

def main():
    with engine.connect() as c:
        tickers = [r[0] for r in c.execute(text(
            "SELECT DISTINCT ticker FROM companies WHERE ticker NOT LIKE '%.TA' ORDER BY ticker"
        ))]
    print(f"Backfilling {len(tickers)} tickers for {START}..{END}")

    ok, skipped, missing, errors = 0, 0, 0, 0
    for i, t in enumerate(tickers, 1):
        if already_has_2018(t):
            skipped += 1
            continue
        try:
            df = historical_prices(t, START, END)
        except Exception as e:
            print(f"  [{i}/{len(tickers)}] {t}: ERROR {e}")
            errors += 1
            time.sleep(1)
            continue
        if df is None or df.empty:
            missing += 1
            continue
        df.to_sql("daily_prices", engine, if_exists="append", index=False)
        ok += 1
        if i % 50 == 0:
            print(f"  [{i}/{len(tickers)}] ok={ok} skipped={skipped} missing={missing} errors={errors}")
    print(f"DONE: ok={ok} skipped={skipped} missing={missing} errors={errors}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Stop vesign, run the backfill, restart**

```bash
scp -i ~/.ssh/id_vesign scripts/backfill_prices_2018_2019.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'systemctl stop vesign && cd /opt/vesign && /opt/vesign/venv/bin/python scripts/backfill_prices_2018_2019.py 2>&1 | tee /var/log/vesign-backfill-2018.log; systemctl start vesign'
```

Expected runtime: ~30 min (1500 tickers × FMP rate limit). The `historical_prices` helper in `data/fmp.py` already handles 429 backoff.

- [ ] **Step 5: Run the assertion again to confirm it passes**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_2018_prices.py'
```

Expected: `OK` with both 2018 and 2019 rows ≥ 200,000 and at least 800 tickers carrying 2018 history.

- [ ] **Step 6: Commit**

```bash
git add scripts/backfill_prices_2018_2019.py scripts/verify_2018_prices.py
git commit -m "feat(scripts): backfill 2018-2019 daily_prices for ML training warmup"
```

### Task 1.2: Backfill VIX for 2018-2019

**Files:**
- Create: `scripts/backfill_vix_2018_2019.py`

- [ ] **Step 1: Implement the script**

```python
"""Fetch ^VIX for 2018-01-01 → 2019-12-31 via yfinance and append to vix table."""
import yfinance as yf
import pandas as pd
from sqlalchemy import text
from data.loaders import engine

START = "2018-01-01"
END = "2020-01-01"  # yfinance end is exclusive

def main():
    with engine.connect() as c:
        n = c.execute(text(
            "SELECT COUNT(*) FROM vix WHERE date >= :s AND date < :e"
        ), {"s": START, "e": END}).scalar()
    if n > 100:
        print(f"VIX already populated for {START}..{END} ({n} rows). Skipping.")
        return

    data = yf.download("^VIX", start=START, end=END, auto_adjust=False, progress=False)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.droplevel(1)
    data = data.reset_index()
    data["date"] = pd.to_datetime(data["Date"]).dt.strftime("%Y-%m-%d")
    out = pd.DataFrame({
        "date": data["date"],
        "vix_open":  data["Open"],
        "vix_high":  data["High"],
        "vix_low":   data["Low"],
        "vix_close": data["Close"],
    })
    out.to_sql("vix", engine, if_exists="append", index=False)
    print(f"Inserted {len(out)} VIX rows for {START}..{END}")

if __name__ == "__main__":
    main()
```

> NOTE: schema check before running. The current `vix` table column names are based on what `update_vix()` writes. Run `PRAGMA table_info(vix)` first if uncertain — adjust column names in the `out` DataFrame to match exactly.

- [ ] **Step 2: Verify vix table schema first**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3; c=sqlite3.connect(\"/opt/vesign/vesign.db\")
print([r[1] for r in c.execute(\"PRAGMA table_info(vix)\").fetchall()])
print(\"first row:\", c.execute(\"SELECT * FROM vix LIMIT 1\").fetchone())
"'
```

Adjust the column names in Step 1's `out` DataFrame to match the actual schema before running.

- [ ] **Step 3: Run the backfill**

```bash
scp -i ~/.ssh/id_vesign scripts/backfill_vix_2018_2019.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/backfill_vix_2018_2019.py'
```

Expected: `Inserted ~503 VIX rows` (≈252 trading days/year × 2).

- [ ] **Step 4: Verify**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3; c=sqlite3.connect(\"/opt/vesign/vesign.db\")
print(c.execute(\"SELECT MIN(date), MAX(date), COUNT(*) FROM vix WHERE date < \\\"2020-01-01\\\"\").fetchone())
"'
```

Expected: min ≈ 2018-01-02, max ≈ 2019-12-31, count ≈ 500.

- [ ] **Step 5: Commit**

```bash
git add scripts/backfill_vix_2018_2019.py
git commit -m "feat(scripts): backfill 2018-2019 VIX for ML training warmup"
```

### Task 1.3: Recompute `features` table from 2018-01-01 onwards

**Files:**
- Create: `scripts/recompute_features_full.py`

- [ ] **Step 1: Implement the recompute**

```python
"""Wipe and rebuild the features table from 2018-01-01 onwards.

Processes one ticker at a time to avoid concatenating 2.5M rows in memory
(droplet is 4GB RAM + 2GB swap).
"""
import gc
import pandas as pd
from sqlalchemy import text
from data.loaders import engine
from features.technical_indicators import add_indicators

def main():
    with engine.begin() as c:
        c.execute(text("DELETE FROM features"))
        tickers = [r[0] for r in c.execute(text(
            "SELECT DISTINCT ticker FROM daily_prices WHERE ticker NOT LIKE '%.TA' ORDER BY ticker"
        ))]
    print(f"Recomputing features for {len(tickers)} tickers")

    written = 0
    for i, t in enumerate(tickers, 1):
        prices = pd.read_sql(
            text("SELECT date,ticker,open,high,low,close,volume FROM daily_prices "
                 "WHERE ticker=:t AND date >= '2018-01-01' ORDER BY date"),
            engine, params={"t": t},
        )
        if prices.empty:
            continue
        feat = add_indicators(prices.copy())
        feat.to_sql("features", engine, if_exists="append", index=False)
        written += len(feat)
        del prices, feat
        if i % 100 == 0:
            gc.collect()
            print(f"  [{i}/{len(tickers)}] rows written: {written:,}")
    print(f"DONE: {written:,} feature rows written")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Stop vesign, run, restart**

```bash
scp -i ~/.ssh/id_vesign scripts/recompute_features_full.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'systemctl stop vesign && cd /opt/vesign && /opt/vesign/venv/bin/python scripts/recompute_features_full.py 2>&1 | tee /var/log/vesign-features-rebuild.log; systemctl start vesign'
```

Expected runtime: ~5-10 min.

- [ ] **Step 3: Verify**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3; c=sqlite3.connect(\"/opt/vesign/vesign.db\")
print(c.execute(\"SELECT MIN(date), MAX(date), COUNT(*) FROM features\").fetchone())
print(\"NaN rsi_factor at end-2019:\",
  c.execute(\"SELECT COUNT(*) FROM features WHERE date >= \\\"2019-12-01\\\" AND date < \\\"2020-01-01\\\" AND rsi_factor IS NULL\").fetchone())
print(\"NaN week52_high at start-2019:\",
  c.execute(\"SELECT COUNT(*) FROM features WHERE date >= \\\"2019-01-01\\\" AND date < \\\"2019-02-01\\\" AND week52_high IS NULL\").fetchone())
"'
```

Expected: min date 2018-01-02, max date today; very few NaN rsi_factor at end of 2019 (warmup complete by then). Some NaN week52_high in early 2019 is expected (rolling(252) needs 252 days = ~1 year of warmup, so values are valid from ~2019-01 onwards).

- [ ] **Step 4: Commit**

```bash
git add scripts/recompute_features_full.py
git commit -m "feat(scripts): rebuild features table from 2018-01-01 onwards"
```

### Task 1.4: Recompute `forward_returns` from 2018-01-01

**Files:**
- Create: `scripts/recompute_forward_returns_full.py`

- [ ] **Step 1: Implement (thin wrapper)**

```python
"""Re-run compute_forward_returns() — picks up the new 2018-2019 prices automatically."""
from features.forward_returns import compute_forward_returns

if __name__ == "__main__":
    compute_forward_returns()
    print("DONE")
```

- [ ] **Step 2: Run**

```bash
scp -i ~/.ssh/id_vesign scripts/recompute_forward_returns_full.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'systemctl stop vesign && cd /opt/vesign && /opt/vesign/venv/bin/python scripts/recompute_forward_returns_full.py; systemctl start vesign'
```

- [ ] **Step 3: Verify**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3; c=sqlite3.connect(\"/opt/vesign/vesign.db\")
print(c.execute(\"SELECT MIN(date), MAX(date), COUNT(*) FROM forward_returns\").fetchone())
print(\"2018 rows:\", c.execute(\"SELECT COUNT(*) FROM forward_returns WHERE date < \\\"2019-01-01\\\"\").fetchone())
"'
```

Expected: min date ~ 2018-01-02; ≥150,000 rows for 2018.

- [ ] **Step 4: Commit**

```bash
git add scripts/recompute_forward_returns_full.py
git commit -m "feat(scripts): rebuild forward_returns including 2018-2019"
```

---

## Phase 2: Walk-forward training module

### Task 2.1: Add `model_cutoff` audit column to predictions schema

**Files:**
- Create: `scripts/migrate_predictions_add_cutoff.py`

- [ ] **Step 1: Implement migration**

```python
"""Add model_cutoff TEXT column to predictions if not present.

Walk-forward rebuild will populate it. Existing rows get '' (will be overwritten
by the rebuild). The verify_no_leak script treats '' as INVALID — so the rebuild
MUST repopulate every row before verification passes.
"""
import sqlite3
DB = "/opt/vesign/vesign.db"

def main():
    c = sqlite3.connect(DB)
    cols = {r[1] for r in c.execute("PRAGMA table_info(predictions)").fetchall()}
    if "model_cutoff" in cols:
        print("model_cutoff already present")
        return
    c.execute("ALTER TABLE predictions ADD COLUMN model_cutoff TEXT NOT NULL DEFAULT ''")
    c.commit()
    print("model_cutoff column added (default '')")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run**

```bash
scp -i ~/.ssh/id_vesign scripts/migrate_predictions_add_cutoff.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/migrate_predictions_add_cutoff.py'
```

- [ ] **Step 3: Commit**

```bash
git add scripts/migrate_predictions_add_cutoff.py
git commit -m "feat(schema): add model_cutoff audit column to predictions"
```

### Task 2.2: Build `models/walk_forward.py`

**Files:**
- Create: `models/walk_forward.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/models/test_walk_forward.py`:

```python
"""Tests for walk-forward retrain/predict orchestrator."""
from datetime import date
import pytest
from models.walk_forward import quarterly_cutoffs, models_dir_for, NoLeakError

def test_quarterly_cutoffs_basic():
    """Cutoffs must be the 1st of Jan/Apr/Jul/Oct, no exceptions."""
    cuts = quarterly_cutoffs(date(2020,1,2), date(2021,5,15))
    expected = [date(2020,1,1), date(2020,4,1), date(2020,7,1), date(2020,10,1),
                date(2021,1,1), date(2021,4,1)]
    assert cuts == expected

def test_quarterly_cutoffs_empty_when_end_before_start():
    assert quarterly_cutoffs(date(2024,5,1), date(2024,4,1)) == []

def test_quarterly_cutoffs_inclusive_of_end_quarter():
    """If end is 2024-12-31, cutoff list ends at 2024-10-01 (the Q4 retrain)."""
    cuts = quarterly_cutoffs(date(2024,1,2), date(2024,12,31))
    assert cuts[-1] == date(2024,10,1)

def test_models_dir_for_includes_iso_date():
    p = models_dir_for(date(2024, 4, 1))
    assert p.name == "2024-04-01"
    assert "ml_models" in str(p) and "walk" in str(p)
```

- [ ] **Step 2: Run tests; expect ImportError**

```bash
cd /Users/inovenos/PycharmProjects/Vesign && venv/bin/python -m pytest tests/models/test_walk_forward.py -v
```

Expected: collection error — `models.walk_forward` doesn't exist yet.

- [ ] **Step 3: Implement `models/walk_forward.py`**

```python
"""Walk-forward training + prediction.

Quarterly cadence: at each Jan/Apr/Jul/Oct 1st cutoff, retrain a new set of
sector + global XGBoost models using `train_factor_weights(train_end_date=cutoff)`.
The trained pickles are stored under `ml_models/walk/{YYYY-MM-DD}/`.

For predicting any date d, we use the model from the LATEST cutoff <= d.
Concretely: predictions for [Q_n, Q_{n+1}) use models from cutoff Q_n.

This module is the only place predictions get written. The output rows carry
the `model_cutoff` audit field so verify_no_leak.py can prove that no row was
predicted by a model trained on data >= the row's date.
"""
import os
import gc
import pickle
import shutil
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from data.loaders import engine
from models.train import train_factor_weights, FEATURE_COLS

ML_WALK_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "ml_models" / "walk"
PREDICTIONS_TABLE = "predictions_walk"  # staging — atomic-swapped into `predictions` later


class NoLeakError(AssertionError):
    pass


def quarterly_cutoffs(start: date, end: date) -> list[date]:
    """Return the list of quarter-start dates that fall within [start, end].

    A quarter-start is the 1st of Jan, Apr, Jul, Oct. The first cutoff is the
    smallest quarter-start >= `start`. The last cutoff is the largest
    quarter-start <= `end`.
    """
    if end < start:
        return []
    QUARTER_MONTHS = (1, 4, 7, 10)
    cuts: list[date] = []
    y = start.year
    while True:
        for m in QUARTER_MONTHS:
            d = date(y, m, 1)
            if d < start:
                continue
            if d > end:
                return cuts
            cuts.append(d)
        y += 1


def models_dir_for(cutoff: date) -> Path:
    return ML_WALK_DIR / cutoff.isoformat()


def train_for_cutoff(cutoff: date, force: bool = False) -> Path:
    """Train sector + global XGBoost models with train_end_date=cutoff.

    Saves pickles to ml_models/walk/{cutoff}/. If the directory already exists
    and force=False, returns the existing path without retraining.
    """
    dest = models_dir_for(cutoff)
    if dest.exists() and not force:
        return dest

    # train.py writes to ml_models/ (root). We swap MLM_MODELS_DIR via env,
    # train, then move the pickles into our walk-forward subdir.
    import models.train as train_mod
    original_dir = train_mod.ML_MODELS_DIR
    staging = ML_WALK_DIR / f"_staging_{cutoff.isoformat()}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    train_mod.ML_MODELS_DIR = str(staging)
    try:
        train_factor_weights(train_end_date=cutoff.isoformat())
    finally:
        train_mod.ML_MODELS_DIR = original_dir

    if dest.exists():
        shutil.rmtree(dest)
    staging.rename(dest)
    return dest


def _load_model(path: Path):
    if not path.exists():
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def predict_period(cutoff: date, period_start: date, period_end_excl: date) -> int:
    """Predict for dates in [period_start, period_end_excl) using models at cutoff.

    Writes rows into PREDICTIONS_TABLE with model_cutoff=cutoff.isoformat().
    Returns the number of rows written. Idempotent per-date (deletes existing
    rows in the staging table for any date about to be re-predicted).
    """
    if cutoff > period_start:
        raise NoLeakError(
            f"Refusing to predict period [{period_start}, {period_end_excl}) "
            f"with model trained at cutoff {cutoff} — that would mean the model "
            f"was trained on data through cutoff but cutoff > period_start, which "
            f"is impossible. Caller bug."
        )

    mdir = models_dir_for(cutoff)
    global_5d = _load_model(mdir / "global_5d.pkl")
    global_20d = _load_model(mdir / "global_20d.pkl")
    if global_5d is None or global_20d is None:
        raise FileNotFoundError(f"Global models missing for cutoff {cutoff}: {mdir}")

    companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)
    sector_cache: dict = {}

    def _sector_models(sector):
        if sector in sector_cache:
            return sector_cache[sector]
        safe = str(sector).replace(" ", "_").replace("/", "_")
        m5 = _load_model(mdir / f"{safe}_5d.pkl") or global_5d
        m20 = _load_model(mdir / f"{safe}_20d.pkl") or global_20d
        sector_cache[sector] = (m5, m20)
        return m5, m20

    written = 0
    cur = period_start
    while cur < period_end_excl:
        feats = pd.read_sql(
            text("SELECT * FROM features WHERE DATE(date) = :d AND ticker NOT LIKE '%.TA'"),
            engine, params={"d": cur.isoformat()},
        )
        if feats.empty:
            cur = cur + timedelta(days=1)
            continue
        df = feats.merge(companies, on="ticker", how="left")
        valid = df[FEATURE_COLS].notna().all(axis=1)
        df["pred_5d"] = float("nan")
        df["pred_20d"] = float("nan")
        for sector, idx in df[valid].groupby("sector").groups.items():
            m5, m20 = _sector_models(sector)
            X = df.loc[idx, FEATURE_COLS]
            df.loc[idx, "pred_5d"] = m5.predict(X)
            df.loc[idx, "pred_20d"] = m20.predict(X)
        no_sec = valid & df["sector"].isna()
        if no_sec.any():
            X_ns = df.loc[no_sec, FEATURE_COLS]
            df.loc[no_sec, "pred_5d"] = global_5d.predict(X_ns)
            df.loc[no_sec, "pred_20d"] = global_20d.predict(X_ns)

        df["prediction_score"] = 0.6 * df["pred_5d"] + 0.4 * df["pred_20d"]
        df["model_cutoff"] = cutoff.isoformat()

        with engine.begin() as c:
            c.execute(text(f"DELETE FROM {PREDICTIONS_TABLE} WHERE DATE(date) = :d"), {"d": cur.isoformat()})
        df[["date", "ticker", "pred_5d", "pred_20d", "prediction_score", "model_cutoff"]].to_sql(
            PREDICTIONS_TABLE, engine, if_exists="append", index=False,
        )
        written += len(df)
        del feats, df
        gc.collect()
        cur = cur + timedelta(days=1)

    return written


def ensure_predictions_walk_table() -> None:
    """Create the staging table with the same shape as predictions plus model_cutoff."""
    with engine.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {PREDICTIONS_TABLE} (
                date TEXT, ticker TEXT,
                pred_5d FLOAT, pred_20d FLOAT, prediction_score FLOAT,
                model_cutoff TEXT NOT NULL
            )
        """))
        c.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{PREDICTIONS_TABLE}_date ON {PREDICTIONS_TABLE}(date)"))
        c.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{PREDICTIONS_TABLE}_ticker ON {PREDICTIONS_TABLE}(ticker)"))


def maybe_retrain_for_today() -> Path | None:
    """For production: if today's quarter-start has no walk model yet, train one.

    Idempotent — safe to call daily. Returns the model dir for today's cutoff,
    or None if no retrain was needed.
    """
    today = date.today()
    qstart_month = ((today.month - 1) // 3) * 3 + 1
    cutoff = date(today.year, qstart_month, 1)
    dest = models_dir_for(cutoff)
    if dest.exists():
        return None
    return train_for_cutoff(cutoff)
```

- [ ] **Step 4: Run tests, watch them pass**

```bash
cd /Users/inovenos/PycharmProjects/Vesign && venv/bin/python -m pytest tests/models/test_walk_forward.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add models/walk_forward.py tests/models/test_walk_forward.py
git commit -m "feat(models): add walk-forward train+predict module"
```

### Task 2.3: Build `verify_no_leak.py`

**Files:**
- Create: `scripts/verify_no_leak.py`

- [ ] **Step 1: Implement**

```python
"""Assert that every prediction row was generated by a model trained only on
data strictly older than the prediction date.

Definition: a prediction is leak-free iff DATE(date) >= model_cutoff.

Usage:
    python scripts/verify_no_leak.py predictions
    python scripts/verify_no_leak.py predictions_walk
"""
import sys, sqlite3

DB = "/opt/vesign/vesign.db"

def main(table: str = "predictions"):
    c = sqlite3.connect(DB)
    cols = {r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
    if "model_cutoff" not in cols:
        print(f"FAIL: {table} has no model_cutoff column — cannot verify")
        sys.exit(2)

    n_total = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    n_blank = c.execute(f"SELECT COUNT(*) FROM {table} WHERE model_cutoff = ''").fetchone()[0]
    n_leak  = c.execute(
        f"SELECT COUNT(*) FROM {table} WHERE model_cutoff != '' AND DATE(date) < DATE(model_cutoff)"
    ).fetchone()[0]

    print(f"{table}: {n_total:,} rows  |  blank model_cutoff: {n_blank:,}  |  leaks: {n_leak:,}")

    if n_blank > 0:
        sample = c.execute(
            f"SELECT date, ticker FROM {table} WHERE model_cutoff = '' LIMIT 5"
        ).fetchall()
        print("  sample blank rows:", sample)
    if n_leak > 0:
        sample = c.execute(
            f"SELECT date, ticker, model_cutoff FROM {table} "
            f"WHERE model_cutoff != '' AND DATE(date) < DATE(model_cutoff) LIMIT 5"
        ).fetchall()
        print("  sample leak rows:", sample)
        sys.exit(1)
    if n_blank > 0:
        sys.exit(1)
    print("OK: no leak detected")
    sys.exit(0)

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "predictions")
```

- [ ] **Step 2: Test on existing predictions table — expect FAIL (every row is blank)**

```bash
scp -i ~/.ssh/id_vesign scripts/verify_no_leak.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_no_leak.py predictions; echo "exit=$?"'
```

Expected: failure showing all 2.5M rows have blank `model_cutoff`. This is correct — current predictions are uncertified.

- [ ] **Step 3: Commit**

```bash
git add scripts/verify_no_leak.py
git commit -m "feat(scripts): add no-leak verifier for predictions tables"
```

### Task 2.4: Single-quarter dry run

- [ ] **Step 1: Run a dry-run for 2024-Q1 only**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
from datetime import date
from models.walk_forward import (
    train_for_cutoff, predict_period, ensure_predictions_walk_table, PREDICTIONS_TABLE
)
ensure_predictions_walk_table()
cutoff = date(2024, 1, 1)
mdir = train_for_cutoff(cutoff)
print(f\"trained → {mdir}\")
n = predict_period(cutoff, date(2024, 1, 1), date(2024, 4, 1))
print(f\"predicted: {n} rows\")
"'
```

Expected: trains a 2024-01-01 model from 2022-2023 data, predicts ~80k rows for Q1 2024.

- [ ] **Step 2: Verify the staging table is leak-free for that quarter**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_no_leak.py predictions_walk'
```

Expected: `OK: no leak detected`.

- [ ] **Step 3: Sanity-check magnitudes**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3
c = sqlite3.connect(\"/opt/vesign/vesign.db\")
print(\"row count:\", c.execute(\"SELECT COUNT(*) FROM predictions_walk\").fetchone())
print(\"date range:\", c.execute(\"SELECT MIN(date), MAX(date) FROM predictions_walk\").fetchone())
print(\"cutoffs used:\", c.execute(\"SELECT DISTINCT model_cutoff FROM predictions_walk\").fetchall())
print(\"score stats:\", c.execute(\"SELECT MIN(prediction_score), AVG(prediction_score), MAX(prediction_score) FROM predictions_walk\").fetchone())
"'
```

Expected: ~80k rows, dates 2024-01-02 through 2024-03-29 (last trading day of Q1), one cutoff = 2024-01-01, prediction_score in roughly [-0.2, +0.2]. If avg is wildly different from current production predictions, investigate before proceeding.

- [ ] **Step 4: Commit (no code change in this task — verification only)**

No commit; if numbers look reasonable proceed to Task 3.1.

---

## Phase 3: Full predictions rebuild

### Task 3.1: Build `rebuild_predictions_walk.py` orchestrator

**Files:**
- Create: `scripts/rebuild_predictions_walk.py`

- [ ] **Step 1: Implement**

```python
"""Wipe and rebuild predictions_walk from 2020-01-02 to today using walk-forward.

Quarterly cadence: for each Q in [2020-Q1, ..., this-quarter):
    - train models with train_end_date = Q.start
    - predict every date in [Q.start, Q+1.start) using those models
For the current open quarter:
    - train with cutoff = current-quarter.start
    - predict from current-quarter.start to today

After this script finishes, scripts/verify_no_leak.py predictions_walk MUST pass.
"""
from datetime import date, timedelta
from sqlalchemy import text
from data.loaders import engine
from models.walk_forward import (
    quarterly_cutoffs, train_for_cutoff, predict_period,
    ensure_predictions_walk_table, PREDICTIONS_TABLE,
)

WALK_START = date(2020, 1, 1)

def main():
    today = date.today()
    ensure_predictions_walk_table()

    with engine.begin() as c:
        c.execute(text(f"DELETE FROM {PREDICTIONS_TABLE}"))

    cuts = quarterly_cutoffs(WALK_START, today)
    print(f"Cutoffs: {len(cuts)}  ({cuts[0]} ... {cuts[-1]})")

    total = 0
    for i, cutoff in enumerate(cuts):
        period_end = cuts[i + 1] if i + 1 < len(cuts) else (today + timedelta(days=1))
        print(f"[{i+1}/{len(cuts)}] cutoff={cutoff} period=[{cutoff},{period_end})")
        train_for_cutoff(cutoff)
        n = predict_period(cutoff, cutoff, period_end)
        total += n
        print(f"  wrote {n:,} rows  (running total: {total:,})")
    print(f"DONE: {total:,} rows written to {PREDICTIONS_TABLE}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run**

```bash
scp -i ~/.ssh/id_vesign scripts/rebuild_predictions_walk.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'systemctl stop vesign && cd /opt/vesign && /opt/vesign/venv/bin/python scripts/rebuild_predictions_walk.py 2>&1 | tee /var/log/vesign-walk-rebuild.log; systemctl start vesign'
```

Expected runtime: ~30-60 min (25 quarters × (training + per-day prediction loops)).

- [ ] **Step 3: Verify no-leak**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_no_leak.py predictions_walk'
```

Expected: `OK: no leak detected`. **If this fails, do not proceed.** Re-investigate `predict_period` and `train_for_cutoff`.

- [ ] **Step 4: Sanity-check coverage**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3; c=sqlite3.connect(\"/opt/vesign/vesign.db\")
print(\"rows:\", c.execute(\"SELECT COUNT(*) FROM predictions_walk\").fetchone())
print(\"range:\", c.execute(\"SELECT MIN(date), MAX(date) FROM predictions_walk\").fetchone())
print(\"distinct cutoffs:\", c.execute(\"SELECT COUNT(DISTINCT model_cutoff) FROM predictions_walk\").fetchone())
"'
```

Expected: ≥2.4M rows, range 2020-01-02 → today, ≥25 distinct cutoffs.

- [ ] **Step 5: Commit**

```bash
git add scripts/rebuild_predictions_walk.py
git commit -m "feat(scripts): rebuild predictions table walk-forward"
```

### Task 3.2: Atomic swap predictions ↔ predictions_walk

**Files:**
- Create: `scripts/swap_predictions_table.py`

- [ ] **Step 1: Implement**

```python
"""Atomic swap: predictions ← predictions_walk.

Renames the contaminated `predictions` to `predictions_legacy` (keeps as backup
until manually dropped) and renames `predictions_walk` to `predictions`. Uses a
single transaction so a crash mid-swap leaves a valid (if old) state.
"""
import sys, sqlite3
DB = "/opt/vesign/vesign.db"

def main():
    c = sqlite3.connect(DB)
    cur = c.cursor()
    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type=\"table\"")}
    if "predictions_walk" not in tables:
        print("FAIL: predictions_walk does not exist")
        sys.exit(1)
    if "predictions_legacy" in tables:
        print("FAIL: predictions_legacy already exists; manually clean up first")
        sys.exit(1)

    cur.execute("BEGIN")
    try:
        cur.execute("ALTER TABLE predictions RENAME TO predictions_legacy")
        cur.execute("ALTER TABLE predictions_walk RENAME TO predictions")
        c.commit()
    except Exception:
        c.rollback()
        raise
    print("OK: predictions swapped (old kept as predictions_legacy)")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run final verification on predictions_walk one more time, then swap**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_no_leak.py predictions_walk && /opt/vesign/venv/bin/python scripts/swap_predictions_table.py'
```

Expected: `OK: no leak detected` followed by `OK: predictions swapped`.

- [ ] **Step 3: Verify the swap**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_no_leak.py predictions'
```

Expected: `OK: no leak detected`.

- [ ] **Step 4: Commit**

```bash
git add scripts/swap_predictions_table.py
git commit -m "feat(scripts): atomic swap of predictions for walk-forward rebuild"
```

---

## Phase 4: Cascade rebuild (signals + trade_log)

### Task 4.1: Rebuild signals from 2020-01-02

**Files:**
- Create: `scripts/rebuild_signals_from_date.py`

- [ ] **Step 1: Implement**

```python
"""Wipe signals from 2020-01-02 onwards and re-run scoring chronologically.

Uses the engine's existing target_date mode. Slow but correct: ~3s/date * 1500
trading days ≈ 75 minutes. Stops vesign before running.
"""
from datetime import date, timedelta
import pandas as pd
from sqlalchemy import text
from data.loaders import engine
from signals.engine import run_scoring

START = "2020-01-02"

def main():
    with engine.begin() as c:
        c.execute(text("DELETE FROM signals WHERE DATE(date) >= :s"), {"s": START})
        dates = [r[0] for r in c.execute(text(
            "SELECT DISTINCT DATE(date) FROM features WHERE DATE(date) >= :s ORDER BY date"
        ), {"s": START})]
    print(f"re-scoring {len(dates)} dates")

    for i, d in enumerate(dates, 1):
        run_scoring(target_date=d)
        if i % 50 == 0:
            print(f"  [{i}/{len(dates)}] {d}")
    print("DONE")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run**

```bash
scp -i ~/.ssh/id_vesign scripts/rebuild_signals_from_date.py root@134.209.82.105:/opt/vesign/scripts/
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'systemctl stop vesign && cd /opt/vesign && /opt/vesign/venv/bin/python scripts/rebuild_signals_from_date.py 2>&1 | tee /var/log/vesign-signals-rebuild.log; systemctl start vesign'
```

Expected runtime: ~75 min.

- [ ] **Step 3: Verify**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3; c=sqlite3.connect(\"/opt/vesign/vesign.db\")
print(\"BUYs by year:\")
for r in c.execute(\"SELECT strftime(\\\"%Y\\\", date) y, COUNT(*) FROM signals WHERE signal=\\\"BUY\\\" GROUP BY y ORDER BY y\"):
    print(f\"  {r[0]}: {r[1]}\")
print(\"row count by year:\")
for r in c.execute(\"SELECT strftime(\\\"%Y\\\", date) y, COUNT(*) FROM signals GROUP BY y ORDER BY y\"):
    print(f\"  {r[0]}: {r[1]:,}\")
"'
```

Expected: BUY counts have shifted from the contaminated baseline. 2020 BUYs likely much higher than current (more BUY firings now that no analyst gate is artificially tight, but ML may also be more conservative). Compare to baseline from Pre-flight 3 — total system trade volume should be in the same order of magnitude.

- [ ] **Step 4: Commit**

```bash
git add scripts/rebuild_signals_from_date.py
git commit -m "feat(scripts): chronological signals rebuild after walk-forward"
```

### Task 4.2: Rebuild trade_log

- [ ] **Step 1: Run trade_log rebuild from BUY signals**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  '/opt/vesign/venv/bin/python -c "
from backtesting.engine import build_trade_log
build_trade_log()
print(\"DONE\")
"'
```

- [ ] **Step 2: Capture honest backtest stats**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3
c = sqlite3.connect(\"/opt/vesign/vesign.db\")
print(\"=== honest baseline (post walk-forward rebuild) ===\")
for cut in (\"2020-01-04\", \"2021-01-04\", \"2022-01-03\"):
    n, w, avg = c.execute(f\"\"\"
        SELECT COUNT(*), SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END), AVG(return_pct)
        FROM trade_log WHERE buy_date>=\\\"{cut}\\\" AND sell_date IS NOT NULL
    \"\"\").fetchone()
    if n:
        print(f\"buy_date >= {cut}: n={n}  wr={w/n*100:.1f}%  avg={avg*100:+.2f}%\")
"'
```

Expected: numbers materially different from Pre-flight 3 baseline. **This is the critical moment** — these stats are the HONEST track record. If win-rate / avg-yield are dramatically lower than pre-rebuild, that's correct (the leak was inflating things).

- [ ] **Step 3: Commit if any code changes**

No code changes in this task; the trade_log rebuilder already exists.

---

## Phase 5: Wire walk-forward into production daily pipeline

### Task 5.1: Add quarterly auto-retrain to run_daily

**Files:**
- Modify: `production/run_daily.py:397` (insert one call before `run_prediction_engine()`)

- [ ] **Step 1: Read the current state of run_daily.py around line 397**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'sed -n "390,410p" /opt/vesign/production/run_daily.py'
```

Confirm current sequence: `compute_features → run_prediction_engine → run_scoring → ...`.

- [ ] **Step 2: Modify run_daily.py**

Insert one line above the existing `run_prediction_engine()` call:

```python
    # Walk-forward: ensure today's quarter has a fresh model before predicting.
    from models.walk_forward import maybe_retrain_for_today
    maybe_retrain_for_today()

    run_prediction_engine()
```

The existing `run_prediction_engine()` (in `models/predict.py`) will continue to be the daily incremental predictor — but it now uses the latest walk-forward model since `maybe_retrain_for_today()` may have just rotated `ml_models/walk/<today's-quarter>/` into existence.

> NOTE: `run_prediction_engine()` currently loads models from `ml_models/global_*.pkl`. Update its `_load_pkl()` paths to read from the most recent `ml_models/walk/<latest>/` subdir. Do this in the same task to keep the change atomic.

In `models/predict.py`, replace:

```python
    global_5d  = _load_pkl(os.path.join(ML_MODELS_DIR, "global_5d.pkl"))
    global_20d = _load_pkl(os.path.join(ML_MODELS_DIR, "global_20d.pkl"))
```

with:

```python
    from models.walk_forward import ML_WALK_DIR
    # Use the latest walk-forward cutoff dir
    walk_dirs = sorted(p for p in ML_WALK_DIR.iterdir() if p.is_dir() and not p.name.startswith("_"))
    if not walk_dirs:
        print("no walk-forward models on disk; falling back to legacy ml_models/")
        latest = ML_MODELS_DIR
    else:
        latest = str(walk_dirs[-1])
    global_5d  = _load_pkl(os.path.join(latest, "global_5d.pkl"))
    global_20d = _load_pkl(os.path.join(latest, "global_20d.pkl"))
```

Apply the same change to the per-sector model lookup in `_get_sector_models`. Keep the legacy `ML_MODELS_DIR` fallback so an empty walk dir doesn't hard-fail the pipeline.

Also update predict.py to write `model_cutoff` for new daily rows (set to the quarter-start of today). The simplest way: derive `cutoff` from `latest` directory name (`Path(latest).name`) and add it to the DataFrame before the `.to_sql()` insert:

```python
    df["model_cutoff"] = Path(latest).name
```

And update the insert column list:

```python
    df[["date", "ticker", "pred_5d", "pred_20d", "prediction_score", "model_cutoff"]].to_sql(...)
```

- [ ] **Step 3: Deploy**

```bash
scp -i ~/.ssh/id_vesign production/run_daily.py root@134.209.82.105:/opt/vesign/production/run_daily.py
scp -i ~/.ssh/id_vesign models/predict.py root@134.209.82.105:/opt/vesign/models/predict.py
```

- [ ] **Step 4: Smoke-test by running the daily pipeline once**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 \
  'systemctl stop vesign && cd /opt/vesign && /opt/vesign/venv/bin/python -m production.run_daily 2>&1 | tail -40; systemctl start vesign'
```

Expected: today's date predicted using the most recent walk-forward model, signals updated, no errors.

- [ ] **Step 5: Verify the new row carries model_cutoff**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 '/opt/vesign/venv/bin/python -c "
import sqlite3; c=sqlite3.connect(\"/opt/vesign/vesign.db\")
print(c.execute(\"SELECT date, ticker, prediction_score, model_cutoff FROM predictions ORDER BY date DESC LIMIT 3\").fetchall())
"'
```

Expected: latest 3 rows have model_cutoff matching the current quarter start (e.g. `2026-04-01`).

- [ ] **Step 6: Commit**

```bash
git add production/run_daily.py models/predict.py
git commit -m "feat(production): wire walk-forward retrain into daily pipeline"
```

### Task 5.2: Add pytest covering retrain decision logic

**Files:**
- Modify: `tests/models/test_walk_forward.py`

- [ ] **Step 1: Add tests**

Append to `tests/models/test_walk_forward.py`:

```python
from datetime import date
from unittest.mock import patch
from models.walk_forward import maybe_retrain_for_today, models_dir_for

def test_maybe_retrain_returns_none_if_models_already_exist(tmp_path, monkeypatch):
    """If today's quarter dir exists, no retrain is triggered."""
    fake_root = tmp_path / "walk"
    fake_root.mkdir()
    monkeypatch.setattr("models.walk_forward.ML_WALK_DIR", fake_root)
    today = date.today()
    qstart_month = ((today.month - 1) // 3) * 3 + 1
    qcutoff = date(today.year, qstart_month, 1)
    (fake_root / qcutoff.isoformat()).mkdir()
    assert maybe_retrain_for_today() is None

def test_maybe_retrain_triggers_when_dir_missing(tmp_path, monkeypatch):
    fake_root = tmp_path / "walk"
    fake_root.mkdir()
    monkeypatch.setattr("models.walk_forward.ML_WALK_DIR", fake_root)
    called = {}
    def fake_train(cutoff, force=False):
        called["cutoff"] = cutoff
        d = fake_root / cutoff.isoformat()
        d.mkdir()
        return d
    monkeypatch.setattr("models.walk_forward.train_for_cutoff", fake_train)
    result = maybe_retrain_for_today()
    assert result is not None
    today = date.today()
    qstart_month = ((today.month - 1) // 3) * 3 + 1
    assert called["cutoff"] == date(today.year, qstart_month, 1)
```

- [ ] **Step 2: Run pytest**

```bash
cd /Users/inovenos/PycharmProjects/Vesign && venv/bin/python -m pytest tests/models/test_walk_forward.py -v
```

Expected: 6 passed total (4 from Task 2.2 + 2 new).

- [ ] **Step 3: Commit**

```bash
git add tests/models/test_walk_forward.py
git commit -m "test(models): cover walk-forward retrain decision logic"
```

### Task 5.3: Document the walk-forward architecture

**Files:**
- Modify: `CLAUDE.md` (repo root) — add a "Walk-forward ML training" section
- Modify: `~/.claude/projects/-Users-inovenos-PycharmProjects-Vesign/memory/MEMORY.md` — add pointer

- [ ] **Step 1: Append to CLAUDE.md**

Insert after the existing ML / pipeline documentation:

```markdown
## Walk-forward ML training (post 2026-05-07 fix)

Models live in `ml_models/walk/<YYYY-MM-DD>/`, one directory per quarterly
cutoff. The daily pipeline calls `models.walk_forward.maybe_retrain_for_today()`
before predicting, so a new pickle set is created on the first day of each
calendar quarter.

`predictions.model_cutoff` records the train_end_date of the model that
generated each row. Invariant: `DATE(date) >= model_cutoff` for every row.
`scripts/verify_no_leak.py predictions` enforces this — run it before any DB
publish or trade_log rebuild.

Older code path (single global model in `ml_models/*.pkl`) is kept as a
fallback inside `models/predict.py` for the case where the walk dir is empty.
Do NOT remove it.
```

- [ ] **Step 2: Add memory pointer**

Add to `MEMORY.md`:

```markdown
- [Walk-forward ML — fix 2026-05-07](feature_walk_forward_ml.md) — quarterly retrain; verify_no_leak.py invariant; ml_models/walk/<cutoff>/ structure
```

Create `feature_walk_forward_ml.md` with the same content as the CLAUDE.md section.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: walk-forward ML architecture"
```

---

## Final verification

- [ ] **End-to-end: run the full no-leak gauntlet**

```bash
ssh -i ~/.ssh/id_vesign root@134.209.82.105 'cd /opt/vesign && /opt/vesign/venv/bin/python scripts/verify_no_leak.py predictions'
```

Expected: `OK: no leak detected`.

- [ ] **Smoke: refresh the frontend and confirm the BUY signals look reasonable**

Open https://ve-sign.com → Signals page → spot-check 3 historical BUY entries (one each from 2021, 2023, 2025). Confirm `prediction_score` values are present and the signals page renders without errors.

- [ ] **Final commit / merge if working on a branch**

```bash
git log --oneline -20
```

Confirm the commit chain matches the plan tasks. If executed on a feature branch, open a PR; otherwise the work is already on `main`.

---

## Self-Review Notes

- **Spec coverage:** every requirement (backfill 2018-2019, walk-forward, no-leak verification, downstream rebuild, daily pipeline integration) maps to a task above.
- **Placeholder scan:** every code block is complete; no "TODO" or "implement later".
- **Type/method consistency:** `models_dir_for`, `train_for_cutoff`, `predict_period`, `quarterly_cutoffs`, `PREDICTIONS_TABLE`, `ML_WALK_DIR`, `maybe_retrain_for_today` — names used consistently across module, scripts, tests.
- **Rollback:** `predictions_legacy` (kept after Task 3.2 swap), `vesign.db.pre_walkforward_<date>` (kept from Pre-flight 2). Either can be reinstated with a rename.
- **Production safety:** vesign service stopped during heavy steps (Phase 1 backfills, Phase 3 predictions rebuild, Phase 4 signals rebuild) per memory's 4 GB OOM constraint.

---

## Risk register

| Risk | Mitigation |
|---|---|
| FMP returns thin data for 2018-2019 (some tickers didn't exist) | Acceptable — walk-forward training simply has fewer flashcards for that ticker; predictions remain valid for years when the ticker existed |
| Quarterly cadence is too coarse and misses rapid market regime shifts | First pass is quarterly; `maybe_retrain_for_today` makes monthly trivial if needed later — change `quarterly_cutoffs` to monthly equivalent |
| signals rebuild produces dramatically different BUY counts → user-facing dashboards confused | Expected. Pre-flight 3 captures the contaminated baseline so the change is documented and intentional |
| OOM during predictions rebuild on 4 GB droplet | Per-day batches in `predict_period`; explicit `gc.collect()` between days; vesign stopped |
| Atomic swap fails leaving `predictions_walk` and `predictions` both renamed | Single-transaction swap in `swap_predictions_table.py`; backup at `vesign.db.pre_walkforward_<date>` for full rollback |
