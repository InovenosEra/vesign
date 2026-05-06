"""Master orchestrator for the ML lookahead fix (Phases 1-4).

Runs sequentially; aborts on first failure. Each phase logs start/end with
timestamps so the morning review can see exactly where any failure happened.

Designed to run in nohup on the server; safe to re-run (each phase script is
idempotent or guarded).

Phases:
  1.1 backfill 2018-2019 prices
  1.2 backfill 2018-2019 VIX
  1.3 recompute features (full rebuild from 2018-01-01)
  1.4 recompute forward_returns (full rebuild)
  2.0 add model_cutoff column to predictions
  3.1 rebuild predictions_walk via walk-forward training
  3.2 verify no leak in predictions_walk
  3.3 atomic swap → predictions
  3.4 verify no leak in (new) predictions
  4.1 rebuild signals from 2020-01-02
  4.2 rebuild trade_log
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PYTHON = "/opt/vesign/venv/bin/python"
ROOT = Path("/opt/vesign")
ENV = {**os.environ, "PYTHONPATH": str(ROOT), "VESIGN_US_ONLY": "1"}


def log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def run(label: str, *args: str) -> None:
    log(f"START  : {label}")
    log(f"  cmd  : {' '.join(args)}")
    t0 = time.time()
    proc = subprocess.run(args, cwd=str(ROOT), env=ENV)
    dt = time.time() - t0
    if proc.returncode != 0:
        log(f"FAIL   : {label}  (rc={proc.returncode}, {dt:.1f}s)")
        log("ABORTING — do not proceed downstream.")
        sys.exit(proc.returncode)
    log(f"OK     : {label}  ({dt:.1f}s)")


def main():
    log("=" * 70)
    log("MASTER ML FIX — starting")
    log("=" * 70)

    # ---------- Phase 1: backfill 2018-2019 ----------
    run("Phase 1.1 — backfill 2018-2019 prices",
        PYTHON, str(ROOT / "scripts" / "backfill_prices_2018_2019.py"))
    run("Phase 1.1.verify — assert 2018-2019 prices present",
        PYTHON, str(ROOT / "scripts" / "verify_2018_prices.py"))
    run("Phase 1.2 — backfill 2018-2019 VIX",
        PYTHON, str(ROOT / "scripts" / "backfill_vix_2018_2019.py"))
    run("Phase 1.3 — recompute features full",
        PYTHON, str(ROOT / "scripts" / "recompute_features_full.py"))
    run("Phase 1.4 — recompute forward_returns",
        PYTHON, str(ROOT / "scripts" / "recompute_forward_returns_full.py"))

    # ---------- Phase 2: schema migration ----------
    run("Phase 2.0 — add model_cutoff column",
        PYTHON, str(ROOT / "scripts" / "migrate_predictions_add_cutoff.py"))

    # ---------- Phase 3: walk-forward predictions rebuild ----------
    run("Phase 3.1 — rebuild predictions_walk via walk-forward",
        PYTHON, str(ROOT / "scripts" / "rebuild_predictions_walk.py"))
    run("Phase 3.2 — verify no leak in predictions_walk",
        PYTHON, str(ROOT / "scripts" / "verify_no_leak.py"), "predictions_walk")
    run("Phase 3.3 — atomic swap predictions ↔ predictions_walk",
        PYTHON, str(ROOT / "scripts" / "swap_predictions_table.py"))
    run("Phase 3.4 — verify no leak in (new) predictions",
        PYTHON, str(ROOT / "scripts" / "verify_no_leak.py"), "predictions")

    # ---------- Phase 4: signals + trade_log rebuild ----------
    run("Phase 4.1 — rebuild signals from 2020-01-02",
        PYTHON, str(ROOT / "scripts" / "rebuild_signals_from_date.py"))
    run("Phase 4.2 — rebuild trade_log",
        PYTHON, "-c", "from backtesting.engine import build_trade_log; build_trade_log(); print('trade_log rebuilt')")

    log("=" * 70)
    log("MASTER ML FIX — ALL PHASES COMPLETE")
    log("=" * 70)


if __name__ == "__main__":
    main()
