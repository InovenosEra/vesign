#!/bin/bash
# Phase 4 (signals + trade_log) only — for resuming after Phase 4 failed.
# Phases 1-3 already completed; predictions table is the leak-free walk-forward
# version, predictions_legacy keeps the old contaminated table as backup.
set -u
LOG_PREFIX="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "============================================================"
echo "PHASE-4 LAUNCHER: stopping vesign at $LOG_PREFIX"
echo "============================================================"
systemctl stop vesign

cd /opt/vesign
export PYTHONPATH=/opt/vesign
export VESIGN_US_ONLY=1

echo "----- Phase 4.1: rebuild signals from 2020-01-02 -----"
/opt/vesign/venv/bin/python scripts/rebuild_signals_from_date.py
rc1=$?
echo "----- Phase 4.1 rc=$rc1 -----"

if [ $rc1 -eq 0 ]; then
    echo "----- Phase 4.2: rebuild trade_log -----"
    /opt/vesign/venv/bin/python -c "from backtesting.engine import build_trade_log; build_trade_log(); print('trade_log rebuilt')"
    rc2=$?
    echo "----- Phase 4.2 rc=$rc2 -----"
else
    echo "skipping Phase 4.2 because Phase 4.1 failed"
    rc2=99
fi

echo "============================================================"
echo "PHASE-4 LAUNCHER: restarting vesign (rc1=$rc1 rc2=$rc2)"
echo "============================================================"
systemctl start vesign

if [ $rc1 -eq 0 ] && [ $rc2 -eq 0 ]; then
    echo "PHASE 4 COMPLETE at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    exit 0
fi
echo "PHASE 4 FAILED rc1=$rc1 rc2=$rc2 at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
exit 1
