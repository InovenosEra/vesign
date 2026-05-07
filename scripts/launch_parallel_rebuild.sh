#!/bin/bash
# Launches 2 parallel rebuild processes for non-overlapping date chunks.
# Process A: 2020-01-02 .. 2023-06-30
# Process B: 2023-07-01 .. 2026-05-05
# Then build_trade_log + final verifications + restart vesign.
set -u
cd /opt/vesign
export PYTHONPATH=/opt/vesign
export VESIGN_US_ONLY=1

LOG_A=/var/log/vesign-rebuild-A.log
LOG_B=/var/log/vesign-rebuild-B.log

echo "============================================================"
echo "PARALLEL LAUNCHER: stopping vesign at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "============================================================"
systemctl stop vesign

echo "Starting Process A (2020-01-02 .. 2023-06-30)..."
nohup /opt/vesign/venv/bin/python /opt/vesign/scripts/rebuild_signals_range.py \
    --start 2020-01-02 --end 2023-06-30 > $LOG_A 2>&1 &
PID_A=$!
echo "  Process A pid=$PID_A logging to $LOG_A"

sleep 2  # Let A start its DELETE before B starts (avoid lock contention on initial DELETE)

echo "Starting Process B (2023-07-01 .. 2026-05-05)..."
nohup /opt/vesign/venv/bin/python /opt/vesign/scripts/rebuild_signals_range.py \
    --start 2023-07-01 --end 2026-05-05 > $LOG_B 2>&1 &
PID_B=$!
echo "  Process B pid=$PID_B logging to $LOG_B"

echo "Waiting for both processes..."
wait $PID_A
RC_A=$?
echo "Process A finished rc=$RC_A"

wait $PID_B
RC_B=$?
echo "Process B finished rc=$RC_B"

if [ $RC_A -ne 0 ] || [ $RC_B -ne 0 ]; then
    echo "ABORT: one of the rebuild processes failed (rc_A=$RC_A rc_B=$RC_B)"
    echo "Restarting vesign and exiting"
    systemctl start vesign
    exit 1
fi

echo "============================================================"
echo "Both rebuilds OK. Running build_trade_log..."
echo "============================================================"
/opt/vesign/venv/bin/python -c "from backtesting.engine import build_trade_log; build_trade_log(); print('trade_log rebuilt')"
RC_TL=$?
echo "build_trade_log rc=$RC_TL"

echo "============================================================"
echo "Restarting vesign"
echo "============================================================"
systemctl start vesign

if [ $RC_TL -ne 0 ]; then
    echo "PARALLEL REBUILD FAILED (trade_log step) at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    exit 1
fi

echo "PARALLEL REBUILD COMPLETE at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
exit 0
