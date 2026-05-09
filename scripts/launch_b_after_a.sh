#!/bin/bash
# Waits for Process A (rebuild_signals_range.py for 2020-2023H1) to finish,
# then runs Process B (2023-07-01..2026-05-05) sequentially, then trade_log,
# then restarts vesign.
#
# Usage: nohup bash /opt/vesign/scripts/launch_b_after_a.sh > /var/log/vesign-b-chain.log 2>&1 < /dev/null &
set -u
PID_A=306414  # Process A python pid

echo "============================================================"
echo "B-CHAIN: started at $(date -u +'%Y-%m-%dT%H:%M:%SZ'), polling for A (PID $PID_A) to exit"
echo "============================================================"

while kill -0 $PID_A 2>/dev/null; do
    sleep 30
done
echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') Process A gone. Last 5 log lines:"
tail -5 /var/log/vesign-rebuild-A.log

# Confirm A finished cleanly by checking the DONE marker
if ! tail -20 /var/log/vesign-rebuild-A.log | grep -q "^\[.*\] DONE range"; then
    echo "ABORT: Process A did not finish cleanly (no DONE marker). Skipping B + trade_log."
    exit 1
fi

cd /opt/vesign
export PYTHONPATH=/opt/vesign
export VESIGN_US_ONLY=1

echo "============================================================"
echo "Starting Process B sequentially (2023-07-01 .. 2026-05-05)"
echo "============================================================"
/opt/vesign/venv/bin/python /opt/vesign/scripts/rebuild_signals_range.py \
    --start 2023-07-01 --end 2026-05-05 2>&1 | tee /var/log/vesign-rebuild-B.log
RC_B=${PIPESTATUS[0]}
echo "Process B finished rc=$RC_B at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"

if [ $RC_B -ne 0 ]; then
    echo "ABORT: Process B failed (rc=$RC_B). Restarting vesign without trade_log."
    systemctl start vesign
    exit 1
fi

echo "============================================================"
echo "Running build_trade_log..."
echo "============================================================"
/opt/vesign/venv/bin/python -c "from backtesting.engine import build_trade_log; build_trade_log(); print('trade_log rebuilt')"
RC_TL=$?

echo "============================================================"
echo "Restarting vesign"
echo "============================================================"
systemctl start vesign

if [ $RC_TL -ne 0 ]; then
    echo "B-CHAIN FAILED: trade_log step rc=$RC_TL"
    exit 1
fi

echo "B-CHAIN COMPLETE at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
exit 0
