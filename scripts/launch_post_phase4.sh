#!/bin/bash
# Polls until Phase 4 launcher exits, then runs Phase 5 catchup:
#   1) run_daily_fast (catches up to May 6 with the new walk-forward path)
#   2) verify_no_leak.py predictions
#   3) re-enable the daily cron entry
#
# Aborts if Phase 4 didn't end with "PHASE 4 COMPLETE" or if run_daily_fast
# / verify fails — leaves cron disabled for human review in that case.
#
# Usage: nohup bash /opt/vesign/scripts/launch_post_phase4.sh > /var/log/vesign-postphase4.log 2>&1 < /dev/null &
set -u
LOG_PREFIX="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "============================================================"
echo "POST-PHASE4: started at $LOG_PREFIX, polling for Phase 4 exit"
echo "============================================================"

while pgrep -f "launch_phase4.sh" > /dev/null; do
    sleep 30
done
echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') Phase 4 launcher process gone."

PHASE4_LOG_TAIL=$(tail -10 /var/log/vesign-phase4.log)
echo "Last lines of /var/log/vesign-phase4.log:"
echo "$PHASE4_LOG_TAIL"

if ! echo "$PHASE4_LOG_TAIL" | grep -q "PHASE 4 COMPLETE"; then
    echo "ABORT: Phase 4 did not complete cleanly. Skipping catchup + cron re-enable."
    exit 1
fi
echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') Phase 4 succeeded — starting catchup."

systemctl stop vesign
cd /opt/vesign
export PYTHONPATH=/opt/vesign
export VESIGN_US_ONLY=1

echo "============================================================"
echo "PHASE 5 catchup: run_daily_fast (May 6 + Phase 5 smoke test)"
echo "============================================================"
/opt/vesign/venv/bin/python -c "from production.run_daily_fast import run_daily_fast; run_daily_fast()"
daily_rc=$?
echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') run_daily_fast finished rc=$daily_rc"

echo "============================================================"
echo "verify_no_leak.py predictions"
echo "============================================================"
/opt/vesign/venv/bin/python /opt/vesign/scripts/verify_no_leak.py predictions
verify_rc=$?
echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') verify_no_leak rc=$verify_rc"

echo "============================================================"
echo "Restarting vesign"
echo "============================================================"
systemctl start vesign

if [ $daily_rc -ne 0 ] || [ $verify_rc -ne 0 ]; then
    echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') ABORT: leaving cron DISABLED for review (daily_rc=$daily_rc verify_rc=$verify_rc)"
    exit 1
fi

echo "============================================================"
echo "Re-enabling daily cron"
echo "============================================================"
crontab -l > /tmp/crontab.before_reenable.txt
# Remove the disabled comment line and uncomment the cron entry
grep -v '^# DISABLED 2026-05-07 for ML fix rebuild' /tmp/crontab.before_reenable.txt \
  | sed 's|^# 0 7 \* \* 1-6 |0 7 * * 1-6 |' \
  > /tmp/crontab.reenabled.txt
crontab /tmp/crontab.reenabled.txt
echo "Final crontab:"
crontab -l
echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') POST-PHASE4 COMPLETE — everything end-to-end."
exit 0
