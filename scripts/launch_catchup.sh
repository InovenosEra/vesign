#!/bin/bash
# Stops vesign, runs the May 6+7 catch-up, restarts vesign, re-enables cron.
set -u
cd /opt/vesign
export PYTHONPATH=/opt/vesign
export VESIGN_US_ONLY=1

echo "===== $(date -u +'%Y-%m-%dT%H:%M:%SZ') CATCHUP START ====="
systemctl stop vesign

/opt/vesign/venv/bin/python /opt/vesign/scripts/catchup_may6_may7.py
RC=$?
echo "catchup script rc=$RC"

if [ $RC -eq 0 ]; then
    echo "===== Verifying leak-free ====="
    /opt/vesign/venv/bin/python /opt/vesign/scripts/verify_no_leak.py predictions
    VERIFY_RC=$?
else
    VERIFY_RC=99
fi

systemctl start vesign

if [ $RC -eq 0 ] && [ $VERIFY_RC -eq 0 ]; then
    echo "===== Re-enabling daily cron ====="
    crontab -l > /tmp/crontab.before_reenable.txt
    grep -v '^# DISABLED 2026-05-07 for ML fix rebuild' /tmp/crontab.before_reenable.txt \
      | sed 's|^# 0 7 \* \* 1-6 |0 7 * * 1-6 |' \
      > /tmp/crontab.reenabled.txt
    crontab /tmp/crontab.reenabled.txt
    echo "Crontab now:"
    crontab -l
    echo "===== $(date -u +'%Y-%m-%dT%H:%M:%SZ') CATCHUP COMPLETE ====="
    exit 0
fi

echo "===== $(date -u +'%Y-%m-%dT%H:%M:%SZ') CATCHUP FAILED rc=$RC verify_rc=$VERIFY_RC — cron LEFT DISABLED ====="
exit 1
