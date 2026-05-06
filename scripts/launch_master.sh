#!/bin/bash
# Launches the master ML fix orchestrator with vesign stopped.
# Restarts vesign at the end regardless of success/failure.
#
# Usage: nohup bash /opt/vesign/scripts/launch_master.sh > /var/log/vesign-ml-fix.log 2>&1 < /dev/null &
set -u
echo "============================================================"
echo "LAUNCHER: stopping vesign at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "============================================================"
systemctl stop vesign
cd /opt/vesign
/opt/vesign/venv/bin/python scripts/master_ml_fix.py
rc=$?
echo "============================================================"
echo "LAUNCHER: restarting vesign (master rc=$rc)"
echo "============================================================"
systemctl start vesign
echo "MASTER FINISHED rc=$rc at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
exit $rc
