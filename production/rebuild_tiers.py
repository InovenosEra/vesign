"""One-shot historical rebuild after the tier gate change (Task 2).
Re-derives signals day-by-day under the new BUY gate (vqs>=6) writing
signal+tier+lot_seq, then rebuilds trade_log + trade_lots. Does NOT recompute
vqs (already stored). Idempotent. Honors VESIGN_DB (see data/loaders.py) so it
can target a copy locally or the prod DB at deploy.

Usage:
    VESIGN_DB=vesign_tier_test.db venv/bin/python -m production.rebuild_tiers
"""
from production.backfill_trailing_stop_dca import backfill_all_signals, rebuild_trade_log


def main():
    backfill_all_signals()   # writes signal + tier + lot_seq for every historical date
    rebuild_trade_log()      # rebuilds trade_log + trade_lots from the new signals
    print("Tier rebuild complete.")


if __name__ == "__main__":
    main()
