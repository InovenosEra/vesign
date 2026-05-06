"""Atomic swap: predictions ← predictions_walk.

Renames the contaminated `predictions` to `predictions_legacy` (kept as backup
until manually dropped) and renames `predictions_walk` to `predictions`. Uses a
single transaction so a crash mid-swap leaves a valid (if old) state.
"""
import sys
import sqlite3

DB = "/opt/vesign/vesign.db"


def main():
    c = sqlite3.connect(DB)
    cur = c.cursor()
    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
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
    print("OK: predictions swapped (old kept as predictions_legacy)", flush=True)


if __name__ == "__main__":
    main()
