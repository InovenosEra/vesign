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
        print("model_cutoff already present", flush=True)
        return
    c.execute("ALTER TABLE predictions ADD COLUMN model_cutoff TEXT NOT NULL DEFAULT ''")
    c.commit()
    print("model_cutoff column added (default '')", flush=True)


if __name__ == "__main__":
    main()
