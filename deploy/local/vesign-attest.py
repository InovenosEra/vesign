#!/usr/bin/env python3
"""
vesign-attest.py — cryptographic attestation of Vesign's fired (BUY/SELL) signals.

Builds a canonical JSON snapshot containing ONLY user-facing signal fields
(date, ticker, direction, tier, health, ml_5d, predicted_upside — no raw
model features, no internal-only scores), writes it to a private local
archive, then commits+pushes ONLY its SHA-256 hash to the public
`vesign-proof` repo. The snapshot itself never leaves this machine before
product launch; the hash alone is enough for third-party, tamper-evident,
timestamped proof that the signal existed no later than the commit time.

Reads the DB strictly read-only (SQLite URI mode=ro) — never writes to
vesign.db.

Usage:
    vesign-attest.py                    # attest the latest expected NYSE trading day
    vesign-attest.py --date 2026-07-16  # attest a specific date (backfill/testing)
    vesign-attest.py --genesis          # one-time: attest ALL historical BUY/SELL signals
    vesign-attest.py --no-push          # build snapshot + hash locally only; skip git. Testing only.

Configuration (env vars, all optional — defaults match the production droplet layout):
    VESIGN_DB_PATH        default: /opt/vesign/vesign.db
    VESIGN_PROOF_ARCHIVE  default: /root/vesign-attest/proof-archive   (private, never pushed)
    VESIGN_PROOF_REPO     default: /root/vesign-proof                 (public repo working copy)
    VESIGN_PROOF_REMOTE   default: git@github-vesign-proof:InovenosEra/vesign-proof.git
    VESIGN_PROOF_BRANCH   default: main

Idempotency: if today's hash file already exists with the same hash, this is
a no-op (exit 0). If it exists with a DIFFERENT hash, this aborts loudly
(exit 1) — an attestation is never overwritten.
"""
import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, UTC
from pathlib import Path

DEFAULT_DB_PATH = "/opt/vesign/vesign.db"
DEFAULT_ARCHIVE_DIR = "/root/vesign-attest/proof-archive"
DEFAULT_REPO_DIR = "/root/vesign-proof"
DEFAULT_REMOTE = "git@github-vesign-proof:InovenosEra/vesign-proof.git"
DEFAULT_BRANCH = "main"
COMMIT_AUTHOR_NAME = "vesign-attest-bot"
COMMIT_AUTHOR_EMAIL = "noreply@ve-sign.com"

# Fields kept are ONLY those shown to users (see backend/entitlements.py MODEL_FIELDS
# for the same "these are the model's signal fields" boundary). Explicitly excluded:
# vqs (never shown as a raw number anywhere — feedback_vqs_internal_only), vesign_score
# and score (internal composites/fallbacks, never rendered), and every raw feature
# column (rsi, macd, bb_*, volume_*, *_factor, *_condition, target_*_price, ...).
SIGNAL_COLUMNS = "date, ticker, signal, tier, health_score, prediction_score, fair_value_upside"

MAX_READY_ATTEMPTS = 3
READY_RETRY_WAIT_SECONDS = 15 * 60


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def fail(msg):
    log(f"ERROR: {msg}")
    sys.exit(1)


def run(cmd, check=True):
    log("$ " + " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        log(result.stdout.strip())
    if result.returncode != 0:
        if result.stderr.strip():
            log("STDERR: " + result.stderr.strip())
        if check:
            fail(f"command failed (exit {result.returncode}): {' '.join(cmd)}")
    return result


def _int_or_none(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _float_or_none(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def build_record(row):
    date_raw, ticker, signal, tier, health_score, prediction_score, fair_value_upside = row
    return {
        "date": str(date_raw)[:10],
        "ticker": ticker,
        "direction": signal,
        "tier": _int_or_none(tier),
        "health": _int_or_none(health_score),
        "ml_5d": _float_or_none(prediction_score),
        "predicted_upside": _float_or_none(fair_value_upside),
    }


def canonical_bytes(records):
    return json.dumps(
        records, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")


def write_snapshot(path: Path, data: bytes) -> str:
    """Write the private snapshot file, returning its sha256. Refuses to
    silently replace an existing snapshot whose content differs."""
    new_hash = hashlib.sha256(data).hexdigest()
    if path.exists():
        existing_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        if existing_hash != new_hash:
            fail(
                f"REFUSING TO OVERWRITE existing private snapshot at {path}: its content hash "
                f"({existing_hash}) differs from what was just generated ({new_hash}). This should "
                f"never happen for a deterministic snapshot of already-final historical data — "
                f"investigate before touching this file."
            )
        return existing_hash
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return new_hash


def connect_db_readonly(db_path: Path):
    import sqlite3
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("SELECT COUNT(*) FROM signals LIMIT 1")
    return conn


def fetch_signal_rows(conn, date_str):
    cur = conn.execute(
        f"SELECT {SIGNAL_COLUMNS} FROM signals "
        f"WHERE DATE(date) = ? AND signal IN ('BUY','SELL') "
        f"ORDER BY ticker",
        (date_str,),
    )
    return cur.fetchall()


def fetch_all_signal_rows(conn):
    cur = conn.execute(
        f"SELECT {SIGNAL_COLUMNS} FROM signals "
        f"WHERE signal IN ('BUY','SELL') "
        f"ORDER BY date, ticker"
    )
    return cur.fetchall()


def latest_signals_date_str(conn):
    row = conn.execute("SELECT DATE(MAX(date)) FROM signals").fetchone()
    return row[0] if row and row[0] else None


def last_closed_nyse_session():
    """Same window/calendar as production/run_daily.py's _last_closed('XNYS'),
    so 'expected' here always matches what the pipeline itself expects."""
    import pandas as pd
    import exchange_calendars as xcals
    from datetime import timedelta

    today = datetime.now(UTC).date()
    cal = xcals.get_calendar("XNYS")
    sessions = cal.sessions_in_range(
        pd.Timestamp(today - timedelta(days=14)),
        pd.Timestamp(today - timedelta(days=1)),
    )
    last = sessions[-1].date() if len(sessions) > 0 else today - timedelta(days=1)
    return last.isoformat()


def wait_for_expected_date(conn, expected):
    latest = None
    for attempt in range(1, MAX_READY_ATTEMPTS + 1):
        latest = latest_signals_date_str(conn)
        if latest is not None and latest >= expected:
            return latest
        log(
            f"signals not yet at expected trading date {expected} "
            f"(latest in DB: {latest}) — attempt {attempt}/{MAX_READY_ATTEMPTS}"
        )
        if attempt < MAX_READY_ATTEMPTS:
            time.sleep(READY_RETRY_WAIT_SECONDS)
    fail(
        f"signals for expected trading date {expected} never appeared after "
        f"{MAX_READY_ATTEMPTS} attempts (latest in DB: {latest})"
    )


def ensure_proof_repo(repo_dir: Path, remote: str, branch: str):
    if not (repo_dir / ".git").is_dir():
        log(f"vesign-proof working copy not found at {repo_dir} — cloning {remote}")
        repo_dir.parent.mkdir(parents=True, exist_ok=True)
        run(["git", "clone", "--branch", branch, remote, str(repo_dir)])
    else:
        run(["git", "-C", str(repo_dir), "fetch", "origin", branch])
        run(["git", "-C", str(repo_dir), "reset", "--hard", f"origin/{branch}"])


def attest(hash_path: Path, hash_hex: str) -> bool:
    """Idempotent hash-file write. Returns True if a new hash was written,
    False if an identical attestation already existed. Aborts (never
    overwrites) if an existing hash differs."""
    if hash_path.exists():
        existing = hash_path.read_text().strip()
        if existing == hash_hex:
            log(f"{hash_path.name} already attested with matching hash — nothing to do")
            return False
        fail(
            f"REFUSING TO OVERWRITE existing attestation at {hash_path}: "
            f"existing hash={existing} new hash={hash_hex}. An attestation is never replaced — "
            f"this means the underlying data changed after being attested, or something upstream "
            f"is non-deterministic. Investigate before doing anything else."
        )
    hash_path.parent.mkdir(parents=True, exist_ok=True)
    hash_path.write_text(hash_hex + "\n")
    return True


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--genesis", action="store_true",
        help="Attest ALL historical BUY/SELL signals as a one-time baseline.",
    )
    p.add_argument(
        "--date", default=None,
        help="Attest this specific YYYY-MM-DD instead of the latest expected trading day "
             "(skips the not-ready retry loop).",
    )
    p.add_argument(
        "--no-push", action="store_true",
        help="Build the snapshot and write the hash file locally only; skip git "
             "clone/fetch/commit/push. For testing.",
    )
    return p.parse_args()


def main():
    args = parse_args()
    db_path = Path(os.environ.get("VESIGN_DB_PATH", DEFAULT_DB_PATH))
    archive_dir = Path(os.environ.get("VESIGN_PROOF_ARCHIVE", DEFAULT_ARCHIVE_DIR))
    repo_dir = Path(os.environ.get("VESIGN_PROOF_REPO", DEFAULT_REPO_DIR))
    remote = os.environ.get("VESIGN_PROOF_REMOTE", DEFAULT_REMOTE)
    branch = os.environ.get("VESIGN_PROOF_BRANCH", DEFAULT_BRANCH)

    if not db_path.exists():
        fail(f"DB not found at {db_path}")

    import sqlite3
    try:
        conn = connect_db_readonly(db_path)
    except sqlite3.Error as e:
        fail(f"cannot open DB read-only at {db_path}: {e}")

    if args.no_push:
        repo_dir.mkdir(parents=True, exist_ok=True)
    else:
        ensure_proof_repo(repo_dir, remote, branch)

    if args.genesis:
        rows = fetch_all_signal_rows(conn)
        if not rows:
            fail("no BUY/SELL signals found in the DB — cannot build a genesis attestation")
        as_of = conn.execute(
            "SELECT DATE(MAX(date)) FROM signals WHERE signal IN ('BUY','SELL')"
        ).fetchone()[0]
        records = [build_record(r) for r in rows]
        records.sort(key=lambda r: (r["date"], r["ticker"]))
        data = canonical_bytes(records)
        snapshot_path = archive_dir / f"genesis-through-{as_of}.json"
        hash_hex = write_snapshot(snapshot_path, data)
        log(f"genesis snapshot: {len(records)} records, {len(data)} bytes -> {snapshot_path}")
        log(f"sha256: {hash_hex}")
        hash_rel = f"genesis-through-{as_of}.sha256"
        commit_message = f"genesis attestation: all history as of {as_of}"
    else:
        if args.date:
            target_date = args.date
            rows = fetch_signal_rows(conn, target_date)
            if not rows:
                fail(f"no BUY/SELL signals found for {target_date} — refusing to attest an empty/missing day")
        else:
            expected = last_closed_nyse_session()
            wait_for_expected_date(conn, expected)
            target_date = expected
            rows = fetch_signal_rows(conn, target_date)
            if not rows:
                fail(f"signals table reached expected trading date {expected} but has zero BUY/SELL rows for it")
        records = [build_record(r) for r in rows]
        records.sort(key=lambda r: r["ticker"])
        data = canonical_bytes(records)
        year = target_date[:4]
        snapshot_path = archive_dir / year / f"{target_date}.json"
        hash_hex = write_snapshot(snapshot_path, data)
        log(f"daily snapshot {target_date}: {len(records)} records, {len(data)} bytes -> {snapshot_path}")
        log(f"sha256: {hash_hex}")
        hash_rel = f"{year}/{target_date}.sha256"
        commit_message = f"attest {target_date}"

    hash_path = repo_dir / hash_rel
    wrote_new = attest(hash_path, hash_hex)
    if not wrote_new:
        sys.exit(0)

    if args.no_push:
        log(f"--no-push: hash file written at {hash_path}, skipping git add/commit/push")
        sys.exit(0)

    run(["git", "-C", str(repo_dir), "add", hash_rel])
    run([
        "git", "-C", str(repo_dir),
        "-c", f"user.name={COMMIT_AUTHOR_NAME}",
        "-c", f"user.email={COMMIT_AUTHOR_EMAIL}",
        "commit", "-m", commit_message,
    ])
    push = subprocess.run(
        ["git", "-C", str(repo_dir), "push", "origin", branch], capture_output=True, text=True
    )
    if push.stdout.strip():
        log(push.stdout.strip())
    if push.returncode != 0:
        if push.stderr.strip():
            log("STDERR: " + push.stderr.strip())
        fail(
            f"git push to vesign-proof FAILED after committing locally — {hash_rel} is committed in "
            f"the local clone but not on origin. Safe to re-run: the snapshot is deterministic, so the "
            f"next run regenerates the identical hash and retries this same commit+push (ensure_proof_repo "
            f"resets to origin first, cleanly discarding this unpushed commit)."
        )
    log(f"attested: {commit_message} ({hash_hex})")


if __name__ == "__main__":
    main()
