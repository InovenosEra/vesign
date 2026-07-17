#!/bin/bash
# vesign-daily-sync.sh — pull a consistent copy of the production vesign.db to local.
#
# Strategy: take a consistent SQLite .backup snapshot on the server, then
# delta-transfer it down (only changed blocks) seeded from the existing local DB,
# so a daily run moves megabytes, not the full ~3.9 GB.
#
# Safety: the new DB is only swapped in if the transfer succeeded AND the
# transferred file is readable AND at least as fresh as the current local DB.
# Otherwise the existing local DB is left untouched.
#
# Run by launchd (com.vesign.dbsync) daily; safe to run by hand anytime.
# Log: ~/Library/Logs/vesign-sync.log

set -u

SERVER="root@134.209.82.105"
KEY="$HOME/.ssh/id_vesign"
REPO="/Users/inovenos/PycharmProjects/Vesign"
LOCAL_DB="$REPO/vesign.db"
TMP_LOCAL="${LOCAL_DB}.syncing"
REMOTE_DB="/opt/vesign/vesign.db"
REMOTE_SNAP="/tmp/vesign-snap.db"
LOG="$HOME/Library/Logs/vesign-sync.log"

# Retry the network steps. A launchd catch-up after the Mac was asleep often fires
# during a DarkWake (background maintenance wake) when Wi-Fi is still down, giving
# "Network is unreachable". sleep() is suspended while the Mac sleeps, so the wait
# naturally spans a DarkWake -> sleep -> real wake; by the time the Mac is genuinely
# up the network is ready and a retry succeeds. 10 x 120s ≈ up to 20 min of patience.
MAX_TRIES=10
TRY_WAIT=120

# ServerAlive*: without these, a connection that dies silently after being
# established (network drops but no FIN/RST reaches this side — the exact
# failure mode that stuck a sync for ~24h on 2026-07-08) just hangs forever;
# ssh has no built-in dead-peer detection otherwise. 15s x 3 missed replies
# = ssh gives up within ~45s, so a truly-dead connection now fails fast and
# falls through to the existing retry loop instead of hanging past it.
SSH=(/usr/bin/ssh -i "$KEY" -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=accept-new)

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG"; }

# Single-instance lock. With multiple launchd windows (08:30/12:30/16:30/20:30) a
# slow run can still be going when the next window fires; without a lock two runs
# would rsync the same temp file and corrupt it. Atomic mkdir; a stale lock left by
# a -9'd run (traps don't fire on SIGKILL) is reclaimed only if its PID is dead.
# The loser exits BEFORE the cleanup trap is armed, so it never frees the winner's lock.
LOCKDIR="${LOCAL_DB}.synclock"
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  oldpid=$(cat "$LOCKDIR/pid" 2>/dev/null)
  if [ -n "$oldpid" ] && kill -0 "$oldpid" 2>/dev/null; then
    log "another sync already running (pid $oldpid) — skipping this run"; exit 0
  fi
  log "stale lock (pid ${oldpid:-?} gone) — reclaiming"
  rm -rf "$LOCKDIR"; mkdir "$LOCKDIR" || { log "ERROR: cannot acquire lock"; exit 1; }
fi
echo $$ > "$LOCKDIR/pid"

cleanup() {
  "${SSH[@]}" "$SERVER" "rm -f $REMOTE_SNAP" >/dev/null 2>&1
  # Remove the temp DB and any SQLite sidecars the guard-read leaves behind
  # (the mv moves only the main file, orphaning -wal/-shm otherwise).
  rm -f "$TMP_LOCAL" "${TMP_LOCAL}-wal" "${TMP_LOCAL}-shm" "${TMP_LOCAL}-journal"
  rm -rf "$LOCKDIR"
}
trap cleanup EXIT

log "=== sync start ==="

# 1. Consistent snapshot on the server (handles concurrent writes via SQLite backup API).
#    Retried to survive a wake-from-DarkWake race where the network isn't up yet.
snap_ok=0
for attempt in $(seq 1 "$MAX_TRIES"); do
  if "${SSH[@]}" "$SERVER" "/opt/vesign/venv/bin/python3 -c 'import sqlite3; s=sqlite3.connect(\"$REMOTE_DB\"); d=sqlite3.connect(\"$REMOTE_SNAP\"); s.backup(d); d.close(); s.close()'" >>"$LOG" 2>&1; then
    snap_ok=1; break
  fi
  log "snapshot attempt $attempt/$MAX_TRIES failed (network not ready?) — waiting ${TRY_WAIT}s"
  sleep "$TRY_WAIT"
done
if [ "$snap_ok" -ne 1 ]; then
  log "ERROR: server snapshot failed after $MAX_TRIES attempts — keeping current local DB"; exit 1
fi
log "server snapshot ready"

LOCAL_MAX=$(/usr/bin/sqlite3 "$LOCAL_DB" "SELECT MAX(date) FROM daily_prices" 2>>"$LOG")
log "current local max(date)=$LOCAL_MAX"

# 2. Seed the destination from the existing local DB, then delta-transfer the snapshot
#    onto it (--no-whole-file forces rsync's block-delta algorithm even though the
#    macOS rsync defaults vary). Only changed blocks cross the network.
if ! /bin/cp "$LOCAL_DB" "$TMP_LOCAL"; then
  log "ERROR: seed copy failed"; exit 1
fi
# Retry rsync too: it can drop mid-transfer (broken pipe). rsync reconciles the
# partial TMP_LOCAL against the source on the next attempt, so re-seeding isn't needed.
rsync_ok=0
for attempt in $(seq 1 "$MAX_TRIES"); do
  if /usr/bin/rsync -a --no-whole-file -e "${SSH[*]}" "$SERVER:$REMOTE_SNAP" "$TMP_LOCAL" >>"$LOG" 2>&1; then
    rsync_ok=1; break
  fi
  log "rsync attempt $attempt/$MAX_TRIES failed (network drop?) — waiting ${TRY_WAIT}s"
  sleep "$TRY_WAIT"
done
if [ "$rsync_ok" -ne 1 ]; then
  log "ERROR: rsync transfer failed after $MAX_TRIES attempts — keeping current local DB"; exit 1
fi
log "delta transfer complete"

# 3. Guard: transferred file must be a readable SQLite DB at least as fresh as local.
NEW_MAX=$(/usr/bin/sqlite3 "$TMP_LOCAL" "SELECT MAX(date) FROM daily_prices" 2>>"$LOG")
if [ -z "$NEW_MAX" ]; then
  log "ERROR: transferred DB unreadable — keeping current local DB"; exit 1
fi
if [[ "$NEW_MAX" < "$LOCAL_MAX" ]]; then
  log "ERROR: transferred DB ($NEW_MAX) is OLDER than local ($LOCAL_MAX) — keeping current"; exit 1
fi

# 3c. Preserve the dev account's signal unlocks across the swap (local-dev only;
#     prod's snapshot has none). Read them from the CURRENT (pre-swap) DB and
#     accumulate into the overlay store; the post-sync hook restores them. Capturing
#     here — not in the hook, which runs post-swap — catches unlocks added since the
#     last sync. Best-effort; never blocks the sync.
OVERLAY_STORE="${VESIGN_OVERLAY_STORE:-$HOME/.vesign/redesign_overlay.db}"
mkdir -p "$(dirname "$OVERLAY_STORE")"
/usr/bin/sqlite3 "$OVERLAY_STORE" \
  "CREATE TABLE IF NOT EXISTS unlocks_overlay (user_id TEXT, kind TEXT, ticker TEXT, signal_date TEXT, created_at TEXT, UNIQUE(user_id,kind,ticker,signal_date));
   ATTACH DATABASE '$LOCAL_DB' AS live;
   INSERT OR IGNORE INTO unlocks_overlay (user_id,kind,ticker,signal_date,created_at)
     SELECT user_id,kind,ticker,signal_date,created_at FROM live.signal_unlocks;" \
  >>"$LOG" 2>&1 \
  && log "preserve: dev unlocks captured to overlay" \
  || log "preserve: unlock capture skipped (non-fatal)"

# 3d. Preserve the cached AI signal explanations across the swap. Prod's snapshot
#     carries none (explainer undeployed), so without this the post-sync hook's
#     precompute_today() regenerates ALL ~136 explanations EVERY sync (4x/day =
#     paid Anthropic calls for the same signal day). Captured pre-swap → restored
#     by the hook → precompute finds them cached → 0 model calls. Best-effort.
/usr/bin/sqlite3 "$OVERLAY_STORE" \
  "CREATE TABLE IF NOT EXISTS expl_overlay (ticker TEXT, signal_date TEXT, payload TEXT, model TEXT, created_at TEXT, UNIQUE(ticker,signal_date));
   ATTACH DATABASE '$LOCAL_DB' AS live;
   INSERT OR IGNORE INTO expl_overlay (ticker,signal_date,payload,model,created_at)
     SELECT ticker,signal_date,payload,model,created_at FROM live.signal_explanations;" \
  >>"$LOG" 2>&1 \
  && log "preserve: AI explanations captured to overlay" \
  || log "preserve: explanation capture skipped (non-fatal)"

# 3e. Preserve the dev account's portfolio holdings across the swap. `holdings` is a
#     feat/ui-redesign-only table (post watchlist/holdings decoupling); prod's snapshot
#     still only has the legacy `watchlist_holdings`, so without this every sync silently
#     dropped the table and any holdings entered locally (bit us 2026-07-08 and 07-09).
#     Read pre-swap, accumulate into the overlay store; the post-sync hook restores them.
/usr/bin/sqlite3 "$OVERLAY_STORE" \
  "CREATE TABLE IF NOT EXISTS holdings_overlay (user_id TEXT, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT, UNIQUE(user_id,ticker,quantity,buy_price,buy_date));
   ATTACH DATABASE '$LOCAL_DB' AS live;
   INSERT OR IGNORE INTO holdings_overlay (user_id,ticker,quantity,buy_price,buy_date)
     SELECT user_id,ticker,quantity,buy_price,buy_date FROM live.holdings;" \
  >>"$LOG" 2>&1 \
  && log "preserve: dev holdings captured to overlay" \
  || log "preserve: holdings capture skipped (non-fatal)"

# 4. Atomic-ish swap. The old DB's -wal/-shm MUST go with it: a leftover WAL
#    from the replaced file gets replayed onto the new one by the next reader,
#    and an auto-checkpoint then writes those foreign pages INTO the new file —
#    permanent "database disk image is malformed" (bit us 06-10 and 06-12).
if ! /bin/mv "$TMP_LOCAL" "$LOCAL_DB"; then
  log "ERROR: final swap failed"; exit 1
fi
rm -f "${LOCAL_DB}-wal" "${LOCAL_DB}-shm" "${LOCAL_DB}-journal"
log "=== sync done: local DB now at $NEW_MAX ==="

# 5. Re-apply the feat/ui-redesign data layer (TTM fundamentals + index_prices)
#    that production's DB lacks, then refresh the stalest. Local-dev only;
#    never fails the sync (the production-mirrored DB above is already swapped in).
log "running redesign overlay hook…"
/Users/inovenos/PycharmProjects/Vesign/venv/bin/python \
  /Users/inovenos/bin/vesign-redesign-overlay.py >> "$LOG" 2>&1 \
  || log "redesign overlay hook failed (non-fatal — DB still has prod data)"
