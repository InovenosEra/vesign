#!/usr/bin/env python3
"""Post-sync hook: keep the feat/ui-redesign data layer alive across the daily
prod->local DB sync.

The 08:30 sync (com.vesign.dbsync) overwrites the local vesign.db with a full
copy of PRODUCTION's database, which does NOT contain the ui-redesign-only data
(production has no fundamentals TTM columns, no index_prices table, and no
market_quotes table). This hook runs right after the swap and:

  1. PRESERVE  — re-applies the locally-generated TTM fundamentals + index_prices
     + market_quotes from a persistent overlay store (instant, no network) so the
     Research/Market cards never show "-" or partial strips on a cold load.
  2. REFRESH   — re-fetches the N stalest fundamentals from FMP + the latest index
     levels + market quotes, so the price-sensitive figures don't drift.
  3. SNAPSHOT  — writes the now-current data back to the overlay for next time.

Local-dev tooling ONLY; never runs on production. Wired into ~/bin/vesign-daily-sync.sh.
See the redesign-deploy-runbook memory for context.
"""
import os
import sys
import sqlite3
import datetime
import traceback

from dotenv import load_dotenv

REPO = "/Users/inovenos/PycharmProjects/Vesign"
# Load the repo .env so BYPASS_USER_ID (the impersonated dev account) is available
# under launchd's minimal env — needed by seed_dev_account().
load_dotenv(os.path.join(REPO, ".env"))
# Overridable for isolated testing (the DB itself is redirected via DB_PATH, which
# data.loaders already honours when building the engine).
OVERLAY = os.environ.get("VESIGN_OVERLAY_STORE") or os.path.expanduser("~/.vesign/redesign_overlay.db")
LOG = os.environ.get("VESIGN_OVERLAY_LOG") or os.path.expanduser("~/Library/Logs/vesign-redesign-overlay.log")
# stalest fundamentals re-fetched from FMP per run (~9-day full cycle); 0 skips the fetch
REFRESH_N = int(os.environ.get("VESIGN_OVERLAY_REFRESH_N", "200"))

FUND_COLS = ["pe_ttm", "eps_ttm", "revenue_ttm", "revenue_growth", "gross_margin",
             "op_margin", "net_margin", "roe", "de_ratio", "fundamentals_updated"]

# Redesign-only tables with an identical (date,ticker,close) schema, restored verbatim.
# (main_table, overlay_table)
FULL_TABLES = [("index_prices", "idx_overlay"), ("market_quotes", "mq_overlay")]
TABLE_DDL = "CREATE TABLE IF NOT EXISTS %s (date DATETIME, ticker TEXT, close FLOAT)"


def log(msg):
    line = "[%s] %s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg)
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass
    print(line)


sys.path.insert(0, REPO)
# Importing data.loaders runs _ensure_fundamentals_columns() at module load, which
# idempotently ALTERs the TTM columns onto the freshly-synced (prod-shaped) table.
from data.loaders import engine            # noqa: E402
from data import fundamentals as F         # noqa: E402
from data import market_data as MD         # noqa: E402
from sqlalchemy import text                # noqa: E402

MAIN_DB = engine.url.database               # absolute path to vesign.db


def restore_from_overlay():
    """PRESERVE: fill the (now-NULL) TTM columns + empty full-copy tables from overlay."""
    if not os.path.exists(OVERLAY):
        log("restore: no overlay yet — skipping (will seed at snapshot)")
        return
    engine.dispose()  # release pooled connections before raw-sqlite writes
    con = sqlite3.connect(MAIN_DB, timeout=30)
    try:
        con.execute("ATTACH DATABASE ? AS ov", (OVERLAY,))
        ov_tables = {r[0] for r in con.execute("SELECT name FROM ov.sqlite_master WHERE type='table'")}
        # --- TTM fundamentals (column overlay) ---
        if "fund_overlay" in ov_tables:
            setc = ", ".join("%s=o.%s" % (c, c) for c in FUND_COLS)
            try:
                con.execute(
                    "UPDATE fundamentals AS f SET %s FROM ov.fund_overlay AS o "
                    "WHERE f.ticker=o.ticker AND f.pe_ttm IS NULL" % setc)
            except sqlite3.OperationalError:
                # Fallback for SQLite < 3.33 (no UPDATE..FROM): per-row.
                rows = con.execute(
                    "SELECT ticker,%s FROM ov.fund_overlay" % ",".join(FUND_COLS)).fetchall()
                con.executemany(
                    "UPDATE fundamentals SET %s WHERE ticker=? AND pe_ttm IS NULL"
                    % ", ".join("%s=?" % c for c in FUND_COLS),
                    [tuple(r[1:]) + (r[0],) for r in rows])
        # --- full-copy tables (index_prices, market_quotes) ---
        for main_t, ov_t in FULL_TABLES:
            con.execute(TABLE_DDL % main_t)
            if ov_t in ov_tables and con.execute("SELECT COUNT(*) FROM %s" % main_t).fetchone()[0] == 0:
                con.execute("INSERT INTO %s (date,ticker,close) "
                            "SELECT date,ticker,close FROM ov.%s" % (main_t, ov_t))
        # --- cached AI signal explanations (avoid regenerating ~136 every sync) ---
        # Restored BEFORE precompute_today() runs (hook step further down), so it
        # finds the latest day already cached and makes zero paid model calls.
        n_expl = 0
        if "expl_overlay" in ov_tables:
            con.execute("""CREATE TABLE IF NOT EXISTS signal_explanations (
                ticker TEXT NOT NULL, signal_date TEXT NOT NULL, payload TEXT NOT NULL,
                model TEXT NOT NULL, created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(ticker, signal_date))""")
            con.execute("""INSERT OR IGNORE INTO signal_explanations
                (ticker, signal_date, payload, model, created_at)
                SELECT ticker, signal_date, payload, model, created_at FROM ov.expl_overlay""")
            n_expl = con.execute("SELECT COUNT(*) FROM signal_explanations").fetchone()[0]
        con.commit()
        nf = con.execute("SELECT COUNT(*) FROM fundamentals WHERE pe_ttm IS NOT NULL").fetchone()[0]
        counts = {t: con.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0] for t, _ in FULL_TABLES}
        log("restore: fundamentals pe_ttm populated=%d, %s, explanations=%d" % (nf, counts, n_expl))
    finally:
        con.close()


def refresh_stalest(n=REFRESH_N):
    """REFRESH: re-fetch the N stalest fundamentals + latest index levels/market quotes."""
    if n <= 0:
        log("refresh: skipped (REFRESH_N=0)")
        return
    today = datetime.date.today().isoformat()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT f.ticker FROM fundamentals f
            LEFT JOIN companies c ON c.ticker = f.ticker
            WHERE COALESCE(c.market,'US') = 'US'
              AND (c.sector IS NULL OR c.sector != 'ETF')
              AND (f.pe_ttm IS NULL OR f.fundamentals_updated IS NULL
                   OR DATE(f.fundamentals_updated) < DATE(:today))
            ORDER BY (f.pe_ttm IS NOT NULL), f.fundamentals_updated ASC
            LIMIT :n
        """), {"today": today, "n": n}).fetchall()
    tickers = [r[0] for r in rows]
    filled = 0
    for t in tickers:
        try:
            fund = F.fetch_fundamentals(t)
            F.store_fundamentals(t, fund, engine, updated=today)
            if fund.get("pe_ttm") is not None or fund.get("net_margin") is not None:
                filled += 1
        except Exception:
            pass
    log("refresh: %d/%d fundamentals re-fetched from FMP" % (filled, len(tickers)))
    for fn, what in ((MD.update_indices, "index levels"), (MD.update_market_quotes, "market quotes")):
        try:
            fn()
            log("refresh: %s brought up to latest" % what)
        except Exception as e:
            log("refresh: %s failed: %s" % (what, e))


def snapshot_overlay():
    """SNAPSHOT: persist the current TTM fundamentals + full-copy tables to the overlay."""
    os.makedirs(os.path.dirname(OVERLAY), exist_ok=True)
    engine.dispose()
    con = sqlite3.connect(MAIN_DB, timeout=30)
    try:
        for main_t, _ in FULL_TABLES:
            con.execute(TABLE_DDL % main_t)  # ensure main tables exist before we read them
        con.execute("ATTACH DATABASE ? AS ov", (OVERLAY,))
        coldefs = ", ".join("%s %s" % (c, "TEXT" if c == "fundamentals_updated" else "FLOAT")
                            for c in FUND_COLS)
        con.execute("CREATE TABLE IF NOT EXISTS ov.fund_overlay (ticker TEXT PRIMARY KEY, %s)" % coldefs)
        cols = ",".join(FUND_COLS)
        con.execute("DELETE FROM ov.fund_overlay")
        con.execute("INSERT INTO ov.fund_overlay (ticker,%s) "
                    "SELECT ticker,%s FROM main.fundamentals WHERE pe_ttm IS NOT NULL" % (cols, cols))
        for main_t, ov_t in FULL_TABLES:
            con.execute(TABLE_DDL % ("ov." + ov_t))
            con.execute("DELETE FROM ov.%s" % ov_t)
            con.execute("INSERT INTO ov.%s (date,ticker,close) "
                        "SELECT date,ticker,close FROM main.%s" % (ov_t, main_t))
        con.commit()
        nf = con.execute("SELECT COUNT(*) FROM ov.fund_overlay").fetchone()[0]
        counts = {ov_t: con.execute("SELECT COUNT(*) FROM ov.%s" % ov_t).fetchone()[0]
                  for _, ov_t in FULL_TABLES}
        log("snapshot: overlay saved (fundamentals=%d, %s)" % (nf, counts))
    finally:
        con.close()


def seed_dev_account():
    """Local-dev only: the prod->local sync overwrites vesign.db, which wipes the
    user_plans / wallets / signal_unlocks of the impersonated dev account (prod has
    no such account). Re-assert a paid plan and top the test wallet UP to a target
    so the Signals paywall stays testable across syncs. Never lowers the balance.
    (Unlocks are preserved separately by restore_dev_unlocks(), below.)

    Gated on BYPASS_USER_ID (the dev impersonation account); a no-op without it.
    Tunable via VESIGN_DEV_PLAN (default 'pro') and VESIGN_DEV_WALLET_CENTS ($50)."""
    uid = os.environ.get("BYPASS_USER_ID")
    if not uid:
        log("dev-account: BYPASS_USER_ID unset — skipping")
        return
    plan = os.environ.get("VESIGN_DEV_PLAN", "pro")
    target = int(os.environ.get("VESIGN_DEV_WALLET_CENTS", "5000"))
    from backend import entitlements as ent
    ent.set_plan(uid, plan)
    bal = ent.get_balance(uid)
    if bal < target:
        ent.credit(uid, target - bal, reason="dev-overlay-topup")
    log("dev-account: %s plan=%s balance_cents=%d"
        % (uid, ent.get_plan(uid), ent.get_balance(uid)))


def restore_dev_unlocks():
    """Local-dev only: re-apply the dev account's signal unlocks that the sync
    script captured (pre-swap) into the overlay store's `unlocks_overlay` table,
    so the Signals paywall stays unlocked across the prod->local sync instead of
    resetting every few hours. Must run AFTER seed_dev_account() — importing
    entitlements there created the signal_unlocks table on the fresh prod-shaped DB.
    Gated on BYPASS_USER_ID; best-effort, never fatal. (Prod is unaffected: prod's
    DB is never swapped, so real users' unlocks already persist permanently.)"""
    uid = os.environ.get("BYPASS_USER_ID")
    if not uid or not os.path.exists(OVERLAY):
        return
    src = sqlite3.connect(OVERLAY, timeout=30)
    try:
        have = {r[0] for r in src.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "unlocks_overlay" not in have:
            return
        rows = src.execute(
            "SELECT user_id,kind,ticker,signal_date,created_at FROM unlocks_overlay "
            "WHERE user_id=?", (uid,)).fetchall()
    finally:
        src.close()
    if not rows:
        return
    with engine.begin() as conn:
        for (u, k, t, d, c) in rows:
            conn.execute(text(
                "INSERT OR IGNORE INTO signal_unlocks (user_id,kind,ticker,signal_date,created_at) "
                "VALUES (:u,:k,:t,:d,:c)"), {"u": u, "k": k, "t": t, "d": d, "c": c})
    log("unlocks: restored %d dev-account unlock(s) from overlay" % len(rows))


def restore_dev_holdings():
    """Local-dev only: re-apply the dev account's portfolio holdings that the sync
    script captured (pre-swap) into the overlay store's `holdings_overlay` table.
    `holdings` is a feat/ui-redesign-only table (prod still has the legacy
    `watchlist_holdings`), so every prod->local swap silently drops it; without this
    restore, the Holdings tab shows "No holdings yet" after every sync (bit us
    2026-07-08 and 07-09). Gated on BYPASS_USER_ID; best-effort, never fatal."""
    uid = os.environ.get("BYPASS_USER_ID")
    if not uid or not os.path.exists(OVERLAY):
        return
    src = sqlite3.connect(OVERLAY, timeout=30)
    try:
        have = {r[0] for r in src.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "holdings_overlay" not in have:
            return
        rows = src.execute(
            "SELECT user_id,ticker,quantity,buy_price,buy_date FROM holdings_overlay "
            "WHERE user_id=?", (uid,)).fetchall()
    finally:
        src.close()
    if not rows:
        return
    engine.dispose()
    con = sqlite3.connect(MAIN_DB, timeout=30)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL,
            ticker TEXT NOT NULL, quantity REAL NOT NULL, buy_price REAL NOT NULL,
            buy_date TEXT NOT NULL)""")
        existing = {tuple(r) for r in con.execute(
            "SELECT user_id,ticker,quantity,buy_price,buy_date FROM holdings WHERE user_id=?", (uid,))}
        new_rows = [r for r in rows if r not in existing]
        con.executemany(
            "INSERT INTO holdings (user_id,ticker,quantity,buy_price,buy_date) VALUES (?,?,?,?,?)",
            new_rows)
        con.commit()
        log("holdings: restored %d/%d dev-account holding(s) from overlay" % (len(new_rows), len(rows)))
    finally:
        con.close()


def main():
    log("=== overlay hook start (db=%s) ===" % MAIN_DB)
    try:
        restore_from_overlay()
    except Exception:
        log("ERROR:\n" + traceback.format_exc())
        sys.exit(1)

    # Re-seed the local dev test account (plan + wallet) — wiped by every sync.
    # Done HERE, right after the fast local restore and BEFORE the ~10-min FMP
    # refresh: a mac shutdown/restart mid-refresh killed the old late-seeding on
    # 06-26 21:10 and 06-27 15:31, leaving the test wallet unseeded ("not enough
    # money"). Strictly non-fatal: must never fail the sync.
    try:
        seed_dev_account()
        restore_dev_unlocks()
        restore_dev_holdings()
    except Exception:
        log("dev-account: skipped (non-fatal):\n" + traceback.format_exc())

    try:
        refresh_stalest()
        snapshot_overlay()
        log("=== overlay hook done ===")
    except Exception:
        log("ERROR:\n" + traceback.format_exc())
        sys.exit(1)

    # Precompute today's BUY/SELL AI explanations so the local Signals page is
    # instant after the sync. Idempotent (cached tickers skip the model call) and
    # strictly non-fatal — must never fail the sync. Post-deploy this is mostly a
    # no-op locally because the synced prod DB already carries the explanations.
    try:
        from data import explanations as EXPL
        res = EXPL.precompute_today()
        log("precompute: signal explanations %s" % res)
    except Exception:
        log("precompute: skipped (non-fatal):\n" + traceback.format_exc())


if __name__ == "__main__":
    main()
