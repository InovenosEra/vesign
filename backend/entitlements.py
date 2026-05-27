"""Subscription tiers, wallet, and per-signal unlocks for the Signals page.

Plan source is our own DB table (admin-controlled, like blocked_users). A
short-circuit dev override (env DEV_PLAN / DEV_WALLET_CENTS) is honored ONLY
when BYPASS_AUTH=1 — i.e. local dev and tests, never production.
"""
import hashlib
import hmac
import os

from sqlalchemy import text

from data.loaders import engine

PLANS = ("free", "pro", "max")
PER_ROW_PRICE_CENTS = 10
SEE_ALL_PRICE_CENTS = 50
PRO_PREVIEW_ROWS = 10

_UNLOCK_SECRET = (
    os.getenv("UNLOCK_SECRET")
    or os.getenv("CLERK_SECRET_KEY")
    or "vesign-dev-unlock-secret"
).encode()

if not os.getenv("UNLOCK_SECRET") and not os.getenv("CLERK_SECRET_KEY"):
    import warnings
    warnings.warn(
        "UNLOCK_SECRET not set — using dev fallback secret; set it in prod .env",
        stacklevel=1,
    )


def _ensure_tables() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_plans (
                user_id    TEXT PRIMARY KEY,
                plan       TEXT NOT NULL DEFAULT 'free',
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS wallets (
                user_id       TEXT PRIMARY KEY,
                balance_cents INTEGER NOT NULL DEFAULT 0
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS signal_unlocks (
                user_id     TEXT NOT NULL,
                kind        TEXT NOT NULL,
                ticker      TEXT NOT NULL,
                signal_date TEXT NOT NULL,
                created_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(user_id, kind, ticker, signal_date)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS wallet_txns (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                reason     TEXT NOT NULL,
                ref        TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """))


_ensure_tables()


def _dev_enabled() -> bool:
    return os.getenv("BYPASS_AUTH") == "1"


def get_plan(user_id: str) -> str:
    if _dev_enabled():
        dev = os.getenv("DEV_PLAN")
        if dev in PLANS:
            return dev
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT plan FROM user_plans WHERE user_id = :u"), {"u": user_id}
        ).fetchone()
    return row[0] if row and row[0] in PLANS else "free"


def set_plan(user_id: str, plan: str) -> None:
    if plan not in PLANS:
        raise ValueError(f"bad plan {plan!r}")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO user_plans (user_id, plan, updated_at)
            VALUES (:u, :p, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET plan = :p, updated_at = datetime('now')
        """), {"u": user_id, "p": plan})


def get_balance(user_id: str) -> int:
    if _dev_enabled():
        raw = os.getenv("DEV_WALLET_CENTS")
        if raw is not None:
            try:
                return int(raw)
            except ValueError:
                pass
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT balance_cents FROM wallets WHERE user_id = :u"), {"u": user_id}
        ).fetchone()
    return int(row[0]) if row else 0


def credit(user_id: str, amount_cents: int, *, reason: str, ref: str | None = None) -> int:
    """Add (or, if negative, remove) balance and log a ledger entry. Returns new balance."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO wallets (user_id, balance_cents) VALUES (:u, :a)
            ON CONFLICT(user_id) DO UPDATE SET balance_cents = balance_cents + :a
        """), {"u": user_id, "a": amount_cents})
        conn.execute(text("""
            INSERT INTO wallet_txns (user_id, amount_cents, reason, ref)
            VALUES (:u, :a, :r, :ref)
        """), {"u": user_id, "a": amount_cents, "r": reason, "ref": ref})
        row = conn.execute(
            text("SELECT balance_cents FROM wallets WHERE user_id = :u"), {"u": user_id}
        ).fetchone()
    return int(row[0])


def get_unlocks(user_id: str) -> set[tuple[str, str, str]]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT kind, ticker, signal_date FROM signal_unlocks WHERE user_id = :u
        """), {"u": user_id}).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def record_unlock(user_id: str, kind: str, ticker: str, signal_date: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT OR IGNORE INTO signal_unlocks (user_id, kind, ticker, signal_date)
            VALUES (:u, :k, :t, :d)
        """), {"u": user_id, "k": kind, "t": ticker, "d": signal_date})


def lock_token(kind: str, ticker: str, signal_date: str) -> str:
    msg = f"{kind}|{ticker}|{signal_date}".encode()
    return hmac.new(_UNLOCK_SECRET, msg, hashlib.sha256).hexdigest()


def _norm_date(value) -> str:
    return str(value or "")[:10]


def _locked_row(kind, signal_date, reason, *, price_cents=None, token=None,
                reveal=None, revealed_values=None) -> dict:
    """A redacted row. Carries NO identifying fields — only what the UI needs
    to render a faded row and (for Pro) a purchase affordance."""
    row = {
        "locked": True,
        "kind": kind,
        "signal_date": signal_date,
        "reason": reason,                # 'upgrade' | 'pay'
        "reveal": reveal or [],
    }
    if price_cents is not None:
        row["unlock_price_cents"] = price_cents
    if token is not None:
        row["lock_token"] = token
    if revealed_values:
        row.update(revealed_values)
    return row


def gate_open_trades(rows, *, plan, unlocks):
    """Redact the open-trades list. `rows` MUST already be sorted by yield desc
    (the endpoint does this). Free: top-10 reveal yield only; rest fully locked.
    Pro: first-10 preview, rest bulk-lockable."""
    if plan == "max":
        return list(rows)
    out = []
    for i, r in enumerate(rows):
        date = _norm_date(r.get("buy_date"))
        ticker = r.get("ticker")
        if plan == "free":
            if i < PRO_PREVIEW_ROWS:
                out.append(_locked_row("open", date, "upgrade", reveal=["yield"],
                                       revealed_values={"unrealized_pct": r.get("unrealized_pct")}))
            else:
                out.append(_locked_row("open", date, "upgrade"))
            continue
        # pro
        if ("open", ticker, date) in unlocks or i < PRO_PREVIEW_ROWS:
            out.append(r)
            continue
        out.append(_locked_row("open", date, "pay", token=lock_token("open", ticker, date)))
    return out


def gate_signals(rows, *, kind, plan, unlocks):
    """Redact a BUY or SELL list for the given plan. Returns a NEW list; full
    rows are passed through by reference (read-only), locked rows are fresh."""
    k = kind.lower()                     # 'buy' | 'sell'
    if plan == "max":
        return list(rows)
    out = []
    for i, r in enumerate(rows):
        date = _norm_date(r.get("date"))
        if plan == "free":
            out.append(_locked_row(k, date, "upgrade"))
            continue
        # pro
        ticker = r.get("ticker")
        if (k, ticker, date) in unlocks:
            out.append(r)
            continue
        if k == "sell" and i < PRO_PREVIEW_ROWS:
            out.append(r)               # free 10-row preview (SELL only)
            continue
        price = PER_ROW_PRICE_CENTS if k == "buy" else None   # SELL is bulk-only
        out.append(_locked_row(k, date, "pay", price_cents=price,
                               token=lock_token(k, ticker, date)))
    return out
