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
# Per-signal unlock price differs by signal kind: BUY $0.20, SELL $0.10.
PER_ROW_PRICE_BY_KIND = {"buy": 20, "sell": 10}
SEE_ALL_PRICE_CENTS = 50        # legacy default exposed via /api/me; live charge is dynamic


def per_row_price_cents(kind: str) -> int:
    """Per-signal unlock price for a BUY/SELL kind, in cents (BUY 20, SELL 10)."""
    return PER_ROW_PRICE_BY_KIND.get((kind or "").lower(), 10)


def see_all_price_cents(n_signals: int, kind: str = "buy") -> int:
    """Bulk 'See all' price: exactly 50% of (n_signals × the kind's per-row), to
    the cent. e.g. 51 SELL × $0.10 = $5.10 → $2.55; 15 BUY × $0.20 = $3.00 → $1.50.
    (No whole-dollar floor — at $0.10/row it rounded small columns down to $0.)"""
    return (max(0, n_signals) * per_row_price_cents(kind)) // 2


# Per-tier unlock pricing (BUY only). Value-weighted: Prime dearest. A tier
# unlocks atomically (no per-row), so the locked count is the full tier or zero.
PER_TIER_RATE_CENTS = {1: 50, 2: 30, 3: 10}   # Prime, Strong, Promising (cents/signal)
TIER_ALL_DISCOUNT_PCT = 0                       # no bundle discount — "Unlock all" = full sum


def tier_of(row) -> int:
    """Bucket a BUY row to a pricing/legend tier: 1=Prime, 2=Strong, else 3=Promising
    (untiered/legacy BUYs fall into Promising, the catch-all lowest tier)."""
    t = row.get("tier")
    return t if t in (1, 2) else 3


def tier_rate_cents(tier: int) -> int:
    return PER_TIER_RATE_CENTS.get(tier, 10)


def tier_unlock_price_cents(tier: int, n_locked: int) -> int:
    """Price to unlock all still-locked signals of one tier: rate × count."""
    return tier_rate_cents(tier) * max(0, n_locked)


def all_tiers_price_cents(locked_by_tier: dict) -> int:
    """Price to unlock ALL still-locked BUY tiers at once: the full per-tier sum
    (TIER_ALL_DISCOUNT_PCT is 0 — no bundle discount), rounded DOWN to the
    nearest 5¢ in case the figure is odd."""
    gross = sum(tier_unlock_price_cents(t, n) for t, n in locked_by_tier.items())
    if gross <= 0:
        return 0
    price = (gross * (100 - TIER_ALL_DISCOUNT_PCT) + 50) // 100   # full sum (no discount)
    return (price // 5) * 5                                       # round DOWN to nearest 5¢


PRO_PREVIEW_ROWS = 10          # open-trades preview (Pro) + free top-N yield reveal
PRO_SELL_PREVIEW_ROWS = 5      # Pro: free SELL signals before pay-to-unlock kicks in
OPEN_UNLOCK_ALL_CENTS = 100    # Pro: flat $1 to permanently unlock ALL open trades (page 1 is
                                # always free; page 2+ is fully locked until this is bought)
SELL_UNLOCK_ALL_CENTS = 100    # Pro: flat $1 to unlock ALL SELL signals, regardless of count

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


# Vesign-model output columns: Max-only across every endpoint (Free AND Pro are
# gated). Analyst-derived upside (fair_value_upside aliased to
# (target_mean-close)/close in most queries) is the allowed "Prediction" and is
# NOT here — except /api/signals/markers returns the RAW model fair_value_upside,
# so that endpoint gates it explicitly.
MODEL_FIELDS = ("signal", "health_score", "prediction_score", "vqs", "vesign_score", "tier", "score")
HOLDING_MODEL_FIELDS = ("signal", "health_score", "prediction_score")


def redact_fields(rows, *, plan, fields):
    """Return copies with the given model fields nulled for non-Max plans, so the
    model output never leaves the server. NON-mutating (some callers pass shared/cached
    rows). Accepts a list of dicts or a single dict; returns the same shape."""
    if plan == "max":
        return rows
    def _redact(d):
        return d if not d else {**d, **{f: None for f in fields if f in d}}
    return _redact(rows) if isinstance(rows, dict) else [_redact(r) for r in rows]


def redact_holdings(rows, *, plan):
    """Holdings keep the analyst target (Prediction); gate signal/health/ML."""
    return redact_fields(rows, plan=plan, fields=HOLDING_MODEL_FIELDS)


OPEN_ALL_UNLOCK_KEY = ("open", "*", "*")   # sentinel written by the flat "unlock all" purchase


def gate_open_trades(rows, *, plan, unlocks):
    """Redact the open-trades list. `rows` MUST already be sorted (the endpoint
    does this) — that order defines what counts as "page 1". Free: top-10
    reveal yield only; rest fully locked. Pro: page 1 (first PRO_PREVIEW_ROWS)
    is always free; every row after that is fully locked (no per-row unlocks)
    until OPEN_ALL_UNLOCK_KEY is in `unlocks`, which then grants every row —
    including ones opened after the purchase."""
    if plan == "max":
        return list(rows)
    if plan == "pro" and OPEN_ALL_UNLOCK_KEY in unlocks:
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
        # pro, full-unlock not purchased
        if i < PRO_PREVIEW_ROWS:
            out.append(r)
        else:
            out.append(_locked_row("open", date, "pay", token=lock_token("open", ticker, date)))
    return out


class InsufficientFunds(Exception):
    pass


def unlock_purchase(user_id: str, *, occurrences, kind, price_cents) -> int:
    """Atomically: skip already-owned occurrences, charge `price_cents` iff at
    least one is new, record unlocks + ledger. Returns the new balance.
    Raises InsufficientFunds. Caller guarantees plan == 'pro'.

    The charge is a single conditional UPDATE (WHERE balance_cents >= price) so
    concurrent requests cannot double-spend or drive the balance negative."""
    with engine.begin() as conn:
        owned = {
            (r[0], r[1]) for r in conn.execute(text(
                "SELECT ticker, signal_date FROM signal_unlocks WHERE user_id = :u AND kind = :k"
            ), {"u": user_id, "k": kind}).fetchall()
        }
        new = [(t, d) for (t, d) in occurrences if (t, d) not in owned]
        if not new:
            bal_row = conn.execute(
                text("SELECT balance_cents FROM wallets WHERE user_id = :u"), {"u": user_id}
            ).fetchone()
            return int(bal_row[0]) if bal_row else 0      # nothing to buy; no charge
        # Atomic check-and-deduct: matches 0 rows if the wallet is missing or
        # underfunded, which is the race-safe "insufficient funds" signal.
        res = conn.execute(text("""
            UPDATE wallets SET balance_cents = balance_cents - :p
            WHERE user_id = :u AND balance_cents >= :p
        """), {"u": user_id, "p": price_cents})
        if res.rowcount != 1:
            raise InsufficientFunds()
        for (t, d) in new:
            conn.execute(text("""
                INSERT OR IGNORE INTO signal_unlocks (user_id, kind, ticker, signal_date)
                VALUES (:u, :k, :t, :d)
            """), {"u": user_id, "k": kind, "t": t, "d": d})
        conn.execute(text("""
            INSERT INTO wallet_txns (user_id, amount_cents, reason, ref)
            VALUES (:u, :a, :r, :ref)
        """), {"u": user_id, "a": -price_cents, "r": f"unlock_{kind}",
               "ref": ",".join(f"{t}:{d}" for t, d in new)})
        bal_row = conn.execute(
            text("SELECT balance_cents FROM wallets WHERE user_id = :u"), {"u": user_id}
        ).fetchone()
        return int(bal_row[0])


def gate_signals_multidate(rows, *, plan, unlocks, latest_date):
    """Redact ONLY the latest-date BUY/SELL rows in a mixed multi-date list
    (e.g. /api/signals). Historical signals and HOLDs are always returned full
    — they're the public track record. No first-N preview here (that's a
    feature of the dedicated Signals-page endpoints). `latest_date` is the
    market's most recent signal date as YYYY-MM-DD, or None to gate nothing."""
    if plan == "max" or not latest_date:
        return list(rows)
    out = []
    for r in rows:
        sig = (r.get("signal") or "").upper()
        date = _norm_date(r.get("date"))
        if sig in ("BUY", "SELL") and date == latest_date:
            k = sig.lower()
            ticker = r.get("ticker")
            if plan == "pro" and (k, ticker, date) in unlocks:
                out.append(r)
            elif plan == "free":
                out.append(_locked_row(k, date, "upgrade"))
            else:  # pro, not unlocked — bulk/tier-only, no per-row token
                out.append(_locked_row(k, date, "pay"))
        else:
            out.append(r)
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
        if k == "sell" and i < PRO_SELL_PREVIEW_ROWS:
            out.append(r)               # free SELL preview (Pro), then bulk-unlock
            continue
        # No per-row unlock: BUY unlocks by tier, SELL by "Unlock all". The locked
        # row carries no token/price — purchase happens from the section header.
        out.append(_locked_row(k, date, "pay"))
    return out
