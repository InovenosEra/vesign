"""Unit tests for backend.entitlements (pure helpers, no HTTP)."""
import os
import shutil
import tempfile
import importlib
import pytest


@pytest.fixture
def ent():
    """Fresh entitlements module bound to a temp DB, dev-gate ON."""
    saved = {k: os.environ.get(k) for k in ("DB_PATH", "BYPASS_AUTH", "DEV_PLAN", "DEV_WALLET_CENTS")}
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "ent.db")
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("DEV_PLAN", None)
    os.environ.pop("DEV_WALLET_CENTS", None)
    import data.loaders as loaders
    importlib.reload(loaders)
    import backend.entitlements as e
    importlib.reload(e)
    yield e
    shutil.rmtree(tmpdir, ignore_errors=True)
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_default_plan_is_free(ent):
    assert ent.get_plan("user_unknown") == "free"


def test_set_and_get_plan(ent):
    ent.set_plan("user_1", "pro")
    assert ent.get_plan("user_1") == "pro"


def test_balance_defaults_zero_and_credits(ent):
    assert ent.get_balance("user_2") == 0
    ent.credit("user_2", 500, reason="seed")
    assert ent.get_balance("user_2") == 500


def test_unlocks_roundtrip(ent):
    assert ent.get_unlocks("user_3") == set()
    ent.record_unlock("user_3", "buy", "AAPL", "2026-05-26")
    assert ("buy", "AAPL", "2026-05-26") in ent.get_unlocks("user_3")


def test_record_unlock_is_idempotent(ent):
    ent.record_unlock("user_4", "buy", "AAPL", "2026-05-26")
    ent.record_unlock("user_4", "buy", "AAPL", "2026-05-26")
    assert len(ent.get_unlocks("user_4")) == 1


def test_dev_plan_override_when_bypass_on(ent):
    os.environ["DEV_PLAN"] = "max"
    assert ent.get_plan("anyone") == "max"
    del os.environ["DEV_PLAN"]


def test_lock_token_is_stable_and_opaque(ent):
    t1 = ent.lock_token("buy", "AAPL", "2026-05-26")
    t2 = ent.lock_token("buy", "AAPL", "2026-05-26")
    assert t1 == t2
    assert "AAPL" not in t1            # ticker must not be derivable from the token
    assert ent.lock_token("buy", "MSFT", "2026-05-26") != t1


def test_set_plan_invalid_raises(ent):
    with pytest.raises(ValueError):
        ent.set_plan("user_x", "platinum")


def _sig(ticker, date="2026-05-26 00:00:00"):
    return {"ticker": ticker, "company": ticker + " Inc", "logo_url": "x.png",
            "close": 100.0, "fair_value_upside": 0.2, "vqs": 9,
            "prediction_score": 0.3, "signal": "BUY", "date": date}


def test_max_sees_full_buy_rows(ent):
    rows = [_sig("AAPL"), _sig("MSFT")]
    out = ent.gate_signals(rows, kind="BUY", plan="max", unlocks=set())
    assert out == rows                         # untouched


def test_free_buy_rows_are_locked_and_carry_no_identity(ent):
    out = ent.gate_signals([_sig("AAPL")], kind="BUY", plan="free", unlocks=set())
    r = out[0]
    assert r["locked"] is True and r["reason"] == "upgrade"
    for leaky in ("ticker", "company", "logo_url", "close", "fair_value_upside", "vqs"):
        assert leaky not in r                  # SECURITY INVARIANT
    assert "unlock_price_cents" not in r       # free can't purchase
    assert "lock_token" not in r


def test_pro_buy_rows_locked_with_per_row_price_and_token(ent):
    out = ent.gate_signals([_sig("AAPL")], kind="BUY", plan="pro", unlocks=set())
    r = out[0]
    assert r["locked"] is True and r["reason"] == "pay"
    assert r["unlock_price_cents"] == ent.per_row_price_cents("buy")
    assert r["lock_token"] == ent.lock_token("buy", "AAPL", "2026-05-26")
    assert "ticker" not in r


def test_pro_buy_row_unlocked_is_full(ent):
    unlocks = {("buy", "AAPL", "2026-05-26")}
    out = ent.gate_signals([_sig("AAPL")], kind="BUY", plan="pro", unlocks=unlocks)
    assert out[0]["ticker"] == "AAPL" and not out[0].get("locked")


def test_pro_sell_preview_then_per_row_unlock(ent):
    n = ent.PRO_SELL_PREVIEW_ROWS
    rows = [_sig(f"T{i}") for i in range(n + 3)]
    for r in rows: r["signal"] = "SELL"
    out = ent.gate_signals(rows, kind="SELL", plan="pro", unlocks=set())
    assert out[0]["ticker"] == "T0"                       # preview visible
    assert out[n - 1]["ticker"] == f"T{n - 1}" and not out[n - 1].get("locked")
    assert out[n]["locked"] is True and out[n]["reason"] == "pay"
    assert out[n]["unlock_price_cents"] == ent.per_row_price_cents("sell")   # per-row unlock allowed
    assert "lock_token" in out[n]


def test_free_sell_rows_all_locked(ent):
    rows = [_sig("AAPL")]; rows[0]["signal"] = "SELL"
    out = ent.gate_signals(rows, kind="SELL", plan="free", unlocks=set())
    assert out[0]["locked"] is True and out[0]["reason"] == "upgrade"


def _open(ticker, yld, buy_date="2026-05-01"):
    return {"ticker": ticker, "company": ticker + " Inc", "logo_url": "x.png",
            "buy_price": 100.0, "current_price": 100.0 + yld,
            "days_held": 10, "unrealized_pct": yld, "buy_date": buy_date}


def test_open_max_full(ent):
    rows = [_open("AAPL", 5.0)]
    assert ent.gate_open_trades(rows, plan="max", unlocks=set()) == rows


def test_open_free_shows_only_yield_on_top_ten(ent):
    rows = [_open(f"T{i}", float(20 - i)) for i in range(12)]  # already yield-desc
    out = ent.gate_open_trades(rows, plan="free", unlocks=set())
    # top 10: locked, but yield (unrealized_pct) revealed, nothing else
    assert out[0]["locked"] is True and out[0]["reason"] == "upgrade"
    assert out[0]["reveal"] == ["yield"]
    assert out[0]["unrealized_pct"] == 20.0
    assert "ticker" not in out[0] and "buy_price" not in out[0]
    # row 11 (index 10): fully locked, NO yield
    assert out[10]["reveal"] == []
    assert "unrealized_pct" not in out[10]


def test_open_pro_previews_ten_then_bulk_locks(ent):
    rows = [_open(f"T{i}", float(20 - i)) for i in range(12)]
    out = ent.gate_open_trades(rows, plan="pro", unlocks=set())
    assert out[0]["ticker"] == "T0"
    assert out[10]["locked"] is True and out[10]["reason"] == "pay"
    assert "unlock_price_cents" not in out[10]      # bulk-only
    assert "lock_token" in out[10]


def test_open_pro_unlocked_row_is_full(ent):
    rows = [_open(f"T{i}", float(20 - i)) for i in range(12)]
    unlocks = {("open", "T10", "2026-05-01")}
    out = ent.gate_open_trades(rows, plan="pro", unlocks=unlocks)
    assert out[10]["ticker"] == "T10" and not out[10].get("locked")


def test_multidate_only_latest_buy_sell_gated(ent):
    latest = "2026-05-26"
    rows = [
        {"ticker": "AAPL", "company": "AAPL Inc", "close": 100, "signal": "BUY",  "date": "2026-05-26 00:00:00"},  # today BUY -> gate
        {"ticker": "MSFT", "company": "MSFT Inc", "close": 200, "signal": "SELL", "date": "2026-05-26 00:00:00"},  # today SELL -> gate
        {"ticker": "NVDA", "company": "NVDA Inc", "close": 300, "signal": "BUY",  "date": "2026-05-20 00:00:00"},  # historical BUY -> keep
        {"ticker": "TSLA", "company": "TSLA Inc", "close": 400, "signal": "HOLD", "date": "2026-05-26 00:00:00"},  # today HOLD -> keep
    ]
    out = ent.gate_signals_multidate(rows, plan="free", unlocks=set(), latest_date=latest)
    assert out[0]["locked"] and "ticker" not in out[0] and out[0]["reason"] == "upgrade"
    assert out[1]["locked"] and "ticker" not in out[1]
    assert out[2]["ticker"] == "NVDA"      # historical BUY untouched
    assert out[3]["ticker"] == "TSLA"      # today's HOLD untouched


def test_multidate_max_untouched(ent):
    rows = [{"ticker": "AAPL", "signal": "BUY", "date": "2026-05-26 00:00:00"}]
    assert ent.gate_signals_multidate(rows, plan="max", unlocks=set(), latest_date="2026-05-26") == rows


def test_multidate_pro_locks_today_buy_with_token_unless_unlocked(ent):
    latest = "2026-05-26"
    rows = [
        {"ticker": "AAPL", "signal": "BUY", "date": "2026-05-26 00:00:00"},
        {"ticker": "MSFT", "signal": "BUY", "date": "2026-05-26 00:00:00"},
    ]
    unlocks = {("buy", "MSFT", "2026-05-26")}
    out = ent.gate_signals_multidate(rows, plan="pro", unlocks=unlocks, latest_date=latest)
    assert out[0]["locked"] and out[0]["reason"] == "pay"
    assert out[0]["unlock_price_cents"] == ent.per_row_price_cents("buy")
    assert out[0]["lock_token"] == ent.lock_token("buy", "AAPL", "2026-05-26")
    assert out[1]["ticker"] == "MSFT"      # unlocked -> full


def test_multidate_pro_today_sell_bulk_only(ent):
    rows = [{"ticker": "AAPL", "signal": "SELL", "date": "2026-05-26 00:00:00"}]
    out = ent.gate_signals_multidate(rows, plan="pro", unlocks=set(), latest_date="2026-05-26")
    assert out[0]["locked"] and out[0]["reason"] == "pay"
    assert "unlock_price_cents" not in out[0]   # SELL bulk-only
    assert "lock_token" in out[0]


def test_multidate_none_latest_gates_nothing(ent):
    rows = [{"ticker": "AAPL", "signal": "BUY", "date": "2026-05-26 00:00:00"}]
    assert ent.gate_signals_multidate(rows, plan="free", unlocks=set(), latest_date=None) == rows


def test_tier_of_buckets_untiered_as_promising(ent):
    assert ent.tier_of({"tier": 1}) == 1
    assert ent.tier_of({"tier": 2}) == 2
    assert ent.tier_of({"tier": 3}) == 3
    assert ent.tier_of({"tier": None}) == 3      # untiered → Promising
    assert ent.tier_of({}) == 3                   # missing → Promising


def test_tier_rate_cents(ent):
    assert ent.tier_rate_cents(1) == 30
    assert ent.tier_rate_cents(2) == 20
    assert ent.tier_rate_cents(3) == 10
    assert ent.tier_rate_cents(99) == 10          # unknown → lowest rate


def test_tier_unlock_price_is_rate_times_count(ent):
    assert ent.tier_unlock_price_cents(2, 3) == 60     # Strong 20¢ × 3
    assert ent.tier_unlock_price_cents(3, 11) == 110   # Promising 10¢ × 11
    assert ent.tier_unlock_price_cents(1, 0) == 0


def test_all_tiers_price_is_15pct_off_sum(ent):
    # 0 Prime, 3 Strong ($0.60), 11 Promising ($1.10) → sum $1.70 → 15% off = $1.45
    price = ent.all_tiers_price_cents({1: 0, 2: 3, 3: 11})
    assert price == 145


def test_all_tiers_price_clamps_to_priciest_tier(ent):
    # Only Promising present: gross 30¢, 15% off = 26¢, but a single tier is 30¢ → clamp up
    assert ent.all_tiers_price_cents({1: 0, 2: 0, 3: 3}) == 30


def test_all_tiers_price_zero_when_nothing_locked(ent):
    assert ent.all_tiers_price_cents({1: 0, 2: 0, 3: 0}) == 0
