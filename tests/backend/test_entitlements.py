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
    assert r["unlock_price_cents"] == ent.PER_ROW_PRICE_CENTS
    assert r["lock_token"] == ent.lock_token("buy", "AAPL", "2026-05-26")
    assert "ticker" not in r


def test_pro_buy_row_unlocked_is_full(ent):
    unlocks = {("buy", "AAPL", "2026-05-26")}
    out = ent.gate_signals([_sig("AAPL")], kind="BUY", plan="pro", unlocks=unlocks)
    assert out[0]["ticker"] == "AAPL" and not out[0].get("locked")


def test_pro_sell_previews_first_ten_then_locks_bulk_only(ent):
    rows = [_sig(f"T{i}", ) for i in range(12)]
    for r in rows: r["signal"] = "SELL"
    out = ent.gate_signals(rows, kind="SELL", plan="pro", unlocks=set())
    assert out[0]["ticker"] == "T0"            # first 10 visible
    assert out[9]["ticker"] == "T9"
    assert out[10]["locked"] is True and out[10]["reason"] == "pay"
    assert "unlock_price_cents" not in out[10] # SELL is bulk-only (no per-row price)
    assert "lock_token" in out[10]


def test_free_sell_rows_all_locked(ent):
    rows = [_sig("AAPL")]; rows[0]["signal"] = "SELL"
    out = ent.gate_signals(rows, kind="SELL", plan="free", unlocks=set())
    assert out[0]["locked"] is True and out[0]["reason"] == "upgrade"
