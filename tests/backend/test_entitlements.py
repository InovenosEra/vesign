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
