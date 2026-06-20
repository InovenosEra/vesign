import importlib, os
import sqlalchemy


def test_vesign_db_env_overrides_config(monkeypatch):
    monkeypatch.setenv("VESIGN_DB", "vesign_tier_test.db")
    import data.loaders as loaders
    importlib.reload(loaders)
    assert loaders.engine.url.database.endswith("vesign_tier_test.db")
    # cleanup: reload without the override so other tests use the real DB
    monkeypatch.delenv("VESIGN_DB", raising=False)
    importlib.reload(loaders)


def test_no_env_uses_config_default(monkeypatch):
    monkeypatch.delenv("VESIGN_DB", raising=False)
    import data.loaders as loaders
    importlib.reload(loaders)
    assert loaders.engine.url.database.endswith("vesign.db")
