import sqlalchemy as sa
from sqlalchemy import text
import signals.news_gate as ng


def _seed(engine):
    with engine.begin() as c:
        c.execute(text(
            "CREATE TABLE signals (ticker TEXT, date TEXT, signal TEXT, "
            "tier INTEGER, news_block_reason TEXT)"))
        rows = [
            ("AAA", "2026-06-19", "BUY", 1, None),   # Prime     -> checked
            ("BBB", "2026-06-19", "BUY", 2, None),   # Potential -> skipped
            ("CCC", "2026-06-19", "BUY", 2, None),   # Potential -> skipped
            ("DDD", "2026-06-19", "HOLD", None, None),  # not a BUY -> skipped
        ]
        for r in rows:
            c.execute(text(
                "INSERT INTO signals VALUES (:t,:d,:s,:tier,:n)"),
                {"t": r[0], "d": r[1], "s": r[2], "tier": r[3], "n": r[4]})


def test_news_gate_only_checks_prime(monkeypatch, tmp_path):
    eng = sa.create_engine(f"sqlite:///{tmp_path}/t.db")
    _seed(eng)
    monkeypatch.setattr(ng, "engine", eng)
    checked = []
    def fake_check(ticker, lookback_days=ng.DEFAULT_LOOKBACK_DAYS):
        checked.append(ticker)
        return (False, None)   # never blocks
    monkeypatch.setattr(ng, "check_news_gate", fake_check)

    result = ng.apply_news_gate(target_date="2026-06-19", verbose=False)

    assert sorted(checked) == ["AAA"]   # only Prime (tier 1); Potential + HOLD skipped
    assert result["checked"] == 1
