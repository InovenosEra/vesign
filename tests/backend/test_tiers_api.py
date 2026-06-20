"""Tests for tier-scoped /api/stats (Part B) and tier field in signals/today (Part A)."""
import sqlalchemy as sa
from sqlalchemy import text
import backend.main as bm


def _seed(engine):
    with engine.begin() as c:
        c.execute(text(
            "CREATE TABLE trade_log "
            "(ticker TEXT, buy_date TEXT, buy_price REAL, sell_date TEXT, sell_price REAL, return_pct REAL)"
        ))
        c.execute(text(
            "CREATE TABLE trade_lots "
            "(ticker TEXT, buy_date TEXT, sell_date TEXT, lot_seq INTEGER, lot_date TEXT, lot_price REAL)"
        ))
        c.execute(text(
            "CREATE TABLE signals (date TEXT, ticker TEXT, signal TEXT, tier INTEGER)"
        ))
        c.execute(text(
            "CREATE TABLE companies (ticker TEXT, market TEXT)"
        ))
        # one Prime (tier 1) winner, one Potential (tier 3) loser
        trades = [
            ("AAA", "2024-01-01", 100.0, "2024-03-01", 150.0, 0.50, 1),   # +50% Prime
            ("BBB", "2024-01-01", 100.0, "2024-03-01",  80.0, -0.20, 3),  # -20% Potential
        ]
        for t, bd, bp, sd, sp, rp, tier in trades:
            c.execute(
                text("INSERT INTO trade_log VALUES (:t,:bd,:bp,:sd,:sp,:rp)"),
                dict(t=t, bd=bd, bp=bp, sd=sd, sp=sp, rp=rp),
            )
            c.execute(
                text("INSERT INTO trade_lots VALUES (:t,:bd,:sd,1,:bd,:bp)"),
                dict(t=t, bd=bd, sd=sd, bp=bp),
            )
            c.execute(
                text("INSERT INTO signals VALUES (:bd,:t,'BUY',:tier)"),
                dict(bd=bd, t=t, tier=tier),
            )
            c.execute(
                text("INSERT INTO companies VALUES (:t,'US')"),
                dict(t=t),
            )


def test_public_stats_prime_only(monkeypatch, tmp_path):
    eng = sa.create_engine(f"sqlite:///{tmp_path}/t.db")
    _seed(eng)
    monkeypatch.setattr(bm, "engine", eng)
    out = bm.public_stats()
    assert out["closed_trades"] == 1          # only the Prime trade
    assert out["win_rate"] == 100.0           # the one Prime trade won
    assert round(out["avg_yield"]) == 50      # +50%, not diluted by the -20% Potential
