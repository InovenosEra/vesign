"""Tests for VQS (Vesign Quality Score) computation — the V2 BUY rule."""
import pytest
from signals.engine import _compute_vqs


def _row(**overrides):
    """Default: every condition False (VQS=0). Override individual fields."""
    base = {
        "vix_close": 15.0,        # < 22 (C1 false)
        "rsi": 50.0,              # not < 35 (C5 false)
        "mom_5d": 0.0,            # not < -0.05 (C4 false)
        "mom_60d": 0.0,           # not < -0.15 (C3 false)
        "log_market_cap": 25.0,   # not < 22 (C7 false)
        "realized_vol_20": 0.20,  # not > 0.5
        "atr_14_pct": 0.02,       # not > 0.04 (C6 false)
        "pred_5d": 0.0,           # not > 0.005 (C8 false)
        "sma_50_dist": 0.0,       # not < -0.07 (C9 false)
    }
    base.update(overrides)
    return base


def test_vqs_zero_when_no_conditions_met():
    assert _compute_vqs(_row()) == 0


def test_vqs_one_per_condition():
    # VIX > 22 only
    assert _compute_vqs(_row(vix_close=23.0)) == 1
    # VIX > 29 also fires — counts as +1 on top of VIX>22 (so VIX=30 → score 2)
    assert _compute_vqs(_row(vix_close=30.0)) == 2
    # mom_60d only
    assert _compute_vqs(_row(mom_60d=-0.20)) == 1
    # RSI only
    assert _compute_vqs(_row(rsi=30.0)) == 1
    # Realized vol high (C6 OR branch)
    assert _compute_vqs(_row(realized_vol_20=0.6)) == 1
    # ATR high (C6 OR branch alt)
    assert _compute_vqs(_row(atr_14_pct=0.05)) == 1
    # Both vol high — still +1 (it's an OR)
    assert _compute_vqs(_row(realized_vol_20=0.6, atr_14_pct=0.05)) == 1


def test_vqs_max_score_9():
    """All 9 conditions met → VQS = 9 (the 'Strong BUY' tier)."""
    assert _compute_vqs(_row(
        vix_close=30.0,        # +1 for >22, +1 for >29  → C1+C2
        mom_60d=-0.20,         # C3
        mom_5d=-0.10,          # C4
        rsi=30.0,              # C5
        realized_vol_20=0.6,   # C6
        log_market_cap=20.0,   # C7
        pred_5d=0.01,          # C8
        sma_50_dist=-0.10,     # C9
    )) == 9


def test_vqs_handles_none_safely():
    """Missing data should treat the condition as false, not raise."""
    row = _row()
    row["pred_5d"] = None
    row["mom_60d"] = None
    assert _compute_vqs(row) == 0


def test_vqs_at_buy_threshold():
    """VQS=8 — one condition shy of 9 — still fires BUY."""
    score = _compute_vqs(_row(
        vix_close=23.0,        # +1 (only >22, not >29) → C1
        mom_60d=-0.20,         # C3
        mom_5d=-0.10,          # C4
        rsi=30.0,              # C5
        realized_vol_20=0.6,   # C6
        log_market_cap=20.0,   # C7
        pred_5d=0.01,          # C8
        sma_50_dist=-0.10,     # C9
    ))
    assert score == 8
