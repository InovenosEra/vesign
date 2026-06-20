from signals.engine import _vqs_to_tier

def test_vqs_to_tier_bands():
    assert _vqs_to_tier(9) == 1
    assert _vqs_to_tier(8) == 2
    assert _vqs_to_tier(7) == 3
    assert _vqs_to_tier(6) == 3

def test_vqs_to_tier_v1_floor():
    # A V1-gate BUY can fire with a low vqs; it floors to tier 3.
    assert _vqs_to_tier(5) == 3
    assert _vqs_to_tier(0) == 3

def test_vqs_to_tier_handles_none_and_nan():
    import math
    assert _vqs_to_tier(None) == 3
    assert _vqs_to_tier(float('nan')) == 3

def test_buy_gate_widened_to_vqs_6():
    """Guard: the V2 BUY threshold must be vqs>=6, not ==9."""
    import inspect, signals.engine as eng
    src = inspect.getsource(eng.run_scoring)
    assert 'today_df["vqs"] >= 6' in src
    assert 'today_df["vqs"] == 9' not in src
