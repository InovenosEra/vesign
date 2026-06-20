from signals.engine import _vqs_to_tier

def test_vqs_to_tier_bands():
    # 2-tier model: Prime = vqs 8-9, Potential = vqs 6-7
    assert _vqs_to_tier(9) == 1
    assert _vqs_to_tier(8) == 1
    assert _vqs_to_tier(7) == 2
    assert _vqs_to_tier(6) == 2

def test_vqs_to_tier_v1_floor():
    # A V1-gate BUY can fire with a low vqs; it floors to Potential (tier 2).
    assert _vqs_to_tier(5) == 2
    assert _vqs_to_tier(0) == 2

def test_vqs_to_tier_handles_none_and_nan():
    import math
    assert _vqs_to_tier(None) == 2
    assert _vqs_to_tier(float('nan')) == 2

def test_buy_gate_widened_to_vqs_6():
    """Guard: the V2 BUY threshold must be vqs>=6, not ==9."""
    import inspect, signals.engine as eng
    src = inspect.getsource(eng.run_scoring)
    assert 'today_df["vqs"] >= 6' in src
    assert 'today_df["vqs"] == 9' not in src
