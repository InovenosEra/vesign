"""Tests for backend.main._fetch_yf_quotes — single retry on partial yfinance batch."""
from unittest.mock import patch
import backend.main as bm

PAIRS = [("GC=F", "Gold"), ("SI=F", "Silver"), ("PL=F", "Platinum")]


def test_full_success_no_retry():
    full = {t: {"price": 1.0, "prev_close": 0.9} for t, _ in PAIRS}
    with patch.object(bm, "_fetch_yf_quotes_once", side_effect=[full]) as m:
        out = bm._fetch_yf_quotes(PAIRS)
    assert out == full
    assert m.call_count == 1  # no retry needed


def test_partial_batch_retries_only_missing_tickers():
    first = {"GC=F": {"price": 4111.7, "prev_close": 4100.1}}          # SI=F, PL=F missing
    retry = {"SI=F": {"price": 58.15, "prev_close": 58.81}}            # PL=F still missing after retry
    with patch.object(bm, "_fetch_yf_quotes_once", side_effect=[first, retry]) as m:
        out = bm._fetch_yf_quotes(PAIRS)
    assert m.call_count == 2
    assert m.call_args_list[1].args[0] == [("SI=F", "Silver"), ("PL=F", "Platinum")]
    assert out == {"GC=F": first["GC=F"], "SI=F": retry["SI=F"]}       # PL=F correctly absent


def test_retry_also_empty_returns_first_partial_result():
    first = {"GC=F": {"price": 4111.7, "prev_close": 4100.1}}
    with patch.object(bm, "_fetch_yf_quotes_once", side_effect=[first, {}]) as m:
        out = bm._fetch_yf_quotes(PAIRS)
    assert m.call_count == 2
    assert out == first


def test_total_failure_returns_none():
    with patch.object(bm, "_fetch_yf_quotes_once", side_effect=[{}, {}]) as m:
        out = bm._fetch_yf_quotes(PAIRS)
    assert m.call_count == 2
    assert out is None
