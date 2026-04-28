"""Smoke tests for backend.exports. Run with: venv/bin/python scripts/smoke_exports.py"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import inspect
import io
import pandas as pd
from openpyxl import load_workbook

from backend.exports import dataframe_to_xlsx_response


def _read_response_body(resp):
    """Drain a FastAPI response into bytes, regardless of sync/async iterator."""
    if hasattr(resp, "body_iterator"):
        it = resp.body_iterator
        if inspect.isasyncgen(it):
            async def _drain():
                return b"".join([chunk async for chunk in it])
            return asyncio.run(_drain())
        return b"".join(it)
    return resp.body


def test_basic_workbook():
    df = pd.DataFrame([
        {"ticker": "AAPL", "close": 220.0, "rsi": 55.1},
        {"ticker": "MSFT", "close": 410.0, "rsi": 48.0},
    ])
    resp = dataframe_to_xlsx_response(df, filename="signals_2026-04-27", sheet_name="signals")

    assert resp.media_type == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert 'attachment; filename="signals_2026-04-27.xlsx"' in resp.headers["content-disposition"]

    body = _read_response_body(resp)
    wb = load_workbook(io.BytesIO(body))
    assert wb.sheetnames == ["signals"]
    ws = wb["signals"]
    assert [c.value for c in ws[1]] == ["ticker", "close", "rsi"]
    assert ws.cell(row=2, column=1).value == "AAPL"
    assert ws.cell(row=3, column=2).value == 410.0
    print("test_basic_workbook PASS")


def test_empty_dataframe_still_emits_header():
    df = pd.DataFrame(columns=["ticker", "close"])
    resp = dataframe_to_xlsx_response(df, filename="empty", sheet_name="signals")
    body = _read_response_body(resp)
    wb = load_workbook(io.BytesIO(body))
    ws = wb["signals"]
    assert [c.value for c in ws[1]] == ["ticker", "close"]
    assert ws.max_row == 1   # header only
    print("test_empty_dataframe_still_emits_header PASS")


def test_pandas_sentinels_serialize_cleanly():
    """NaN, NaT, pd.NA and Timestamp must coerce — they appear in every SQL-backed export."""
    df = pd.DataFrame([
        {"ticker": "AAPL", "close": 220.0, "buy_date": pd.Timestamp("2026-04-15")},
        {"ticker": "MSFT", "close": float("nan"), "buy_date": pd.NaT},
    ])
    resp = dataframe_to_xlsx_response(df, filename="sentinels", sheet_name="s")
    body = _read_response_body(resp)
    wb = load_workbook(io.BytesIO(body))
    ws = wb["s"]
    assert ws.cell(row=2, column=2).value == 220.0
    assert ws.cell(row=3, column=2).value is None   # NaN → None
    assert ws.cell(row=3, column=3).value is None   # NaT → None
    print("test_pandas_sentinels_serialize_cleanly PASS")


if __name__ == "__main__":
    test_basic_workbook()
    test_pandas_sentinels_serialize_cleanly()
    test_empty_dataframe_still_emits_header()
    print("\nAll exports.py smoke tests passed.")
