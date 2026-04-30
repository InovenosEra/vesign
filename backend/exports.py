"""XLSX export helpers — DataFrame or SQL cursor → FastAPI StreamingResponse.

Two entry points:

* `dataframe_to_xlsx_response` — for small/medium exports where loading the
  whole result into a DataFrame is fine.
* `cursor_to_xlsx_response` — for large exports (e.g. signals at 12 months
  ~ 379K rows). Streams rows straight from a SQLAlchemy result into an
  openpyxl write_only workbook, saves to a temp file, then streams the
  file back. Never materializes the whole DataFrame in RAM, which OOM-killed
  uvicorn on the 4GB server.
"""
from __future__ import annotations

import io
import os
import re
import tempfile

import pandas as pd
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

XLSX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


def _cell_value(v):
    """Coerce pandas-specific sentinels (NaN, NaT, pd.NA) and Timestamps for openpyxl."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def _new_workbook(columns, sheet_name):
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title=sheet_name[:31])
    bold = Font(bold=True)
    header = []
    for col_name in columns:
        c = WriteOnlyCell(ws, value=col_name)
        c.font = bold
        header.append(c)
    ws.append(header)
    ws.freeze_panes = "A2"
    for col_idx, col_name in enumerate(columns, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = max(12, len(str(col_name)) + 2)
    return wb, ws


def _save_and_stream(wb, filename: str) -> StreamingResponse:
    """Save the workbook to a temp file and stream it back; delete after read.

    Saving to a file (not BytesIO) keeps peak RAM low for large workbooks.
    """
    tmp = tempfile.NamedTemporaryFile(prefix="vesign_export_", suffix=".xlsx", delete=False)
    tmp.close()
    try:
        wb.save(tmp.name)
    except Exception:
        os.unlink(tmp.name)
        raise

    def _file_iter():
        try:
            with open(tmp.name, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    yield chunk
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    safe_name = re.sub(r'[\r\n"\\]', '', filename)
    headers = {
        "content-disposition": f'attachment; filename="{safe_name}.xlsx"',
    }
    return StreamingResponse(_file_iter(), media_type=XLSX_MEDIA_TYPE, headers=headers)


def dataframe_to_xlsx_response(
    df: pd.DataFrame,
    filename: str,
    sheet_name: str = "Sheet1",
    column_formats: dict | None = None,
) -> StreamingResponse:
    """Build an XLSX from `df` and return it as a download attachment.

    `filename` should NOT include the .xlsx extension — it's added here.
    Empty DataFrames produce a header-only workbook (caller's intent: "no rows
    matched my filters" should not be a hard error).

    `column_formats` optionally maps a column name to an Excel number-format
    string applied to every data cell in that column (e.g. {"sell_date":
    "dd/mm/yy", "return_pct": "0.00%"}).
    """
    columns = list(df.columns)
    wb, ws = _new_workbook(columns, sheet_name)

    formats = column_formats or {}
    col_fmts = [formats.get(name) for name in columns]

    for row in df.itertuples(index=False, name=None):
        cells = []
        for fmt, raw in zip(col_fmts, row):
            cell = WriteOnlyCell(ws, value=_cell_value(raw))
            if fmt:
                cell.number_format = fmt
            cells.append(cell)
        ws.append(cells)

    return _save_and_stream(wb, filename)


def cursor_to_xlsx_response(
    conn,
    sql,
    params: dict,
    filename: str,
    sheet_name: str = "Sheet1",
) -> StreamingResponse:
    """Stream SQL rows directly into an XLSX, bypassing pandas.

    Used by large exports where loading every row into a DataFrame would OOM.
    `conn` is an active SQLAlchemy connection. `sql` is a `text()` clause.
    No per-column formatting (callers needing it should pre-format their
    SELECT or use `dataframe_to_xlsx_response` instead).
    """
    result = conn.execute(sql, params)
    columns = list(result.keys())
    wb, ws = _new_workbook(columns, sheet_name)

    for row in result:
        ws.append([_cell_value(v) for v in row])

    return _save_and_stream(wb, filename)
