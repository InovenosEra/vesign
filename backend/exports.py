"""XLSX export helpers — DataFrame → FastAPI StreamingResponse.

Uses openpyxl's write_only mode so large exports (e.g. 12-month signals
~375K rows) don't materialize the whole workbook in memory and OOM-kill
the 2GB production server.
"""
from __future__ import annotations

import io
import re

import pandas as pd
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

XLSX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


def _write_dataframe_to_workbook(
    df: pd.DataFrame,
    sheet_name: str,
    column_formats: dict | None = None,
) -> Workbook:
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title=sheet_name[:31])  # Excel sheet-name limit

    columns = list(df.columns)
    bold = Font(bold=True)

    header_cells = []
    for col_name in columns:
        c = WriteOnlyCell(ws, value=col_name)
        c.font = bold
        header_cells.append(c)
    ws.append(header_cells)

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

    ws.freeze_panes = "A2"

    # Column widths sized off header text only — cheap heuristic that avoids
    # walking every row.
    for col_idx, col_name in enumerate(columns, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = max(12, len(str(col_name)) + 2)

    return wb


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
    wb = _write_dataframe_to_workbook(df, sheet_name=sheet_name, column_formats=column_formats)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    safe_name = re.sub(r'[\r\n"\\]', '', filename)
    headers = {
        "content-disposition": f'attachment; filename="{safe_name}.xlsx"',
    }
    return StreamingResponse(buf, media_type=XLSX_MEDIA_TYPE, headers=headers)
