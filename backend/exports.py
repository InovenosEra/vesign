"""XLSX export helpers — DataFrame → FastAPI StreamingResponse."""
from __future__ import annotations

import io
import re

import pandas as pd
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

XLSX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


def _write_dataframe_to_workbook(df: pd.DataFrame, sheet_name: str) -> Workbook:
    wb = Workbook(write_only=False)
    ws = wb.active
    ws.title = sheet_name[:31]  # Excel sheet-name limit

    columns = list(df.columns)
    ws.append(columns)

    # Pandas timestamps and NaN don't serialize cleanly — coerce here.
    for row in df.itertuples(index=False, name=None):
        ws.append([_cell_value(v) for v in row])

    # Make headers bold and freeze the header row so users can scroll.
    for col_idx, _ in enumerate(columns, start=1):
        ws.cell(row=1, column=col_idx).font = ws.cell(row=1, column=col_idx).font.copy(bold=True)
    ws.freeze_panes = "A2"

    # Auto-size columns based on the header length (cheap heuristic that
    # avoids walking every row for large exports).
    for col_idx, col_name in enumerate(columns, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = max(12, len(str(col_name)) + 2)

    return wb


def _cell_value(v):
    """Coerce pandas-specific sentinels (NaN, NaT, pd.NA) and Timestamps for openpyxl."""
    if v is None:
        return None
    # pd.isna handles NaN, NaT, and pd.NA. Guard against array-like / unhashable inputs.
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
) -> StreamingResponse:
    """Build an XLSX from `df` and return it as a download attachment.

    `filename` should NOT include the .xlsx extension — it's added here.
    Empty DataFrames produce a header-only workbook (caller's intent: "no rows
    matched my filters" should not be a hard error).
    """
    wb = _write_dataframe_to_workbook(df, sheet_name=sheet_name)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    safe_name = re.sub(r'[\r\n"\\]', '', filename)
    headers = {
        "content-disposition": f'attachment; filename="{safe_name}.xlsx"',
    }
    return StreamingResponse(buf, media_type=XLSX_MEDIA_TYPE, headers=headers)
