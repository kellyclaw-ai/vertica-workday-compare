"""Minimal SQL runner for Vertica.

Edit SQL below, run script, get XLSX in /output.

Usage:
  python3 scripts/run_vertica_sql.py
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from openpyxl import Workbook

from app.db import run_query
from app.settings import settings

# --------------------
# EDIT THESE
# --------------------
TARGET = "left"  # "left" or "right"

SQL = """
SELECT 1 AS example_col
""".strip()

# Optional query params for %s placeholders in SQL.
# Example: PARAMS = ("12345",)
PARAMS = None
# --------------------


def main() -> None:
    conn = settings.left.model_dump() if TARGET == "left" else settings.right.model_dump()

    cols, rows, sec = run_query(conn, SQL, PARAMS)

    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"vertica_query_{TARGET}_{ts}.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "query_results"
    ws.append(cols)
    for row in rows:
        ws.append(list(row))
    wb.save(out_path)

    print(f"Rows: {len(rows)}")
    print(f"Seconds: {sec:.3f}")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
