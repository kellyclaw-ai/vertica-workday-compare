"""Export raw per-table employee_id extracts to an Excel workbook.

- Interactive CLI table selection.
- Runs SELECT * for each selected table.
- Interactive data scope options:
  - single employee_id
  - random/semi-random 100-row sample
  - full table
- Sorts by effective date when present (effective_dt or effective_date).
- Highlights cells whose values changed vs the previous record (by effective date).

Usage:
  python3 scripts/export_employee_tables.py
  python3 scripts/export_employee_tables.py --out out.xlsx

Notes:
- This script uses the same Vertica connection settings as the FastAPI app (app/settings.py).
- For table listing it prefers schema enumeration; if schema isn't provided it tries to infer it
  from the mapping workbook.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import PatternFill

from app.db import get_schema_tables, get_table_columns, run_query
from app.mapping_store import load_mapping
from app.settings import settings


MISMATCH_FILL = PatternFill(start_color="FFF3B0", end_color="FFF3B0", fill_type="solid")


def _sheet_name(name: str) -> str:
    safe = name.replace("/", "_").replace("\\", "_").replace(".", "_")
    return safe[:31] if len(safe) > 31 else safe


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_table(table_name: str) -> str:
    return ".".join(_quote_ident(part) for part in table_name.split("."))


def _norm(v: Any):
    # Treat blank/whitespace strings as NULL
    if v is None:
        return None
    if isinstance(v, str):
        sv = v.strip()
        return None if sv == "" else sv
    return v


def _distinct(a: Any, b: Any) -> bool:
    na = _norm(a)
    nb = _norm(b)
    if na is None and nb is None:
        return False
    return na != nb


def _find_effective_col(columns: list[str]) -> str | None:
    for c in columns:
        cl = c.lower()
        if cl in {"effective_dt", "effective_date"}:
            return c
    return None


def _sortable_dt(v: Any):
    if v is None:
        return datetime.min
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return datetime.min
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return datetime.min
    return datetime.min


def _write_table(ws, columns: list[str], rows: list[dict[str, Any]], *, changed_cols_by_row: list[set[str]] | None = None):
    ws.append(columns)
    for i, r in enumerate(rows):
        ws.append([r.get(c) for c in columns])
        if changed_cols_by_row is None:
            continue
        changed = changed_cols_by_row[i] if i < len(changed_cols_by_row) else set()
        if not changed:
            continue
        # Row in worksheet is offset by 1 for header
        row_idx = i + 2
        for j, c in enumerate(columns):
            if c in changed:
                ws.cell(row=row_idx, column=j + 1).fill = MISMATCH_FILL


def _parse_selection(selection: str, n: int) -> list[int]:
    """Parse '1,2,5-8,all' into 0-based indices."""
    s = selection.strip().lower()
    if not s:
        return []
    if s in {"a", "all", "*"}:
        return list(range(n))

    out: set[int] = set()
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                start = int(a)
                end = int(b)
            except ValueError:
                continue
            for k in range(min(start, end), max(start, end) + 1):
                if 1 <= k <= n:
                    out.add(k - 1)
        else:
            try:
                k = int(part)
            except ValueError:
                continue
            if 1 <= k <= n:
                out.add(k - 1)
    return sorted(out)


@dataclass
class TableRef:
    side: str  # left|right
    table: str  # fully-qualified schema.table


def _infer_schema_from_mapping(which: str) -> str | None:
    # Infer schema from the mapping file (first mapped table that contains a schema prefix)
    table_maps, _field_maps, _value_maps = load_mapping(settings.mapping_file)
    cand = None
    if which == "left":
        cand = next((tm.left_table for tm in table_maps if tm.left_table and "." in tm.left_table), None)
    else:
        cand = next((tm.right_table for tm in table_maps if tm.right_table and "." in tm.right_table), None)
    if cand and "." in cand:
        return cand.split(".", 1)[0]
    return None


def _list_tables(conn: dict, schema: str) -> list[str]:
    tables = get_schema_tables(conn, schema)
    # get_schema_tables returns unqualified names; qualify for querying.
    return [f"{schema}.{t}" for t in tables]


def _trace_explorer_table_refs() -> list[TableRef]:
    table_maps, _field_maps, _value_maps = load_mapping(settings.mapping_file)

    left_tables = sorted({tm.left_table for tm in table_maps if tm.left_table})

    right_schema = _infer_schema_from_mapping("right")
    right_tables: list[str] = []
    if right_schema:
        try:
            right_tables = _list_tables(settings.right.model_dump(), right_schema)
        except Exception:
            right_tables = []
    if not right_tables:
        right_tables = sorted({tm.right_table for tm in table_maps if tm.right_table})

    return ([TableRef("left", t) for t in left_tables] + [TableRef("right", t) for t in right_tables])


def export_table_ref(
    wb: Workbook,
    ref: TableRef,
    *,
    scope: str,
    employee_id: str | None = None,
    sample_size: int = 100,
):
    conn = settings.left.model_dump() if ref.side == "left" else settings.right.model_dump()
    table = ref.table

    cols = get_table_columns(conn, table)
    eff_col = _find_effective_col(cols)
    eff_order_sql = f" ORDER BY {_quote_ident(eff_col)} ASC NULLS LAST" if eff_col else ""

    if scope == "employee":
        if not employee_id:
            raise ValueError("employee_id is required for employee scope")
        sql = f"SELECT * FROM {_quote_table(table)} WHERE employee_id = %s{eff_order_sql}"
        params: tuple[Any, ...] | None = (employee_id,)
    elif scope == "sample100":
        # Semi-random sample using ORDER BY RANDOM(). If effective date exists, re-sort sampled set by effective date.
        if eff_col:
            sql = (
                f"SELECT * FROM ("
                f"SELECT * FROM {_quote_table(table)} ORDER BY RANDOM() LIMIT {int(sample_size)}"
                f") s ORDER BY {_quote_ident(eff_col)} ASC NULLS LAST"
            )
        else:
            sql = f"SELECT * FROM {_quote_table(table)} ORDER BY RANDOM() LIMIT {int(sample_size)}"
        params = None
    elif scope == "full":
        sql = f"SELECT * FROM {_quote_table(table)}{eff_order_sql}"
        params = None
    else:
        raise ValueError(f"Unsupported scope: {scope}")

    out_cols, out_rows, _sec = run_query(conn, sql, params)
    rows = [dict(zip(out_cols, r)) for r in out_rows]

    # Defensive sort in Python too
    if eff_col and eff_col in out_cols:
        rows = sorted(rows, key=lambda r: _sortable_dt(r.get(eff_col)))

    # Build per-row changed columns (relative to previous row)
    changed_cols_by_row: list[set[str]] = []
    ignore = {"employee_id", "effective_dt", "effective_date"}
    prev: dict[str, Any] | None = None
    for r in rows:
        changed: set[str] = set()
        if prev is not None:
            for c in out_cols:
                if c.lower() in ignore:
                    continue
                if _distinct(r.get(c), prev.get(c)):
                    changed.add(c)
        changed_cols_by_row.append(changed)
        prev = r

    # Sheet name includes side to avoid collisions when left/right share table base names
    ws = wb.create_sheet(_sheet_name(f"{ref.side}_{table}"))
    _write_table(ws, out_cols, rows, changed_cols_by_row=changed_cols_by_row)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="Output xlsx path")
    args = ap.parse_args()

    print("\nData scope:")
    print("  1) Single employee_id")
    print("  2) Random/semi-random 100 rows")
    print("  3) Full table")

    scope_choice = ""
    while scope_choice not in {"1", "2", "3"}:
        scope_choice = input("Choose scope [1/2/3]: ").strip()

    scope = {"1": "employee", "2": "sample100", "3": "full"}[scope_choice]

    employee_id: str | None = None
    if scope == "employee":
        eid = ""
        while not eid:
            eid = input("Enter employee_id: ").strip()
            if not eid:
                print("employee_id is required.")
        employee_id = eid

    refs = _trace_explorer_table_refs()
    if not refs:
        raise SystemExit("No tables found (mapping file empty?)")

    print(f"Found {len(refs)} tables (same list as trace explorer).")
    for i, r in enumerate(refs, start=1):
        side_tag = "L" if r.side == "left" else "R"
        print(f"{i:>4}: [{side_tag}] {r.table}")

    selected: list[TableRef] = []
    while True:
        raw = input("\nSelect tables (e.g. 1,2,5-7 or 'all'). Press Enter when done: ").strip()
        if raw == "":
            break
        idxs = _parse_selection(raw, len(refs))
        if not idxs:
            print("No valid selections parsed; try again.")
            continue
        for ix in idxs:
            r = refs[ix]
            if all((r.side != s.side or r.table != s.table) for s in selected):
                selected.append(r)
        print(f"Selected so far ({len(selected)}): {', '.join([f'{s.side}:{s.table}' for s in selected[:6]])}{' ...' if len(selected) > 6 else ''}")

    if not selected:
        raise SystemExit("No tables selected.")

    wb = Workbook()
    wb.remove(wb.active)  # remove default sheet

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_label = "employee" if scope == "employee" else ("sample100" if scope == "sample100" else "full")
    suffix = f"_{employee_id}" if employee_id else ""
    out_path = Path(args.out) if args.out else Path(f"output/table_extract_{scope_label}{suffix}_{ts}.xlsx")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    for ref in selected:
        export_table_ref(wb, ref, scope=scope, employee_id=employee_id, sample_size=100)

    wb.save(out_path)
    print(f"\nWrote workbook: {out_path}")


if __name__ == "__main__":
    main()
