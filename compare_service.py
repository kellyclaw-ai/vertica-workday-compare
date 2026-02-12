from __future__ import annotations

from typing import Any
from app.db import run_query
from app.mapping_store import field_map_for_table, key_map_for_table
from app.models import FieldMap


def _rows_to_dicts(cols: list[str], rows: list[tuple]) -> list[dict[str, Any]]:
    return [dict(zip(cols, r)) for r in rows]


def compare_tables(
    left_conn: dict,
    right_conn: dict,
    left_table: str,
    right_table: str,
    all_field_maps: list[FieldMap],
    limit: int = 500,
    god_mode: bool = False,
) -> dict[str, Any]:
    fmaps = field_map_for_table(all_field_maps, left_table, right_table)
    kmaps = key_map_for_table(all_field_maps, left_table, right_table)
    if not fmaps:
        raise ValueError("No field mappings for selected table pair")
    if not kmaps:
        raise ValueError("No key mappings for selected table pair")

    left_cols = ", ".join([f"{f.left_field}" for f in fmaps])
    right_cols = ", ".join([f"{f.right_field}" for f in fmaps])

    left_sql = f"SELECT {left_cols} FROM {left_table} LIMIT {int(limit)}"
    right_sql = f"SELECT {right_cols} FROM {right_table} LIMIT {int(limit)}"

    l_cols, l_rows, l_sec = run_query(left_conn, left_sql)
    r_cols, r_rows, r_sec = run_query(right_conn, right_sql)

    left_data = _rows_to_dicts(l_cols, l_rows)
    right_data = _rows_to_dicts(r_cols, r_rows)

    # Build key tuples using mapped key fields.
    lk = [k.left_field for k in kmaps]
    rk = [k.right_field for k in kmaps]

    left_ix = {tuple(row.get(k) for k in lk): row for row in left_data}
    right_ix = {tuple(row.get(k) for k in rk): row for row in right_data}

    left_keys = set(left_ix)
    right_keys = set(right_ix)

    left_only = [left_ix[k] for k in sorted(left_keys - right_keys)]
    right_only = [right_ix[k] for k in sorted(right_keys - left_keys)]

    mismatched = []
    for k in sorted(left_keys & right_keys):
        lrow = left_ix[k]
        rrow = right_ix[k]
        diffs = {}
        for f in fmaps:
            lv = lrow.get(f.left_field)
            rv = rrow.get(f.right_field)
            if lv != rv:
                diffs[f"{f.left_field} != {f.right_field}"] = {"left": lv, "right": rv}
        if diffs:
            mismatched.append({"key": k, "diffs": diffs, "left": lrow, "right": rrow})

    trace = {
        "left_sql": left_sql if god_mode else "hidden",
        "right_sql": right_sql if god_mode else "hidden",
        "left_seconds": l_sec,
        "right_seconds": r_sec,
        "left_rows": len(left_data),
        "right_rows": len(right_data),
        "key_fields": {"left": lk, "right": rk},
    }

    return {
        "left_only": left_only,
        "right_only": right_only,
        "mismatched": mismatched,
        "trace": trace,
    }


def employee_trace(
    conn: dict,
    table: str,
    employee_id: str,
    fields: list[str],
    employee_field_name: str,
) -> dict[str, Any]:
    cols = ", ".join(fields) if fields else "*"
    sql = f"SELECT {cols} FROM {table} WHERE {employee_field_name} = %s LIMIT 200"
    out_cols, rows, sec = run_query(conn, sql, (employee_id,))
    return {
        "table": table,
        "sql": sql,
        "seconds": sec,
        "rows": _rows_to_dicts(out_cols, rows),
    }
