from __future__ import annotations

import time
from typing import Any
import vertica_python


def run_query(conn_info: dict[str, Any], sql: str, params: tuple | None = None) -> tuple[list[str], list[tuple], float]:
    t0 = time.time()
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    elapsed = time.time() - t0
    return cols, rows, elapsed


def get_table_columns(conn_info: dict[str, Any], table_name: str) -> list[str]:
    # table_name expected as schema.table
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = "public", table_name

    sql = """
        SELECT column_name
        FROM v_catalog.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    cols, rows, _ = run_query(conn_info, sql, (schema, table))
    if not rows:
        return []
    i = cols.index("column_name")
    return [r[i] for r in rows]
