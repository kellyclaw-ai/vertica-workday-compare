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
