from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path("mappings/jobs.sqlite")


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            name TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return c


def save_job(job_type: str, name: str, payload: dict[str, Any]) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO jobs(job_type, name, payload_json) VALUES(?,?,?)",
            (job_type, name, json.dumps(payload)),
        )
        return int(cur.lastrowid)


def list_jobs() -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, job_type, name, payload_json, created_at FROM jobs ORDER BY id DESC"
        ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "job_type": r[1],
                "name": r[2],
                "payload": json.loads(r[3]),
                "created_at": r[4],
            }
        )
    return out


def get_job(job_id: int) -> dict[str, Any] | None:
    with _conn() as c:
        r = c.execute(
            "SELECT id, job_type, name, payload_json, created_at FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
    if not r:
        return None
    return {
        "id": r[0],
        "job_type": r[1],
        "name": r[2],
        "payload": json.loads(r[3]),
        "created_at": r[4],
    }
