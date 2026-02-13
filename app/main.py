from __future__ import annotations

import re
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.compare_service import compare_tables, employee_trace
from app.db import get_table_columns, get_schema_tables
from app.mapping_store import load_mapping, field_map_for_table, key_map_for_table
from app.settings import settings

app = FastAPI(title="Vertica Workday Compare")
templates = Jinja2Templates(directory="templates")


def _conn_left() -> dict:
    return settings.left.model_dump()


def _conn_right() -> dict:
    return settings.right.model_dump()


def _unmapped_fields_report(left_table: str, right_table: str) -> dict:
    left_cols = get_table_columns(_conn_left(), left_table)
    right_cols = get_table_columns(_conn_right(), right_table)

    _, all_field_maps, _value_maps = load_mapping(settings.mapping_file)
    pair_field_maps = [
        f for f in all_field_maps
        if f.left_table == left_table and f.right_table == right_table
    ]

    mapped_left_fields = {f.left_field for f in pair_field_maps}
    mapped_right_fields = {f.right_field for f in pair_field_maps}

    left_unmapped = [c for c in left_cols if c not in mapped_left_fields]
    right_unmapped = [c for c in right_cols if c not in mapped_right_fields]

    return {
        "left_only_not_in_field_map": left_unmapped,
        "right_only_not_in_field_map": right_unmapped,
        "either_side_not_in_field_map": {
            "left": left_unmapped,
            "right": right_unmapped,
        },
    }


def _render_home(request: Request, compare_result=None, trace_frames=None, employee_id=""):
    table_maps, field_maps, value_maps = load_mapping(settings.mapping_file)

    # Schema table lists (used by trace explorer)
    left_schema = None
    right_schema = None
    if table_maps:
        if table_maps[0].left_table and "." in table_maps[0].left_table:
            left_schema = table_maps[0].left_table.split(".", 1)[0]
        if table_maps[0].right_table and "." in table_maps[0].right_table:
            right_schema = table_maps[0].right_table.split(".", 1)[0]

    # LEFT tables: always use table_map (mapping file) for the checklist
    left_schema_tables = sorted({tm.left_table for tm in table_maps if tm.left_table})

    # RIGHT tables: prefer live schema enumeration; fallback to mapped right tables
    right_schema_tables: list[str] = []
    if right_schema:
        try:
            right_schema_tables = get_schema_tables(_conn_right(), right_schema)
        except Exception:
            right_schema_tables = []
    if not right_schema_tables:
        right_schema_tables = sorted({tm.right_table for tm in table_maps if tm.right_table})

    trace_table_refs = ([{"side": "left", "table": t} for t in left_schema_tables]
                        + [{"side": "right", "table": t} for t in right_schema_tables])

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "table_maps": table_maps,
            "field_maps": field_maps,
            "value_maps": value_maps,
            "trace_table_refs": trace_table_refs,
            "god_mode_default": settings.god_mode_default,
            "compare_result": compare_result,
            "trace_frames": trace_frames or [],
            "employee_id": employee_id,
        },
    )


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return _render_home(request)


@app.get("/introspect")
def introspect(left_table: str, right_table: str):
    left_cols = get_table_columns(_conn_left(), left_table)
    right_cols = get_table_columns(_conn_right(), right_table)

    _, all_field_maps, _value_maps = load_mapping(settings.mapping_file)
    pair_field_maps = [
        f for f in all_field_maps
        if f.left_table == left_table and f.right_table == right_table
    ]

    unmapped_fields = _unmapped_fields_report(left_table, right_table)

    def norm(n: str) -> str:
        return re.sub(r"[^a-z0-9]", "", n.lower())

    right_by_norm = {norm(c): c for c in right_cols}
    suggestions = []
    for lc in left_cols:
        rc = right_by_norm.get(norm(lc))
        if rc:
            suggestions.append({"left_field": lc, "right_field": rc, "confidence": "high"})

    return JSONResponse(
        {
            "left_table": left_table,
            "right_table": right_table,
            "left_columns": left_cols,
            "right_columns": right_cols,
            "field_map_pairs": [
                {
                    "left_field": f.left_field,
                    "right_field": f.right_field,
                    "is_key": f.is_key,
                    "compare": f.compare,
                }
                for f in pair_field_maps
            ],
            "unmapped_fields": unmapped_fields,
            "suggested_field_mappings": suggestions,
        }
    )


@app.post("/compare", response_class=HTMLResponse)
def compare_ui(
    request: Request,
    left_table: str = Form(...),
    right_table: str = Form(...),
    employee_id: str = Form(""),
    sample_employee_ids: bool = Form(False),
    god_mode: bool = Form(False),
    trim_strings: bool = Form(True),
    nullish_equal: bool = Form(True),
    number_precision: int = Form(6),
):
    _, field_maps, value_maps = load_mapping(settings.mapping_file)
    result = compare_tables(
        _conn_left(),
        _conn_right(),
        left_table,
        right_table,
        field_maps,
        value_maps,
        employee_id.strip() or None,
        sample_employee_ids,
        100,
        god_mode,
        trim_strings,
        nullish_equal,
        number_precision,
        output_dir="output",
    )
    return _render_home(
        request,
        compare_result={
            "left_table": left_table,
            "right_table": right_table,
            "employee_id": employee_id,
            "sample_employee_ids": sample_employee_ids,
            "result": result,
            "unmapped_fields": result["unmapped_fields"],
        },
    )


@app.post("/trace", response_class=HTMLResponse)
async def trace_ui(request: Request):
    form = await request.form()
    employee_id = str(form.get("employee_id", "")).strip()
    god_mode = str(form.get("god_mode", "")).lower() in {"on", "true", "1", "yes"}

    _table_maps, _field_maps, _value_maps = load_mapping(settings.mapping_file)
    selected_refs = form.getlist("table_ref")

    frames = []
    for ref in selected_refs:
        # ref format: "left|schema.table" or "right|schema.table"
        if "|" not in ref:
            continue
        side, table = ref.split("|", 1)
        side = side.strip().lower()
        table = table.strip()

        conn = _conn_left() if side == "left" else _conn_right()

        # Prefer source order (schema order). Ensure employee_id first when present.
        cols = get_table_columns(conn, table)
        if cols and any(c.lower() == "employee_id" for c in cols):
            cols = [c for c in cols if c.lower() == "employee_id"] + [c for c in cols if c.lower() != "employee_id"]

        # Always sort results by effective date when present.
        eff_col = None
        for c in cols or []:
            cl = c.lower()
            if cl == "effective_dt" or cl == "effective_date":
                eff_col = c
                break

        frame = employee_trace(conn, table, employee_id, cols, "employee_id", order_by=eff_col)

        # Highlight cells whose values differ from the previous record (as ordered by effective date).
        # We store a per-row list of changed columns for the template to style.
        if frame.get("rows") and frame.get("columns"):
            ignore = {"employee_id", "effective_dt", "effective_date"}

            def _norm(v):
                # Treat blank/whitespace strings as NULL for change detection
                if v is None:
                    return None
                if isinstance(v, str):
                    sv = v.strip()
                    return None if sv == "" else sv
                return v

            def _distinct(a, b) -> bool:
                # Null-safe "distinct" check (with blank-as-null normalization)
                na = _norm(a)
                nb = _norm(b)
                if na is None and nb is None:
                    return False
                return na != nb

            prev = None
            for r in frame["rows"]:
                r["__changed_cols"] = []
                if prev is not None:
                    for c in frame["columns"]:
                        if c.lower() in ignore:
                            continue
                        if _distinct(r.get(c), prev.get(c)):
                            r["__changed_cols"].append(c)
                prev = r

        if not god_mode:
            frame["sql"] = "hidden"

        frames.append({"side": side, "table": table, "frame": frame})

    return _render_home(request, trace_frames=frames, employee_id=employee_id)
