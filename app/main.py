from __future__ import annotations

import re
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.compare_service import compare_tables, employee_trace
from app.db import get_table_columns
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

    _, all_field_maps = load_mapping(settings.mapping_file)
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
    table_maps, field_maps = load_mapping(settings.mapping_file)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "table_maps": table_maps,
            "field_maps": field_maps,
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

    _, all_field_maps = load_mapping(settings.mapping_file)
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
    _, field_maps = load_mapping(settings.mapping_file)
    result = compare_tables(
        _conn_left(),
        _conn_right(),
        left_table,
        right_table,
        field_maps,
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

    table_maps, field_maps = load_mapping(settings.mapping_file)
    selected_tables = form.getlist("left_table")

    frames = []
    for lt in selected_tables:
        tm = next((t for t in table_maps if t.left_table == lt), None)
        if not tm:
            continue
        fmaps = field_map_for_table(field_maps, tm.left_table, tm.right_table)
        kmaps = key_map_for_table(field_maps, tm.left_table, tm.right_table)
        left_keys = [k.left_field for k in kmaps]
        right_keys = [k.right_field for k in kmaps]

        l_fields_all = [f.left_field for f in fmaps]
        r_fields_all = [f.right_field for f in fmaps]

        selected_left_fields = form.getlist(f"fields_left::{tm.left_table}")
        selected_right_fields = form.getlist(f"fields_right::{tm.right_table}")

        l_fields = selected_left_fields or l_fields_all
        r_fields = selected_right_fields or r_fields_all

        # Ensure key fields are present and at the beginning
        l_fields = left_keys + [f for f in l_fields if f not in set(left_keys)]
        r_fields = right_keys + [f for f in r_fields if f not in set(right_keys)]

        # Prefer source/table column order while keeping keys first
        left_order = get_table_columns(_conn_left(), tm.left_table)
        right_order = get_table_columns(_conn_right(), tm.right_table)
        if left_order:
            l_nonkeys = [c for c in left_order if c in set(l_fields) and c not in set(left_keys)]
            l_fields = left_keys + l_nonkeys
        if right_order:
            r_nonkeys = [c for c in right_order if c in set(r_fields) and c not in set(right_keys)]
            r_fields = right_keys + r_nonkeys

        left_frame = employee_trace(_conn_left(), tm.left_table, employee_id, l_fields, "employee_id")
        right_frame = employee_trace(_conn_right(), tm.right_table, employee_id, r_fields, "employee_id")

        # Sort trace rows by primary keys (if available)
        if left_keys and left_frame.get("rows"):
            left_frame["rows"] = sorted(
                left_frame["rows"],
                key=lambda r: tuple(str(r.get(k, "")) for k in left_keys),
            )
        if right_keys and right_frame.get("rows"):
            right_frame["rows"] = sorted(
                right_frame["rows"],
                key=lambda r: tuple(str(r.get(k, "")) for k in right_keys),
            )

        if not god_mode:
            left_frame["sql"] = "hidden"
            right_frame["sql"] = "hidden"

        frames.append({"left_table": tm.left_table, "right_table": tm.right_table, "left": left_frame, "right": right_frame})

    return _render_home(request, trace_frames=frames, employee_id=employee_id)
