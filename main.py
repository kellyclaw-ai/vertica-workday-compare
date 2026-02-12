from __future__ import annotations

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.compare_service import compare_tables, employee_trace
from app.mapping_store import load_mapping, field_map_for_table
from app.settings import settings

app = FastAPI(title="Vertica Workday Compare")
templates = Jinja2Templates(directory="templates")


def _conn_left() -> dict:
    return settings.left.model_dump()


def _conn_right() -> dict:
    return settings.right.model_dump()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    table_maps, field_maps = load_mapping(settings.mapping_file)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "table_maps": table_maps,
            "field_maps": field_maps,
            "god_mode_default": settings.god_mode_default,
        },
    )


@app.post("/compare", response_class=HTMLResponse)
def compare_ui(
    request: Request,
    left_table: str = Form(...),
    right_table: str = Form(...),
    limit: int = Form(500),
    god_mode: bool = Form(False),
):
    _, field_maps = load_mapping(settings.mapping_file)
    result = compare_tables(
        _conn_left(), _conn_right(), left_table, right_table, field_maps, limit, god_mode
    )
    return templates.TemplateResponse(
        "partials_compare_result.html",
        {"request": request, "result": result, "left_table": left_table, "right_table": right_table},
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
        l_fields_all = [f.left_field for f in fmaps]
        r_fields_all = [f.right_field for f in fmaps]

        selected_left_fields = form.getlist(f"fields_left::{tm.left_table}")
        selected_right_fields = form.getlist(f"fields_right::{tm.right_table}")

        l_fields = selected_left_fields or l_fields_all
        r_fields = selected_right_fields or r_fields_all

        # Placeholder: assumes employee_id field name is employee_id on both sides.
        left_frame = employee_trace(_conn_left(), tm.left_table, employee_id, l_fields, "employee_id")
        right_frame = employee_trace(_conn_right(), tm.right_table, employee_id, r_fields, "employee_id")

        if not god_mode:
            left_frame["sql"] = "hidden"
            right_frame["sql"] = "hidden"

        frames.append({
            "left_table": tm.left_table,
            "right_table": tm.right_table,
            "left": left_frame,
            "right": right_frame,
        })

    return templates.TemplateResponse(
        "partials_trace_result.html",
        {"request": request, "frames": frames, "employee_id": employee_id},
    )
