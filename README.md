# vertica-workday-compare

Localhost Python tool for comparing Workday data mirrored across two Vertica environments (e.g., DEV vs PROD) with field-name drift.

## Features (v0 scaffold)

1. **Excel mapping template** for table/field mapping.
2. **Table comparison API/UI** to show row-level differences.
3. **God mode tracing**: SQL, timing, row counts, execution metadata.
4. **Employee explorer UI**:
   - input `employee_id`
   - select mapped table pairs
   - per-table frame output (no union)
   - per-table multi-select field picker

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
python scripts/make_sample_mapping.py
uvicorn app.main:app --reload --host 127.0.0.1 --port 8020
```

Open: http://127.0.0.1:8020

## Mapping file

Generated at:

- `mappings/workday_mapping_sample.xlsx`

Sheets:
- `table_map` (left_table -> right_table)
- `field_map` (left_table.left_field -> right_table.right_field, key flag, compare flag)

## Vertica config placeholders

Edit `app/settings.py` connection placeholders for:
- left env (e.g., DEV)
- right env (e.g., PROD)

## System Overview

This app is a localhost-first FastAPI service with a server-rendered UI (Jinja templates) that compares mapped Workday data across two Vertica environments.

### End-to-end flow

1. **Load mapping definitions**
   - `app/mapping_store.py` loads table + field mappings from the Excel file.
   - Table pairs come from `table_map`.
   - Key/compare field behavior comes from `field_map`.

2. **Connect to both Vertica environments**
   - `app/settings.py` defines two connections:
     - `left` (typically DEV)
     - `right` (typically PROD)
   - SSL is enabled by default (`ssl: true`) for both connections.

3. **Run comparison**
   - `POST /compare` calls `app/compare_service.py::compare_tables`.
   - The service builds key-based lookups from mapped fields, normalizes values, and returns:
     - `left_only`
     - `right_only`
     - `mismatched`
   - Optional normalization controls:
     - trim strings
     - treat null/empty-like values as equal
     - numeric rounding precision

4. **Trace and debugging (“God mode”)**
   - When enabled, SQL text and timing metadata are included to explain exactly what was executed.
   - `POST /trace` provides per-table left/right record frames for a single employee.

5. **Schema introspection and mapping help**
   - `GET /introspect` fetches columns from both sides (`v_catalog.columns`) and suggests likely mappings based on normalized field names.

6. **Export + saved jobs**
   - `POST /compare/export` exports diff output to XLSX or CSV.
   - `app/jobs_store.py` persists saved compare/trace job metadata in local SQLite for replay workflows.

### Key modules

- `app/main.py` — routes + orchestration
- `app/db.py` — Vertica query execution + schema column lookup
- `app/compare_service.py` — diff logic, normalization, trace payloads
- `app/mapping_store.py` — mapping workbook loading/helpers
- `app/jobs_store.py` — saved job persistence
- `templates/index.html` — main UI

## Notes

- Designed for localhost only.
- Uses placeholders where needed.
- Comparison strategy currently uses key fields from mapping; can be expanded to hash/full-row diff.
