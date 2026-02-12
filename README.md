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

## Notes

- Designed for localhost only.
- Uses placeholders where needed.
- Comparison strategy currently uses key fields from mapping; can be expanded to hash/full-row diff.
