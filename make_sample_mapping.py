from pathlib import Path
from openpyxl import Workbook

out = Path("mappings/workday_mapping_sample.xlsx")
out.parent.mkdir(parents=True, exist_ok=True)

wb = Workbook()
ws1 = wb.active
ws1.title = "table_map"
ws1.append(["left_table", "right_table", "active"])
ws1.append(["wd_dev.worker_core", "wd_prod.worker_core", True])
ws1.append(["wd_dev.worker_comp", "wd_prod.worker_comp", True])
ws1.append(["wd_dev.worker_org", "wd_prod.worker_org", True])

ws2 = wb.create_sheet("field_map")
ws2.append(["left_table", "left_field", "right_table", "right_field", "is_key", "compare"])
rows = [
    ["wd_dev.worker_core", "employee_id", "wd_prod.worker_core", "employee_id", True, True],
    ["wd_dev.worker_core", "first_name", "wd_prod.worker_core", "fname", False, True],
    ["wd_dev.worker_core", "last_name", "wd_prod.worker_core", "lname", False, True],
    ["wd_dev.worker_core", "active_flag", "wd_prod.worker_core", "is_active", False, True],
    ["wd_dev.worker_comp", "employee_id", "wd_prod.worker_comp", "employee_id", True, True],
    ["wd_dev.worker_comp", "base_salary", "wd_prod.worker_comp", "base_pay", False, True],
    ["wd_dev.worker_comp", "currency_code", "wd_prod.worker_comp", "currency", False, True],
    ["wd_dev.worker_org", "employee_id", "wd_prod.worker_org", "employee_id", True, True],
    ["wd_dev.worker_org", "cost_center", "wd_prod.worker_org", "cost_center_code", False, True],
    ["wd_dev.worker_org", "manager_id", "wd_prod.worker_org", "mgr_employee_id", False, True],
]
for r in rows:
    ws2.append(r)

wb.save(out)
print(f"Wrote {out}")
