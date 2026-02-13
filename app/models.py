from pydantic import BaseModel
from typing import Any


class TableMap(BaseModel):
    left_table: str
    right_table: str
    active: bool = True


class FieldMap(BaseModel):
    left_table: str
    left_field: str
    right_table: str
    right_field: str
    is_key: bool = False
    compare: bool = True


class ValueMap(BaseModel):
    # Per-table/field canonicalization rule.
    # Applies independently on left and right tables.
    table: str
    field: str
    table_value: Any
    canonical_value: Any


class CompareRequest(BaseModel):
    left_table: str
    right_table: str
    limit: int = 500
    god_mode: bool = False


class EmployeeTraceRequest(BaseModel):
    employee_id: str
    selected_tables: list[str]
    selected_fields: dict[str, list[str]]  # key=left_table
    god_mode: bool = False


class DiffResult(BaseModel):
    left_only: list[dict[str, Any]]
    right_only: list[dict[str, Any]]
    mismatched: list[dict[str, Any]]
    trace: dict[str, Any]
