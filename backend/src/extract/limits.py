"""Parameter budget helpers for extraction batch sizing."""

from __future__ import annotations

from typing import Any, List, Mapping, Sequence, Tuple

from .dicom_mappings import INSTANCE_FIELD_MAP, STACK_DEFINING_FIELDS

# PostgreSQL hard limit for bind parameters per statement
MAX_PG_PARAMS = 65_535
# Reserve a small safety margin so we never flirt with the absolute ceiling
PG_PARAM_MARGIN = 512

_INSTANCE_BASE_COLUMNS = {"series_id", "series_stack_id", "series_instance_uid", "sop_instance_uid", "dicom_file_path"}
# Instance columns = base + all fields from INSTANCE_FIELD_MAP except stack-defining ones
_INSTANCE_INSERT_COLUMNS = _INSTANCE_BASE_COLUMNS | (set(INSTANCE_FIELD_MAP.keys()) - STACK_DEFINING_FIELDS)


def estimate_instance_params_per_row() -> int:
    """Return the number of parameters used per inserted instance row."""

    return len(_INSTANCE_INSERT_COLUMNS)


def safe_rows_for_params(params_per_row: int, margin: int = PG_PARAM_MARGIN) -> int:
    """Compute max rows per statement for a given parameter footprint."""

    budget = max(1, MAX_PG_PARAMS - max(0, margin))
    per_row = max(1, params_per_row)
    return max(1, budget // per_row)


def calculate_safe_instance_batch_rows(*, margin: int = PG_PARAM_MARGIN) -> int:
    """Return a conservative safe batch size for instance inserts."""

    params_per_row = estimate_instance_params_per_row()
    return safe_rows_for_params(params_per_row, margin)


def build_parameter_chunk_plan(
    rows: Sequence[Mapping[str, Any]],
    *,
    margin: int = PG_PARAM_MARGIN,
) -> Tuple[List[List[Mapping[str, Any]]], int, int]:
    """Split rows into chunks that satisfy PostgreSQL's parameter budget.

    Returns a tuple of (chunks, params_per_row, max_rows_per_chunk).
    """

    rows_list: List[Mapping[str, Any]] = list(rows)
    if not rows_list:
        max_rows = safe_rows_for_params(1, margin)
        return ([], 0, max_rows)

    union_keys: set[str] = set()
    for row in rows_list:
        union_keys.update(row.keys())
    params_per_row = max(1, len(union_keys))
    max_rows = safe_rows_for_params(params_per_row, margin)

    if len(rows_list) <= max_rows:
        return ([rows_list], params_per_row, max_rows)

    chunks: List[List[Mapping[str, Any]]] = []
    for start in range(0, len(rows_list), max_rows):
        chunks.append(list(rows_list[start : start + max_rows]))
    return (chunks, params_per_row, max_rows)
