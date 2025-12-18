from extract.dicom_mappings import INSTANCE_FIELD_MAP
from extract.limits import (
    build_parameter_chunk_plan,
    calculate_safe_instance_batch_rows,
    estimate_instance_params_per_row,
    safe_rows_for_params,
)


def _make_row(idx: int) -> dict:
    base = {
        "series_id": idx,
        "series_instance_uid": f"series-{idx}",
        "sop_instance_uid": f"sop-{idx}",
        "dicom_file_path": f"path-{idx}.dcm",
    }
    for offset, column in enumerate(INSTANCE_FIELD_MAP.keys()):
        base[column] = offset + idx
    return base


def test_safe_batch_rows_matches_formula():
    per_row = estimate_instance_params_per_row()
    expected = safe_rows_for_params(per_row)
    assert calculate_safe_instance_batch_rows() == expected


def test_chunk_plan_splits_when_exceeding_budget():
    safe_rows = calculate_safe_instance_batch_rows()
    rows = [_make_row(i) for i in range(safe_rows + 7)]

    chunks, params_per_row, limit = build_parameter_chunk_plan(rows)

    assert params_per_row == len(INSTANCE_FIELD_MAP) + 4
    assert limit == safe_rows
    assert len(chunks) == ((len(rows) + limit - 1) // limit)
    assert sum(len(chunk) for chunk in chunks) == len(rows)
    assert all(len(chunk) <= limit for chunk in chunks)


def test_chunk_plan_single_chunk_within_budget():
    safe_rows = calculate_safe_instance_batch_rows()
    rows = [_make_row(i) for i in range(min(10, safe_rows))]

    chunks, _, limit = build_parameter_chunk_plan(rows)

    assert len(chunks) == 1
    assert chunks[0] == rows
    assert limit >= len(rows)
