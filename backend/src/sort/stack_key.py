"""Stack key generation for multi-stack series.

This module provides functions to generate human-readable "stack keys" that
explain why a series was split into multiple stacks. For example:
- "multi_echo" - series has multiple echo times
- "multi_orientation" - series has multiple image orientations
- "multi_bed" - PET series with multiple bed positions
"""

from __future__ import annotations

from typing import Any

# Mapping from series_stack table column names (stack_*) to logical names
# used in the stack key categorization logic below.
DB_COLUMN_TO_LOGICAL = {
    'stack_echo_time': 'echo_time',
    'stack_echo_numbers': 'echo_numbers',
    'stack_inversion_time': 'inversion_time',
    'stack_repetition_time': 'repetition_time',
    'stack_flip_angle': 'flip_angle',
    'stack_receive_coil_name': 'receive_coil_name',
    'stack_image_orientation': 'image_orientation_patient',
    'stack_image_type': 'image_type',
    'stack_xray_exposure': 'xray_exposure',
    'stack_kvp': 'kvp',
    'stack_tube_current': 'tube_current',
    'stack_pet_bed_index': 'pet_bed_index',
    'stack_pet_frame_type': 'pet_frame_type',
}


def generate_stack_key_from_db(series_stacks: list[Any]) -> str | None:
    """Generate stack_key from series_stack records (DB column names).
    
    Same logic as generate_stack_key() but works with StackForFinalization
    records that have stack_* prefixed column names.

    Args:
        series_stacks: All stacks belonging to same series (StackForFinalization)

    Returns:
        Stack key string or None for single-stack series
    """
    if len(series_stacks) == 1:
        return None

    # Find which columns vary between stacks
    varying_columns = find_varying_columns_db(series_stacks)

    # Return general category based on varying columns
    if 'echo_time' in varying_columns or 'echo_numbers' in varying_columns:
        return "multi_echo"

    if 'image_type' in varying_columns:
        return "image_type_variation"

    if 'image_orientation_patient' in varying_columns:
        return "multi_orientation"

    if 'pet_bed_index' in varying_columns:
        return "multi_bed"

    if 'inversion_time' in varying_columns:
        return "multi_ti"

    if 'flip_angle' in varying_columns:
        return "multi_flip_angle"

    if 'receive_coil_name' in varying_columns:
        return "multi_coil"

    # Multiple parameters vary
    if len(varying_columns) > 1:
        return "multi_parameter"

    # Unknown reason (shouldn't happen)
    return "multi_stack"


def find_varying_columns_db(stacks: list[Any]) -> set[str]:
    """Identify which technical parameters differ between stacks (DB column names).
    
    Works with StackForFinalization records that have stack_* prefixed column names.
    Returns logical column names (without stack_ prefix) for compatibility with
    the stack key logic.
    """
    varying = set()

    # Check DB columns and map to logical names
    for db_col, logical_name in DB_COLUMN_TO_LOGICAL.items():
        values = {getattr(stack, db_col, None) for stack in stacks}
        if len(values) > 1:  # More than one unique value
            varying.add(logical_name)

    return varying
