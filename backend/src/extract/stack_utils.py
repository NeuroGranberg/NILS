"""Stack computation utilities for extraction pipeline.

This module provides functions to compute stack signatures and build stack records
during DICOM extraction, eliminating the need for post-hoc stack discovery.

The key insight is that stack membership can be determined per-instance without
needing all instances of a series upfront. Each instance's stack is defined by
its technical parameters (echo time, orientation, etc.).
"""

from __future__ import annotations

from typing import Any


def compute_orientation(iop_string: str | None) -> tuple[str, float]:
    """Parse ImageOrientationPatient string to categorical orientation and confidence.
    
    The ImageOrientationPatient DICOM tag contains 6 direction cosines that define
    the orientation of the image plane. This function computes:
    1. The normal vector via cross product
    2. The dominant axis (X=Sagittal, Y=Coronal, Z=Axial)
    3. A confidence score (how aligned to the canonical axis)
    
    Args:
        iop_string: ImageOrientationPatient as backslash-separated string
                    Format: "rx\\ry\\rz\\cx\\cy\\cz" (6 floats)
                    
    Returns:
        Tuple of (orientation, confidence) where:
        - orientation: "Axial" | "Coronal" | "Sagittal"
        - confidence: 0.0-1.0 (1.0 = perfectly aligned to axis)
        
    Examples:
        >>> compute_orientation("1\\0\\0\\0\\1\\0")  # Pure axial
        ("Axial", 1.0)
        >>> compute_orientation("0\\1\\0\\0\\0\\1")  # Pure sagittal
        ("Sagittal", 1.0)
        >>> compute_orientation(None)  # Missing data
        ("Axial", 0.5)
    """
    if not iop_string:
        return ("Axial", 0.5)  # Default with low confidence
    
    try:
        # Clean up string and split
        cleaned = str(iop_string).replace("[", "").replace("]", "").replace("'", "").replace('"', "").strip()
        parts = cleaned.split("\\")
        
        if len(parts) < 6:
            return ("Axial", 0.5)
        
        # Parse row and column direction cosines
        rx, ry, rz = float(parts[0]), float(parts[1]), float(parts[2])
        cx, cy, cz = float(parts[3]), float(parts[4]), float(parts[5])
        
        # Cross product gives normal vector (perpendicular to image plane)
        nx = ry * cz - rz * cy
        ny = rz * cx - rx * cz
        nz = rx * cy - ry * cx
        
        # Normalize the vector
        magnitude = (nx**2 + ny**2 + nz**2) ** 0.5
        if magnitude < 1e-10:
            return ("Axial", 0.5)
        
        abs_nx = abs(nx) / magnitude
        abs_ny = abs(ny) / magnitude
        abs_nz = abs(nz) / magnitude
        
        # Confidence = max component (1.0 means perfectly aligned)
        confidence = max(abs_nx, abs_ny, abs_nz)
        # Clamp to valid range
        confidence = max(0.0, min(1.0, confidence))
        
        # Determine orientation based on dominant axis
        # X-axis dominant → Sagittal (left-right view)
        # Y-axis dominant → Coronal (front-back view)
        # Z-axis dominant → Axial (top-down view)
        if abs_nx >= abs_ny and abs_nx >= abs_nz:
            return ("Sagittal", confidence)
        elif abs_ny >= abs_nx and abs_ny >= abs_nz:
            return ("Coronal", confidence)
        else:
            return ("Axial", confidence)
            
    except (ValueError, IndexError, TypeError):
        return ("Axial", 0.5)


def _round_or_none(value: Any, decimals: int) -> float | None:
    """Round a numeric value if not None, preserving None.
    
    Args:
        value: Numeric value or None
        decimals: Number of decimal places
        
    Returns:
        Rounded value or None
    """
    if value is None:
        return None
    try:
        return round(float(value), decimals)
    except (ValueError, TypeError):
        return None


def compute_stack_signature(series_instance_uid: str, instance_fields: dict) -> tuple:
    """Compute unique fingerprint for an instance's stack membership.
    
    Two instances with the same signature belong to the same stack.
    Uses series_instance_uid (not series_id) so it can be computed without
    database lookup.
    
    The signature includes all modality fields - non-applicable fields will be
    None and won't affect grouping (e.g., CT instance has echo_time=None).
    
    Args:
        series_instance_uid: DICOM SeriesInstanceUID
        instance_fields: Dict of instance-level DICOM fields
        
    Returns:
        Tuple that can be used as a dict key for caching
    """
    orientation, _ = compute_orientation(
        instance_fields.get("image_orientation_patient")
    )
    
    return (
        series_instance_uid,
        # MR fields (None for CT/PET - no effect on grouping)
        _round_or_none(instance_fields.get("echo_time"), 2),
        _round_or_none(instance_fields.get("inversion_time"), 1),
        instance_fields.get("echo_numbers"),
        instance_fields.get("echo_train_length"),
        _round_or_none(instance_fields.get("repetition_time"), 1),
        _round_or_none(instance_fields.get("flip_angle"), 1),
        instance_fields.get("receive_coil_name"),
        # CT fields (None for MR/PET)
        instance_fields.get("xray_exposure"),
        _round_or_none(instance_fields.get("kvp"), 0),
        _round_or_none(instance_fields.get("tube_current"), 0),
        # PET fields (None for MR/CT)
        instance_fields.get("pet_bed_index"),
        instance_fields.get("pet_frame_type"),
        # Common fields
        orientation,
        instance_fields.get("image_type"),
    )


def build_stack_row(
    series_id: int,
    stack_index: int,
    modality: str,
    instance_fields: dict,
) -> dict:
    """Build a series_stack INSERT row from instance fields.
    
    This creates the dict that will be inserted into the series_stack table.
    Called when a new stack is discovered for a series.
    
    Args:
        series_id: Database series_id (FK)
        stack_index: Index of this stack within the series (0, 1, 2...)
        modality: Modality code (MR, CT, PT, etc.)
        instance_fields: Dict of instance-level DICOM fields
        
    Returns:
        Dict ready for SQLAlchemy insert
    """
    orientation, confidence = compute_orientation(
        instance_fields.get("image_orientation_patient")
    )
    
    return {
        "series_id": series_id,
        "stack_index": stack_index,
        "stack_modality": modality,
        "stack_key": None,  # Can be computed later if needed
        # MR fields
        "stack_echo_time": _round_or_none(instance_fields.get("echo_time"), 2),
        "stack_inversion_time": _round_or_none(instance_fields.get("inversion_time"), 1),
        "stack_echo_numbers": instance_fields.get("echo_numbers"),
        "stack_echo_train_length": instance_fields.get("echo_train_length"),
        "stack_repetition_time": _round_or_none(instance_fields.get("repetition_time"), 1),
        "stack_flip_angle": _round_or_none(instance_fields.get("flip_angle"), 1),
        "stack_receive_coil_name": instance_fields.get("receive_coil_name"),
        # CT fields
        "stack_xray_exposure": instance_fields.get("xray_exposure"),
        "stack_kvp": _round_or_none(instance_fields.get("kvp"), 0),
        "stack_tube_current": _round_or_none(instance_fields.get("tube_current"), 0),
        # PET fields
        "stack_pet_bed_index": instance_fields.get("pet_bed_index"),
        "stack_pet_frame_type": instance_fields.get("pet_frame_type"),
        # Common + computed
        "stack_image_orientation": orientation,
        "stack_orientation_confidence": confidence,
        "stack_image_type": instance_fields.get("image_type"),
        "stack_n_instances": None,  # Can be updated later or left NULL
    }


def signature_from_stack_record(
    series_instance_uid: str,
    stack_echo_time: float | None,
    stack_inversion_time: float | None,
    stack_echo_numbers: str | None,
    stack_echo_train_length: int | None,
    stack_repetition_time: float | None,
    stack_flip_angle: float | None,
    stack_receive_coil_name: str | None,
    stack_xray_exposure: float | None,
    stack_kvp: float | None,
    stack_tube_current: float | None,
    stack_pet_bed_index: int | None,
    stack_pet_frame_type: str | None,
    stack_image_orientation: str | None,
    stack_image_type: str | None,
) -> tuple:
    """Reconstruct a stack signature from database record fields.
    
    This is the inverse of compute_stack_signature() - used to match
    existing stacks in the database against pending instance signatures.
    
    Args:
        series_instance_uid: SeriesInstanceUID for the series
        stack_*: Fields from series_stack table
        
    Returns:
        Tuple matching format of compute_stack_signature()
    """
    return (
        series_instance_uid,
        stack_echo_time,
        stack_inversion_time,
        stack_echo_numbers,
        stack_echo_train_length,
        stack_repetition_time,
        stack_flip_angle,
        stack_receive_coil_name,
        stack_xray_exposure,
        stack_kvp,
        stack_tube_current,
        stack_pet_bed_index,
        stack_pet_frame_type,
        stack_image_orientation,
        stack_image_type,
    )
