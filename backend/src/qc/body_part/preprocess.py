"""Slice extraction and per-slice preprocessing for Body Part QC.

The encoders expect 224×224 RGB tensors. Our DICOM stacks come in arbitrary
sizes and dynamic ranges. To make embeddings stable across vendors and
protocols, we deliberately ignore DICOM ``WindowCenter/Width`` and use a
simple percentile-clip + z-score pipeline.

The training set uses a single slice per stack (typically the central one).
Inference uses 3 central slices and probability-averages them.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


# Image size the encoders expect.
TARGET_SIZE: int = 224


@dataclass(frozen=True)
class PreprocessConfig:
    clip_pct: tuple[float, float] = (1.0, 99.0)
    zscore: bool = True
    size: int = TARGET_SIZE
    version: str = "v1"  # bump if any of the above changes


DEFAULT_PREPROCESS = PreprocessConfig()


def central_slice_indices(num_slices: int, n: int = 3) -> list[int]:
    """Return up to ``n`` slice indices clustered around the middle.

    For very small stacks (n_slices < n), the returned list is shorter
    and contains no duplicates.
    """
    if num_slices <= 0:
        return []
    mid = num_slices // 2
    half = n // 2
    raw = [mid + i - half for i in range(n)]
    out: list[int] = []
    seen: set[int] = set()
    for idx in raw:
        clamped = max(0, min(num_slices - 1, idx))
        if clamped not in seen:
            seen.add(clamped)
            out.append(clamped)
    return out


def orientation_slice_indices(
    num_slices: int,
    orientation: Optional[str],
    n_default: int = 3,
) -> list[int]:
    """Return slice indices adapted to the scan orientation.

    - **Axial**: 5 slices spread at 20%, 35%, 50%, 65%, 80% of the stack
      so that top-brain and bottom-spine patterns are captured separately.
    - **Sagittal / Coronal / Unknown / None**: same 3 central slices as
      ``central_slice_indices`` — reuses existing embedding cache.
      The improvement for sag/cor comes from center-weighted *aggregation*
      in the inference step, not from different slice positions.
    """
    if num_slices <= 0:
        return []
    ori = (orientation or "").strip().lower()
    if ori == "axial":
        fracs = [0.20, 0.35, 0.50, 0.65, 0.80]
        out: list[int] = []
        seen: set[int] = set()
        for f in fracs:
            idx = max(0, min(num_slices - 1, int(f * num_slices)))
            if idx not in seen:
                seen.add(idx)
                out.append(idx)
        return out
    return central_slice_indices(num_slices, n=n_default)


def preprocess_slice(
    arr: np.ndarray,
    *,
    rescale_slope: float = 1.0,
    rescale_intercept: float = 0.0,
    config: PreprocessConfig = DEFAULT_PREPROCESS,
) -> np.ndarray:
    """Convert a raw DICOM slice (2D float/int) into a 3-channel uint8
    image of shape (size, size, 3).

    Steps:
        1. apply rescale slope/intercept,
        2. percentile-clip to (1, 99) by default,
        3. z-score normalize (mean=0, std=1),
        4. linearly map to [0, 255] uint8,
        5. letterbox (preserve aspect, pad with 0) to ``size × size``,
        6. broadcast to RGB.

    Returns:
        np.ndarray of shape (size, size, 3) and dtype uint8.
    """
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D slice, got shape {arr.shape}")
    a = arr.astype(np.float32) * float(rescale_slope) + float(rescale_intercept)

    lo, hi = np.percentile(a, list(config.clip_pct))
    if hi <= lo:
        hi = lo + 1.0
    a = np.clip(a, lo, hi)

    if config.zscore:
        std = a.std()
        if std > 1e-6:
            a = (a - a.mean()) / std

    a_min, a_max = a.min(), a.max()
    if a_max - a_min > 1e-6:
        a = (a - a_min) / (a_max - a_min) * 255.0
    else:
        a = np.zeros_like(a)
    a = a.astype(np.uint8)

    # Letterbox to size × size while preserving aspect ratio.
    a = _letterbox_to_square(a, config.size)

    # Broadcast to 3 channels (no actual color, just for encoder API).
    rgb = np.stack([a, a, a], axis=-1)
    return rgb


def _letterbox_to_square(arr: np.ndarray, size: int) -> np.ndarray:
    """Resize ``arr`` to fit inside (size, size) preserving aspect ratio,
    padding with zeros."""
    from PIL import Image

    h, w = arr.shape
    img = Image.fromarray(arr, mode="L")
    scale = size / max(h, w)
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    img = img.resize((new_w, new_h), Image.Resampling.BILINEAR)
    canvas = Image.new("L", (size, size), 0)
    paste_x = (size - new_w) // 2
    paste_y = (size - new_h) // 2
    canvas.paste(img, (paste_x, paste_y))
    return np.asarray(canvas, dtype=np.uint8)


# ---------------------------------------------------------------------------
# Frame loader — wraps pydicom + the existing path resolver
# ---------------------------------------------------------------------------


def load_slice_array_from_dicom(
    dicom_file_path: str,
    *,
    frame_index: int = 0,
) -> Optional[tuple[np.ndarray, float, float]]:
    """Read a DICOM file and return (pixel_array_2d, rescale_slope, rescale_intercept).

    For multi-frame Enhanced DICOM, ``frame_index`` selects a frame; for
    classic single-frame DICOM, ``frame_index`` is ignored.

    Returns ``None`` if the file cannot be read or has no pixel data.
    """
    try:
        import pydicom  # local import — pydicom is heavy
    except ImportError:  # pragma: no cover
        logger.error("pydicom not installed in this environment.")
        return None
    try:
        ds = pydicom.dcmread(dicom_file_path)
    except Exception:
        logger.exception("Failed to read DICOM file: %s", dicom_file_path)
        return None
    try:
        arr = ds.pixel_array
    except Exception:
        logger.exception("DICOM has no readable pixel data: %s", dicom_file_path)
        return None
    if arr.ndim == 3:
        idx = max(0, min(frame_index, arr.shape[0] - 1))
        arr = arr[idx]
    slope = float(getattr(ds, "RescaleSlope", 1) or 1)
    intercept = float(getattr(ds, "RescaleIntercept", 0) or 0)
    return arr, slope, intercept
