"""DICOM Service - provides metadata and file paths for DICOM viewing."""

from __future__ import annotations

import io
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
from PIL import Image
from sqlalchemy import text

from metadata_db.session import SessionLocal as MetadataSessionLocal


@lru_cache(maxsize=10000)
def _cached_resolve_file_path(
    dicom_file_path: str, cohort_path: Optional[str]
) -> Optional[str]:
    """
    Cached file path resolution.

    This function is cached to avoid repeated filesystem checks during
    rapid slice scrolling in the DICOM viewer.
    """
    if not dicom_file_path:
        return None

    # Check if already absolute and exists
    if Path(dicom_file_path).is_absolute():
        if Path(dicom_file_path).exists():
            return dicom_file_path
        # Absolute path doesn't exist - fall through to try other resolutions

    # Try cohort source_path first (most reliable)
    if cohort_path:
        full_path = Path(cohort_path) / dicom_file_path
        if full_path.exists():
            return str(full_path)

    # Try DATA_ROOT
    data_root = os.environ.get("DATA_ROOT", "/app/data")
    full_path = Path(data_root) / dicom_file_path

    if full_path.exists():
        return str(full_path)

    # Try DATA_ROOTS (JSON array)
    data_roots_str = os.environ.get("DATA_ROOTS", "[]")
    try:
        data_roots = json.loads(data_roots_str)
        for root in data_roots:
            full_path = Path(root) / dicom_file_path
            if full_path.exists():
                return str(full_path)
    except json.JSONDecodeError:
        pass

    # Last resort: try cohort_path even if it doesn't exist (for Docker mounts)
    if cohort_path:
        return str(Path(cohort_path) / dicom_file_path)

    # Return original path (may work if mounted correctly)
    return dicom_file_path


def clear_path_cache() -> None:
    """Clear the file path resolution cache (e.g., after data changes)."""
    _cached_resolve_file_path.cache_clear()


class DicomService:
    """Service for DICOM file access and metadata."""

    def get_series_metadata(
        self, series_uid: str, stack_index: int = 0
    ) -> Optional[dict]:
        """
        Get series metadata in Cornerstone.js compatible format.

        Returns metadata including:
        - Series-level info (description, modality, etc.)
        - List of instances with URLs and rendering parameters
        """
        with MetadataSessionLocal() as meta_db:
            # Get series info
            series_query = """
                SELECT
                    s.series_id,
                    s.series_instance_uid,
                    s.series_description,
                    s.modality,
                    s.series_number,
                    st.study_instance_uid,
                    st.study_description,
                    st.study_date
                FROM series s
                JOIN study st ON s.study_id = st.study_id
                WHERE s.series_instance_uid = :series_uid
            """
            series_result = meta_db.execute(
                text(series_query), {"series_uid": series_uid}
            )
            series_row = series_result.fetchone()

            if not series_row:
                return None

            # Get instances for this series, ordered by slice location
            instances_query = """
                SELECT
                    i.instance_id,
                    i.sop_instance_uid,
                    i.instance_number,
                    i.slice_location,
                    i.dicom_file_path,
                    i.rows,
                    i.columns,
                    i.pixel_spacing,
                    i.window_center,
                    i.window_width,
                    i.rescale_intercept,
                    i.rescale_slope,
                    i.bits_allocated,
                    i.bits_stored,
                    i.high_bit,
                    i.pixel_representation,
                    i.number_of_frames
                FROM instance i
                WHERE i.series_instance_uid = :series_uid
                ORDER BY
                    COALESCE(i.slice_location, i.instance_number, 0) ASC,
                    i.instance_number ASC
            """
            instances_result = meta_db.execute(
                text(instances_query), {"series_uid": series_uid}
            )
            instance_rows = instances_result.fetchall()

            if not instance_rows:
                return None

            # Build instances list with Cornerstone-compatible metadata
            instances = []
            for idx, inst in enumerate(instance_rows):
                instance_meta = {
                    "instanceId": inst.instance_id,
                    "sopInstanceUid": inst.sop_instance_uid,
                    "instanceNumber": inst.instance_number,
                    "sliceLocation": inst.slice_location,
                    "sliceIndex": idx,
                    # URL for fetching the DICOM file
                    "url": f"/api/qc/dicom/file/{inst.instance_id}",
                    # Cornerstone rendering parameters
                    "rows": inst.rows,
                    "columns": inst.columns,
                    "pixelSpacing": inst.pixel_spacing,
                    "windowCenter": inst.window_center,
                    "windowWidth": inst.window_width,
                    "rescaleIntercept": inst.rescale_intercept,
                    "rescaleSlope": inst.rescale_slope,
                    "bitsAllocated": inst.bits_allocated,
                    "bitsStored": inst.bits_stored,
                    "highBit": inst.high_bit,
                    "pixelRepresentation": inst.pixel_representation,
                    "numberOfFrames": inst.number_of_frames or 1,
                }
                instances.append(instance_meta)

            return {
                "seriesInstanceUid": series_row.series_instance_uid,
                "seriesDescription": series_row.series_description,
                "modality": series_row.modality,
                "seriesNumber": series_row.series_number,
                "studyInstanceUid": series_row.study_instance_uid,
                "studyDescription": series_row.study_description,
                "studyDate": series_row.study_date,
                "stackIndex": stack_index,
                "totalInstances": len(instances),
                "instances": instances,
            }

    def get_instance_file_path(self, instance_id: int) -> Optional[str]:
        """Get the file path for a DICOM instance by instance ID."""
        with MetadataSessionLocal() as meta_db:
            # Get dicom_file_path and also try to get cohort source_path for resolution
            query = """
                SELECT
                    i.dicom_file_path,
                    c.path as cohort_path
                FROM instance i
                JOIN series s ON i.series_instance_uid = s.series_instance_uid
                LEFT JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                LEFT JOIN cohort c ON sc.cohort_id = c.cohort_id
                WHERE i.instance_id = :instance_id
            """
            result = meta_db.execute(text(query), {"instance_id": instance_id})
            row = result.fetchone()

            if not row or not row.dicom_file_path:
                return None

            return self._resolve_file_path(row.dicom_file_path, row.cohort_path)

    def get_instance_file_path_by_uid(self, sop_instance_uid: str) -> Optional[str]:
        """Get the file path for a DICOM instance by SOP Instance UID."""
        with MetadataSessionLocal() as meta_db:
            query = """
                SELECT
                    i.dicom_file_path,
                    c.path as cohort_path
                FROM instance i
                JOIN series s ON i.series_instance_uid = s.series_instance_uid
                LEFT JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                LEFT JOIN cohort c ON sc.cohort_id = c.cohort_id
                WHERE i.sop_instance_uid = :sop_uid
            """
            result = meta_db.execute(text(query), {"sop_uid": sop_instance_uid})
            row = result.fetchone()

            if not row or not row.dicom_file_path:
                return None

            return self._resolve_file_path(row.dicom_file_path, row.cohort_path)

    def _resolve_file_path(
        self, dicom_file_path: str, cohort_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Resolve a DICOM file path to an absolute path.

        Uses the cached module-level function for performance during
        rapid slice scrolling.
        """
        return _cached_resolve_file_path(dicom_file_path, cohort_path)

    def render_instance_to_png(
        self,
        instance_id: int,
        window_center: Optional[float] = None,
        window_width: Optional[float] = None,
        size: Optional[int] = None,
    ) -> Optional[bytes]:
        """
        Render a DICOM instance to PNG bytes.

        This is used for the simple viewer (before Cornerstone.js integration).
        Uses pydicom for reading and PIL for PNG encoding.

        Args:
            instance_id: Instance ID
            window_center: Override window center (optional)
            window_width: Override window width (optional)
            size: Resize to this max dimension (optional, for thumbnails)

        Returns:
            PNG image bytes or None if not found
        """
        import pydicom

        # Get file path
        file_path = self.get_instance_file_path(instance_id)
        if file_path is None or not Path(file_path).exists():
            return None

        try:
            # Read DICOM file
            ds = pydicom.dcmread(file_path)

            # Get pixel array
            pixel_array = ds.pixel_array

            # Handle multi-frame (take first frame)
            if len(pixel_array.shape) == 3:
                pixel_array = pixel_array[0]

            # Apply rescale slope/intercept if present
            slope = getattr(ds, "RescaleSlope", 1)
            intercept = getattr(ds, "RescaleIntercept", 0)
            pixel_array = pixel_array * slope + intercept

            # Get window values (use provided or from DICOM)
            wc = window_center
            ww = window_width

            if wc is None:
                wc = getattr(ds, "WindowCenter", None)
                if isinstance(wc, pydicom.multival.MultiValue):
                    wc = wc[0]
            if ww is None:
                ww = getattr(ds, "WindowWidth", None)
                if isinstance(ww, pydicom.multival.MultiValue):
                    ww = ww[0]

            # Default windowing if not available
            if wc is None or ww is None:
                wc = (pixel_array.max() + pixel_array.min()) / 2
                ww = pixel_array.max() - pixel_array.min()

            # Apply windowing
            low = wc - ww / 2
            high = wc + ww / 2
            pixel_array = np.clip(pixel_array, low, high)

            # Normalize to 0-255
            if high > low:
                pixel_array = (pixel_array - low) / (high - low) * 255
            else:
                pixel_array = np.zeros_like(pixel_array)

            pixel_array = pixel_array.astype(np.uint8)

            # Create PIL image
            image = Image.fromarray(pixel_array)

            # Handle photometric interpretation (invert if needed)
            photometric = getattr(ds, "PhotometricInterpretation", "MONOCHROME2")
            if photometric == "MONOCHROME1":
                image = Image.eval(image, lambda x: 255 - x)

            # Resize if requested
            if size:
                image.thumbnail((size, size), Image.Resampling.LANCZOS)

            # Convert to PNG bytes
            buffer = io.BytesIO()
            image.save(buffer, format="PNG", optimize=True)
            return buffer.getvalue()

        except Exception as e:
            # Log error but return None
            print(f"Error rendering DICOM {instance_id}: {e}")
            return None

    def get_series_instance_ids(
        self, series_uid: str, stack_index: int = 0
    ) -> list[int]:
        """Get list of instance IDs for a specific stack within a series, ordered by slice location."""
        with MetadataSessionLocal() as meta_db:
            # Join with series_stack to filter by stack_index
            query = """
                SELECT i.instance_id
                FROM instance i
                JOIN series_stack ss ON i.series_stack_id = ss.series_stack_id
                WHERE i.series_instance_uid = :series_uid
                  AND ss.stack_index = :stack_index
                ORDER BY
                    COALESCE(i.slice_location, i.instance_number, 0) ASC,
                    i.instance_number ASC
            """
            result = meta_db.execute(
                text(query),
                {"series_uid": series_uid, "stack_index": stack_index}
            )
            return [row.instance_id for row in result.fetchall()]

    def get_middle_instance_id(self, series_uid: str, stack_index: int = 0) -> Optional[int]:
        """Get the middle instance ID for thumbnail generation."""
        instance_ids = self.get_series_instance_ids(series_uid, stack_index)
        if not instance_ids:
            return None
        return instance_ids[len(instance_ids) // 2]

    def get_sister_series(self, series_uid: str) -> list[dict]:
        """
        Find related series from the same study for comparison.

        For contrast QC, this finds other T1w series from the same study
        that could be pre/post contrast pairs.

        Returns a list of series with basic metadata for comparison selection.
        """
        with MetadataSessionLocal() as meta_db:
            # First get the study_id for the given series
            study_query = """
                SELECT s.study_id, st.study_instance_uid
                FROM series s
                JOIN study st ON s.study_id = st.study_id
                WHERE s.series_instance_uid = :series_uid
            """
            study_result = meta_db.execute(text(study_query), {"series_uid": series_uid})
            study_row = study_result.fetchone()

            if not study_row:
                return []

            # Find related series from the same study
            # Focus on T1w and similar anatomical series for contrast comparison
            sisters_query = """
                SELECT
                    s.series_instance_uid,
                    s.series_description,
                    s.series_number,
                    s.modality,
                    scc.base,
                    scc.technique,
                    scc.post_contrast,
                    scc.directory_type,
                    COUNT(DISTINCT i.instance_id) as instance_count
                FROM series s
                LEFT JOIN series_classification_cache scc
                    ON s.series_instance_uid = scc.series_instance_uid
                LEFT JOIN instance i
                    ON s.series_instance_uid = i.series_instance_uid
                WHERE s.study_id = :study_id
                  AND s.series_instance_uid != :current_series_uid
                  AND s.modality = 'MR'
                GROUP BY
                    s.series_instance_uid,
                    s.series_description,
                    s.series_number,
                    s.modality,
                    scc.base,
                    scc.technique,
                    scc.post_contrast,
                    scc.directory_type
                ORDER BY s.series_number ASC NULLS LAST
            """
            sisters_result = meta_db.execute(
                text(sisters_query),
                {"study_id": study_row.study_id, "current_series_uid": series_uid}
            )

            sisters = []
            for row in sisters_result.fetchall():
                sisters.append({
                    "seriesInstanceUid": row.series_instance_uid,
                    "seriesDescription": row.series_description,
                    "seriesNumber": row.series_number,
                    "modality": row.modality,
                    "base": row.base,
                    "technique": row.technique,
                    "postContrast": row.post_contrast,
                    "directoryType": row.directory_type,
                    "instanceCount": row.instance_count,
                    "thumbnailUrl": f"/api/qc/dicom/{row.series_instance_uid}/thumbnail",
                })

            return sisters

    def get_t1w_contrast_pairs(self, series_uid: str) -> dict:
        """
        Find potential pre/post contrast T1w pairs for the given series.

        Returns series grouped by likely contrast status for easy comparison.
        """
        sisters = self.get_sister_series(series_uid)

        # Filter for T1w and similar contrast-relevant series
        t1w_series = [
            s for s in sisters
            if s.get("base") in ("T1w", None) or "T1" in (s.get("seriesDescription") or "").upper()
        ]

        # Group by known contrast status
        pre_contrast = [s for s in t1w_series if s.get("postContrast") == 0]
        post_contrast = [s for s in t1w_series if s.get("postContrast") == 1]
        unknown_contrast = [s for s in t1w_series if s.get("postContrast") is None]

        return {
            "preContrast": pre_contrast,
            "postContrast": post_contrast,
            "unknownContrast": unknown_contrast,
            "allSisters": sisters,
        }


# Global service instance
dicom_service = DicomService()
