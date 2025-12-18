"""Advanced date recovery from DICOM UIDs and alternative sources.

This module provides functions to extract study dates from DICOM UIDs when
standard date fields (study_date, series_date, acquisition_date, content_date)
are all NULL.

Common UID patterns with embedded dates:
- 1.2.840.113619.2.55.3.2831207781.199.20220115.093045.123
- 1.2.826.0.1.3680043.8.498.20211203125417123456
"""

from __future__ import annotations

import re
import logging
from datetime import datetime, date
from typing import Any, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)


def extract_date_from_uid(uid: str, min_year: int, max_year: int) -> Optional[date]:
    """Extract YYYYMMDD from DICOM UID using regex pattern matching.
    
    Searches for 8 consecutive digits that form a valid calendar date within
    the specified year range. This filters out random numbers that happen to
    match the date pattern.
    
    Args:
        uid: DICOM UID string (e.g., StudyInstanceUID, SeriesInstanceUID)
        min_year: Minimum acceptable year (e.g., 1980)
        max_year: Maximum acceptable year (e.g., current year + 1)
    
    Returns:
        date object if valid date found, None otherwise
    
    Example:
        >>> extract_date_from_uid("1.2.840.113619.2.55.3.20220115.093045", 2000, 2025)
        date(2022, 1, 15)
    """
    if not uid:
        return None
    
    # Regex pattern: YYYY(01-12)(01-31)
    # Matches dates like 20220115, 19991231, etc.
    pattern = re.compile(r'(19\d{2}|20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])')
    
    matches = pattern.findall(uid)
    
    for year, month, day in matches:
        candidate = f"{year}{month}{day}"
        try:
            dt = datetime.strptime(candidate, "%Y%m%d")
            # Validate year range
            if min_year <= dt.year <= max_year:
                logger.debug(f"Found date {dt.date()} in UID: {uid[:50]}...")
                return dt.date()
        except ValueError:
            # Invalid date (e.g., Feb 30, Apr 31)
            continue
    
    return None


def recover_study_date_from_metadata(
    conn: Any,
    study_id: int,
    min_year: int,
    max_year: int
) -> tuple[Optional[date], Optional[str]]:
    """Attempt to recover study date from UIDs and alternative sources.
    
    Searches through various DICOM UID fields in priority order:
    1. study.study_instance_uid (most reliable)
    2. series.series_instance_uid
    3. series.frame_of_reference_uid
    4. series.media_storage_sop_instance_uid
    5. instance.sop_instance_uid (least reliable, but last resort)
    
    Args:
        conn: Database connection
        study_id: Study ID to recover date for
        min_year: Minimum acceptable year
        max_year: Maximum acceptable year
    
    Returns:
        Tuple of (recovered_date, source_description) or (None, None)
    
    Example:
        >>> recover_study_date_from_metadata(conn, 12345, 1980, 2025)
        (date(2022, 1, 15), 'study.study_instance_uid')
    """
    # Query to get study + related series/instance UIDs
    # LIMIT 100 to avoid scanning millions of instances for large studies
    query = text("""
    SELECT 
        s.study_instance_uid,
        ser.series_instance_uid,
        ser.frame_of_reference_uid,
        ser.media_storage_sop_instance_uid,
        i.sop_instance_uid
    FROM study s
    LEFT JOIN series ser ON s.study_id = ser.study_id
    LEFT JOIN instance i ON ser.series_id = i.series_id
    WHERE s.study_id = :study_id
    LIMIT 100
    """)
    
    result = conn.execute(query, {"study_id": study_id})
    rows = result.fetchall()
    
    if not rows:
        logger.warning(f"No metadata found for study_id={study_id}")
        return None, None
    
    # Try each UID field in priority order
    uid_fields = [
        (0, 'study.study_instance_uid'),
        (1, 'series.series_instance_uid'),
        (2, 'series.frame_of_reference_uid'),
        (3, 'series.media_storage_sop_instance_uid'),
        (4, 'instance.sop_instance_uid'),
    ]
    
    # Track which UIDs we've already checked (avoid duplicates)
    checked_uids = set()
    
    for row in rows:
        for field_idx, source_desc in uid_fields:
            uid_value = row[field_idx]
            
            # Skip NULL or already-checked UIDs
            if not uid_value or uid_value in checked_uids:
                continue
            
            checked_uids.add(uid_value)
            
            # Try to extract date from this UID
            recovered_date = extract_date_from_uid(uid_value, min_year, max_year)
            if recovered_date:
                logger.info(
                    f"Recovered date {recovered_date} from {source_desc} "
                    f"for study_id={study_id}"
                )
                return recovered_date, source_desc
    
    logger.debug(f"No valid date found in UIDs for study_id={study_id}")
    return None, None
