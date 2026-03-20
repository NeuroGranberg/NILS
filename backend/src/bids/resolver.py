"""Manifest resolution for standalone BIDS export.

Parses CSV/JSON selection manifests and resolves them to series_stack_ids.

Resolution rules:
  - subject_code only -> all sessions, all stacks for that subject
  - subject_code + study_date -> all stacks for that session
  - subject_code + study_date + series_stack_id(s) -> exact stacks
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from metadata_db.session import SessionLocal as MetadataSessionLocal


@dataclass
class ManifestEntry:
    subject_code: str
    study_date: str | None = None
    series_stack_ids: list[int] | None = None


@dataclass
class ResolvedEntry:
    subject_code: str
    study_date: str
    series_stack_id: int
    directory_type: str | None = None
    base: str | None = None
    dicom_origin_cohort: str | None = None


@dataclass
class ResolveResult:
    total_subjects: int = 0
    total_sessions: int = 0
    total_stacks: int = 0
    resolved_stack_ids: list[int] = field(default_factory=list)
    resolved_entries: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    detected_cohorts: list[str] = field(default_factory=list)  # Auto-detected cohort(s) from stacks


def parse_manifest_json(data: list[dict[str, Any]]) -> list[ManifestEntry]:
    """Parse a JSON manifest into ManifestEntry objects."""
    entries: list[ManifestEntry] = []
    for i, row in enumerate(data):
        subject_code = row.get("subject_code")
        if not subject_code or not str(subject_code).strip():
            continue

        study_date = row.get("study_date")
        if study_date is not None:
            study_date = str(study_date).strip() or None

        stack_ids: list[int] | None = None
        if "series_stack_ids" in row and row["series_stack_ids"]:
            raw = row["series_stack_ids"]
            stack_ids = [int(sid) for sid in raw] if isinstance(raw, list) else [int(raw)]
        elif "series_stack_id" in row and row["series_stack_id"] not in (None, "", ""):
            stack_ids = [int(row["series_stack_id"])]

        entries.append(ManifestEntry(
            subject_code=str(subject_code).strip(),
            study_date=study_date,
            series_stack_ids=stack_ids,
        ))
    return entries


def parse_manifest_csv(csv_text: str) -> list[ManifestEntry]:
    """Parse a CSV manifest string into ManifestEntry objects.
    
    Supports flexible column names:
    - Subject: subject_code, subject_id, patient_id, patient_code, subj, sub, id
    - Date: study_date, session_date, scan_date, mri_date, date, session
    - Stack: series_stack_id, stack_id, flair_stack_id, series_id, stack
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    
    # Get actual fieldnames and create case-insensitive lookup
    fieldnames = reader.fieldnames or []
    field_lower_map = {f.lower(): f for f in fieldnames}
    
    # Find subject column (required)
    subject_col = None
    for candidate in ['subject_code', 'subject_id', 'patient_id', 'patient_code', 'subj', 'sub', 'id']:
        if candidate in field_lower_map:
            subject_col = field_lower_map[candidate]
            break
    
    # Find date column (optional)
    date_col = None
    for candidate in ['study_date', 'session_date', 'scan_date', 'mri_date', 'date', 'session']:
        if candidate in field_lower_map:
            date_col = field_lower_map[candidate]
            break
    
    # Find stack ID column (optional)
    stack_col = None
    for candidate in ['series_stack_id', 'stack_id', 'flair_stack_id', 'series_id', 'stack']:
        if candidate in field_lower_map:
            stack_col = field_lower_map[candidate]
            break
    
    entries: list[ManifestEntry] = []
    for row in reader:
        # Get subject code
        subject_code = ""
        if subject_col:
            subject_code = (row.get(subject_col) or "").strip()
        if not subject_code:
            continue

        # Get study date
        study_date = None
        if date_col:
            study_date = (row.get(date_col) or "").strip() or None

        # Get stack IDs
        stack_ids: list[int] | None = None
        if stack_col:
            raw_id = (row.get(stack_col) or "").strip()
            if raw_id:
                try:
                    stack_ids = [int(raw_id)]
                except ValueError:
                    pass  # Skip invalid stack IDs

        entries.append(ManifestEntry(
            subject_code=subject_code,
            study_date=study_date,
            series_stack_ids=stack_ids,
        ))
    return entries


def parse_manifest(content: str, format: str = "auto") -> list[ManifestEntry]:
    """Parse manifest content, auto-detecting format if needed."""
    if format == "auto":
        stripped = content.strip()
        if stripped.startswith("[") or stripped.startswith("{"):
            format = "json"
        else:
            format = "csv"

    if format == "json":
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
        return parse_manifest_json(data)
    else:
        return parse_manifest_csv(content)


def _resolve_identifiers_to_subject_codes(
    identifiers: set[str],
    db,
) -> dict[str, str]:
    """Look up identifiers in subject_other_identifiers and return mapping.

    Returns dict mapping other_identifier -> subject_code for any matches found.
    Checks across ALL id_types so any identifier format works.
    """
    if not identifiers:
        return {}

    placeholders = ", ".join(f":id_{i}" for i in range(len(identifiers)))
    sql = f"""
        SELECT DISTINCT soi.other_identifier, subj.subject_code
        FROM subject_other_identifiers soi
        JOIN subject subj ON soi.subject_id = subj.subject_id
        WHERE soi.other_identifier IN ({placeholders})
    """
    params = {f"id_{i}": v for i, v in enumerate(sorted(identifiers))}
    rows = db.execute(text(sql), params).fetchall()
    return {row.other_identifier: row.subject_code for row in rows}


def resolve_manifest(entries: list[ManifestEntry]) -> ResolveResult:
    """Resolve manifest entries to series_stack_ids via database lookup.

    Resolution strategy:
    1. Try matching subject_code directly against subject.subject_code
    2. For any unresolved subjects, try matching against subject_other_identifiers
       (looks across ALL id_types so any identifier format works)
    3. subject_code only -> all stacks for that subject
    4. subject_code + study_date -> all stacks for that session
    5. subject_code + study_date + stack_ids -> exact stacks
    """
    if not entries:
        return ResolveResult()

    # Separate entries by granularity for efficient querying
    subject_only: set[str] = set()
    subject_session: set[tuple[str, str]] = set()
    explicit_stack_ids: set[int] = set()
    all_subject_codes: set[str] = set()

    for entry in entries:
        all_subject_codes.add(entry.subject_code)
        if entry.series_stack_ids:
            explicit_stack_ids.update(entry.series_stack_ids)
        elif entry.study_date:
            subject_session.add((entry.subject_code, entry.study_date))
        else:
            subject_only.add(entry.subject_code)

    with MetadataSessionLocal() as db:
        # Phase 1: Resolve via subject_other_identifiers to get canonical subject_codes
        # This allows users to use ANY identifier type in their manifest
        alt_id_map = _resolve_identifiers_to_subject_codes(all_subject_codes, db)

        # Remap: replace alternative identifiers with canonical subject_codes
        identifier_resolved: dict[str, str] = {}  # original -> canonical
        remapped_subject_only: set[str] = set()
        remapped_subject_session: set[tuple[str, str]] = set()

        for sc in subject_only:
            canonical = alt_id_map.get(sc, sc)
            if canonical != sc:
                identifier_resolved[sc] = canonical
            remapped_subject_only.add(canonical)

        for sc, sd in subject_session:
            canonical = alt_id_map.get(sc, sc)
            if canonical != sc:
                identifier_resolved[sc] = canonical
            remapped_subject_session.add((canonical, sd))

        # Build query with UNION of all three resolution levels
        query_parts: list[str] = []
        params: dict = {}

        base_select = """
            SELECT DISTINCT
                scc.series_stack_id,
                COALESCE(subj.subject_code, 'unknown') AS subject_code,
                COALESCE(st.study_date::text, 'unknown') AS study_date,
                scc.directory_type,
                scc.base,
                scc.modifier_csv,
                scc.technique,
                scc.dicom_origin_cohort
            FROM series_classification_cache scc
            JOIN series s ON scc.series_instance_uid = s.series_instance_uid
            JOIN study st ON s.study_id = st.study_id
            LEFT JOIN subject subj ON s.subject_id = subj.subject_id
        """

        # Level 1: subject_code only -> all stacks
        if remapped_subject_only:
            placeholders = ", ".join(f":subj_{i}" for i in range(len(remapped_subject_only)))
            query_parts.append(f"{base_select} WHERE subj.subject_code IN ({placeholders})")
            for i, sc in enumerate(sorted(remapped_subject_only)):
                params[f"subj_{i}"] = sc

        # Level 2: subject_code + study_date -> all stacks for that session
        for idx, (sc, sd) in enumerate(sorted(remapped_subject_session)):
            query_parts.append(
                f"{base_select} WHERE subj.subject_code = :ss_subj_{idx} AND st.study_date::text = :ss_date_{idx}"
            )
            params[f"ss_subj_{idx}"] = sc
            params[f"ss_date_{idx}"] = sd

        # Level 3: explicit stack IDs
        if explicit_stack_ids:
            placeholders = ", ".join(f":eid_{i}" for i in range(len(explicit_stack_ids)))
            query_parts.append(f"{base_select} WHERE scc.series_stack_id IN ({placeholders})")
            for i, sid in enumerate(sorted(explicit_stack_ids)):
                params[f"eid_{i}"] = sid

        if not query_parts:
            return ResolveResult()

        full_sql = " UNION ".join(query_parts)
        rows = db.execute(text(full_sql), params).fetchall()

    # Build result
    resolved_ids: list[int] = []
    resolved_entries: list[dict] = []
    subjects_seen: set[str] = set()
    sessions_seen: set[tuple[str, str]] = set()
    cohorts_seen: set[str] = set()

    for row in rows:
        resolved_ids.append(row.series_stack_id)
        subjects_seen.add(row.subject_code)
        sessions_seen.add((row.subject_code, row.study_date))
        if row.dicom_origin_cohort:
            cohorts_seen.add(row.dicom_origin_cohort)
        resolved_entries.append({
            "series_stack_id": row.series_stack_id,
            "subject_code": row.subject_code,
            "study_date": row.study_date,
            "directory_type": row.directory_type,
            "base": row.base,
            "modifier": row.modifier_csv or "",
            "technique": row.technique or "",
            "dicom_origin_cohort": row.dicom_origin_cohort,
        })

    # Generate warnings for unresolved entries
    warnings: list[str] = []
    resolved_subjects = {e["subject_code"] for e in resolved_entries}
    for sc in all_subject_codes:
        canonical = identifier_resolved.get(sc, sc)
        if canonical not in resolved_subjects:
            warnings.append(f"Subject '{sc}' not found in database")
        elif sc in identifier_resolved:
            warnings.append(
                f"Subject '{sc}' resolved via alternative identifier to '{identifier_resolved[sc]}'"
            )

    for sc, sd in subject_session:
        canonical = identifier_resolved.get(sc, sc)
        if (canonical, sd) not in sessions_seen:
            warnings.append(f"No data found for subject '{sc}' on date '{sd}'")

    resolved_stack_set = set(resolved_ids)
    for sid in explicit_stack_ids:
        if sid not in resolved_stack_set:
            warnings.append(f"Stack ID {sid} not found in database")

    return ResolveResult(
        total_subjects=len(subjects_seen),
        total_sessions=len(sessions_seen),
        total_stacks=len(resolved_ids),
        resolved_stack_ids=sorted(resolved_ids),
        resolved_entries=resolved_entries,
        warnings=warnings,
        detected_cohorts=sorted(cohorts_seen),
    )
