"""Main Acquisition Selection QC service.

Powers the "Main Acquisition QC" module in the frontend. Lets reviewers walk
through a cohort one session at a time, filter to a class of acquisitions,
see matching stacks grouped by "same-acquisition" identity, and tag each
stack as main / pre / post.

Persistence:
- ``series_classification_cache.classification_string`` - CSV of tokens; the
  literal token ``main_acquisition`` marks the chosen stack per bundle.
- ``series_classification_cache.post_contrast`` - 1=post, 0=pre, NULL=unknown.
- ``series_classification_cache.manual_review_reasons_csv`` - review tokens
  starting with ``contrast:`` are cleared when pre/post is set manually.

No schema migrations required.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Literal, Optional

from sqlalchemy import text

from metadata_db.session import SessionLocal as MetadataSessionLocal
from qc.cache_tokens import add_token, has_token, remove_prefix, remove_token
from qc import main_acquisition_cache


MAIN_TOKEN = "main_acquisition"

# Facet keys the frontend filter bar exposes. Order matters for display.
FACETS = (
    "directory_type",
    "provenance",
    "orientation",
    "base",
    "mr_acquisition_type",
    "technique",
    "modifier_csv",
)

NONE_SENTINEL = "__none__"

# Columns in the filterable view; keyed by facet name.
FACET_COLUMN_SQL = {
    "directory_type": "scc.directory_type",
    "provenance": "scc.provenance",
    "orientation": "scc.orientation_patient",
    "base": "scc.base",
    "mr_acquisition_type": "sf.mr_acquisition_type",
    "technique": "scc.technique",
    "modifier_csv": "scc.modifier_csv",
}

# Orientation abbreviation used in bundle titles (mirrors BIDS exporter).
ORIENT_ABBREV = {"Axial": "Ax", "Coronal": "Cor", "Sagittal": "Sag"}
BODY_PART_PREFIX = {"spine": "SC", "neck": "Neck", "brain-neck": "BrainNeck"}


# --------------------------------------------------------------------------- #
# DTOs
# --------------------------------------------------------------------------- #


@dataclass
class FilterState:
    """Each facet is either None (any) or a list of literal values.

    The literal ``__none__`` represents a NULL/empty value.
    """
    directory_type: Optional[list[str]] = None
    provenance: Optional[list[str]] = None
    orientation: Optional[list[str]] = None
    base: Optional[list[str]] = None
    mr_acquisition_type: Optional[list[str]] = None
    technique: Optional[list[str]] = None
    modifier_csv: Optional[list[str]] = None

    @classmethod
    def from_dict(cls, data: dict | None) -> "FilterState":
        data = data or {}
        kwargs = {}
        for facet in FACETS:
            v = data.get(facet)
            if v is None:
                kwargs[facet] = None
            elif isinstance(v, list):
                kwargs[facet] = [str(x) for x in v]
            else:
                kwargs[facet] = [str(v)]
        return cls(**kwargs)

    def to_dict(self) -> dict:
        return {facet: getattr(self, facet) for facet in FACETS}


# --------------------------------------------------------------------------- #
# Service
# --------------------------------------------------------------------------- #


class MainAcquisitionService:
    """Service for Main Acquisition Selection QC."""

    # ---- cohort resolution ---------------------------------------------- #

    def _resolve_cohort_name(self, cohort_id: int) -> Optional[str]:
        from metadata_db.resolve import get_cohort_name_from_app_db
        return get_cohort_name_from_app_db(cohort_id)

    @staticmethod
    def _invalidate_cohort_cache_by_name(cohort_name: Optional[str]) -> None:
        """Drop cached session lists for the cohort with this name.

        ``set_stack_role`` knows the cohort *name* (from
        ``series_classification_cache.dicom_origin_cohort``) but not the
        application-DB cohort id, which is what the cache is keyed by. We
        translate name → id via a lightweight raw lookup.
        """
        if not cohort_name:
            return
        try:
            from db.session import engine
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT id FROM cohorts WHERE LOWER(name) = LOWER(:n)"),
                    {"n": cohort_name},
                ).fetchone()
                if row and row.id is not None:
                    main_acquisition_cache.invalidate_cohort(int(row.id))
        except Exception:
            # Cache invalidation must never break the write path; on any
            # failure simply fall back to the TTL.
            pass

    # ---- filter clause builder ------------------------------------------ #

    def _build_filter_clauses(
        self,
        filters: FilterState,
        skip_facet: Optional[str] = None,
    ) -> tuple[list[str], dict]:
        """Build WHERE clauses and parameters from a FilterState.

        ``skip_facet`` lets the filter-options query exclude a specific facet
        from its own WHERE clause so its dropdown shows all still-compatible
        values.
        """
        clauses: list[str] = []
        params: dict = {}
        for facet in FACETS:
            if facet == skip_facet:
                continue
            values = getattr(filters, facet)
            if not values:
                continue
            col = FACET_COLUMN_SQL[facet]
            sub: list[str] = []
            has_none = False
            literal_idx = 0
            for v in values:
                if v == NONE_SENTINEL:
                    has_none = True
                    continue
                pkey = f"{facet}_{literal_idx}"
                literal_idx += 1
                sub.append(f"{col} = :{pkey}")
                params[pkey] = v
            if has_none:
                sub.append(f"({col} IS NULL OR {col} = '')")
            if sub:
                clauses.append("(" + " OR ".join(sub) + ")")
        return clauses, params

    def _cohort_clause(self) -> str:
        return "LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)"

    def _base_joins(self) -> str:
        return """
            FROM series_classification_cache scc
            JOIN series s ON scc.series_instance_uid = s.series_instance_uid
            JOIN study st ON s.study_id = st.study_id
            LEFT JOIN subject subj ON s.subject_id = subj.subject_id
            LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
            LEFT JOIN stack_fingerprint sf ON ss.series_stack_id = sf.series_stack_id
        """

    # ---- filter options (cascading facets) ------------------------------ #

    def get_filter_options(self, cohort_id: int, filters: FilterState) -> dict:
        cohort_name = self._resolve_cohort_name(cohort_id)
        if not cohort_name:
            return {facet: [] for facet in FACETS}

        results: dict[str, list[str]] = {facet: [] for facet in FACETS}

        with MetadataSessionLocal() as meta_db:
            for facet in FACETS:
                clauses, params = self._build_filter_clauses(filters, skip_facet=facet)
                where_extra = (" AND " + " AND ".join(clauses)) if clauses else ""
                col = FACET_COLUMN_SQL[facet]
                query = f"""
                    SELECT DISTINCT COALESCE(NULLIF({col}, ''), NULL) AS v
                    {self._base_joins()}
                    WHERE {self._cohort_clause()}{where_extra}
                """
                rows = meta_db.execute(
                    text(query),
                    {"cohort_name": cohort_name, **params},
                ).fetchall()
                values: list[str] = []
                has_null = False
                for row in rows:
                    if row.v is None:
                        has_null = True
                    else:
                        values.append(row.v)
                values.sort(key=lambda x: x.lower())
                if has_null:
                    values.insert(0, NONE_SENTINEL)
                results[facet] = values

        return results

    # ---- session enumeration -------------------------------------------- #

    def _session_key_sql(self) -> str:
        return "subj.subject_code, st.study_date"

    def get_sessions(self, cohort_id: int, filters: FilterState, display_id_type: Optional[str] = None, only_multi_stack: bool = False) -> dict:
        # Short-circuit: in-process TTL cache. The session list is cohort-wide
        # and dominates the cost of "Next session" navigation.
        cache_key = main_acquisition_cache.make_key(
            cohort_id, filters.to_dict(), display_id_type, only_multi_stack
        )
        cached = main_acquisition_cache.get(cache_key)
        if cached is not None:
            return cached

        cohort_name = self._resolve_cohort_name(cohort_id)
        if not cohort_name:
            return {
                "total_subjects": 0,
                "total_sessions": 0,
                "total_stacks": 0,
                "sessions": [],
            }

        clauses, params = self._build_filter_clauses(filters)
        where_extra = (" AND " + " AND ".join(clauses)) if clauses else ""

        # When only_multi_stack is set, restrict to sessions that have at
        # least one bundle-key group with >1 stacks (i.e. a MAIN choice).
        multi_stack_clause = ""
        # Pre-compute set of (subject_id, study_date) that have multi-stack bundles.
        multi_stack_session_set: set[tuple] | None = None
        if only_multi_stack:
            multi_stack_session_set = set()

        with MetadataSessionLocal() as meta_db:
            # Pre-compute sessions with multi-stack bundles (one fast query).
            if multi_stack_session_set is not None:
                ms_sql = f"""
                    SELECT s2.subject_id, st2.study_date::text AS study_date
                    FROM series_classification_cache scc2
                    JOIN series s2 ON scc2.series_instance_uid = s2.series_instance_uid
                    JOIN study st2 ON s2.study_id = st2.study_id
                    LEFT JOIN series_stack ss2 ON scc2.series_stack_id = ss2.series_stack_id
                    LEFT JOIN stack_fingerprint sf2 ON ss2.series_stack_id = sf2.series_stack_id
                    WHERE LOWER(scc2.dicom_origin_cohort) = LOWER(:cohort_name)
                    {where_extra.replace('scc.', 'scc2.').replace('sf.', 'sf2.')}
                    GROUP BY
                        s2.subject_id, st2.study_date,
                        COALESCE(scc2.body_part, ''),
                        COALESCE(scc2.orientation_patient, ''),
                        COALESCE(scc2.base, ''),
                        COALESCE(sf2.mr_acquisition_type, ''),
                        COALESCE(scc2.modifier_csv, ''),
                        COALESCE(scc2.technique, ''),
                        COALESCE(scc2.acceleration_csv, ''),
                        COALESCE(scc2.construct_csv, ''),
                        COALESCE(scc2.directory_type, ''),
                        ss2.stack_echo_time,
                        ss2.stack_repetition_time,
                        ss2.stack_inversion_time,
                        ss2.stack_flip_angle
                    HAVING COUNT(DISTINCT scc2.series_stack_id) > 1
                """
                ms_rows = meta_db.execute(
                    text(ms_sql), {"cohort_name": cohort_name, **params}
                ).fetchall()
                for r in ms_rows:
                    multi_stack_session_set.add((r.subject_id, r.study_date))

            # Total stacks first (distinct stacks matching the filter).
            count_sql = f"""
                SELECT COUNT(DISTINCT scc.series_stack_id)
                {self._base_joins()}
                WHERE {self._cohort_clause()}{where_extra}
            """
            total_stacks = meta_db.execute(
                text(count_sql),
                {"cohort_name": cohort_name, **params},
            ).scalar() or 0

            # Session list (ordered by display_id_type if set).
            sort_by = display_id_type or "code"
            sess_sql = f"""
                SELECT subj.subject_code AS subject_code,
                       subj.subject_id AS subject_id,
                       st.study_date::text AS study_date,
                       COUNT(DISTINCT scc.series_stack_id) AS stack_count
                {self._base_joins()}
                WHERE {self._cohort_clause()}{where_extra}
                GROUP BY subj.subject_code, subj.subject_id, st.study_date
                ORDER BY
                    CASE WHEN :sort_by = 'code' THEN subj.subject_code END ASC,
                    CASE WHEN :sort_by != 'code' THEN
                        (SELECT soi_sort.other_identifier
                         FROM subject_other_identifiers soi_sort
                         JOIN id_types it_sort ON soi_sort.id_type_id = it_sort.id_type_id
                         WHERE soi_sort.subject_id = subj.subject_id
                         AND it_sort.id_type_name = :sort_by
                         LIMIT 1)
                    END ASC NULLS LAST,
                    subj.subject_code ASC NULLS LAST,
                    st.study_date NULLS LAST
            """
            rows = meta_db.execute(
                text(sess_sql),
                {"cohort_name": cohort_name, "sort_by": sort_by, **params},
            ).fetchall()

            # Filter to sessions with multi-stack bundles if requested.
            if multi_stack_session_set is not None:
                rows = [
                    r for r in rows
                    if (r.subject_id, r.study_date) in multi_stack_session_set
                ]

            sessions = [
                {
                    "index": i,
                    "subject_code": row.subject_code or "",
                    "study_date": row.study_date or "",
                    "stack_count": int(row.stack_count or 0),
                    "display_id": None,
                }
                for i, row in enumerate(rows)
            ]
            subjects = {s["subject_code"] for s in sessions if s["subject_code"]}
            subject_ids = [row.subject_id for row in rows if row.subject_id]
            unique_subject_ids = list(set(subject_ids))

            # Resolve alternative display IDs if requested.
            if display_id_type and display_id_type != "code" and subjects:
                id_rows = meta_db.execute(
                    text("""
                        SELECT subj.subject_code, soi.other_identifier
                        FROM subject_other_identifiers soi
                        JOIN subject subj ON soi.subject_id = subj.subject_id
                        JOIN id_types it ON soi.id_type_id = it.id_type_id
                        WHERE it.id_type_name = :id_type
                          AND subj.subject_code = ANY(:codes)
                    """),
                    {"id_type": display_id_type, "codes": list(subjects)},
                ).fetchall()
                id_map = {r.subject_code: r.other_identifier for r in id_rows}
                for s in sessions:
                    s["display_id"] = id_map.get(s["subject_code"])

            # Collect available id types from subjects in this result set.
            available_id_types = ["code"]
            if unique_subject_ids:
                id_type_rows = meta_db.execute(
                    text("""
                        SELECT DISTINCT it.id_type_name
                        FROM subject_other_identifiers soi
                        JOIN id_types it ON soi.id_type_id = it.id_type_id
                        WHERE soi.subject_id = ANY(:subject_ids)
                        ORDER BY it.id_type_name
                    """),
                    {"subject_ids": unique_subject_ids},
                ).fetchall()
                available_id_types += [r.id_type_name for r in id_type_rows]

        result = {
            "total_subjects": len(subjects),
            "total_sessions": len(sessions),
            "total_stacks": int(total_stacks),
            "sessions": sessions,
            "available_id_types": available_id_types,
        }
        main_acquisition_cache.set(cache_key, result)
        return result

    # ---- bundles for one session ---------------------------------------- #

    def get_session_bundles(
        self,
        cohort_id: int,
        filters: FilterState,
        session_index: int,
        display_id_type: Optional[str] = None,
        only_multi_stack: bool = False,
    ) -> dict:
        sessions_payload = self.get_sessions(
            cohort_id, filters,
            display_id_type=display_id_type,
            only_multi_stack=only_multi_stack,
        )
        sessions = sessions_payload["sessions"]
        if session_index < 0 or session_index >= len(sessions):
            return {
                "session_index": session_index,
                "session": None,
                "bundles": [],
                "total_sessions": sessions_payload["total_sessions"],
            }

        target = sessions[session_index]
        cohort_name = self._resolve_cohort_name(cohort_id)
        if not cohort_name:
            return {
                "session_index": session_index,
                "session": target,
                "bundles": [],
                "total_sessions": sessions_payload["total_sessions"],
            }

        clauses, params = self._build_filter_clauses(filters)
        where_extra = (" AND " + " AND ".join(clauses)) if clauses else ""

        with MetadataSessionLocal() as meta_db:
            stacks_sql = f"""
                SELECT scc.series_stack_id,
                       scc.series_instance_uid,
                       scc.directory_type,
                       scc.base,
                       scc.technique,
                       scc.modifier_csv,
                       scc.construct_csv,
                       scc.acceleration_csv,
                       scc.provenance,
                       scc.post_contrast,
                       scc.body_part,
                       scc.spinal_cord,
                       scc.orientation_patient AS orientation,
                       scc.classification_string,
                       scc.manual_review_reasons_csv,
                       scc.slices_count,
                       scc.dwi_b_value,
                       scc.dwi_pe_direction,
                       scc.dwi_n_directions,
                       sf.mr_acquisition_type,
                       ss.stack_echo_time,
                       ss.stack_repetition_time,
                       ss.stack_inversion_time,
                       ss.stack_flip_angle,
                       ss.stack_index,
                       ss.stack_key,
                       s.series_time::text AS series_time
                {self._base_joins()}
                WHERE {self._cohort_clause()}{where_extra}
                  AND subj.subject_code = :session_subject
                  AND st.study_date::text = :session_date
                ORDER BY s.series_time NULLS LAST,
                         ss.stack_index NULLS LAST,
                         scc.series_stack_id
            """
            rows = meta_db.execute(
                text(stacks_sql),
                {
                    "cohort_name": cohort_name,
                    "session_subject": target["subject_code"],
                    "session_date": target["study_date"],
                    **params,
                },
            ).fetchall()

        bundles = self._group_bundles(rows)
        return {
            "session_index": session_index,
            "session": target,
            "bundles": bundles,
            "total_sessions": sessions_payload["total_sessions"],
        }

    # ---- bundle identity & title ---------------------------------------- #

    @staticmethod
    def _norm_csv(value: Optional[str]) -> str:
        if not value:
            return ""
        parts = sorted({p.strip() for p in value.split(",") if p.strip()})
        return ",".join(parts)

    @staticmethod
    def _bundle_key_fields(row) -> tuple:
        """Collision key matching the BIDS exporter minus post_contrast.

        Includes DWI tuple for dwi stacks and the within-series stack_key so
        that echo/TI/FA/IR variants stay together per spec.
        """
        dwi_tuple: tuple
        if (row.directory_type or "") == "dwi":
            dwi_tuple = (row.dwi_b_value, row.dwi_pe_direction, row.dwi_n_directions)
        else:
            dwi_tuple = ()
        return (
            row.body_part or "",
            row.spinal_cord or 0,
            row.orientation or "",
            row.base or "",
            row.mr_acquisition_type or "",
            MainAcquisitionService._norm_csv(row.modifier_csv),
            row.technique or "",
            MainAcquisitionService._norm_csv(row.acceleration_csv),
            MainAcquisitionService._norm_csv(row.construct_csv),
            row.directory_type or "",
            dwi_tuple,
            row.stack_echo_time,
            row.stack_repetition_time,
            row.stack_inversion_time,
            row.stack_flip_angle,
        )

    @staticmethod
    def _bundle_title(row) -> str:
        """Export-style stack name minus the _CE and minus any _N collision suffix.

        Kept deliberately in-sync with ``bids/exporter.py::_build_stack_name``.
        """
        orient = row.orientation or ""
        orient_part = ORIENT_ABBREV.get(orient, orient[:3]) if orient else ""
        acq_type = row.mr_acquisition_type or ""
        mods = (row.modifier_csv or "").replace(",", "-")
        accel = (row.acceleration_csv or "").replace(",", "-")
        construct = (row.construct_csv or "").replace(",", "-")
        parts = [
            p for p in (
                orient_part,
                row.base or "",
                acq_type,
                mods,
                row.technique or "",
                accel,
                construct,
            ) if p
        ]
        body_prefix = BODY_PART_PREFIX.get(row.body_part or "")
        if body_prefix:
            parts.insert(0, body_prefix)
        elif row.spinal_cord and not row.body_part:
            parts.insert(0, "SC")
        name = "_".join(parts) if parts else "unknown"
        if (row.directory_type or "") == "dwi":
            dwi_parts: list[str] = []
            if row.dwi_b_value is not None:
                try:
                    dwi_parts.append(f"b{int(row.dwi_b_value)}")
                except Exception:
                    pass
            if row.dwi_pe_direction:
                dwi_parts.append(row.dwi_pe_direction)
            if row.dwi_n_directions:
                dwi_parts.append(f"{row.dwi_n_directions}dir")
            if dwi_parts:
                name = f"{name}_{'_'.join(dwi_parts)}"
        return name

    @staticmethod
    def _bundle_id(key: tuple) -> str:
        raw = "|".join(str(x) for x in key)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    # ---- grouping ------------------------------------------------------- #

    def _group_bundles(self, rows) -> list[dict]:
        groups: dict[tuple, dict] = {}
        seen_stack_ids: set[int] = set()

        for row in rows:
            key = self._bundle_key_fields(row)
            bundle = groups.get(key)
            if not bundle:
                bundle = {
                    "bundle_id": self._bundle_id(key),
                    "title": self._bundle_title(row),
                    "directory_type": row.directory_type,
                    "base": row.base,
                    "technique": row.technique,
                    "modifier_csv": row.modifier_csv,
                    "orientation": row.orientation,
                    "body_part": row.body_part,
                    "provenance": row.provenance,
                    "acceleration_csv": row.acceleration_csv,
                    "echo_time": row.stack_echo_time,
                    "repetition_time": row.stack_repetition_time,
                    "inversion_time": row.stack_inversion_time,
                    "flip_angle": row.stack_flip_angle,
                    "stacks": [],
                }
                groups[key] = bundle

            sid = int(row.series_stack_id)
            if sid in seen_stack_ids:
                continue
            seen_stack_ids.add(sid)

            is_main = has_token(row.classification_string, MAIN_TOKEN)
            bundle["stacks"].append({
                "series_stack_id": int(row.series_stack_id),
                "series_instance_uid": row.series_instance_uid,
                "stack_index": int(row.stack_index or 0),
                "series_time": row.series_time,
                "series_number": None,
                "slices_count": int(row.slices_count or 0) if row.slices_count is not None else None,
                "post_contrast": int(row.post_contrast) if row.post_contrast is not None else None,
                "is_main": bool(is_main),
                "stack_key": row.stack_key,
                "provenance": row.provenance,
                "echo_time": row.stack_echo_time,
                "repetition_time": row.stack_repetition_time,
                "inversion_time": row.stack_inversion_time,
                "flip_angle": row.stack_flip_angle,
                "thumbnail_url": f"/api/qc/dicom/{row.series_instance_uid}/thumbnail?stack_index={int(row.stack_index or 0)}",
            })

        # Deterministic order + suggested_main per bundle.
        out: list[dict] = []
        for bundle in groups.values():
            stacks = bundle["stacks"]
            stacks.sort(key=lambda s: (
                s.get("series_time") or "",
                s.get("stack_index") or 0,
                s.get("series_stack_id"),
            ))
            bundle["suggested_main_stack_id"] = self._suggest_main(stacks)
            out.append(bundle)

        # Stable bundle ordering: by title.
        out.sort(key=lambda b: b["title"].lower())
        return out

    @staticmethod
    def _suggest_main(stacks: list[dict]) -> Optional[int]:
        """Return the stack id most likely to be the main acquisition.

        Rule: latest ``series_time`` wins; largest ``slices_count`` breaks ties.
        If both are tied across stacks, no main is suggested.
        """
        if not stacks:
            return None

        def sort_key(s: dict) -> tuple:
            return (s.get("series_time") or "", s.get("slices_count") or 0)

        ordered = sorted(stacks, key=sort_key, reverse=True)
        top = ordered[0]
        if len(ordered) > 1:
            runner = ordered[1]
            if sort_key(top) == sort_key(runner):
                return None
        return top["series_stack_id"]

    # ---- role mutation -------------------------------------------------- #

    def set_stack_role(
        self,
        series_stack_id: int,
        role: Literal["main", "pre", "post"],
        value: bool,
    ) -> dict:
        """Apply a role change for a single stack.

        - role=main, value=True -> set ``main_acquisition`` on this stack and
          remove it from all sibling stacks in the same bundle.
        - role=main, value=False -> remove ``main_acquisition`` on this stack.
        - role=pre, value=True   -> post_contrast=0, drop ``contrast:*`` reasons.
        - role=pre, value=False  -> if currently 0, set NULL (matches UI toggle).
        - role=post, value=True  -> post_contrast=1, drop ``contrast:*`` reasons.
        - role=post, value=False -> if currently 1, set NULL.
        """
        with MetadataSessionLocal() as meta_db:
            row = meta_db.execute(
                text(
                    """
                    SELECT scc.series_stack_id,
                           scc.series_instance_uid,
                           scc.classification_string,
                           scc.manual_review_reasons_csv,
                           scc.manual_review_required,
                           scc.post_contrast,
                           scc.directory_type,
                           scc.base,
                           scc.technique,
                           scc.modifier_csv,
                           scc.construct_csv,
                           scc.acceleration_csv,
                           scc.body_part,
                           scc.spinal_cord,
                           scc.orientation_patient AS orientation,
                           scc.dwi_b_value,
                           scc.dwi_pe_direction,
                           scc.dwi_n_directions,
                           scc.subject_id,
                           scc.study_id,
                           scc.dicom_origin_cohort,
                           sf.mr_acquisition_type
                    FROM series_classification_cache scc
                    LEFT JOIN stack_fingerprint sf ON scc.series_stack_id = sf.series_stack_id
                    WHERE scc.series_stack_id = :sid
                    """
                ),
                {"sid": series_stack_id},
            ).fetchone()
            if not row:
                raise ValueError(f"series_stack_id {series_stack_id} not found")

            # Invalidate the session-list cache for this cohort. The session
            # listing itself doesn't change with role toggles, but stack-level
            # state (post_contrast / main flag) feeds the bundles view, so we
            # err on the side of invalidating; this is cheap.
            self._invalidate_cohort_cache_by_name(row.dicom_origin_cohort)

            cleared_main_stack_ids: list[int] = []

            if role == "main":
                if value:
                    # Find siblings in the same bundle (same session + same key)
                    # that currently carry main_acquisition and clear them.
                    siblings = self._find_siblings(meta_db, row)
                    for sib in siblings:
                        if sib.series_stack_id == series_stack_id:
                            continue
                        if has_token(sib.classification_string, MAIN_TOKEN):
                            new_cs = remove_token(sib.classification_string, MAIN_TOKEN)
                            meta_db.execute(
                                text(
                                    "UPDATE series_classification_cache "
                                    "SET classification_string = :cs "
                                    "WHERE series_stack_id = :sid"
                                ),
                                {"cs": new_cs, "sid": sib.series_stack_id},
                            )
                            cleared_main_stack_ids.append(int(sib.series_stack_id))
                    new_cs = add_token(row.classification_string, MAIN_TOKEN)
                else:
                    new_cs = remove_token(row.classification_string, MAIN_TOKEN)

                meta_db.execute(
                    text(
                        "UPDATE series_classification_cache "
                        "SET classification_string = :cs "
                        "WHERE series_stack_id = :sid"
                    ),
                    {"cs": new_cs, "sid": series_stack_id},
                )
                meta_db.commit()
                return {
                    "series_stack_id": series_stack_id,
                    "is_main": bool(value),
                    "cleared_main_stack_ids": cleared_main_stack_ids,
                    "post_contrast": row.post_contrast,
                }

            # pre/post -> mutate post_contrast (tri-state toggle)
            current = row.post_contrast
            if role == "pre":
                new_pc = 0 if value else (None if current == 0 else current)
            elif role == "post":
                new_pc = 1 if value else (None if current == 1 else current)
            else:
                raise ValueError(f"unknown role {role!r}")

            new_reasons = row.manual_review_reasons_csv
            new_required = row.manual_review_required
            if new_pc is not None:
                new_reasons = remove_prefix(new_reasons, "contrast:")
                if not new_reasons:
                    new_required = 0

            meta_db.execute(
                text(
                    """
                    UPDATE series_classification_cache
                    SET post_contrast = :pc,
                        manual_review_reasons_csv = :reasons,
                        manual_review_required = :req
                    WHERE series_stack_id = :sid
                    """
                ),
                {
                    "pc": new_pc,
                    "reasons": new_reasons,
                    "req": new_required,
                    "sid": series_stack_id,
                },
            )
            meta_db.commit()

            return {
                "series_stack_id": series_stack_id,
                "post_contrast": new_pc,
                "cleared_main_stack_ids": [],
                "is_main": has_token(row.classification_string, MAIN_TOKEN),
            }

    # ---- identifier types ----------------------------------------------- #

    def get_id_types_for_cohort(self, cohort_id: int) -> list[str]:
        """Return the distinct identifier type names available for a cohort.

        Always starts with ``"code"`` (the canonical subject_code).
        """
        cohort_name = self._resolve_cohort_name(cohort_id)
        if not cohort_name:
            return ["code"]

        with MetadataSessionLocal() as meta_db:
            rows = meta_db.execute(
                text("""
                    SELECT DISTINCT it.id_type_name
                    FROM id_types it
                    JOIN subject_other_identifiers soi ON it.id_type_id = soi.id_type_id
                    JOIN subject_cohorts sc ON soi.subject_id = sc.subject_id
                    JOIN cohort c ON sc.cohort_id = c.cohort_id
                    WHERE LOWER(c.name) = LOWER(:cohort_name)
                    ORDER BY it.id_type_name
                """),
                {"cohort_name": cohort_name},
            ).fetchall()
        return ["code"] + [r.id_type_name for r in rows]

    # ---- subject resolution (canonical + alternative identifiers) ------- #

    def resolve_subject_identifier(self, identifier: str) -> Optional[str]:
        """Return the canonical ``subject_code`` for any identifier.

        Matches (case-insensitive) against:
        - ``subject.subject_code``
        - ``subject_other_identifiers.other_identifier`` (across all id_types)
        """
        ident = (identifier or "").strip()
        if not ident:
            return None
        with MetadataSessionLocal() as meta_db:
            row = meta_db.execute(
                text(
                    """
                    SELECT subject_code
                    FROM subject
                    WHERE LOWER(subject_code) = LOWER(:v)
                    LIMIT 1
                    """
                ),
                {"v": ident},
            ).fetchone()
            if row:
                return row.subject_code

            row = meta_db.execute(
                text(
                    """
                    SELECT subj.subject_code
                    FROM subject_other_identifiers soi
                    JOIN subject subj ON soi.subject_id = subj.subject_id
                    WHERE LOWER(soi.other_identifier) = LOWER(:v)
                    LIMIT 1
                    """
                ),
                {"v": ident},
            ).fetchone()
            return row.subject_code if row else None

    # ---- sibling lookup for one-main-per-bundle enforcement ------------- #

    def _find_siblings(self, meta_db, row) -> list:
        """Return all stacks in the same bundle as ``row``."""
        dt = row.directory_type or ""
        dwi_clauses: list[str] = []
        params: dict = {
            "subject_id": row.subject_id,
            "study_id": row.study_id,
            "body_part": row.body_part or "",
            "spinal_cord": row.spinal_cord or 0,
            "orientation": row.orientation or "",
            "base": row.base or "",
            "acq_type": row.mr_acquisition_type or "",
            "technique": row.technique or "",
            "directory_type": dt,
            "modifier_norm": self._norm_csv(row.modifier_csv),
            "accel_norm": self._norm_csv(row.acceleration_csv),
            "construct_norm": self._norm_csv(row.construct_csv),
        }
        if dt == "dwi":
            dwi_clauses = [
                "scc.dwi_b_value IS NOT DISTINCT FROM :dwi_b",
                "scc.dwi_pe_direction IS NOT DISTINCT FROM :dwi_pe",
                "scc.dwi_n_directions IS NOT DISTINCT FROM :dwi_n",
            ]
            params.update({
                "dwi_b": row.dwi_b_value,
                "dwi_pe": row.dwi_pe_direction,
                "dwi_n": row.dwi_n_directions,
            })
        dwi_where = (" AND " + " AND ".join(dwi_clauses)) if dwi_clauses else ""

        sql = f"""
            SELECT scc.series_stack_id,
                   scc.classification_string
            FROM series_classification_cache scc
            JOIN series s ON scc.series_instance_uid = s.series_instance_uid
            LEFT JOIN stack_fingerprint sf ON scc.series_stack_id = sf.series_stack_id
            WHERE s.subject_id = :subject_id
              AND s.study_id = :study_id
              AND COALESCE(scc.body_part, '') = :body_part
              AND COALESCE(scc.spinal_cord, 0) = :spinal_cord
              AND COALESCE(scc.orientation_patient, '') = :orientation
              AND COALESCE(scc.base, '') = :base
              AND COALESCE(sf.mr_acquisition_type, '') = :acq_type
              AND COALESCE(scc.technique, '') = :technique
              AND COALESCE(scc.directory_type, '') = :directory_type
              AND COALESCE(
                  (SELECT string_agg(tok, ',' ORDER BY tok)
                   FROM unnest(string_to_array(COALESCE(scc.modifier_csv, ''), ',')) AS tok
                   WHERE btrim(tok) <> ''),
                  ''
              ) = :modifier_norm
              AND COALESCE(
                  (SELECT string_agg(tok, ',' ORDER BY tok)
                   FROM unnest(string_to_array(COALESCE(scc.acceleration_csv, ''), ',')) AS tok
                   WHERE btrim(tok) <> ''),
                  ''
              ) = :accel_norm
              AND COALESCE(
                  (SELECT string_agg(tok, ',' ORDER BY tok)
                   FROM unnest(string_to_array(COALESCE(scc.construct_csv, ''), ',')) AS tok
                   WHERE btrim(tok) <> ''),
                  ''
              ) = :construct_norm
              {dwi_where}
        """
        return list(meta_db.execute(text(sql), params).fetchall())


main_acquisition_service = MainAcquisitionService()
