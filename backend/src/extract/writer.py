"""Database writer for extraction batches."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from contextlib import AbstractAsyncContextManager
from typing import Awaitable, Callable, Optional

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

from metadata_db import bootstrap
from metadata_db.schema import (
    Cohort,
    CTSeriesDetails,
    IngestConflict,
    Instance,
    MRISeriesDetails,
    PETSeriesDetails,
    Series,
    SeriesStack,
    Study,
    Subject,
    SubjectCohort,
    SubjectOtherIdentifier,
)

from .stack_utils import compute_stack_signature, build_stack_row, signature_from_stack_record
from .dicom_mappings import STACK_DEFINING_FIELDS
from metadata_db.session import SessionLocal

from .batching import BatchSizeController
from .config import DuplicatePolicy, ExtractionConfig
from .limits import build_parameter_chunk_plan, calculate_safe_instance_batch_rows
from .resume_index import ExistingPathIndex, split_subject_relative
from .profiler import get_global_profiler
from .subject_mapping import subject_code_gen
from .worker import InstancePayload, normalize_modality
from jobs.control import JobControl


logger = logging.getLogger(__name__)


ProgressCallback = Callable[[int, int], Awaitable[None] | None]
_CONTROL_POLL_SECONDS = 0.5


class Writer(AbstractAsyncContextManager["Writer"]):
    def __init__(
        self,
        *,
        config: ExtractionConfig,
        queue: asyncio.Queue,
        job_id: Optional[int],
        progress_cb: Optional[ProgressCallback],
        batch_controller: BatchSizeController,
        control: Optional[JobControl] = None,
        path_index: Optional[ExistingPathIndex] = None,
    ) -> None:
        self.config = config
        self.queue = queue
        self.job_id = job_id
        self.progress_cb = progress_cb
        self._batch_controller = batch_controller
        self._control = control
        self._path_index = path_index
        self._session = None
        self._cohort_id: Optional[int] = None
        self._normalized_cohort_name = (config.cohort_name or "").strip().lower()
        self._subject_cache: dict[tuple[str, str], int] = {}
        self._study_cache: dict[str, int] = {}
        self._series_cache: dict[str, int] = {}
        # Stack caches: keyed by series_instance_uid for stable lookups
        self._stack_cache: dict[tuple, int] = {}  # signature -> series_stack_id
        self._series_stack_counter: dict[str, int] = {}  # series_instance_uid -> next stack_index
        # Reverse lookup: series_id -> series_instance_uid (for reconstructing signatures from DB)
        self._series_id_to_uid: dict[int, str] = {}
        self._subject_identifier_cache: set[int] = set()
        self._modality_fallback_logged: set[str] = set()
        self._subject_id_type_id = config.subject_id_type_id
        bootstrap()
        self._reported_safe_batch_rows = calculate_safe_instance_batch_rows()
        self._subjects_inserted = 0
        self._studies_inserted = 0
        self._series_inserted = 0
        self._stacks_inserted = 0
        self._instances_inserted = 0

    async def __aenter__(self) -> "Writer":
        self._session = SessionLocal()
        self._cohort_id = self._ensure_cohort(self._session)
        # Commit the cohort creation/lookup to release any row locks
        # This allows multiple writers to initialize concurrently
        self._session.commit()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        if self._session is not None:
            if exc:
                self._session.rollback()
            self._session.close()

    async def consume(self, total_subjects: int) -> None:
        assert self._session is not None
        session = self._session
        profiler = get_global_profiler()
        
        while True:
            await self._checkpoint()
            queue_start = time.perf_counter()
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=_CONTROL_POLL_SECONDS)
                if profiler:
                    profiler.record("queue_get", time.perf_counter() - queue_start)
            except asyncio.TimeoutError:
                continue
            if item is None:
                break
            subject_key, series_uid, batch, last_instance, completed = item
            if batch:
                await self._checkpoint()
                start = time.perf_counter()
                try:
                    self._write_batch(session, batch)
                except SQLAlchemyError as exc:
                    sample = batch[0]
                    logger.exception(
                        "DB write failed job_id=%s subject=%s series=%s modality=%s file=%s",
                        self.job_id,
                        sample.subject_key,
                        sample.series_uid,
                        sample.modality,
                        sample.file_path,
                    )
                    raise
                self._update_path_index(batch)
                write_duration = time.perf_counter() - start
                
                commit_start = time.perf_counter()
                session.commit()
                commit_duration = time.perf_counter() - commit_start
                
                total_duration = time.perf_counter() - start
                self._batch_controller.record(len(batch), total_duration)
                
                if profiler:
                    profiler.record("db_write_batch", write_duration)
                    profiler.record("db_commit", commit_duration)
            if completed:
                await self._checkpoint()
                commit_start = time.perf_counter()
                session.commit()
                if profiler:
                    profiler.record("db_commit_final", time.perf_counter() - commit_start)

    def _write_batch(self, session, batch: list[InstancePayload]) -> None:
        if not batch:
            return
        subject_ids = self._bulk_ensure_subjects(session, batch)
        study_ids = self._bulk_ensure_studies(session, batch, subject_ids)
        series_ids = self._bulk_ensure_series(session, batch, subject_ids, study_ids)
        stack_ids = self._bulk_ensure_stacks(session, batch, series_ids)
        self._bulk_ensure_instances(session, batch, series_ids, stack_ids)

    def _update_path_index(self, batch: list[InstancePayload]) -> None:
        if not self._path_index:
            return
        for payload in batch:
            subject_key, subject_relative = split_subject_relative(payload.file_path)
            if subject_key:
                self._path_index.add(subject_key, subject_relative)

    def _ensure_cohort(self, session) -> int:
        desired_path = str(self.config.raw_root)
        # Use case-insensitive lookup to match existing cohorts regardless of stored case
        stmt = select(Cohort).where(func.lower(Cohort.name) == self._normalized_cohort_name)
        cohort = session.execute(stmt).scalar_one_or_none()
        if cohort:
            if getattr(cohort, "path", None) != desired_path:
                session.execute(
                    update(Cohort)
                    .where(Cohort.cohort_id == cohort.cohort_id)
                    .values(path=desired_path)
                )
                session.flush()
            return cohort.cohort_id

        insert_stmt = insert(Cohort).values(
            name=self._normalized_cohort_name,
            owner="system",
            path=desired_path,
        )
        try:
            result = session.execute(insert_stmt.returning(Cohort.cohort_id))
            cohort_id = result.scalar_one()
            session.flush()
            return cohort_id
        except IntegrityError:
            session.rollback()
            # Re-query with case-insensitive lookup in case of race condition
            cohort = session.execute(stmt).scalar_one_or_none()
            if cohort is None:
                # Fallback: try direct lookup if case-insensitive didn't work
                fallback_stmt = select(Cohort).where(Cohort.name == self._normalized_cohort_name)
                cohort = session.execute(fallback_stmt).scalar_one_or_none()
            if cohort is None:
                raise RuntimeError(f"Failed to find or create cohort '{self._normalized_cohort_name}'")
            if getattr(cohort, "path", None) != desired_path:
                session.execute(
                    update(Cohort)
                    .where(Cohort.cohort_id == cohort.cohort_id)
                    .values(path=desired_path)
                )
                session.flush()
            return cohort.cohort_id

    def _bulk_ensure_subjects(self, session, batch: list[InstancePayload]) -> list[int]:
        subject_ids: list[int] = [0] * len(batch)
        pending: dict[tuple[str, str], dict] = {}
        representative_payloads: dict[int, InstancePayload] = {}

        for idx, payload in enumerate(batch):
            if not payload.patient_id:
                raise ValueError("PatientID is required for subject ingestion")
            cache_key = (payload.patient_id, self._normalized_cohort_name)
            cached = self._subject_cache.get(cache_key)
            if cached is not None:
                subject_ids[idx] = cached
                representative_payloads.setdefault(cached, payload)
                continue
            entry = pending.setdefault(cache_key, {"indices": [], "payload": payload})
            entry["indices"].append(idx)

        # Existing mappings via subject_other_identifiers
        if pending and self._subject_id_type_id is not None:
            patient_ids = [key[0] for key in pending.keys()]
            identifier_stmt = (
                select(SubjectOtherIdentifier.other_identifier, SubjectOtherIdentifier.subject_id)
                .where(SubjectOtherIdentifier.id_type_id == self._subject_id_type_id)
                .where(SubjectOtherIdentifier.other_identifier.in_(patient_ids))
            )
            identifier_rows = session.execute(identifier_stmt).all()
            if identifier_rows:
                for row in identifier_rows:
                    cache_key = (row.other_identifier, self._normalized_cohort_name)
                    if cache_key not in pending:
                        continue
                    subject_id = row.subject_id
                    subject_ids_for_entry = pending.pop(cache_key)
                    for idx in subject_ids_for_entry["indices"]:
                        subject_ids[idx] = subject_id
                    self._subject_cache[cache_key] = subject_id
                    representative_payloads.setdefault(subject_id, subject_ids_for_entry["payload"])

        if pending:
            insert_rows = []
            code_to_key: dict[str, tuple[str, str]] = {}
            for cache_key, entry in pending.items():
                payload = entry["payload"]
                subject_code = payload.subject_code or subject_code_gen(payload.patient_id, self._normalized_cohort_name)
                code_to_key[subject_code] = cache_key
                insert_rows.append(
                    {
                        "subject_code": subject_code,
                        "patient_name": payload.patient_name,
                        "patient_sex": None,
                        "patient_birth_date": None,
                    }
                )

            inserted = (
                session.execute(
                    insert(Subject)
                    .values(insert_rows)
                    .on_conflict_do_nothing()
                    .returning(Subject.subject_code, Subject.subject_id)
                )
                .mappings()
                .all()
            )
            self._subjects_inserted += len(inserted)
            code_to_id = {row["subject_code"]: row["subject_id"] for row in inserted}
            missing_codes = [code for code in code_to_key.keys() if code not in code_to_id]
            if missing_codes:
                existing = (
                    session.execute(
                        select(Subject.subject_code, Subject.subject_id).where(Subject.subject_code.in_(missing_codes))
                    )
                    .mappings()
                    .all()
                )
                for row in existing:
                    code_to_id[row["subject_code"]] = row["subject_id"]

            for code, subject_id in code_to_id.items():
                cache_key = code_to_key[code]
                entry = pending.pop(cache_key, None)
                if not entry:
                    continue
                for idx in entry["indices"]:
                    subject_ids[idx] = subject_id
                self._subject_cache[cache_key] = subject_id
                representative_payloads.setdefault(subject_id, entry["payload"])

        resolved_ids = {sid for sid in subject_ids if sid}
        if not resolved_ids:
            raise RuntimeError("Failed to resolve any subjects in batch")

        if self._cohort_id is not None:
            rows = [
                {"subject_id": sid, "cohort_id": self._cohort_id}
                for sid in resolved_ids
            ]
            insert_stmt = insert(SubjectCohort).values(rows)
            bind = session.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""

            if dialect_name in {"postgresql", "sqlite"}:
                insert_stmt = insert_stmt.on_conflict_do_nothing(
                    index_elements=[SubjectCohort.subject_id, SubjectCohort.cohort_id]
                )
                session.execute(insert_stmt)
            else:
                existing_pairs = {
                    (row.subject_id, row.cohort_id)
                    for row in session.execute(
                        select(SubjectCohort.subject_id, SubjectCohort.cohort_id)
                        .where(SubjectCohort.cohort_id == self._cohort_id)
                        .where(SubjectCohort.subject_id.in_(resolved_ids))
                    )
                }
                pending_rows = [
                    row for row in rows if (row["subject_id"], row["cohort_id"]) not in existing_pairs
                ]
                if pending_rows:
                    session.execute(insert(SubjectCohort).values(pending_rows))

        if self._subject_id_type_id is not None:
            for sid, payload in representative_payloads.items():
                if sid in self._subject_identifier_cache:
                    continue
                self._ensure_subject_identifier(session, sid, payload)
                self._subject_identifier_cache.add(sid)

        return subject_ids

    def _ensure_subject_identifier(self, session, subject_id: int, payload: InstancePayload) -> None:
        if self._subject_id_type_id is None or not payload.patient_id:
            return

        stmt = select(SubjectOtherIdentifier).where(
            SubjectOtherIdentifier.subject_id == subject_id,
            SubjectOtherIdentifier.id_type_id == self._subject_id_type_id,
        )
        existing = session.execute(stmt).scalar_one_or_none()
        if existing:
            current_value = getattr(existing, "other_identifier", None)
            if current_value != payload.patient_id:
                self._log_conflict(
                    session,
                    "subject_identifier",
                    payload.subject_code,
                    "Conflicting patient identifier for subject",
                    payload.file_path,
                )
                session.execute(
                    update(SubjectOtherIdentifier)
                    .where(SubjectOtherIdentifier.subject_other_identifier_id == existing.subject_other_identifier_id)
                    .values(other_identifier=payload.patient_id)
                )
            return

        insert_stmt = (
            insert(SubjectOtherIdentifier)
            .values(
                subject_id=subject_id,
                id_type_id=self._subject_id_type_id,
                other_identifier=payload.patient_id,
            )
            .on_conflict_do_nothing()
        )
        session.execute(insert_stmt)

    def _bulk_ensure_studies(
        self,
        session,
        batch: list[InstancePayload],
        subject_ids: list[int],
    ) -> list[int]:
        study_ids: list[int] = [0] * len(batch)
        pending: dict[str, dict] = {}

        for idx, payload in enumerate(batch):
            cached = self._study_cache.get(payload.study_uid)
            if cached:
                study_ids[idx] = cached
                continue
            entry = pending.setdefault(
                payload.study_uid,
                {"indices": [], "payload": payload, "subject_id": subject_ids[idx]},
            )
            entry["indices"].append(idx)

        if pending:
            stmt = select(Study).where(Study.study_instance_uid.in_(pending.keys()))
            for existing in session.execute(stmt).scalars():
                entry = pending.pop(existing.study_instance_uid, None)
                if entry is None:
                    continue
                study_id = existing.study_id
                if existing.subject_id != entry["subject_id"]:
                    self._log_conflict(
                        session,
                        "study",
                        existing.study_instance_uid,
                        "Study assigned to a different subject; re-linking based on DICOM tags",
                        entry["payload"].file_path,
                    )
                    session.execute(
                        update(Study)
                        .where(Study.study_id == study_id)
                        .values(subject_id=entry["subject_id"])
                    )
                    session.flush()
                self._study_cache[existing.study_instance_uid] = study_id
                for idx in entry["indices"]:
                    study_ids[idx] = study_id

        if pending:
            rows = []
            for uid, entry in pending.items():
                values = {"study_instance_uid": uid, "subject_id": entry["subject_id"]}
                values.update(entry["payload"].study_fields)
                rows.append(values)
            inserted = (
                session.execute(
                    insert(Study)
                    .values(rows)
                    .on_conflict_do_nothing()
                    .returning(Study.study_instance_uid, Study.study_id)
                )
                .mappings()
                .all()
            )
            self._studies_inserted += len(inserted)
            inserted_map = {row["study_instance_uid"]: row["study_id"] for row in inserted}
            remaining = [uid for uid in pending.keys() if uid not in inserted_map]
            if remaining:
                existing_rows = (
                    session.execute(
                        select(Study.study_instance_uid, Study.study_id).where(Study.study_instance_uid.in_(remaining))
                    )
                    .mappings()
                    .all()
                )
                for row in existing_rows:
                    inserted_map[row["study_instance_uid"]] = row["study_id"]
            for uid, entry in pending.items():
                study_id = inserted_map[uid]
                self._study_cache[uid] = study_id
                for idx in entry["indices"]:
                    study_ids[idx] = study_id

        if any(study_id == 0 for study_id in study_ids):
            raise RuntimeError("Failed to resolve study identifiers for batch")

        return study_ids

    def _bulk_ensure_series(
        self,
        session,
        batch: list[InstancePayload],
        subject_ids: list[int],
        study_ids: list[int],
    ) -> list[int]:
        series_ids: list[int] = [0] * len(batch)
        pending: dict[str, dict] = {}

        for idx, payload in enumerate(batch):
            cached = self._series_cache.get(payload.series_uid)
            if cached:
                series_ids[idx] = cached
                continue
            entry = pending.setdefault(
                payload.series_uid,
                {
                    "indices": [],
                    "payload": payload,
                    "subject_id": subject_ids[idx],
                    "study_id": study_ids[idx],
                },
            )
            entry["indices"].append(idx)

        if pending:
            stmt = select(Series).where(Series.series_instance_uid.in_(pending.keys()))
            for existing in session.execute(stmt).scalars():
                entry = pending.pop(existing.series_instance_uid, None)
                if entry is None:
                    continue
                series_id = existing.series_id
                updates = {}
                if existing.subject_id != entry["subject_id"]:
                    updates["subject_id"] = entry["subject_id"]
                if existing.study_id != entry["study_id"]:
                    updates["study_id"] = entry["study_id"]
                if updates:
                    self._log_conflict(
                        session,
                        "series",
                        existing.series_instance_uid,
                        "Series metadata linked to a different subject/study; re-linking",
                        entry["payload"].file_path,
                    )
                    session.execute(update(Series).where(Series.series_id == series_id).values(**updates))
                    session.flush()
                self._series_cache[existing.series_instance_uid] = series_id
                self._series_id_to_uid[series_id] = existing.series_instance_uid
                for idx in entry["indices"]:
                    series_ids[idx] = series_id

        if pending:
            rows = []
            for uid, entry in pending.items():
                payload = entry["payload"]
                values = {
                    "series_instance_uid": uid,
                    "modality": self._resolve_series_modality(payload),
                    "study_id": entry["study_id"],
                    "subject_id": entry["subject_id"],
                }
                for key, value in payload.series_fields.items():
                    if key == "modality":
                        continue
                    values[key] = value
                rows.append(values)
            inserted = (
                session.execute(
                    insert(Series)
                    .values(rows)
                    .on_conflict_do_nothing()
                    .returning(Series.series_instance_uid, Series.series_id)
                )
                .mappings()
                .all()
            )
            self._series_inserted += len(inserted)
            inserted_map = {row["series_instance_uid"]: row["series_id"] for row in inserted}
            remaining = [uid for uid in pending.keys() if uid not in inserted_map]
            if remaining:
                existing_rows = (
                    session.execute(
                        select(Series.series_instance_uid, Series.series_id).where(Series.series_instance_uid.in_(remaining))
                    )
                    .mappings()
                    .all()
                )
                for row in existing_rows:
                    inserted_map[row["series_instance_uid"]] = row["series_id"]
            for uid, entry in pending.items():
                series_id = inserted_map[uid]
                self._series_cache[uid] = series_id
                self._series_id_to_uid[series_id] = uid
                for idx in entry["indices"]:
                    series_ids[idx] = series_id

        if any(series_id == 0 for series_id in series_ids):
            raise RuntimeError("Failed to resolve series identifiers for batch")

        # Update modality-specific detail tables per unique series
        unique_series_payloads = {}
        for idx, payload in enumerate(batch):
            series_id = series_ids[idx]
            if series_id not in unique_series_payloads:
                unique_series_payloads[series_id] = payload
        for series_id, payload in unique_series_payloads.items():
            self._upsert_modality_details(session, series_id, payload)

        return series_ids

    def _resolve_series_modality(self, payload: InstancePayload) -> str:
        normalized = normalize_modality(payload.modality) or normalize_modality(payload.series_fields.get("modality"))
        if normalized:
            return normalized
        if payload.series_uid not in self._modality_fallback_logged:
            logger.warning(
                "Missing modality for series %s; defaulting to OT (file=%s)",
                payload.series_uid,
                payload.file_path,
            )
            self._modality_fallback_logged.add(payload.series_uid)
        return "OT"

    def _bulk_ensure_stacks(
        self,
        session,
        batch: list[InstancePayload],
        series_ids: list[int],
    ) -> list[int]:
        """Ensure series_stack records exist for each instance in the batch.
        
        Uses the same Cache → Bulk Query → Bulk Insert pattern as other bulk_ensure methods.
        Stack signatures are keyed by series_instance_uid (stable, available from payload).
        
        Args:
            session: Database session
            batch: List of InstancePayload objects
            series_ids: Corresponding series_id for each payload
            
        Returns:
            List of series_stack_id for each instance in batch
        """
        stack_ids: list[int] = [0] * len(batch)
        pending: dict[tuple, dict] = {}
        
        # 1. Check cache first
        for idx, (payload, series_id) in enumerate(zip(batch, series_ids)):
            sig = compute_stack_signature(payload.series_uid, payload.instance_fields)
            
            cached = self._stack_cache.get(sig)
            if cached:
                stack_ids[idx] = cached
                continue
            
            entry = pending.setdefault(sig, {
                "indices": [],
                "series_id": series_id,
                "series_uid": payload.series_uid,
                "payload": payload,
            })
            entry["indices"].append(idx)
        
        # 2. Bulk query existing stacks for series in this batch
        if pending:
            series_ids_to_check = {entry["series_id"] for entry in pending.values()}
            stmt = select(SeriesStack).where(SeriesStack.series_id.in_(series_ids_to_check))
            
            for existing in session.execute(stmt).scalars():
                # Reconstruct signature from DB record
                series_uid = self._series_id_to_uid.get(existing.series_id)
                if not series_uid:
                    continue
                
                db_sig = signature_from_stack_record(
                    series_uid,
                    existing.stack_echo_time,
                    existing.stack_inversion_time,
                    existing.stack_echo_numbers,
                    existing.stack_echo_train_length,
                    existing.stack_repetition_time,
                    existing.stack_flip_angle,
                    existing.stack_receive_coil_name,
                    existing.stack_xray_exposure,
                    existing.stack_kvp,
                    existing.stack_tube_current,
                    existing.stack_pet_bed_index,
                    existing.stack_pet_frame_type,
                    existing.stack_image_orientation,
                    existing.stack_image_type,
                )
                
                if db_sig in pending:
                    self._stack_cache[db_sig] = existing.series_stack_id
                    # Also update the counter if we found a higher index
                    current_max = self._series_stack_counter.get(series_uid, 0)
                    if existing.stack_index >= current_max:
                        self._series_stack_counter[series_uid] = existing.stack_index + 1
                    
                    for idx in pending[db_sig]["indices"]:
                        stack_ids[idx] = existing.series_stack_id
                    del pending[db_sig]
        
        # 3. Bulk insert new stacks
        if pending:
            rows = []
            sig_to_stack_key: dict[tuple, tuple[int, int]] = {}  # sig -> (series_id, stack_index)
            
            for sig, entry in pending.items():
                series_uid = entry["series_uid"]
                series_id = entry["series_id"]
                payload = entry["payload"]
                
                # Get next stack_index for this series
                stack_index = self._series_stack_counter.get(series_uid, 0)
                self._series_stack_counter[series_uid] = stack_index + 1
                
                row = build_stack_row(
                    series_id=series_id,
                    stack_index=stack_index,
                    modality=payload.modality,
                    instance_fields=payload.instance_fields,
                )
                rows.append(row)
                sig_to_stack_key[sig] = (series_id, stack_index)
            
            # Insert with ON CONFLICT DO NOTHING, return IDs
            inserted = (
                session.execute(
                    insert(SeriesStack)
                    .values(rows)
                    .on_conflict_do_nothing()
                    .returning(SeriesStack.series_stack_id, SeriesStack.series_id, SeriesStack.stack_index)
                )
                .mappings()
                .all()
            )
            self._stacks_inserted += len(inserted)
            
            # Build lookup: (series_id, stack_index) -> series_stack_id
            inserted_lookup = {
                (row["series_id"], row["stack_index"]): row["series_stack_id"]
                for row in inserted
            }
            
            # Map inserted stacks to cache and stack_ids
            for sig, (series_id, stack_index) in sig_to_stack_key.items():
                stack_id = inserted_lookup.get((series_id, stack_index))
                if stack_id:
                    self._stack_cache[sig] = stack_id
                    for idx in pending[sig]["indices"]:
                        stack_ids[idx] = stack_id
            
            # Handle entries that weren't inserted (conflict) - query them
            remaining_sigs = [sig for sig in pending if sig not in self._stack_cache]
            if remaining_sigs:
                # Re-query to get IDs for stacks that hit conflict
                remaining_series_ids = {pending[sig]["series_id"] for sig in remaining_sigs}
                stmt = select(SeriesStack).where(SeriesStack.series_id.in_(remaining_series_ids))
                
                for existing in session.execute(stmt).scalars():
                    series_uid = self._series_id_to_uid.get(existing.series_id)
                    if not series_uid:
                        continue
                    
                    db_sig = signature_from_stack_record(
                        series_uid,
                        existing.stack_echo_time,
                        existing.stack_inversion_time,
                        existing.stack_echo_numbers,
                        existing.stack_echo_train_length,
                        existing.stack_repetition_time,
                        existing.stack_flip_angle,
                        existing.stack_receive_coil_name,
                        existing.stack_xray_exposure,
                        existing.stack_kvp,
                        existing.stack_tube_current,
                        existing.stack_pet_bed_index,
                        existing.stack_pet_frame_type,
                        existing.stack_image_orientation,
                        existing.stack_image_type,
                    )
                    
                    if db_sig in pending and db_sig not in self._stack_cache:
                        self._stack_cache[db_sig] = existing.series_stack_id
                        for idx in pending[db_sig]["indices"]:
                            stack_ids[idx] = existing.series_stack_id
        
        if any(stack_id == 0 for stack_id in stack_ids):
            # Log which ones failed
            failed_indices = [i for i, sid in enumerate(stack_ids) if sid == 0]
            if failed_indices:
                sample = batch[failed_indices[0]]
                logger.error(
                    "Failed to resolve stack for %d instances. Sample: series=%s, file=%s",
                    len(failed_indices),
                    sample.series_uid,
                    sample.file_path,
                )
            raise RuntimeError("Failed to resolve stack identifiers for batch")
        
        return stack_ids

    def _bulk_ensure_instances(self, session, batch: list[InstancePayload], series_ids: list[int], stack_ids: list[int]) -> None:
        rows = []
        for payload, series_id, stack_id in zip(batch, series_ids, stack_ids):
            values = {
                "series_id": series_id,
                "series_stack_id": stack_id,  # Set FK at insert time!
                "series_instance_uid": payload.series_uid,
                "sop_instance_uid": payload.sop_uid,
                "dicom_file_path": payload.file_path,
            }
            # Add instance fields but exclude stack-defining fields (stored in series_stack only)
            for key, value in payload.instance_fields.items():
                if key not in STACK_DEFINING_FIELDS:
                    values[key] = value
            rows.append(values)

        if not rows:
            return
        chunks, params_per_row, chunk_limit = build_parameter_chunk_plan(rows)
        if len(chunks) > 1:
            logger.info(
                "Splitting instance batch of %d rows into %d chunks (<= %d rows, %d params/row) to satisfy PostgreSQL limits",
                len(rows),
                len(chunks),
                chunk_limit,
                params_per_row,
            )

        for chunk in chunks:
            stmt = insert(Instance).values(chunk)
            if self.config.duplicate_policy == DuplicatePolicy.OVERWRITE:
                excluded = stmt.excluded
                update_values = {column: getattr(excluded, column) for column in chunk[0].keys()}
                stmt = stmt.on_conflict_do_update(
                    index_elements=[Instance.sop_instance_uid],
                    set_=update_values,
                )
                session.execute(stmt)
            else:
                stmt = stmt.on_conflict_do_nothing().returning(Instance.sop_instance_uid)
                inserted = {row[0] for row in session.execute(stmt)}
                self._instances_inserted += len(inserted)
                log_duplicates = (
                    not self.config.resume
                    and self.config.duplicate_policy in {DuplicatePolicy.SKIP, DuplicatePolicy.APPEND_SERIES}
                )
                if log_duplicates:
                    for payload in chunk:
                        if payload.sop_uid not in inserted:
                            self._log_conflict(
                                session,
                                "instance",
                                payload.sop_uid,
                                "Duplicate SOP Instance",
                                payload.file_path,
                            )

    def _upsert_modality_details(self, session, series_id: int, payload: InstancePayload) -> None:
        modality = (payload.modality or "").upper()
        if modality == "MR":
            self._upsert_detail_record(session, series_id, MRISeriesDetails, payload.series_uid, payload.mri_fields)
        elif modality == "CT":
            self._upsert_detail_record(session, series_id, CTSeriesDetails, payload.series_uid, payload.ct_fields)
        elif modality in {"PT", "PET"}:
            self._upsert_detail_record(session, series_id, PETSeriesDetails, payload.series_uid, payload.pet_fields)

    def _upsert_detail_record(self, session, series_id: int, model, series_uid: str, fields: dict) -> None:
        if not fields or all(value is None for value in fields.values()):
            return
        values = {"series_id": series_id, "series_instance_uid": series_uid}
        values.update(fields)
        stmt = (
            insert(model)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[model.series_id],
                set_={key: value for key, value in values.items() if key != "series_id"},
            )
        )
        session.execute(stmt)

    def _log_conflict(self, session, scope: str, uid: str, message: str, file_path: Optional[str]) -> None:
        if self._cohort_id is None:
            return
        stmt = (
            insert(IngestConflict)
            .values(
                cohort_id=self._cohort_id,
                scope=scope,
                uid=uid,
                message=message,
                file_path=file_path,
            )
            .on_conflict_do_nothing()
        )
        session.execute(stmt)

    def snapshot_metrics(self) -> dict[str, int]:
        return {
            "subjects": self._subjects_inserted,
            "studies": self._studies_inserted,
            "series": self._series_inserted,
            "stacks": self._stacks_inserted,
            "instances": self._instances_inserted,
            "safe_batch_rows": self._reported_safe_batch_rows,
        }

    async def _checkpoint(self) -> None:
        if self._control is None:
            return
        await self._control.checkpoint(self.job_id)
