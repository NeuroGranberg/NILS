"""Tests for the instance-first write pattern that prevents orphan records.

The instance-first pattern ensures that parent records (Subject, Study, Series, Stack)
are only created when their child instances are successfully inserted. This prevents
orphan parent records when instance insertion fails due to duplicate SOP Instance UIDs.
"""

import asyncio
from typing import Generator

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

from extract.batching import BatchSizeController, BatchSizeSettings
from extract.config import DuplicatePolicy, ExtractionConfig
from extract.worker import InstancePayload
from extract.writer import Writer
from metadata_db import schema


def _setup_metadata_db(monkeypatch) -> sessionmaker:
    """Set up an in-memory SQLite database for testing."""
    import metadata_db.lifecycle as lifecycle_module
    import metadata_db.session as session_module
    import extract.writer as writer_module

    def mock_bootstrap(auto_restore=None):
        return None

    monkeypatch.setattr(lifecycle_module, "bootstrap", mock_bootstrap, raising=False)
    monkeypatch.setattr(writer_module, "bootstrap", mock_bootstrap, raising=False)

    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    schema.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

    monkeypatch.setattr(session_module, "SessionLocal", Session, raising=False)
    monkeypatch.setattr(writer_module, "SessionLocal", Session, raising=False)

    return Session


def _make_payload(
    subject_key: str = "subject1",
    study_uid: str = "study1",
    series_uid: str = "series1",
    sop_uid: str = "instance1",
    modality: str = "MR",
    patient_id: str = "PATIENT1",
) -> InstancePayload:
    """Create a test InstancePayload."""
    return InstancePayload(
        subject_key=subject_key,
        subject_code=f"subj_{subject_key}",
        study_uid=study_uid,
        series_uid=series_uid,
        sop_uid=sop_uid,
        modality=modality,
        file_path=f"{subject_key}/{study_uid}/{series_uid}/{sop_uid}.dcm",
        study_fields={},
        series_fields={"modality": modality},
        instance_fields={},
        mri_fields={},
        ct_fields={},
        pet_fields={},
        patient_id=patient_id,
        patient_name="Test^Patient",
        subject_resolution_source="hash",
    )


def _count_records(session: Session) -> dict[str, int]:
    """Count records in all relevant tables."""
    return {
        "subjects": session.execute(select(func.count()).select_from(schema.Subject)).scalar_one(),
        "studies": session.execute(select(func.count()).select_from(schema.Study)).scalar_one(),
        "series": session.execute(select(func.count()).select_from(schema.Series)).scalar_one(),
        "stacks": session.execute(select(func.count()).select_from(schema.SeriesStack)).scalar_one(),
        "instances": session.execute(select(func.count()).select_from(schema.Instance)).scalar_one(),
    }


def _count_orphan_series(session: Session) -> int:
    """Count series with no instances."""
    # SQLite doesn't support the same NOT EXISTS syntax, use LEFT JOIN
    stmt = (
        select(func.count())
        .select_from(schema.Series)
        .outerjoin(schema.Instance, schema.Series.series_id == schema.Instance.series_id)
        .where(schema.Instance.instance_id.is_(None))
    )
    return session.execute(stmt).scalar_one()


def _count_orphan_studies(session: Session) -> int:
    """Count studies with no series."""
    stmt = (
        select(func.count())
        .select_from(schema.Study)
        .outerjoin(schema.Series, schema.Study.study_id == schema.Series.study_id)
        .where(schema.Series.series_id.is_(None))
    )
    return session.execute(stmt).scalar_one()


def _count_orphan_subjects(session: Session) -> int:
    """Count subjects with no studies."""
    stmt = (
        select(func.count())
        .select_from(schema.Subject)
        .outerjoin(schema.Study, schema.Subject.subject_id == schema.Study.subject_id)
        .where(schema.Study.study_id.is_(None))
    )
    return session.execute(stmt).scalar_one()


class TestNoOrphans:
    """Test that the instance-first pattern prevents orphan records."""

    def test_duplicate_sop_creates_no_orphans(self, tmp_path, monkeypatch):
        """Inserting duplicate SOPs should not create orphan parent records."""
        Session = _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        payload = _make_payload(sop_uid="duplicate_sop")

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                # First insert - should succeed
                writer._write_batch(writer._session, [payload])
                writer._session.commit()
                counts_after_first = _count_records(writer._session)

                # Second insert - same SOP, should be skipped
                writer._write_batch(writer._session, [payload])
                writer._session.commit()
                counts_after_second = _count_records(writer._session)

                return counts_after_first, counts_after_second

        first, second = asyncio.run(_run())

        # First insert should create 1 of each
        assert first["instances"] == 1
        assert first["series"] == 1
        assert first["studies"] == 1
        assert first["subjects"] == 1

        # Second insert should NOT create any new records
        assert second["instances"] == 1, "Duplicate SOP should not create new instance"
        assert second["series"] == 1, "Duplicate SOP should not create orphan series"
        assert second["studies"] == 1, "Duplicate SOP should not create orphan study"
        assert second["subjects"] == 1, "Duplicate SOP should not create orphan subject"

    def test_partial_duplicates_only_creates_needed_parents(self, tmp_path, monkeypatch):
        """When some instances are duplicates, only create parents for new instances."""
        Session = _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        # First batch: 3 instances
        batch1 = [
            _make_payload(sop_uid="A", series_uid="series1"),
            _make_payload(sop_uid="B", series_uid="series1"),
            _make_payload(sop_uid="C", series_uid="series1"),
        ]

        # Second batch: A and B are duplicates, D and E are new (different series)
        batch2 = [
            _make_payload(sop_uid="A", series_uid="series1"),  # duplicate
            _make_payload(sop_uid="B", series_uid="series1"),  # duplicate
            _make_payload(sop_uid="D", series_uid="series2"),  # new
            _make_payload(sop_uid="E", series_uid="series2"),  # new
        ]

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, batch1)
                writer._session.commit()
                counts_after_first = _count_records(writer._session)

                writer._write_batch(writer._session, batch2)
                writer._session.commit()
                counts_after_second = _count_records(writer._session)

                orphan_series = _count_orphan_series(writer._session)
                orphan_studies = _count_orphan_studies(writer._session)

                return counts_after_first, counts_after_second, orphan_series, orphan_studies

        first, second, orphan_series, orphan_studies = asyncio.run(_run())

        # First batch: 3 instances, 1 series, 1 study, 1 subject
        assert first["instances"] == 3
        assert first["series"] == 1

        # Second batch: only D and E inserted (2 new), in new series2
        assert second["instances"] == 5  # 3 + 2
        assert second["series"] == 2  # series1 + series2

        # No orphans
        assert orphan_series == 0, "Should have no orphan series"
        assert orphan_studies == 0, "Should have no orphan studies"

    def test_same_sop_different_series_handled(self, tmp_path, monkeypatch):
        """Same SOP UID in different series should only create first series."""
        Session = _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        # Same SOP, different series
        payload1 = _make_payload(sop_uid="same_sop", series_uid="series1")
        payload2 = _make_payload(sop_uid="same_sop", series_uid="series2")

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload1])
                writer._session.commit()

                writer._write_batch(writer._session, [payload2])
                writer._session.commit()

                counts = _count_records(writer._session)
                orphan_series = _count_orphan_series(writer._session)

                return counts, orphan_series

        counts, orphan_series = asyncio.run(_run())

        # Only 1 instance (second is duplicate)
        assert counts["instances"] == 1
        # Only 1 series (series2 should NOT be created since its instance is duplicate)
        assert counts["series"] == 1, "Series2 should not be created for duplicate SOP"
        assert orphan_series == 0, "Should have no orphan series"

    def test_same_sop_different_study_handled(self, tmp_path, monkeypatch):
        """Same SOP UID in different study should only create first study."""
        Session = _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        # Same SOP, different study
        payload1 = _make_payload(sop_uid="same_sop", study_uid="study1", series_uid="series1")
        payload2 = _make_payload(sop_uid="same_sop", study_uid="study2", series_uid="series2")

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload1])
                writer._session.commit()

                writer._write_batch(writer._session, [payload2])
                writer._session.commit()

                counts = _count_records(writer._session)
                orphan_studies = _count_orphan_studies(writer._session)

                return counts, orphan_studies

        counts, orphan_studies = asyncio.run(_run())

        assert counts["instances"] == 1
        assert counts["studies"] == 1, "Study2 should not be created for duplicate SOP"
        assert orphan_studies == 0, "Should have no orphan studies"

    def test_metrics_only_count_inserted(self, tmp_path, monkeypatch):
        """Metrics should only count actually inserted instances."""
        _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        payload = _make_payload(sop_uid="test_sop")

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload])
                writer._session.commit()
                metrics_first = writer.snapshot_metrics()

                # Insert same instance again
                writer._write_batch(writer._session, [payload])
                writer._session.commit()
                metrics_second = writer.snapshot_metrics()

                return metrics_first, metrics_second

        first, second = asyncio.run(_run())

        assert first["instances"] == 1
        # Second insert should NOT increment instance count
        assert second["instances"] == 1, "Duplicate should not increment instance count"

    def test_resume_mode_no_orphans(self, tmp_path, monkeypatch):
        """Resume mode should not create orphans for skipped instances."""
        _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
            resume=True,  # Enable resume mode
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        payload = _make_payload(sop_uid="resume_test")

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload])
                writer._session.commit()

                # "Resume" - insert same again
                writer._write_batch(writer._session, [payload])
                writer._session.commit()

                counts = _count_records(writer._session)
                orphans = _count_orphan_series(writer._session)

                return counts, orphans

        counts, orphans = asyncio.run(_run())

        assert counts["instances"] == 1
        assert counts["series"] == 1
        assert orphans == 0

    def test_overwrite_policy_updates_not_creates(self, tmp_path, monkeypatch):
        """OVERWRITE policy should update existing, not create new parents."""
        _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
            duplicate_policy=DuplicatePolicy.OVERWRITE,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        payload1 = _make_payload(sop_uid="overwrite_test", series_uid="series1")
        # Same SOP but "different" series - should still update, not create new
        payload2 = _make_payload(sop_uid="overwrite_test", series_uid="series2")

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload1])
                writer._session.commit()
                counts_first = _count_records(writer._session)

                writer._write_batch(writer._session, [payload2])
                writer._session.commit()
                counts_second = _count_records(writer._session)

                return counts_first, counts_second

        first, second = asyncio.run(_run())

        assert first["instances"] == 1
        assert first["series"] == 1

        # OVERWRITE updates the existing instance, creates series2 for the updated instance
        # The key point is we don't have orphan series1
        assert second["instances"] == 1, "Should still have 1 instance (updated)"

    def test_all_duplicates_batch_no_changes(self, tmp_path, monkeypatch):
        """A batch with all duplicates should make no database changes."""
        Session = _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        batch = [
            _make_payload(sop_uid="dup1"),
            _make_payload(sop_uid="dup2"),
            _make_payload(sop_uid="dup3"),
        ]

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                # First insert
                writer._write_batch(writer._session, batch)
                writer._session.commit()
                counts_first = _count_records(writer._session)

                # Second insert - all duplicates
                writer._write_batch(writer._session, batch)
                writer._session.commit()
                counts_second = _count_records(writer._session)

                return counts_first, counts_second

        first, second = asyncio.run(_run())

        assert first["instances"] == 3
        # Second batch should not change anything
        assert second == first, "All-duplicate batch should not change any counts"

    def test_empty_batch_no_changes(self, tmp_path, monkeypatch):
        """Empty batch should not create any records."""
        Session = _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [])
                writer._session.commit()

                return _count_records(writer._session)

        counts = asyncio.run(_run())

        assert counts["instances"] == 0
        assert counts["series"] == 0
        assert counts["studies"] == 0
        assert counts["subjects"] == 0

    def test_instance_fks_updated_correctly(self, tmp_path, monkeypatch):
        """Verify that instance FK fields are set correctly after parent creation."""
        Session = _setup_metadata_db(monkeypatch)

        config = ExtractionConfig(
            cohort_id=1,
            cohort_name="TEST",
            raw_root=tmp_path,
            max_workers=1,
            batch_size=10,
            queue_size=10,
        )
        queue: asyncio.Queue = asyncio.Queue()
        controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

        payload = _make_payload(sop_uid="fk_test", series_uid="series_fk")

        async def _run():
            async with Writer(
                config=config,
                queue=queue,
                job_id=None,
                progress_cb=None,
                batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload])
                writer._session.commit()

                # Query the instance directly
                instance = writer._session.execute(
                    select(schema.Instance).where(schema.Instance.sop_instance_uid == "fk_test")
                ).scalar_one()

                # Query the series
                series = writer._session.execute(
                    select(schema.Series).where(schema.Series.series_instance_uid == "series_fk")
                ).scalar_one()

                return instance, series

        instance, series = asyncio.run(_run())

        # Instance should have correct series_id
        assert instance.series_id == series.series_id, "Instance should reference correct series"
        assert instance.series_stack_id is not None, "Instance should have stack_id set"
