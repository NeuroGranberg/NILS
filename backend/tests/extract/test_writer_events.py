"""Tests for imaging session event creation during extraction."""

from __future__ import annotations

import asyncio
from datetime import date

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from extract.batching import BatchSizeController, BatchSizeSettings
from extract.config import ExtractionConfig
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
    Sess = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

    monkeypatch.setattr(session_module, "SessionLocal", Sess, raising=False)
    monkeypatch.setattr(writer_module, "SessionLocal", Sess, raising=False)

    # Seed imaging observation types
    with Sess() as session:
        for obs_id, cat, name in [
            (11, "Imaging", "MRI Scan"),
            (12, "Imaging", "CT Scan"),
            (13, "Imaging", "PET Scan"),
        ]:
            session.execute(text(
                "INSERT OR IGNORE INTO observation_types "
                "(observation_type_id, category, name, is_active, is_primary) "
                "VALUES (:id, :cat, :name, 1, 0)"
            ), {"id": obs_id, "cat": cat, "name": name})
        session.commit()

    return Sess


def _make_payload(
    subject_key: str = "subject1",
    study_uid: str = "study1",
    series_uid: str = "series1",
    sop_uid: str = "instance1",
    modality: str = "MR",
    study_date: date | None = None,
) -> InstancePayload:
    if study_date is None:
        study_date = date(2024, 3, 15)
    return InstancePayload(
        subject_key=subject_key,
        subject_code=f"subj_{subject_key}",
        study_uid=study_uid,
        series_uid=series_uid,
        sop_uid=sop_uid,
        modality=modality,
        file_path=f"{subject_key}/{study_uid}/{series_uid}/{sop_uid}.dcm",
        study_fields={"study_date": study_date},
        series_fields={"modality": modality},
        instance_fields={},
        mri_fields={},
        ct_fields={},
        pet_fields={},
        patient_id="PATIENT1",
        patient_name="Test^Patient",
        subject_resolution_source="hash",
    )


def _make_writer(tmp_path, monkeypatch) -> tuple:
    Sess = _setup_metadata_db(monkeypatch)
    config = ExtractionConfig(
        cohort_id=1,
        cohort_name="TEST",
        raw_root=tmp_path,
        max_workers=1,
        batch_size=10,
        queue_size=10,
    )
    queue: asyncio.Queue = asyncio.Queue()
    controller = BatchSizeController(
        BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False)
    )
    return config, queue, controller, Sess


class TestWriterEvents:
    def test_new_study_gets_event_id(self, tmp_path, monkeypatch):
        """A newly inserted study gets an event row and event_id set."""
        config, queue, controller, Sess = _make_writer(tmp_path, monkeypatch)

        payload = _make_payload(
            study_uid="1.2.3.10",
            series_uid="1.2.3.100",
            sop_uid="1.2.3.1000",
            study_date=date(2024, 3, 15),
        )

        async def _run():
            async with Writer(
                config=config, queue=queue, job_id=None,
                progress_cb=None, batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload])
                writer._session.commit()

                events = writer._session.execute(select(schema.Event)).scalars().all()
                assert len(events) == 1
                evt = events[0]
                assert evt.observation_type_id == 11  # MR
                assert str(evt.event_date) == "2024-03-15"

                study = writer._session.execute(
                    select(schema.Study).where(schema.Study.study_instance_uid == "1.2.3.10")
                ).scalar_one()
                assert study.event_id == evt.event_id

        asyncio.get_event_loop().run_until_complete(_run())

    def test_same_session_shares_event(self, tmp_path, monkeypatch):
        """Two studies on the same date for the same subject share one event."""
        config, queue, controller, Sess = _make_writer(tmp_path, monkeypatch)

        payload1 = _make_payload(
            study_uid="1.2.3.10",
            series_uid="1.2.3.100",
            sop_uid="1.2.3.1000",
            study_date=date(2024, 3, 15),
        )
        payload2 = _make_payload(
            study_uid="1.2.3.11",
            series_uid="1.2.3.101",
            sop_uid="1.2.3.1001",
            study_date=date(2024, 3, 15),
        )

        async def _run():
            async with Writer(
                config=config, queue=queue, job_id=None,
                progress_cb=None, batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload1, payload2])
                writer._session.commit()

                events = writer._session.execute(select(schema.Event)).scalars().all()
                assert len(events) == 1

                studies = writer._session.execute(
                    select(schema.Study).order_by(schema.Study.study_id)
                ).scalars().all()
                assert len(studies) == 2
                assert studies[0].event_id == studies[1].event_id == events[0].event_id

        asyncio.get_event_loop().run_until_complete(_run())

    def test_different_dates_different_events(self, tmp_path, monkeypatch):
        """Two studies on different dates get different events."""
        config, queue, controller, Sess = _make_writer(tmp_path, monkeypatch)

        payload1 = _make_payload(
            study_uid="1.2.3.10",
            series_uid="1.2.3.100",
            sop_uid="1.2.3.1000",
            study_date=date(2024, 3, 15),
        )
        payload2 = _make_payload(
            study_uid="1.2.3.11",
            series_uid="1.2.3.101",
            sop_uid="1.2.3.1001",
            study_date=date(2024, 6, 20),
        )

        async def _run():
            async with Writer(
                config=config, queue=queue, job_id=None,
                progress_cb=None, batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload1, payload2])
                writer._session.commit()

                events = writer._session.execute(
                    select(schema.Event).order_by(schema.Event.event_date)
                ).scalars().all()
                assert len(events) == 2
                assert str(events[0].event_date) == "2024-03-15"
                assert str(events[1].event_date) == "2024-06-20"

                study1 = writer._session.execute(
                    select(schema.Study).where(schema.Study.study_instance_uid == "1.2.3.10")
                ).scalar_one()
                study2 = writer._session.execute(
                    select(schema.Study).where(schema.Study.study_instance_uid == "1.2.3.11")
                ).scalar_one()
                assert study1.event_id != study2.event_id

        asyncio.get_event_loop().run_until_complete(_run())

    def test_existing_study_without_event_gets_fixed(self, tmp_path, monkeypatch):
        """A pre-existing study with event_id=NULL gets linked when a new batch arrives."""
        config, queue, controller, Sess = _make_writer(tmp_path, monkeypatch)

        # Pre-create subject and study with event_id = NULL
        with Sess() as session:
            session.execute(text(
                "INSERT INTO subject (subject_id, subject_code, is_active) VALUES (1, 'subj_subject1', 1)"
            ))
            session.execute(text(
                "INSERT INTO cohort (cohort_id, name, owner, path, is_active) VALUES (1, 'test', 'test', '/test', 1)"
            ))
            session.execute(text(
                "INSERT INTO study (study_id, study_instance_uid, subject_id, study_date, event_id) "
                "VALUES (1, '1.2.3.OLD', 1, '2024-03-15', NULL)"
            ))
            session.commit()

        # Now process a new batch with a new study on the same date
        payload = _make_payload(
            study_uid="1.2.3.NEW",
            series_uid="1.2.3.200",
            sop_uid="1.2.3.2000",
            study_date=date(2024, 3, 15),
        )

        async def _run():
            async with Writer(
                config=config, queue=queue, job_id=None,
                progress_cb=None, batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [payload])
                writer._session.commit()

                # New study should have event_id set
                new_study = writer._session.execute(
                    select(schema.Study).where(schema.Study.study_instance_uid == "1.2.3.NEW")
                ).scalar_one()
                assert new_study.event_id is not None

                # The event was created for this session
                events = writer._session.execute(select(schema.Event)).scalars().all()
                assert len(events) == 1

        asyncio.get_event_loop().run_until_complete(_run())

    def test_different_modalities_different_events(self, tmp_path, monkeypatch):
        """MR and CT studies on the same date produce different events."""
        config, queue, controller, Sess = _make_writer(tmp_path, monkeypatch)

        mr_payload = _make_payload(
            study_uid="1.2.3.10",
            series_uid="1.2.3.100",
            sop_uid="1.2.3.1000",
            modality="MR",
            study_date=date(2024, 3, 15),
        )
        ct_payload = _make_payload(
            study_uid="1.2.3.11",
            series_uid="1.2.3.101",
            sop_uid="1.2.3.1001",
            modality="CT",
            study_date=date(2024, 3, 15),
        )

        async def _run():
            async with Writer(
                config=config, queue=queue, job_id=None,
                progress_cb=None, batch_controller=controller,
            ) as writer:
                writer._write_batch(writer._session, [mr_payload, ct_payload])
                writer._session.commit()

                events = writer._session.execute(
                    select(schema.Event).order_by(schema.Event.observation_type_id)
                ).scalars().all()
                assert len(events) == 2
                assert events[0].observation_type_id == 11  # MR
                assert events[1].observation_type_id == 12  # CT

                mr_study = writer._session.execute(
                    select(schema.Study).where(schema.Study.study_instance_uid == "1.2.3.10")
                ).scalar_one()
                ct_study = writer._session.execute(
                    select(schema.Study).where(schema.Study.study_instance_uid == "1.2.3.11")
                ).scalar_one()
                assert mr_study.event_id != ct_study.event_id

        asyncio.get_event_loop().run_until_complete(_run())
