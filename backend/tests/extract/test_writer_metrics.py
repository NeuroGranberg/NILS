import asyncio

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from extract.batching import BatchSizeController, BatchSizeSettings
from extract.config import ExtractionConfig
from extract.worker import InstancePayload
from extract.writer import Writer
from metadata_db import schema


def _setup_metadata_db(monkeypatch):
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


def test_writer_metrics_counts_new_records(tmp_path, monkeypatch):
    _setup_metadata_db(monkeypatch)

    raw_root = tmp_path / "input"
    raw_root.mkdir()

    config = ExtractionConfig(
        cohort_id=1,
        cohort_name="TEST",
        raw_root=raw_root,
        max_workers=1,
        batch_size=10,
        queue_size=10,
    )

    queue: asyncio.Queue = asyncio.Queue()
    controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

    payload = InstancePayload(
        subject_key="subject1",
        subject_code="subj1",
        study_uid="study1",
        series_uid="series1",
        sop_uid="instance1",
        modality="MR",
        file_path="subject1/file1.dcm",
        study_fields={},
        series_fields={"modality": "MR"},
        instance_fields={},
        mri_fields={},
        ct_fields={},
        pet_fields={},
        patient_id="PATIENT1",
        patient_name="Test^Patient",
        subject_resolution_source="hash",
    )

    async def _run() -> dict[str, int]:
        async with Writer(
            config=config,
            queue=queue,
            job_id=None,
            progress_cb=None,
            batch_controller=controller,
        ) as writer:
            writer._write_batch(writer._session, [payload])  # type: ignore[arg-type]
            writer._session.commit()  # type: ignore[union-attr]

            writer._write_batch(writer._session, [payload])  # duplicates should not change metrics
            writer._session.commit()  # type: ignore[union-attr]

            return writer.snapshot_metrics()

    metrics = asyncio.run(_run())

    assert metrics["subjects"] == 1
    assert metrics["studies"] == 1
    assert metrics["series"] == 1
    assert metrics["instances"] == 1
    assert metrics["safe_batch_rows"] > 0


def test_writer_defaults_modality_when_missing(tmp_path, monkeypatch):
    Session = _setup_metadata_db(monkeypatch)

    raw_root = tmp_path / "input_missing_modality"
    raw_root.mkdir()

    config = ExtractionConfig(
        cohort_id=1,
        cohort_name="TEST",
        raw_root=raw_root,
        max_workers=1,
        batch_size=10,
        queue_size=10,
    )

    queue: asyncio.Queue = asyncio.Queue()
    controller = BatchSizeController(BatchSizeSettings(initial=10, minimum=10, maximum=10, target_ms=200, enabled=False))

    payload = InstancePayload(
        subject_key="subject1",
        subject_code="subj1",
        study_uid="study1",
        series_uid="series_missing_modality",
        sop_uid="instance1",
        modality="",
        file_path="subject1/file1.dcm",
        study_fields={},
        series_fields={},
        instance_fields={},
        mri_fields={},
        ct_fields={},
        pet_fields={},
        patient_id="PATIENT1",
        patient_name="Test^Patient",
        subject_resolution_source="hash",
    )

    async def _run() -> None:
        async with Writer(
            config=config,
            queue=queue,
            job_id=None,
            progress_cb=None,
            batch_controller=controller,
        ) as writer:
            writer._write_batch(writer._session, [payload])  # type: ignore[arg-type]
            writer._session.commit()  # type: ignore[union-attr]

    asyncio.run(_run())

    with Session() as session:
        series = session.execute(select(schema.Series)).scalar_one()
        assert series.modality == "OT"
