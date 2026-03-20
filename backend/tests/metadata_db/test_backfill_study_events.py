"""Tests for the backfill_study_events migration."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, func, inspect, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from metadata_db import schema
from metadata_db.migrations.backfill_study_events import (
    MODALITY_TO_OBSERVATION_TYPE,
    _needs_migration,
    run_migration,
)


@pytest.fixture
def engine_and_session():
    """In-memory SQLite database with full schema."""
    eng = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    schema.Base.metadata.create_all(eng)
    Sess = sessionmaker(bind=eng, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
    return eng, Sess


def _seed_observation_types(session: Session) -> None:
    """Insert the imaging observation types needed for tests."""
    for obs_id, cat, name, desc in [
        (11, "Imaging", "MRI Scan", "MRI imaging study performed"),
        (12, "Imaging", "CT Scan", "CT imaging study performed"),
        (13, "Imaging", "PET Scan", "PET imaging study performed"),
    ]:
        session.execute(text(
            "INSERT OR IGNORE INTO observation_types (observation_type_id, category, name, description, is_active, is_primary) "
            "VALUES (:id, :cat, :name, :desc, 1, 0)"
        ), {"id": obs_id, "cat": cat, "name": name, "desc": desc})
    session.commit()


def _add_subject(session: Session, subject_id: int, code: str) -> None:
    session.execute(text(
        "INSERT INTO subject (subject_id, subject_code, is_active) VALUES (:id, :code, 1)"
    ), {"id": subject_id, "code": code})


def _add_study(session: Session, study_id: int, uid: str, subject_id: int, study_date: str, event_id: int | None = None) -> None:
    session.execute(text(
        "INSERT INTO study (study_id, study_instance_uid, subject_id, study_date, event_id) "
        "VALUES (:sid, :uid, :subj, :dt, :eid)"
    ), {"sid": study_id, "uid": uid, "subj": subject_id, "dt": study_date, "eid": event_id})


def _add_series(session: Session, series_id: int, uid: str, study_id: int, modality: str) -> None:
    session.execute(text(
        "INSERT INTO series (series_id, series_instance_uid, study_id, modality, subject_id) "
        "VALUES (:sid, :uid, :stid, :mod, 0)"
    ), {"sid": series_id, "uid": uid, "stid": study_id, "mod": modality})


def test_backfill_creates_events_for_existing_studies(engine_and_session):
    """Studies without event_id get linked to newly created events."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15")
        _add_series(session, 100, "1.2.3.100", 10, "MR")
        session.commit()

    result = run_migration(eng, dry_run=False)
    assert result["success"]
    assert result["events_inserted"] == 1
    assert result["studies_linked"] == 1

    with Sess() as session:
        events = session.execute(select(schema.Event)).scalars().all()
        assert len(events) == 1
        evt = events[0]
        assert evt.subject_id == 1
        assert evt.observation_type_id == 11  # MRI
        assert str(evt.event_date) == "2024-03-15"

        study = session.execute(
            select(schema.Study).where(schema.Study.study_id == 10)
        ).scalar_one()
        assert study.event_id == evt.event_id


def test_backfill_groups_by_session(engine_and_session):
    """Multiple studies on same date for same subject share one event."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")
        # 3 MR studies on same date
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15")
        _add_study(session, 11, "1.2.3.11", 1, "2024-03-15")
        _add_study(session, 12, "1.2.3.12", 1, "2024-03-15")
        # 1 MR study on different date
        _add_study(session, 13, "1.2.3.13", 1, "2024-06-20")
        for i, study_id in enumerate([10, 11, 12, 13]):
            _add_series(session, 100 + i, f"1.2.3.10{i}", study_id, "MR")
        session.commit()

    result = run_migration(eng, dry_run=False)
    assert result["success"]
    assert result["events_inserted"] == 2  # 2 distinct dates
    assert result["studies_linked"] == 4

    with Sess() as session:
        events = session.execute(
            select(schema.Event).order_by(schema.Event.event_date)
        ).scalars().all()
        assert len(events) == 2

        # Studies 10, 11, 12 share the March event
        march_event = events[0]
        studies_march = session.execute(
            select(schema.Study).where(schema.Study.event_id == march_event.event_id)
        ).scalars().all()
        assert len(studies_march) == 3

        # Study 13 has the June event
        june_event = events[1]
        studies_june = session.execute(
            select(schema.Study).where(schema.Study.event_id == june_event.event_id)
        ).scalars().all()
        assert len(studies_june) == 1


def test_backfill_handles_mixed_modalities(engine_and_session):
    """MR and CT studies on same date produce separate events."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15")
        _add_study(session, 11, "1.2.3.11", 1, "2024-03-15")
        _add_series(session, 100, "1.2.3.100", 10, "MR")
        _add_series(session, 101, "1.2.3.101", 11, "CT")
        session.commit()

    result = run_migration(eng, dry_run=False)
    assert result["success"]
    assert result["events_inserted"] == 2  # MR + CT = 2 events

    with Sess() as session:
        events = session.execute(
            select(schema.Event).order_by(schema.Event.observation_type_id)
        ).scalars().all()
        assert len(events) == 2
        assert events[0].observation_type_id == 11  # MR
        assert events[1].observation_type_id == 12  # CT


def test_backfill_idempotent(engine_and_session):
    """Running migration twice produces the same result."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15")
        _add_series(session, 100, "1.2.3.100", 10, "MR")
        session.commit()

    result1 = run_migration(eng, dry_run=False)
    assert result1["success"]
    assert result1["events_inserted"] == 1

    result2 = run_migration(eng, dry_run=False)
    assert result2["success"]
    assert result2["already_migrated"]

    with Sess() as session:
        event_count = session.execute(select(func.count()).select_from(schema.Event)).scalar_one()
        assert event_count == 1


def test_backfill_skips_already_linked_studies(engine_and_session):
    """Studies that already have event_id are not touched."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")

        # Create an event manually
        session.execute(text(
            "INSERT INTO event (event_id, subject_id, observation_type_id, event_date) "
            "VALUES (99, 1, 11, '2024-03-15')"
        ))
        # Study already linked
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15", event_id=99)
        _add_series(session, 100, "1.2.3.100", 10, "MR")

        # Another study NOT linked
        _add_study(session, 11, "1.2.3.11", 1, "2024-06-20")
        _add_series(session, 101, "1.2.3.101", 11, "MR")
        session.commit()

    result = run_migration(eng, dry_run=False)
    assert result["success"]
    assert result["events_inserted"] == 1  # Only the June event
    assert result["studies_linked"] == 1

    with Sess() as session:
        # Study 10 still has the original event_id
        study10 = session.execute(
            select(schema.Study).where(schema.Study.study_id == 10)
        ).scalar_one()
        assert study10.event_id == 99

        # Study 11 got a new event
        study11 = session.execute(
            select(schema.Study).where(schema.Study.study_id == 11)
        ).scalar_one()
        assert study11.event_id is not None
        assert study11.event_id != 99


def test_needs_migration_false_when_all_linked(engine_and_session):
    """Detection returns False when no studies have NULL event_id."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")
        session.execute(text(
            "INSERT INTO event (event_id, subject_id, observation_type_id, event_date) "
            "VALUES (1, 1, 11, '2024-03-15')"
        ))
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15", event_id=1)
        _add_series(session, 100, "1.2.3.100", 10, "MR")
        session.commit()

    with eng.connect() as conn:
        assert not _needs_migration(conn)


def test_backfill_skips_studies_without_series(engine_and_session):
    """Studies with no series rows are skipped (no modality derivable)."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15")
        # No series added for study 10
        session.commit()

    result = run_migration(eng, dry_run=False)
    assert result["success"]
    assert result["events_inserted"] == 0
    assert result["studies_linked"] == 0

    with Sess() as session:
        study = session.execute(
            select(schema.Study).where(schema.Study.study_id == 10)
        ).scalar_one()
        assert study.event_id is None


def test_backfill_dominant_modality(engine_and_session):
    """When a study has mixed modality series, the dominant one wins."""
    eng, Sess = engine_and_session
    with Sess() as session:
        _seed_observation_types(session)
        _add_subject(session, 1, "SUBJ001")
        _add_study(session, 10, "1.2.3.10", 1, "2024-03-15")
        # 5 MR series, 2 CT series -> dominant = MR
        for i in range(5):
            _add_series(session, 100 + i, f"1.2.3.1{i:02d}", 10, "MR")
        for i in range(2):
            _add_series(session, 200 + i, f"1.2.3.2{i:02d}", 10, "CT")
        session.commit()

    result = run_migration(eng, dry_run=False)
    assert result["success"]

    with Sess() as session:
        evt = session.execute(select(schema.Event)).scalar_one()
        assert evt.observation_type_id == 11  # MR is dominant
