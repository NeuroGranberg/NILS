from __future__ import annotations

import importlib

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError


@pytest.fixture
def lifecycle_modules(tmp_path, monkeypatch):
    db_file = tmp_path / "metadata.sqlite"
    backup_dir = tmp_path / "backups"
    monkeypatch.setenv("METADATA_DATABASE_URL", f"sqlite+pysqlite:///{db_file}")
    monkeypatch.setenv("METADATA_BACKUP_DIR", str(backup_dir))
    monkeypatch.setenv("METADATA_BACKUP_ENABLED", "false")
    monkeypatch.setenv("METADATA_AUTO_RESTORE", "false")

    import metadata_db.config as config
    import metadata_db.session as session
    import metadata_db.lifecycle as lifecycle
    import metadata_db.schema as schema

    config.get_settings.cache_clear()
    config.get_backup_settings.cache_clear()

    importlib.reload(config)
    importlib.reload(session)
    lifecycle = importlib.reload(lifecycle)
    importlib.reload(schema)

    return lifecycle, session


def test_bootstrap_creates_schema(lifecycle_modules):
    lifecycle, session = lifecycle_modules

    version = lifecycle.bootstrap(auto_restore=False)
    assert version == "1.2.0"

    insp = inspect(session.engine)
    tables = set(insp.get_table_names())
    for expected in {"study", "series", "instance", "schema_version"}:
        assert expected in tables


def test_bootstrap_idempotent(lifecycle_modules):
    lifecycle, _ = lifecycle_modules
    first = lifecycle.bootstrap(auto_restore=False)
    second = lifecycle.bootstrap(auto_restore=False)
    assert first == second == "1.2.0"


def test_cohort_name_has_unique_index(lifecycle_modules):
    lifecycle, session = lifecycle_modules
    lifecycle.bootstrap(auto_restore=False)

    with session.engine.connect() as conn:
        rows = conn.execute(text("PRAGMA index_list('cohort')")).fetchall()
    unique_indexes = {row[1] for row in rows if row[2]}
    assert "idx_cohort_name_unique" in unique_indexes


def test_duplicate_cohort_name_rejected(lifecycle_modules):
    lifecycle, session = lifecycle_modules
    lifecycle.bootstrap(auto_restore=False)

    from metadata_db.schema import Cohort

    db = session.SessionLocal()
    try:
        db.add(Cohort(name="ALS", owner="system", path="/tmp/a"))
        db.commit()
        db.add(Cohort(name="ALS", owner="system", path="/tmp/b"))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()
    finally:
        db.close()


def test_duplicate_subject_cohort_rejected(lifecycle_modules):
    lifecycle, session = lifecycle_modules
    lifecycle.bootstrap(auto_restore=False)

    from metadata_db.schema import Cohort, Subject, SubjectCohort

    db = session.SessionLocal()
    try:
        subject = Subject(subject_code="S001", patient_name="Test")
        cohort = Cohort(name="Test Cohort", owner="system", path="/tmp/path")
        db.add_all([subject, cohort])
        db.flush()

        db.add(SubjectCohort(subject_id=subject.subject_id, cohort_id=cohort.cohort_id))
        db.commit()

        db.add(SubjectCohort(subject_id=subject.subject_id, cohort_id=cohort.cohort_id))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()
    finally:
        db.close()
