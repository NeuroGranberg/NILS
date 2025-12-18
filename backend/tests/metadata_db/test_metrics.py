from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def metrics_context(tmp_path, monkeypatch):
    db_file = tmp_path / "metadata.sqlite"
    backup_dir = tmp_path / "backups"
    monkeypatch.setenv("METADATA_DATABASE_URL", f"sqlite+pysqlite:///{db_file}")
    monkeypatch.setenv("METADATA_BACKUP_DIR", str(backup_dir))
    monkeypatch.setenv("METADATA_BACKUP_ENABLED", "false")
    monkeypatch.setenv("METADATA_AUTO_RESTORE", "false")

    import metadata_db.config as config_module
    import metadata_db.session as session_module
    import metadata_db.schema as schema_module
    import metadata_db.metrics as metrics_module

    config_module.get_settings.cache_clear()
    config_module.get_backup_settings.cache_clear()

    config_module = importlib.reload(config_module)
    session_module = importlib.reload(session_module)
    schema_module = importlib.reload(schema_module)
    metrics_module = importlib.reload(metrics_module)

    schema_module.Base.metadata.create_all(session_module.engine)

    return {
        "SessionLocal": session_module.SessionLocal,
        "schema": schema_module,
        "metrics": metrics_module,
    }


def test_get_cohort_metrics_counts_unique_subjects(metrics_context):
    schema = metrics_context["schema"]
    SessionLocal = metrics_context["SessionLocal"]
    metrics_module = metrics_context["metrics"]

    with SessionLocal() as session:
        subject = schema.Subject(subject_code="S001", patient_name="Test")
        cohort = schema.Cohort(name="Metrics Cohort", owner="system", path="/tmp/path")
        session.add_all([subject, cohort])
        session.flush()

        session.add(schema.SubjectCohort(subject_id=subject.subject_id, cohort_id=cohort.cohort_id))
        session.flush()

        study = schema.Study(study_instance_uid="1.2.3", subject_id=subject.subject_id)
        session.add(study)
        session.flush()

        series_a = schema.Series(
            series_instance_uid="1.2.3.4",
            modality="MR",
            study_id=study.study_id,
            subject_id=subject.subject_id,
        )
        series_b = schema.Series(
            series_instance_uid="1.2.3.5",
            modality="MR",
            study_id=study.study_id,
            subject_id=subject.subject_id,
        )
        session.add_all([series_a, series_b])
        session.flush()

        instance_a = schema.Instance(
            series_id=series_a.series_id,
            series_instance_uid=series_a.series_instance_uid,
            sop_instance_uid="1.2.3.4.1",
            dicom_file_path="file_a.dcm",
        )
        instance_b = schema.Instance(
            series_id=series_b.series_id,
            series_instance_uid=series_b.series_instance_uid,
            sop_instance_uid="1.2.3.5.1",
            dicom_file_path="file_b.dcm",
        )
        session.add_all([instance_a, instance_b])
        session.commit()

        cohort_id = cohort.cohort_id

    metrics = metrics_module.get_cohort_metrics(cohort_id)
    assert metrics == {
        "subjects": 1,
        "studies": 1,
        "series": 2,
        "instances": 2,
    }
