from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import copy

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, func
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker

from api import server
from api.routes import imports as imports_route
from api.routes import metadata_subjects as metadata_subjects_route
from api.routes import metadata_cohorts as metadata_cohorts_route
from metadata_db import schema
from metadata_db import session as metadata_session


def _setup_metadata_db(monkeypatch):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    schema.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

    # Patch modules where the database connection is actually used
    monkeypatch.setattr(metadata_session, "engine", engine)
    monkeypatch.setattr(metadata_session, "SessionLocal", Session)
    
    # Patch route modules that import engine/SessionLocal directly
    monkeypatch.setattr(imports_route, "metadata_engine", engine)
    monkeypatch.setattr(imports_route, "MetadataSessionLocal", Session)
    monkeypatch.setattr(metadata_subjects_route, "metadata_engine", engine)
    monkeypatch.setattr(metadata_subjects_route, "MetadataSessionLocal", Session)
    monkeypatch.setattr(metadata_cohorts_route, "metadata_engine", engine)
    monkeypatch.setattr(metadata_cohorts_route, "MetadataSessionLocal", Session)
    
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    return engine, Session


def _create_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_subject_import_preview_and_apply(tmp_path, monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        id_type = schema.IdType(id_type_name="MRN", description="Medical Record Number")
        session.add(id_type)
        session.commit()
        id_type_id = id_type.id_type_id

    csv_path = tmp_path / "subjects.csv"
    _create_csv(
        csv_path,
        [
            {
                "subject_code": "S100",
                "patient_name": "Alice",
                "patient_birth_date": "1985-03-12",
                "mrn": "MRN-001",
            },
            {
                "subject_code": "S200",
                "patient_name": "Bob",
                "patient_birth_date": "23/07/1978",
                "mrn": "MRN-002",
            },
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    base_payload = {
        "filePath": str(csv_path),
        "subjectFields": {
            "subject_code": {"column": "subject_code"},
            "patient_name": {"column": "patient_name"},
            "patient_birth_date": {"column": "patient_birth_date"},
        },
        "identifiers": [
            {
                "idTypeId": id_type_id,
                "value": {"column": "mrn"},
            }
        ],
    }

    preview_response = client.post("/api/metadata/imports/subjects/preview", json=base_payload)
    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["processedRows"] == 2
    assert preview_payload["rows"][0]["subject"]["patient_name"] == "Alice"
    assert preview_payload["rows"][0]["subject"]["patient_birth_date"] == "1985-03-12"
    assert preview_payload["rows"][0]["existing"] is False

    apply_response = client.post("/api/metadata/imports/subjects/apply", json=base_payload)
    assert apply_response.status_code == 200
    apply_payload = apply_response.json()
    assert apply_payload["subjectsInserted"] == 2
    assert apply_payload["identifiersInserted"] == 2

    with Session() as session:
        subjects = session.scalars(select(schema.Subject)).all()
        assert {subject.subject_code for subject in subjects} == {"S100", "S200"}
        birth_dates = {subject.subject_code: subject.patient_birth_date for subject in subjects}
        assert birth_dates["S100"] == date(1985, 3, 12)
        assert birth_dates["S200"] == date(1978, 7, 23)
        identifiers = session.scalars(select(schema.SubjectOtherIdentifier)).all()
        assert len(identifiers) == 2

    update_csv_path = tmp_path / "subjects_update.csv"
    _create_csv(
        update_csv_path,
        [
            {
                "subject_code": "S100",
                "patient_name": "Alice Updated",
                "patient_birth_date": "1985-03-12",
                "mrn": "MRN-001",
            },
            {
                "subject_code": "S200",
                "patient_name": "Bob Updated",
                "patient_birth_date": "1978-07-23",
                "mrn": "MRN-002",
            },
        ],
    )

    update_payload = copy.deepcopy(base_payload)
    update_payload["filePath"] = str(update_csv_path)

    update_preview_response = client.post("/api/metadata/imports/subjects/preview", json=update_payload)
    assert update_preview_response.status_code == 200
    update_preview = update_preview_response.json()
    assert update_preview["processedRows"] == 2
    existing_flags = {row["subject"]["subject_code"]: row for row in update_preview["rows"]}
    assert existing_flags["S100"]["existing"] is True
    assert existing_flags["S100"]["existingSubject"]["patient_name"] == "Alice"
    assert existing_flags["S100"]["subject"]["patient_name"] == "Alice Updated"

    update_apply_response = client.post("/api/metadata/imports/subjects/apply", json=update_payload)
    assert update_apply_response.status_code == 200
    update_result = update_apply_response.json()
    assert update_result["subjectsInserted"] == 0
    assert update_result["subjectsUpdated"] == 2

    detail_response = client.get("/api/metadata/subjects/S100")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["subjectCode"] == "S100"
    assert detail_payload["patientName"] == "Alice Updated"
    assert detail_payload["patientBirthDate"] == "1985-03-12"

    # Dry run update should not persist changes
    dry_csv_path = tmp_path / "subjects_dry.csv"
    _create_csv(
        dry_csv_path,
        [
            {
                "subject_code": "S100",
                "patient_name": "Alice",
                "patient_birth_date": "1985.03.12",
                "mrn": "MRN-001",
            },
            {
                "subject_code": "S200",
                "patient_name": "Bob",
                "patient_birth_date": "1978-07-23",
                "mrn": "MRN-002",
            },
            {
                "subject_code": "S300",
                "patient_name": "Charlie",
                "patient_birth_date": "19900105",
                "mrn": "MRN-003",
            },
        ],
    )

    dry_payload = copy.deepcopy(base_payload)
    dry_payload["filePath"] = str(dry_csv_path)
    dry_payload["dryRun"] = True

    dry_run_response = client.post("/api/metadata/imports/subjects/apply", json=dry_payload)
    assert dry_run_response.status_code == 200

    with Session() as session:
        count_after = session.scalar(select(func.count()).select_from(schema.Subject))
        assert count_after == 2
        s300 = session.scalar(select(schema.Subject).where(schema.Subject.subject_code == "S300"))
        assert s300 is None


def test_subject_import_sparse_dataset(tmp_path, monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    csv_path = tmp_path / "subjects_sparse.csv"
    _create_csv(
        csv_path,
        [
            {
                "KI-ID": "00014c74b355dd53",
                "DOB": "19450117",
                "Sex": "female",
                "Date_of_diagnosis": "19960615",
                "Date_of_onset": "19890701",
                "Disease": "MS",
            },
            {
                "KI-ID": "0015e91e494d1a04",
                "DOB": "19620228",
                "Sex": "female",
                "Date_of_diagnosis": "20180703",
                "Date_of_onset": "20120715",
                "Disease": "MS",
            },
            {
                "KI-ID": "0024f0673b498191",
                "DOB": "19700307",
                "Sex": "male",
                "Date_of_diagnosis": "20080331",
                "Date_of_onset": "20080101",
                "Disease": "MS",
            },
            {
                "KI-ID": "002a0b64ff4e3b41",
                "DOB": "19750110",
                "Sex": "female",
                "Date_of_diagnosis": "20060718",
                "Date_of_onset": "19991115",
                "Disease": "MS",
            },
            {
                "KI-ID": "003f90e11c4095f8",
                "DOB": "19400924",
                "Sex": "male",
                "Date_of_diagnosis": "",
                "Date_of_onset": "",
                "Disease": "MS",
            },
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    base_payload = {
        "filePath": str(csv_path),
        "subjectFields": {
            "subject_code": {"column": "KI-ID"},
            "patient_birth_date": {"column": "DOB"},
            "patient_sex": {"column": "Sex"},
        },
        "cohort": None,
        "identifiers": [],
    }

    preview_response = client.post("/api/metadata/imports/subjects/preview", json=base_payload)
    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["processedRows"] == 5
    assert all(row["existing"] is False for row in preview_payload["rows"])

    dry_payload = {**base_payload, "dryRun": True}
    dry_response = client.post("/api/metadata/imports/subjects/apply", json=dry_payload)
    assert dry_response.status_code == 200

    apply_response = client.post("/api/metadata/imports/subjects/apply", json=base_payload)
    assert apply_response.status_code == 200

    with Session() as session:
        subjects = session.scalars(select(schema.Subject)).all()
        assert len(subjects) == 5
        stored_codes = {subject.subject_code for subject in subjects}
        assert "003f90e11c4095f8" in stored_codes


def test_cohort_import_preview_and_apply(tmp_path, monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    initial_csv = tmp_path / "cohorts.csv"
    _create_csv(
        initial_csv,
        [
            {
                "cohort_id": "101",
                "name": "ALS",
                "owner": "Dr. Neuro",
                "path": "/cohorts/als",
                "description": "ALS baseline cohort",
            },
            {
                "cohort_id": "102",
                "name": "MS",
                "owner": "Dr. Auto",
                "path": "/cohorts/ms",
                "description": "MS longitudinal cohort",
            },
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    base_payload = {
        "filePath": str(initial_csv),
        "cohortFields": {
            "name": {"column": "name"},
            "owner": {"column": "owner"},
            "path": {"column": "path"},
            "description": {"column": "description"},
        },
    }

    preview_response = client.post("/api/metadata/imports/cohorts/preview", json=base_payload)
    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["processedRows"] == 2
    assert preview_payload["rows"][0]["existing"] is False

    apply_response = client.post("/api/metadata/imports/cohorts/apply", json=base_payload)
    assert apply_response.status_code == 200
    apply_payload = apply_response.json()
    assert apply_payload["cohortsInserted"] == 2
    assert apply_payload["cohortsUpdated"] == 0

    with Session() as session:
        cohorts = session.scalars(select(schema.Cohort)).all()
        assert {cohort.name for cohort in cohorts} == {"als", "ms"}
        als = next(cohort for cohort in cohorts if cohort.name == "als")
        als_id = als.cohort_id
        assert als.owner == "Dr. Neuro"

    update_csv = tmp_path / "cohorts_update.csv"
    _create_csv(
        update_csv,
        [
            {
                "name": "ALS",
                "owner": "Dr. Updated",
                "path": "",
                "description": "Updated description",
            }
        ],
    )

    update_payload = {
        "filePath": str(update_csv),
        "cohortFields": {
            "name": {"column": "name"},
            "owner": {"column": "owner"},
            "path": {"column": "path"},
            "description": {"column": "description"},
        },
        "dryRun": False,
    }

    update_preview = client.post("/api/metadata/imports/cohorts/preview", json=update_payload)
    assert update_preview.status_code == 200
    preview_data = update_preview.json()
    assert preview_data["processedRows"] == 1
    assert preview_data["rows"][0]["existing"] is True

    apply_update = client.post("/api/metadata/imports/cohorts/apply", json=update_payload)
    assert apply_update.status_code == 200
    update_result = apply_update.json()
    assert update_result["cohortsInserted"] == 0
    assert update_result["cohortsUpdated"] == 1

    detail_response = client.get(f"/api/metadata/cohorts/{als_id}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["cohortId"] == als_id
    assert detail_payload["name"] == "als"
    assert detail_payload["owner"] == "Dr. Updated"
    # Blank path should be ignored with skipBlankUpdates defaulting to true
    assert detail_payload["path"] == "/cohorts/als"


def test_cohort_import_manual_defaults(tmp_path, monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    manual_csv = tmp_path / "cohort_manual.csv"
    _create_csv(
        manual_csv,
        [
            {
                "cohort_id": "301",
                "name": "Minimal Cohort",
            }
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    payload = {
        "filePath": str(manual_csv),
        "cohortFields": {
            "name": {"column": "name"},
        },
    }

    response = client.post("/api/metadata/imports/cohorts/apply", json=payload)
    assert response.status_code == 200
    result = response.json()
    assert result["cohortsInserted"] == 1
    assert result["cohortsUpdated"] == 0

    with Session() as session:
        stored = session.scalar(
            select(schema.Cohort).where(schema.Cohort.name == "minimal cohort")
        )
        assert stored is not None
        assert stored.cohort_id is not None
        assert stored.name == "minimal cohort"
        assert stored.owner == ""
        assert stored.path == ""


def test_cohort_import_manual_update_preserves_required(tmp_path, monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    initial_csv = tmp_path / "cohort_initial.csv"
    _create_csv(
        initial_csv,
        [
            {
                "cohort_id": "401",
                "name": "Baseline",
                "owner": "Lead",
                "path": "/baseline",
            }
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    base_payload = {
        "filePath": str(initial_csv),
        "cohortFields": {
            "name": {"column": "name"},
            "owner": {"column": "owner"},
            "path": {"column": "path"},
        },
    }

    response = client.post("/api/metadata/imports/cohorts/apply", json=base_payload)
    assert response.status_code == 200

    update_csv = tmp_path / "cohort_update.csv"
    _create_csv(
        update_csv,
        [
            {
                "name": "Baseline",
            }
        ],
    )

    update_payload = {
        "filePath": str(update_csv),
        "cohortFields": {
            "name": {"column": "name"},
        },
    }

    update_response = client.post("/api/metadata/imports/cohorts/apply", json=update_payload)
    assert update_response.status_code == 200

    with Session() as session:
        stored = session.scalar(
            select(schema.Cohort).where(schema.Cohort.name == "baseline")
        )
        assert stored is not None
        assert stored.name == "baseline"
        assert stored.owner == "Lead"
        assert stored.path == "/baseline"
