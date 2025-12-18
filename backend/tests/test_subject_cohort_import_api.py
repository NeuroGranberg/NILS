from __future__ import annotations

import csv
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api import server
from api.routes import imports as imports_route
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
    monkeypatch.setattr(metadata_cohorts_route, "metadata_engine", engine)
    monkeypatch.setattr(metadata_cohorts_route, "MetadataSessionLocal", Session)
    
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    return engine, Session


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_subject_cohort_import_preview_and_apply(tmp_path, monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        subjects = [
            schema.Subject(subject_code="S001", patient_name="Alice"),
            schema.Subject(subject_code="S002", patient_name="Bob"),
        ]
        cohorts = [
            schema.Cohort(name="Alpha", owner="Owner A", path="/alpha", description="A", is_active=1),
            schema.Cohort(name="Beta", owner="Owner B", path="/beta", description="B", is_active=1),
        ]
        session.add_all(subjects + cohorts)
        session.flush()
        session.add(schema.SubjectCohort(subject_id=subjects[0].subject_id, cohort_id=cohorts[0].cohort_id))
        session.commit()

    csv_path = tmp_path / "subject_cohorts.csv"
    _write_csv(
        csv_path,
        [
            {"subject_code": "S001"},
            {"subject_code": "S002"},
            {"subject_code": "S999"},
            {"subject_code": ""},
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    payload = {
        "filePath": str(csv_path),
        "subjectField": {"column": "subject_code"},
        "staticCohortName": "Alpha",
        "options": {"membershipMode": "append"},
    }

    preview_response = client.post("/api/metadata/imports/subject-cohorts/preview", json=payload)
    assert preview_response.status_code == 200
    preview = preview_response.json()
    assert preview["totalRows"] == 4
    assert preview["processedRows"] == 3  # one row skipped due to blank subject code
    assert preview["skippedRows"] == 1

    first_row = preview["rows"][0]
    assert first_row["subjectCode"] == "S001"
    assert first_row["cohortName"] == "Alpha"
    assert first_row["subjectExists"] is True
    assert first_row["cohortExists"] is True
    assert first_row["alreadyMember"] is True

    apply_response = client.post("/api/metadata/imports/subject-cohorts/apply", json=payload)
    assert apply_response.status_code == 200
    result = apply_response.json()
    assert result["membershipsInserted"] == 1  # S002 -> Alpha
    assert result["membershipsExisting"] == 1  # S001 already linked
    assert result["subjectsMissing"] == 1
    assert result["cohortsMissing"] == 0
    assert result["rowsSkipped"] == 1

    with Session() as session:
        memberships = session.execute(
            select(schema.Subject.subject_code, schema.Cohort.name)
            .select_from(schema.SubjectCohort)
            .join(schema.Subject, schema.Subject.subject_id == schema.SubjectCohort.subject_id)
            .join(schema.Cohort, schema.Cohort.cohort_id == schema.SubjectCohort.cohort_id)
        ).all()
        pair_set = {(row[0], row[1]) for row in memberships}
        assert pair_set == {("S001", "Alpha"), ("S002", "Alpha")}

    replace_csv = tmp_path / "subject_cohorts_replace.csv"
    _write_csv(
        replace_csv,
        [
            {"subject_code": "S001", "cohort_name": "Beta"},
        ],
    )

    replace_payload = {
        "filePath": str(replace_csv),
        "subjectField": {"column": "subject_code"},
        "staticCohortName": "Beta",
        "options": {"membershipMode": "replace"},
    }

    replace_response = client.post("/api/metadata/imports/subject-cohorts/apply", json=replace_payload)
    assert replace_response.status_code == 200
    replace_result = replace_response.json()
    assert replace_result["membershipsInserted"] == 1
    assert replace_result["membershipsExisting"] == 0

    with Session() as session:
        memberships = session.execute(
            select(schema.Subject.subject_code, schema.Cohort.name)
            .select_from(schema.SubjectCohort)
            .join(schema.Subject, schema.Subject.subject_id == schema.SubjectCohort.subject_id)
            .join(schema.Cohort, schema.Cohort.cohort_id == schema.SubjectCohort.cohort_id)
        ).all()
        pair_set = {(row[0], row[1]) for row in memberships}
        assert pair_set == {("S001", "Beta"), ("S002", "Alpha")}

    static_csv = tmp_path / "subject_cohorts_static.csv"
    _write_csv(
        static_csv,
        [
            {"subject_code": "S002"},
        ],
    )

    static_payload = {
        "filePath": str(static_csv),
        "subjectField": {"column": "subject_code"},
        "staticCohortName": "Beta",
        "options": {"membershipMode": "append"},
    }

    static_response = client.post("/api/metadata/imports/subject-cohorts/apply", json=static_payload)
    assert static_response.status_code == 200
    static_result = static_response.json()
    assert static_result["membershipsInserted"] == 1

    with Session() as session:
        memberships = session.execute(
            select(schema.Subject.subject_code, schema.Cohort.name)
            .select_from(schema.SubjectCohort)
            .join(schema.Subject, schema.Subject.subject_id == schema.SubjectCohort.subject_id)
            .join(schema.Cohort, schema.Cohort.cohort_id == schema.SubjectCohort.cohort_id)
        ).all()
        pair_set = {(row[0], row[1]) for row in memberships}
        assert pair_set == {("S001", "Beta"), ("S002", "Alpha"), ("S002", "Beta")}


def test_cohort_update_preserves_required_fields(tmp_path, monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        cohort = schema.Cohort(name="ALS", owner="System", path="/als", description="ALS Cohort", is_active=1)
        session.add(cohort)
        session.commit()

    csv_path = tmp_path / "cohorts.csv"
    _write_csv(
        csv_path,
        [
            {"name": "ALS", "owner": "System", "path": "/als", "description": "ALS Cohort ki"},
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    payload = {
        "filePath": str(csv_path),
        "cohortFields": {
            "name": {"column": "name"},
            "owner": {"column": "owner"},
            "path": {"column": "path"},
            "description": {"column": "description"},
        },
        "options": {"skipBlankUpdates": True},
    }

    preview = client.post("/api/metadata/imports/cohorts/preview", json=payload)
    assert preview.status_code == 200
    preview_payload = preview.json()
    assert preview_payload["rows"][0]["existing"] is True

    response = client.post("/api/metadata/imports/cohorts/apply", json=payload)
    assert response.status_code == 200

    with Session() as session:
        rows = session.execute(
            select(schema.Cohort.name, schema.Cohort.owner, schema.Cohort.path, schema.Cohort.description)
            .where(func.lower(schema.Cohort.name) == "als")
        ).all()
        assert len(rows) == 1
        record = rows[0]
        assert record.owner == "System"
        assert record.path == "/als"
        assert record.description == "ALS Cohort ki"


def test_subject_cohort_manual_endpoints(tmp_path, monkeypatch):
    _, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        subject = schema.Subject(subject_code="S010", patient_name="Manual")
        cohort = schema.Cohort(name="ManualCohort", owner="Owner", path="/manual", description="", is_active=1)
        session.add_all([subject, cohort])
        session.flush()
        session.add(schema.SubjectCohort(subject_id=subject.subject_id, cohort_id=cohort.cohort_id))
        session.commit()

    app = server.create_app()
    client = TestClient(app)

    memberships_response = client.get("/api/metadata/subject-cohorts/S010")
    assert memberships_response.status_code == 200
    memberships_payload = memberships_response.json()
    assert memberships_payload["subjectCode"] == "S010"
    assert len(memberships_payload["memberships"]) == 1
    assert memberships_payload["memberships"][0]["cohortName"] == "ManualCohort"

    delete_response = client.request(
        "DELETE",
        "/api/metadata/subject-cohorts",
        json={"subjectCode": "S010", "cohortName": "ManualCohort"},
    )
    assert delete_response.status_code == 204

    empty_response = client.get("/api/metadata/subject-cohorts/S010")
    assert empty_response.status_code == 200
    assert empty_response.json()["memberships"] == []

    missing_subject_response = client.get("/api/metadata/subject-cohorts/UNKNOWN")
    assert missing_subject_response.status_code == 404

    missing_delete_response = client.request(
        "DELETE",
        "/api/metadata/subject-cohorts",
        json={"subjectCode": "S010", "cohortName": "ManualCohort"},
    )
    assert missing_delete_response.status_code == 404


def test_subject_cohort_fields_endpoint(monkeypatch):
    _setup_metadata_db(monkeypatch)
    app = server.create_app()
    client = TestClient(app)

    response = client.get("/api/metadata/imports/subject-cohorts/fields")
    assert response.status_code == 200
    payload = response.json()
    assert payload["subjectField"]["name"] == "subject_code"
    assert "cohortField" not in payload


def test_metadata_cohort_listing(tmp_path, monkeypatch):
    _, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        cohorts = [
            schema.Cohort(name="Alpha", owner="Owner A", path="/alpha", description="A", is_active=1),
            schema.Cohort(name="Beta", owner="Owner B", path="/beta", description="B", is_active=0),
        ]
        session.add_all(cohorts)
        session.commit()

    app = server.create_app()
    client = TestClient(app)

    response = client.get("/api/metadata/cohorts")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    names = {entry["name"] for entry in payload}
    assert names == {"Alpha", "Beta"}
    alpha_entry = next(entry for entry in payload if entry["name"] == "Alpha")
    assert alpha_entry["isActive"] is True
