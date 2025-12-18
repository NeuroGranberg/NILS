from __future__ import annotations

import csv
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api import server
from api.routes import imports as imports_route
from api.routes import metadata_subjects as metadata_subjects_route
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


def test_subject_identifier_import_preview_apply(monkeypatch, tmp_path):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        subjects = [schema.Subject(subject_code="S001"), schema.Subject(subject_code="S002"), schema.Subject(subject_code="S999")]
        id_types = [
            schema.IdType(id_type_name="MRN", description="Medical Record Number"),
            schema.IdType(id_type_name="Research ID"),
        ]
        session.add_all(subjects + id_types)
        session.flush()
        session.add(
            schema.SubjectOtherIdentifier(
                subject_id=subjects[0].subject_id,
                id_type_id=id_types[0].id_type_id,
                other_identifier="MRN-OLD",
            )
        )
        session.commit()
        mrn_id = id_types[0].id_type_id

    csv_path = tmp_path / "identifiers.csv"
    _write_csv(
        csv_path,
        [
            {"subject_code": "S001", "identifier": "MRN-123"},
            {"subject_code": "S002", "identifier": "MRN-456"},
            {"subject_code": "S888", "identifier": "MRN-789"},
            {"subject_code": "", "identifier": "EMPTY"},
        ],
    )

    app = server.create_app()
    client = TestClient(app)

    payload = {
        "filePath": str(csv_path),
        "subjectField": {"column": "subject_code"},
        "identifierField": {"column": "identifier"},
        "staticIdTypeId": mrn_id,
        "options": {"mode": "append"},
    }

    preview_response = client.post(
        "/api/metadata/imports/subject-other-identifiers/preview", json=payload
    )
    assert preview_response.status_code == 200
    preview = preview_response.json()
    assert preview["totalRows"] == 4
    assert preview["processedRows"] == 3
    assert preview["skippedRows"] == 1
    assert preview["identifiersInserted"] == 2

    apply_response = client.post(
        "/api/metadata/imports/subject-other-identifiers/apply", json=payload
    )
    assert apply_response.status_code == 200
    result = apply_response.json()
    assert result["identifiersInserted"] == 2  # S001 replaced, S002 new
    assert result["identifiersUpdated"] == 1  # S001 updated value
    assert result["subjectsMissing"] == 1  # S888

    replace_csv = tmp_path / "identifiers_replace.csv"
    _write_csv(
        replace_csv,
        [
            {"subject_code": "S002", "identifier": "MRN-000"},
        ],
    )

    replace_payload = {
        "filePath": str(replace_csv),
        "subjectField": {"column": "subject_code"},
        "identifierField": {"column": "identifier"},
        "staticIdTypeId": mrn_id,
        "options": {"mode": "replace"},
    }

    replace_response = client.post(
        "/api/metadata/imports/subject-other-identifiers/apply", json=replace_payload
    )
    assert replace_response.status_code == 200
    replace_result = replace_response.json()
    assert replace_result["identifiersInserted"] == 1
    assert replace_result["identifiersUpdated"] == 0

    with Session() as session:
        identifiers = session.execute(
            select(schema.Subject.subject_code, schema.SubjectOtherIdentifier.other_identifier)
            .select_from(schema.SubjectOtherIdentifier)
            .join(schema.Subject, schema.Subject.subject_id == schema.SubjectOtherIdentifier.subject_id)
        ).all()
        values = {row[0]: row[1] for row in identifiers}
        assert values == {"S002": "MRN-000"}


def test_subject_identifier_manual_endpoints(monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        subject = schema.Subject(subject_code="S101")
        session.add(subject)
        session.flush()
        id_type = schema.IdType(id_type_name="National ID")
        session.add(id_type)
        session.commit()
        subject_id = subject.subject_id
        id_type_id = id_type.id_type_id

    app = server.create_app()
    client = TestClient(app)

    detail_response = client.get("/api/metadata/subject-other-identifiers/S101")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["subjectExists"] is True
    assert any(item["idTypeId"] == id_type_id for item in detail["identifiers"])

    upsert_response = client.post(
        "/api/metadata/subject-other-identifiers",
        json={"subjectCode": "S101", "idTypeId": id_type_id, "identifierValue": "NAT-001"},
    )
    assert upsert_response.status_code == 200
    upsert_payload = upsert_response.json()
    assert upsert_payload["identifierValue"] == "NAT-001"

    delete_response = client.request(
        "DELETE",
        "/api/metadata/subject-other-identifiers",
        json={"subjectCode": "S101", "idTypeId": id_type_id},
    )
    assert delete_response.status_code == 204

    missing_delete = client.request(
        "DELETE",
        "/api/metadata/subject-other-identifiers",
        json={"subjectCode": "S101", "idTypeId": id_type_id},
    )
    assert missing_delete.status_code == 404

    with Session() as session:
        count = session.execute(
            select(func.count()).select_from(schema.SubjectOtherIdentifier).where(
                schema.SubjectOtherIdentifier.subject_id == subject_id
            )
        ).scalar_one()
        assert count == 0


def test_subject_identifier_import_invalid_id_type(monkeypatch, tmp_path):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        session.add(schema.Subject(subject_code="S001"))
        session.commit()

    csv_path = tmp_path / "identifiers.csv"
    _write_csv(csv_path, [{"subject_code": "S001", "identifier": "VALUE"}])

    app = server.create_app()
    client = TestClient(app)

    payload = {
        "filePath": str(csv_path),
        "subjectField": {"column": "subject_code"},
        "identifierField": {"column": "identifier"},
        "staticIdTypeId": 999,
    }

    response = client.post("/api/metadata/imports/subject-other-identifiers/preview", json=payload)
    assert response.status_code == 400
    assert "Identifier type not found" in response.json()["detail"]
