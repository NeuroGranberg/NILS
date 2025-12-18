from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api import server
from api.routes import id_types as id_types_route
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
    monkeypatch.setattr(id_types_route, "metadata_engine", engine)
    
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    return engine, Session


def _create_subject_with_identifier(session, id_type: schema.IdType) -> None:
    subject = schema.Subject(subject_code="S100", patient_name="Identifier Test")
    session.add(subject)
    session.flush()
    session.add(
        schema.SubjectOtherIdentifier(
            subject_id=subject.subject_id,
            id_type_id=id_type.id_type_id,
            other_identifier="ABC-123",
        )
    )


def test_id_type_list_and_create(monkeypatch):
    _setup_metadata_db(monkeypatch)

    app = server.create_app()
    client = TestClient(app)

    response = client.get("/api/metadata/id-types")
    assert response.status_code == 200
    assert response.json() == {"items": []}

    create_payload = {"name": "MRN", "description": "Medical Record Number"}
    create_response = client.post("/api/metadata/id-types", json=create_payload)
    assert create_response.status_code == 201
    created = create_response.json()
    assert created["name"] == "MRN"
    assert created["description"] == "Medical Record Number"
    assert created["identifiersCount"] == 0

    duplicate_response = client.post("/api/metadata/id-types", json={"name": "mrn"})
    assert duplicate_response.status_code == 409

    list_response = client.get("/api/metadata/id-types")
    assert list_response.status_code == 200
    items = list_response.json()["items"]
    assert len(items) == 1
    assert items[0]["name"] == "MRN"


def test_id_type_update_duplicate_and_not_found(monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        record_a = schema.IdType(id_type_name="Primary", description="A")
        record_b = schema.IdType(id_type_name="Secondary", description="B")
        session.add_all([record_a, record_b])
        session.commit()
        record_a_id = record_a.id_type_id

    app = server.create_app()
    client = TestClient(app)

    update_response = client.put(
        f"/api/metadata/id-types/{record_a_id}",
        json={"name": "Primary Updated", "description": "Updated"},
    )
    assert update_response.status_code == 200
    payload = update_response.json()
    assert payload["name"] == "Primary Updated"
    assert payload["description"] == "Updated"

    conflict_response = client.put(
        f"/api/metadata/id-types/{record_a_id}",
        json={"name": "secondary"},
    )
    assert conflict_response.status_code == 409

    missing_response = client.put("/api/metadata/id-types/9999", json={"name": "Ghost"})
    assert missing_response.status_code == 404


def test_id_type_delete_cascade(monkeypatch):
    engine, Session = _setup_metadata_db(monkeypatch)

    with Session() as session:
        id_type = schema.IdType(id_type_name="Legacy", description="Legacy identifiers")
        session.add(id_type)
        session.flush()
        id_type_id = id_type.id_type_id
        _create_subject_with_identifier(session, id_type)
        session.commit()

    app = server.create_app()
    client = TestClient(app)

    delete_response = client.delete(f"/api/metadata/id-types/{id_type_id}")
    assert delete_response.status_code == 200
    payload = delete_response.json()
    assert payload == {
        "id": id_type_id,
        "name": "Legacy",
        "identifiersDeleted": 1,
    }

    with Session() as session:
        remaining = session.execute(select(schema.IdType)).scalars().all()
        assert remaining == []
        identifier_rows = session.execute(select(schema.SubjectOtherIdentifier)).scalars().all()
        assert identifier_rows == []
