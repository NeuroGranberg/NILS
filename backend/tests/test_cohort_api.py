from contextlib import contextmanager
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api import server
from api.routes import metadata_cohorts as metadata_cohorts_route
from cohorts.models import Base, Cohort, CreateCohortPayload
from cohorts.service import cohort_service
from metadata_db import schema
from metadata_db import session as metadata_session


def test_create_cohort_normalizes_and_upserts(tmp_path: Path, monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
    Base.metadata.create_all(engine)

    @contextmanager
    def _test_session_scope():
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("cohorts.service.engine", engine)
    monkeypatch.setattr("cohorts.service.session_scope", _test_session_scope)
    original_initialized = cohort_service._initialized
    cohort_service._initialized = False  # ensure tables are created on the new engine

    try:
        source_root = tmp_path / "input"
        source_root.mkdir()

        payload = CreateCohortPayload(
            name="NeuroStudy",
            source_path=str(source_root),
            description="Initial description",
            tags=["demo"],
            anonymization_enabled=False,
        )

        created = cohort_service.create_cohort(payload)
        assert created.name == "neurostudy"

        updated_payload = CreateCohortPayload(
            name="NEUROStudy",
            source_path=str(source_root / "updated"),
            description="Updated description",
            tags=["updated"],
            anonymization_enabled=True,
        )

        updated = cohort_service.create_cohort(updated_payload)
        assert updated.id == created.id
        assert updated.name == "neurostudy"
        assert updated.description == "Updated description"
        assert updated.anonymization_enabled is True
        assert updated.tags == ["updated"]

        with SessionLocal() as session:
            rows = session.execute(select(Cohort)).scalars().all()
            assert len(rows) == 1
            record = rows[0]
            assert record.name == "neurostudy"
            assert record.description == "Updated description"
            assert record.source_path.endswith("updated")
            assert record.anonymization_enabled is True
            assert record.tags == ["updated"]
    finally:
        cohort_service._initialized = original_initialized


def _setup_metadata_api(monkeypatch):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    schema.Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

    # Patch modules where the database connection is actually used
    monkeypatch.setattr(metadata_session, "engine", engine)
    monkeypatch.setattr(metadata_session, "SessionLocal", SessionLocal)
    
    # Patch route modules that import engine/SessionLocal directly
    monkeypatch.setattr(metadata_cohorts_route, "metadata_engine", engine)
    monkeypatch.setattr(metadata_cohorts_route, "MetadataSessionLocal", SessionLocal)
    
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    return engine, SessionLocal


def test_metadata_cohort_upsert_endpoint(monkeypatch):
    _, SessionLocal = _setup_metadata_api(monkeypatch)

    app = server.create_app()
    client = TestClient(app)

    create_payload = {
        "name": "NeuroStudy",
        "owner": "Clinical Ops",
        "path": "/data/als",
        "description": "ALS cohort",
    }

    create_response = client.post("/api/metadata/cohorts", json=create_payload)
    assert create_response.status_code == 201
    created = create_response.json()
    assert created["name"] == "neurostudy"
    assert created["owner"] == "Clinical Ops"

    with SessionLocal() as session:
        record = session.execute(
            select(schema.Cohort).where(schema.Cohort.cohort_id == created["cohortId"])
        ).scalar_one()
        assert record.name == "neurostudy"
        assert record.owner == "Clinical Ops"
        assert record.path == "/data/als"
        assert record.description == "ALS cohort"
        assert record.is_active == 1

    update_payload = {
        "owner": "Data Ops",
        "path": "/data/als-updated",
        "description": "Updated description",
        "isActive": False,
    }

    update_response = client.put("/api/metadata/cohorts/neurostudy", json=update_payload)
    assert update_response.status_code == 200
    updated = update_response.json()
    assert updated["owner"] == "Data Ops"
    assert updated["path"] == "/data/als-updated"
    assert updated["isActive"] is False

    with SessionLocal() as session:
        record = session.execute(
            select(schema.Cohort).where(schema.Cohort.cohort_id == created["cohortId"])
        ).scalar_one()
        assert record.owner == "Data Ops"
        assert record.path == "/data/als-updated"
        assert record.is_active == 0

    invalid_response = client.post(
        "/api/metadata/cohorts",
        json={"name": "New Cohort", "owner": " ", "path": "/data/new"},
    )
    assert invalid_response.status_code == 400
