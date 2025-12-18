from __future__ import annotations

import copy
import datetime as dt
from pathlib import Path
from types import SimpleNamespace
import importlib

from fastapi.testclient import TestClient

from api import server
from api.routes import backups as backups_route
from api.routes import database as database_route
from api.routes import metadata_tables as metadata_tables_route
from api.routes import id_types as id_types_route
from api.routes import cohorts as cohorts_route
from api.routes import jobs as jobs_route
from jobs.models import JobDTO, JobStatus
import metadata_db.schema as metadata_schema
from metadata_db.schema import Subject
from metadata_db import session as metadata_session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


def test_metadata_backup_endpoints(tmp_path, monkeypatch):
    backups_dir = tmp_path / "backups"
    backups_dir.mkdir()
    existing = backups_dir / "metadata_20240101T000000Z.dump"
    existing.write_text("one")

    restore_calls: list[Path] = []

    class DummyManager:
        def __init__(self) -> None:
            self.backup_settings = SimpleNamespace(directory=backups_dir)
            self.last_note: str | None = None

        def list_backups(self):
            return sorted(backups_dir.glob("metadata_*.dump"))

        def create_backup(self, directory: str | Path | None = None, *, note: str | None = None) -> Path:
            self.last_note = note
            created = backups_dir / "metadata_created.dump"
            created.write_text("created")
            return created

        def restore(self, dump_path: Path) -> Path:
            restore_calls.append(dump_path)
            return dump_path

        def latest_backup(self) -> Path | None:
            backups = self.list_backups()
            return backups[-1] if backups else None

        def ensure_backup_path(self, candidate: str | Path | None) -> Path:
            if candidate:
                return Path(candidate)
            latest = self.latest_backup()
            if not latest:
                raise RuntimeError("no backups")
            return latest

    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: "1.0")
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)
    
    # Patch the backups route module
    monkeypatch.setattr(backups_route, "MetadataBackupManager", lambda: DummyManager())

    app = server.create_app()
    app.state.submit_restore_job = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    client = TestClient(app)

    list_response = client.get("/api/metadata/backups")
    assert list_response.status_code == 200
    listed = list_response.json()
    assert any(entry["filename"] == existing.name for entry in listed)
    assert listed[0]["database"] == "metadata"

    create_response = client.post("/api/metadata/backups")
    assert create_response.status_code == 200
    created = create_response.json()
    assert created["filename"] == "metadata_created.dump"
    assert created["database"] == "metadata"

    restore_response = client.post("/api/metadata/restore", json={"path": str(existing)})
    assert restore_response.status_code == 202
    restore_payload = restore_response.json()
    assert restore_payload["backup"]["filename"] == existing.name
    assert restore_payload["job"]["stageId"] == "database_restore"
    assert restore_payload["job"]["status"] == "completed"
    assert restore_calls and restore_calls[0] == existing

    # Restore without explicit path falls back to latest backup
    latest_response = client.post("/api/metadata/restore")
    assert latest_response.status_code == 202
    latest_payload = latest_response.json()
    assert latest_payload["backup"]["filename"] == "metadata_created.dump"
    assert latest_payload["backup"]["database"] == "metadata"
    assert latest_payload["job"]["status"] == "completed"


def test_extract_run_returns_job_payload(monkeypatch, tmp_path):
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir()

    class DummyCohort:
        def __init__(self, data: dict) -> None:
            self._data = data

        def __getattr__(self, item: str):  # pragma: no cover - simple attribute proxy
            return self._data[item]

        def model_dump(self, mode: str = "json") -> dict:
            return copy.deepcopy(self._data)

    class DummyCohortService:
        def __init__(self) -> None:
            now = dt.datetime.now(dt.timezone.utc).isoformat()
            self.data = {
                "id": 1,
                "name": "ALS",
                "source_path": str(dataset_root),
                "stages": [
                    {
                        "id": "extract",
                        "title": "Metadata Extraction",
                        "description": "",
                        "status": "pending",
                        "progress": 0,
                        "runs": [],
                        "config": {},
                    }
                ],
                "total_subjects": 0,
                "total_sessions": 0,
                "total_series": 0,
                "completion_percentage": 0,
                "updated_at": now,
                "created_at": now,
                "anonymization_enabled": False,
                "tags": [],
                "status": "pending",
            }

        def list_cohorts(self):  # pragma: no cover - not used in assertion
            return [DummyCohort(self.data)]

        def get_cohort(self, cohort_id: int):
            if cohort_id != self.data["id"]:
                return None
            return DummyCohort(self.data)

        def create_cohort(self, payload):  # pragma: no cover - unused
            raise NotImplementedError

    class DummyJobService:
        def __init__(self) -> None:
            self.jobs: dict[int, dict] = {}
            self.counter = 0

        def _dto(self, job: dict) -> JobDTO:
            return JobDTO(
                id=job["id"],
                name=job["name"],
                stage=job["stage"],
                status=job["status"],
                progress=job["progress"],
                created_at=job["created_at"],
                started_at=job["started_at"],
                finished_at=job["finished_at"],
                last_error=job["last_error"],
                config=job["config"],
                metrics=job.get("metrics"),
            )

        def create_job(self, *, stage: str, config: dict, name: str | None = None) -> JobDTO:
            self.counter += 1
            job = {
                "id": self.counter,
                "name": name,
                "stage": stage,
                "status": JobStatus.QUEUED,
                "progress": 0,
                "created_at": dt.datetime.now(dt.timezone.utc),
                "started_at": None,
                "finished_at": None,
                "last_error": None,
                "config": dict(config),
                "metrics": None,
            }
            self.jobs[job["id"]] = job
            return self._dto(job)

        def mark_running(self, job_id: int) -> None:
            job = self.jobs[job_id]
            job["status"] = JobStatus.RUNNING
            job["started_at"] = dt.datetime.now(dt.timezone.utc)

        def mark_completed(self, job_id: int) -> None:
            job = self.jobs[job_id]
            job["status"] = JobStatus.COMPLETED
            job["finished_at"] = dt.datetime.now(dt.timezone.utc)
            job["progress"] = 100

        def mark_failed(self, job_id: int, error: str) -> None:
            job = self.jobs[job_id]
            job["status"] = JobStatus.FAILED
            job["finished_at"] = dt.datetime.now(dt.timezone.utc)
            job["last_error"] = error

        def update_progress(self, job_id: int, progress: int) -> None:
            self.jobs[job_id]["progress"] = progress

        def update_metrics(self, job_id: int, metrics: dict) -> None:
            job = self.jobs[job_id]
            merged = dict(job.get("metrics") or {})
            merged.update(metrics)
            job["metrics"] = merged

        def register_control(self, job_id: int, control) -> None:
            self.jobs.setdefault(job_id, {})["control"] = control

        def unregister_control(self, job_id: int) -> None:
            self.jobs.get(job_id, {}).pop("control", None)

        def list_jobs(self, *, cohort_id: int | None = None, stage: str | None = None) -> list[JobDTO]:
            jobs = [self._dto(job) for job in self.jobs.values()]
            if cohort_id is not None:
                jobs = [job for job in jobs if job.config.get("cohort_id") == cohort_id]
            if stage is not None:
                jobs = [job for job in jobs if job.stage == stage]
            return jobs

        def list_jobs_for_stage(self, cohort_id: int, stage: str, limit: int = 10) -> list[JobDTO]:
            jobs = [
                job
                for job in sorted(self.jobs.values(), key=lambda record: record["created_at"], reverse=True)
                if job["config"].get("cohort_id") == cohort_id and job["stage"] == stage
            ]
            return [self._dto(job) for job in jobs[:limit]]

        def get_job(self, job_id: int) -> JobDTO | None:
            job = self.jobs.get(job_id)
            return self._dto(job) if job else None

    dummy_cohort_service = DummyCohortService()
    dummy_job_service = DummyJobService()

    def dummy_setup(selected_root: Path):
        raw = selected_root / "derivatives" / "dcm-raw"
        original = selected_root / "derivatives" / "dcm-original"
        raw.mkdir(parents=True, exist_ok=True)
        original.mkdir(parents=True, exist_ok=True)
        return SimpleNamespace(source_path=original, output_path=raw, status=None)

    def dummy_run_extraction(config, progress, job_id, control=None):  # pragma: no cover - simple stub
        if progress:
            progress(1, 3)
            progress(3, 3)
    
        return SimpleNamespace(
            total_subjects=3,
            baseline_completed=0,
            completed_total=3,
            metrics={"subjects": 1, "studies": 2, "series": 3, "instances": 4},
        )

    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: "1.0.0")
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)
    
    # Patch route modules where services are used
    monkeypatch.setattr(cohorts_route, "cohort_service", dummy_cohort_service)
    monkeypatch.setattr(cohorts_route, "job_service", dummy_job_service)
    monkeypatch.setattr(cohorts_route, "run_extraction", dummy_run_extraction)
    monkeypatch.setattr(cohorts_route, "get_cohort_metrics", lambda cohort_id: {
        "subjects": 99,
        "studies": 98,
        "series": 97,
        "instances": 96,
    })
    monkeypatch.setattr(jobs_route, "job_service", dummy_job_service)
    monkeypatch.setattr(jobs_route, "cohort_service", dummy_cohort_service)

    import anonymize.config as anonymize_config

    monkeypatch.setattr(anonymize_config, "setup_derivatives_folders", dummy_setup)

    app = server.create_app()
    client = TestClient(app)

    response = client.post("/api/cohorts/1/stages/extract/run", json={})
    assert response.status_code == 200
    payload = response.json()
    assert "job" in payload
    job_payload = payload["job"]
    assert job_payload["status"] == "completed"
    assert job_payload["cohortId"] == 1
    assert job_payload["progress"] == 100
    assert job_payload["metrics"] == {"subjects": 1, "studies": 2, "series": 3, "instances": 4}

    stage_after = dummy_cohort_service.data["stages"][0]
    assert stage_after["status"] == "completed"
    assert stage_after.get("runs")
    assert stage_after["runs"][-1]["status"] == "completed"
    assert stage_after["runs"][-1]["metrics"] == {"subjects": 1, "studies": 2, "series": 3, "instances": 4}

    jobs_response = client.get("/api/jobs")
    assert jobs_response.status_code == 200
    jobs_body = jobs_response.json()
    assert len(jobs_body) == 1
    assert jobs_body[0]["status"] == "completed"
    assert jobs_body[0]["metrics"] == {"subjects": 1, "studies": 2, "series": 3, "instances": 4}

    cohort_response = client.get("/api/cohorts/1")
    assert cohort_response.status_code == 200
    cohort_body = cohort_response.json()
    assert cohort_body["extract_job"]["status"] == "completed"
    assert cohort_body["extract_job"]["metrics"] == {"subjects": 1, "studies": 2, "series": 3, "instances": 4}
    assert cohort_body["extract_history"], "Extract history should include the recent run"
    assert cohort_body["extract_history"][0]["metrics"] == {"subjects": 1, "studies": 2, "series": 3, "instances": 4}


def test_database_backup_api(tmp_path, monkeypatch):
    metadata_dir = tmp_path / "metadata_backups"
    app_dir = tmp_path / "app_backups"
    metadata_dir.mkdir()
    app_dir.mkdir()

    class DummyManager:
        def __init__(self, base: Path, label: str) -> None:
            self.base = base
            self.label = label
            self.counter = 0
            self.last_note: str | None = None

        def list_backups(self):
            return sorted(self.base.glob("*.dump"))

        def create_backup(self, directory: str | Path | None = None, *, note: str | None = None) -> Path:
            target_dir = self.base if directory is None else Path(directory)
            target_dir.mkdir(parents=True, exist_ok=True)
            self.counter += 1
            name = f"{self.label}_{self.counter}.dump"
            path = target_dir / name
            path.write_text("backup")
            self.last_note = note
            return path

        def ensure_backup_path(self, candidate: str | Path | None) -> Path:
            if candidate:
                return Path(candidate)
            dumps = self.list_backups()
            if not dumps:
                raise RuntimeError("no backups")
            return dumps[-1]

        def restore(self, dump_path: Path) -> Path:
            return dump_path

        def delete_backup(self, candidate: str | Path) -> Path:
            path = Path(candidate)
            if not path.exists():
                raise RuntimeError("missing")
            path.unlink()
            sidecar = Path(f"{path}.json")
            if sidecar.exists():
                sidecar.unlink()
            return path

    managers = {
        database_route.DatabaseKey.METADATA: DummyManager(metadata_dir, "metadata"),
        database_route.DatabaseKey.APPLICATION: DummyManager(app_dir, "application"),
    }

    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: "1.0")
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)
    
    # Patch the database route module
    monkeypatch.setattr(database_route, "_get_backup_manager", lambda key: managers[key])

    app = server.create_app()
    app.state.submit_restore_job = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    client = TestClient(app)

    # Seed existing files
    existing_meta = metadata_dir / "metadata_seed.dump"
    existing_meta.write_text("seed")
    existing_app = app_dir / "application_seed.dump"
    existing_app.write_text("seed")
    (existing_app.parent / f"{existing_app.name}.json").write_text("{}")

    list_all = client.get("/api/database/backups")
    assert list_all.status_code == 200
    payload = list_all.json()
    assert {entry["database"] for entry in payload} == {"metadata", "application"}

    create_app_backup = client.post(
        "/api/database/backups",
        json={"database": "application"},
    )
    assert create_app_backup.status_code == 200
    created_name = create_app_backup.json()["filename"]
    assert created_name.startswith("application_")
    assert created_name.endswith(".dump")
    assert (app_dir / created_name).exists()

    metadata_note = "routine metadata backup"
    create_metadata_backup = client.post(
        "/api/database/backups",
        json={"database": "metadata", "note": metadata_note},
    )
    assert create_metadata_backup.status_code == 200
    assert managers[server.DatabaseKey.METADATA].last_note == metadata_note

    restore_app = client.post(
        "/api/database/restore",
        json={"database": "application", "path": str(existing_app)},
    )
    assert restore_app.status_code == 202
    restore_payload = restore_app.json()
    assert restore_payload["backup"]["filename"] == existing_app.name
    assert restore_payload["backup"]["database"] == "application"
    assert restore_payload["job"]["stageId"] == "database_restore"
    assert restore_payload["job"]["status"] == "completed"

    delete_response = client.request(
        "DELETE",
        "/api/database/backups",
        json={"database": "application", "path": str(existing_app)},
    )
    assert delete_response.status_code == 204
    assert not existing_app.exists()
    assert not (existing_app.parent / f"{existing_app.name}.json").exists()

    after_delete = client.get("/api/database/backups")
    assert after_delete.status_code == 200
    remaining = after_delete.json()
    assert all(entry["path"] != str(existing_app) for entry in remaining)


def test_database_summary_endpoint(monkeypatch):
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: "1.0")
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)
    call_order = []

    def fake_estimate(session, table):
        name = table.name
        call_order.append(name)
        if name == "subject":
            return 5
        if name == "study":
            return 10
        return 0

    # Patch the database route module
    monkeypatch.setattr(database_route, "_estimate_row_count", fake_estimate)
    monkeypatch.setattr(
        database_route,
        "_application_table_counts",
        lambda: {
            "cohorts": 3,
            "jobs": 7,
            "job_runs": 14,
            "anonymize_study_audits": 11,
            "anonymize_leaf_summaries": 9,
        },
    )

    app = server.create_app()
    client = TestClient(app)

    response = client.get("/api/database/summary")
    assert response.status_code == 200
    summaries = {item["database"]: item for item in response.json()}
    assert summaries["metadata"]["tables"]["subjects"] == 5
    assert summaries["application"]["tables"]["jobs"] == 7
    assert summaries["application"]["tables"]["anonymize_study_audits"] == 11
    assert summaries["application"]["tables"]["anonymize_leaf_summaries"] == 9


def test_metadata_tables_query(monkeypatch):
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: "1.0")
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    metadata_schema.Base.metadata.create_all(engine)
    TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
    
    # Patch modules where the database connection is used
    monkeypatch.setattr(metadata_session, "engine", engine)
    monkeypatch.setattr(metadata_session, "SessionLocal", TestSessionLocal)
    monkeypatch.setattr(metadata_tables_route, "metadata_engine", engine)
    monkeypatch.setattr(metadata_tables_route, "MetadataSessionLocal", TestSessionLocal)

    app = server.create_app()
    client = TestClient(app)

    with TestSessionLocal() as session:
        subject = Subject(subject_code="abc123")
        session.add(subject)
        session.commit()

    list_response = client.get("/api/metadata/tables")
    assert list_response.status_code == 200
    tables = list_response.json()
    subject_table = next(table for table in tables if table["name"] == "subject")
    assert subject_table["row_count"] == 1
    assert any(table["name"] == "cohort" for table in tables)
    cohort_table = next(table for table in tables if table["name"] == "cohort")
    assert cohort_table["row_count"] == 0

    columns_payload = [
        {
            "data": column["name"],
            "name": column["name"],
            "searchable": column["searchable"],
            "orderable": column["orderable"],
        }
        for column in subject_table["columns"]
    ]

    query_payload = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "order": [{"column": 0, "dir": "asc"}],
        "columns": columns_payload,
        "search": {"value": "", "regex": False},
    }

    query_response = client.post("/api/metadata/tables/subject/query", json=query_payload)
    assert query_response.status_code == 200
    body = query_response.json()
    assert body["recordsTotal"] == 1
    assert body["recordsFiltered"] == 1
    assert len(body["data"]) == 1
    row = body["data"][0]
    assert row["subject_code"] == "abc123"


def test_id_type_creation_endpoint(monkeypatch):
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: "1.0")
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    metadata_schema.Base.metadata.create_all(engine)
    TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
    
    # Patch modules where the database connection is used
    monkeypatch.setattr(metadata_session, "engine", engine)
    monkeypatch.setattr(metadata_session, "SessionLocal", TestSessionLocal)
    monkeypatch.setattr(id_types_route, "metadata_engine", engine)

    app = server.create_app()
    client = TestClient(app)

    list_response = client.get("/api/metadata/id-types")
    assert list_response.status_code == 200
    assert list_response.json() == {"items": []}

    create_response = client.post(
        "/api/metadata/id-types",
        json={"name": "KIALS", "description": "Kessler ALS registry"},
    )
    assert create_response.status_code == 201
    created_payload = create_response.json()
    assert created_payload["name"] == "KIALS"
    assert created_payload["description"] == "Kessler ALS registry"
    assert isinstance(created_payload["id"], int)

    list_after = client.get("/api/metadata/id-types")
    assert list_after.status_code == 200
    after_items = list_after.json()["items"]
    assert len(after_items) == 1
    assert after_items[0]["name"] == "KIALS"

    duplicate_response = client.post("/api/metadata/id-types", json={"name": "kials"})
    assert duplicate_response.status_code == 409

    empty_response = client.post("/api/metadata/id-types", json={"name": "   "})
    assert empty_response.status_code == 400
