from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from api import server
from cohorts.service import cohort_service
from cohorts.models import CreateCohortPayload


def test_folder_examples_endpoint_returns_relative_paths(tmp_path: Path, monkeypatch):
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    source_root = tmp_path / "cohort"
    (source_root / "subject1" / "visitA").mkdir(parents=True)
    (source_root / "subject1" / "visitA" / "scan1.dcm").write_text("data")

    payload = CreateCohortPayload(
        name=f"Sample Cohort {uuid4()}",
        source_path=str(source_root),
        tags=[],
        anonymization_enabled=True,
    )
    cohort = cohort_service.create_cohort(payload)

    app = server.create_app()
    client = TestClient(app)

    response = client.get(f"/api/cohorts/{cohort.id}/examples/folders?limit=1")
    assert response.status_code == 200

    data = response.json()
    assert "paths" in data
    assert len(data["paths"]) <= 1
    if data["paths"]:
        first_segment = data["paths"][0].split('/')[0]
        assert first_segment == "subject1"


def test_folder_examples_endpoint_handles_missing_cohort(monkeypatch):
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)
    
    app = server.create_app()
    client = TestClient(app)

    response = client.get("/api/cohorts/9999/examples/folders")
    assert response.status_code == 404
