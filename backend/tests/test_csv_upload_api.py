from fastapi.testclient import TestClient

from api import server
from api.utils import csv as csv_utils


def test_csv_upload_and_columns(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    # Patch the actual module where CSV_UPLOAD_DIR is used
    monkeypatch.setattr(csv_utils, "CSV_UPLOAD_DIR", upload_dir)
    upload_dir.mkdir(parents=True, exist_ok=True)

    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)

    app = server.create_app()
    client = TestClient(app)

    files = {"file": ("mapping.csv", b"old,new\nA,B\n", "text/csv")}

    response = client.post("/api/uploads/csv", files=files)
    assert response.status_code == 200
    data = response.json()
    token = data["token"]
    assert data["columns"] == ["old", "new"]
    stored_csv = upload_dir / f"{token}.csv"
    assert stored_csv.exists()

    columns_response = client.get(f"/api/uploads/csv/{token}/columns")
    assert columns_response.status_code == 200
    columns_data = columns_response.json()
    assert columns_data["columns"] == ["old", "new"]
