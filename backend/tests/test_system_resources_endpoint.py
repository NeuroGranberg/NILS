from api import server
from fastapi.testclient import TestClient


def test_system_resources_endpoint_returns_recommendations(monkeypatch):
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)
    
    app = server.create_app()
    client = TestClient(app)

    response = client.get("/api/system/resources")
    assert response.status_code == 200

    data = response.json()
    assert data["cpu_count"] >= 1
    assert data["recommended_processes"] >= 1
    assert data["recommended_workers"] >= 1
    assert data["memory_total"] >= data["memory_available"] >= 0
    assert data["recommended_queue_depth"] >= 10
    assert data["recommended_batch_size"] >= 10
    assert data["recommended_adaptive_min_batch"] <= data["recommended_adaptive_max_batch"]
    assert data["recommended_series_workers_per_subject"] >= 1
    assert data["recommended_db_writer_pool"] >= 1
    assert data["safe_instance_batch_rows"] >= data["recommended_batch_size"]
    assert data["max_workers_cap"] >= data["recommended_workers"]
    assert data["max_batch_cap"] >= data["recommended_batch_size"]
    assert data["max_queue_cap"] >= data["recommended_queue_depth"]
    assert data["max_db_writer_pool_cap"] >= data["recommended_db_writer_pool"]
