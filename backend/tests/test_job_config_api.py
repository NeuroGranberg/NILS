import datetime as dt

from fastapi.testclient import TestClient

from api import server
from api.routes import jobs as jobs_route
from cohorts.models import CohortDTO
from jobs.models import JobDTO, JobStatus


def _build_job(status: JobStatus = JobStatus.PAUSED) -> JobDTO:
    now = dt.datetime.now(dt.timezone.utc)
    return JobDTO(
        id=1,
        name="extract",
        stage="extract",
        status=status,
        progress=25,
        created_at=now,
        started_at=now,
        finished_at=None,
        last_error=None,
        config={
            "cohort_id": 101,
            "max_workers": 4,
            "batch_size": 100,
            "queue_size": 10,
            "series_workers_per_subject": 1,
            "adaptive_batching_enabled": False,
            "target_tx_ms": 200,
            "min_batch_size": 50,
            "max_batch_size": 1000,
        },
        metrics=None,
    )


def _build_cohort() -> CohortDTO:
    now = dt.datetime.now(dt.timezone.utc)
    return CohortDTO(
        id=101,
        name="als",
        source_path="/data/als",
        description=None,
        tags=[],
        anonymization_enabled=False,
        created_at=now,
        updated_at=now,
        status="idle",
        total_subjects=0,
        total_sessions=0,
        total_series=0,
        completion_percentage=0,
        stages=[
            {
                "id": "extract",
                "status": "paused",
                "progress": 25,
                "job_id": 1,
                "runs": [],
                "config": {
                    "maxWorkers": 4,
                    "batchSize": 100,
                    "queueSize": 10,
                    "seriesWorkersPerSubject": 1,
                    "adaptiveBatchingEnabled": False,
                    "adaptiveTargetTxMs": 200,
                    "adaptiveMinBatchSize": 50,
                    "adaptiveMaxBatchSize": 1000,
                },
            }
        ],
    )


class DummyJobService:
    def __init__(self, job: JobDTO) -> None:
        self.job = job

    def get_job(self, job_id: int) -> JobDTO | None:
        return self.job if job_id == self.job.id else None

    def update_config(self, job_id: int, config: dict) -> JobDTO:
        model = self.job.model_copy(update={"config": config})
        self.job = model
        return self.job


class DummyCohortService:
    def __init__(self, cohort: CohortDTO) -> None:
        self.cohort = cohort

    def get_cohort(self, cohort_id: int) -> CohortDTO | None:
        return self.cohort if cohort_id == self.cohort.id else None

    def list_cohorts(self) -> list[CohortDTO]:
        return [self.cohort]


class DummyPipelineService:
    """Mock pipeline service for testing config updates."""
    
    def __init__(self):
        self.step_configs: dict[tuple[int, str, str], dict] = {}
        self.update_calls: list[tuple[int, str, str, dict]] = []
    
    def get_step_config(self, cohort_id: int, stage_id: str, step_id: str) -> dict | None:
        return self.step_configs.get((cohort_id, stage_id, step_id), {
            "maxWorkers": 4,
            "batchSize": 100,
            "queueSize": 10,
        })
    
    def update_step_config(self, cohort_id: int, stage_id: str, step_id: str, config: dict) -> None:
        self.update_calls.append((cohort_id, stage_id, step_id, config))
        self.step_configs[(cohort_id, stage_id, step_id)] = config


def _build_app(monkeypatch, job_status: JobStatus = JobStatus.PAUSED) -> tuple[TestClient, DummyJobService, DummyCohortService, DummyPipelineService]:
    dummy_job_service = DummyJobService(_build_job(status=job_status))
    dummy_cohort_service = DummyCohortService(_build_cohort())
    dummy_pipeline_service = DummyPipelineService()
    
    # Patch the route module where services are actually used
    monkeypatch.setattr(jobs_route, "job_service", dummy_job_service)
    monkeypatch.setattr(jobs_route, "cohort_service", dummy_cohort_service)
    
    # Patch the pipeline service import in _update_extract_stage_config
    import nils_dataset_pipeline
    monkeypatch.setattr(nils_dataset_pipeline, "nils_pipeline_service", dummy_pipeline_service)
    
    # Patch server-level startup functions
    monkeypatch.setattr(server, "metadata_bootstrap", lambda: None)
    monkeypatch.setattr(server, "reconcile_stage_jobs", lambda: None)
    
    app = server.create_app()
    return TestClient(app), dummy_job_service, dummy_cohort_service, dummy_pipeline_service


def test_update_extract_job_config(monkeypatch):
    client, job_service, cohort_service, pipeline_service = _build_app(monkeypatch)

    response = client.patch(
        "/api/jobs/1/config",
        json={"maxWorkers": 8, "batchSize": 250, "adaptiveBatchingEnabled": True, "adaptiveTargetTxMs": 150},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["max_workers"] == 8
    assert payload["config"]["batch_size"] == 250
    assert payload["config"]["adaptive_batching_enabled"] is True
    assert payload["config"]["target_tx_ms"] == 150

    assert job_service.job.config["max_workers"] == 8
    # Pipeline service should receive update call
    assert pipeline_service.update_calls, "Pipeline service should receive config updates"
    last_config = pipeline_service.update_calls[-1][3]  # (cohort_id, stage_id, step_id, config)
    assert last_config["maxWorkers"] == 8
    assert last_config["batchSize"] == 250
    assert last_config["adaptiveBatchingEnabled"] is True


def test_update_requires_paused_job(monkeypatch):
    client, _, _, _ = _build_app(monkeypatch, job_status=JobStatus.RUNNING)

    response = client.patch(
        "/api/jobs/1/config",
        json={"maxWorkers": 6},
    )

    assert response.status_code == 400
    assert response.json()["detail"].lower().startswith("job must be paused")


def test_update_db_writer_pool_size(monkeypatch):
    """Test that dbWriterPoolSize can be updated via the PATCH endpoint."""
    client, job_service, cohort_service, pipeline_service = _build_app(monkeypatch)

    response = client.patch(
        "/api/jobs/1/config",
        json={"dbWriterPoolSize": 4},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["db_writer_pool_size"] == 4

    assert job_service.job.config["db_writer_pool_size"] == 4
    assert pipeline_service.update_calls, "Pipeline service should receive config updates"
    last_config = pipeline_service.update_calls[-1][3]
    assert last_config["dbWriterPoolSize"] == 4


def test_update_process_pool_settings(monkeypatch):
    """Test that useProcessPool and processPoolWorkers can be updated."""
    client, job_service, cohort_service, pipeline_service = _build_app(monkeypatch)

    response = client.patch(
        "/api/jobs/1/config",
        json={"useProcessPool": False, "processPoolWorkers": 8},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["use_process_pool"] is False
    assert payload["config"]["process_pool_workers"] == 8

    assert job_service.job.config["use_process_pool"] is False
    assert job_service.job.config["process_pool_workers"] == 8
    last_config = pipeline_service.update_calls[-1][3]
    assert last_config["useProcessPool"] is False
    assert last_config["processPoolWorkers"] == 8


def test_update_series_workers_per_subject(monkeypatch):
    """Test that seriesWorkersPerSubject can be updated."""
    client, job_service, cohort_service, pipeline_service = _build_app(monkeypatch)

    response = client.patch(
        "/api/jobs/1/config",
        json={"seriesWorkersPerSubject": 4},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["series_workers_per_subject"] == 4

    assert job_service.job.config["series_workers_per_subject"] == 4
    last_config = pipeline_service.update_calls[-1][3]
    assert last_config["seriesWorkersPerSubject"] == 4


def test_update_all_new_fields_together(monkeypatch):
    """Test that all newly added fields can be updated together."""
    client, job_service, _, _ = _build_app(monkeypatch)

    response = client.patch(
        "/api/jobs/1/config",
        json={
            "dbWriterPoolSize": 3,
            "useProcessPool": True,
            "processPoolWorkers": 16,
            "seriesWorkersPerSubject": 2,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["db_writer_pool_size"] == 3
    assert payload["config"]["use_process_pool"] is True
    assert payload["config"]["process_pool_workers"] == 16
    assert payload["config"]["series_workers_per_subject"] == 2
