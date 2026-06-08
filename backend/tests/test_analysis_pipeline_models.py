"""DB-free tests for the analysis-pipeline ORM models + DTOs (frozen design §4).

There is NO running Postgres in this environment. These tests assert *table
registration* on ``cohorts.models.Base.metadata`` (column names, constraints,
the cross-Base ``use_alter`` FK to ``jobs.id``), the ``InputSource`` enum values,
and an in-memory model → DTO round-trip. No live DB / engine is touched.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from cohorts.models import Base

# Register the NilsDatasetPipelineStep mapper so configure_mappers() can resolve
# Cohort.pipeline_steps. The instantiation tests below (AnalysisPipelineRun(...),
# __mapper__.relationships) trigger a full mapper configuration across the shared
# app-DB Base; without this import the Cohort mapper fails to locate
# NilsDatasetPipelineStep. The sibling is now SQLite-safe (its JSONB columns use
# .with_variant(JSON(), "sqlite")), so this import no longer pollutes any
# downstream create_all test — mirrors cohort_body_part_qc/_fixtures.py.
from nils_dataset_pipeline import models as _nils_models  # noqa: F401
from analysis_pipeline.models import (
    AnalysisPipeline,
    AnalysisPipelineDTO,
    AnalysisPipelineRepo,
    AnalysisPipelineRepoDTO,
    AnalysisPipelineRun,
    AnalysisPipelineRunDTO,
    AnalysisPipelineRunStatus,
    InputSource,
)


# ---------------------------------------------------------------------------
# Table registration
# ---------------------------------------------------------------------------

THREE_TABLES = (
    "analysis_pipeline_repos",
    "analysis_pipelines",
    "analysis_pipeline_runs",
)


def test_three_tables_register_on_app_base():
    tables = Base.metadata.tables
    for name in THREE_TABLES:
        assert name in tables, f"{name} not registered on cohorts.models.Base"


def test_repo_columns_and_unique_constraint():
    table = Base.metadata.tables["analysis_pipeline_repos"]
    cols = set(table.columns.keys())
    assert {
        "id",
        "url",
        "sha",
        "default_branch",
        "version",
        "created_at",
        "updated_at",
    } <= cols
    # Unique on (url, sha).
    uniques = {
        tuple(sorted(c.name for c in con.columns))
        for con in table.constraints
        if con.__class__.__name__ == "UniqueConstraint"
    }
    assert ("sha", "url") in uniques


def test_pipeline_columns_fk_and_unique():
    table = Base.metadata.tables["analysis_pipelines"]
    cols = set(table.columns.keys())
    assert {
        "id",
        "repo_id",
        "slug",
        "name",
        "description",
        "work_unit",
        "analysis_level",
        "image_digest",
        "schema_version",
        "descriptor",
        "version",
        "created_at",
        "updated_at",
    } <= cols
    # FK repo_id -> analysis_pipeline_repos.id
    fk_targets = {fk.target_fullname for fk in table.foreign_keys}
    assert "analysis_pipeline_repos.id" in fk_targets
    # Unique on (repo_id, slug).
    uniques = {
        tuple(sorted(c.name for c in con.columns))
        for con in table.constraints
        if con.__class__.__name__ == "UniqueConstraint"
    }
    assert ("repo_id", "slug") in uniques


def test_run_columns_indexes_and_cross_base_job_fk():
    table = Base.metadata.tables["analysis_pipeline_runs"]
    cols = set(table.columns.keys())
    assert {
        "id",
        "pipeline_id",
        "input_source",
        "stack_ids",
        "external_path",
        "params",
        "repo_sha",
        "image_digest",
        "input_path",
        "output_path",
        "work_dir",
        "retain_input",
        "prefect_run_id",
        "provenance",
        "status",
        "last_error",
        "job_id",
        "created_at",
        "updated_at",
    } <= cols

    fk_targets = {fk.target_fullname for fk in table.foreign_keys}
    # pipeline_id -> analysis_pipelines.id
    assert "analysis_pipelines.id" in fk_targets
    # Cross-Base FK to jobs.id (use_alter, SET NULL).
    assert "jobs.id" in fk_targets

    # The jobs.id FK must be use_alter + ondelete SET NULL.
    job_fks = [fk for fk in table.foreign_keys if fk.target_fullname == "jobs.id"]
    assert len(job_fks) == 1
    job_fk = job_fks[0]
    assert job_fk.use_alter is True
    assert (job_fk.ondelete or "").upper() == "SET NULL"

    # Declared indexes on pipeline_id and status.
    index_cols = {
        tuple(c.name for c in idx.columns) for idx in table.indexes
    }
    assert ("pipeline_id",) in index_cols
    assert ("status",) in index_cols


def test_run_job_id_has_no_orm_relationship_to_job():
    # Cross-Base FK buys status mirroring only; there is NO ORM relationship to
    # Job (it lives on a different Base). Access is via job_service.get_job.
    rels = {rel.key for rel in AnalysisPipelineRun.__mapper__.relationships}
    assert "job" not in rels
    assert "pipeline" in rels


def test_descriptor_and_jsonb_columns_are_not_null_where_required():
    pipelines = Base.metadata.tables["analysis_pipelines"]
    assert pipelines.columns["descriptor"].nullable is False
    runs = Base.metadata.tables["analysis_pipeline_runs"]
    # stack_ids is DATA and nullable; params/provenance are NOT NULL.
    assert runs.columns["stack_ids"].nullable is True
    assert runs.columns["params"].nullable is False
    assert runs.columns["provenance"].nullable is False


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


def test_input_source_enum_values():
    assert InputSource.DB_SUBSET.value == "db_subset"
    assert InputSource.EXTERNAL_PATH.value == "external_path"
    # run_output is a forward-compat seam — NOT present in v1.
    assert not hasattr(InputSource, "RUN_OUTPUT")
    assert {m.value for m in InputSource} == {"db_subset", "external_path"}


def test_run_status_enum_values():
    expected = {
        "pending",
        "materializing",
        "launching",
        "running",
        "completed",
        "completed_with_warnings",
        "failed",
        "canceled",
    }
    assert {m.value for m in AnalysisPipelineRunStatus} == expected


# ---------------------------------------------------------------------------
# In-memory model instantiation + DTO round-trip (no DB)
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_repo_model_dto_round_trip():
    ts = _now()
    repo = AnalysisPipelineRepo(
        id=1,
        url="https://example.test/repo.git",
        sha="a" * 40,
        default_branch="main",
        version=1,
        created_at=ts,
        updated_at=ts,
    )
    dto = AnalysisPipelineRepoDTO.model_validate(repo)
    assert dto.id == 1
    assert dto.url == "https://example.test/repo.git"
    assert dto.sha == "a" * 40
    assert dto.version == 1


def test_pipeline_model_dto_round_trip():
    ts = _now()
    pipeline = AnalysisPipeline(
        id=5,
        repo_id=1,
        slug="mriqc",
        name="MRIQC",
        description="quality control",
        work_unit="subject",
        analysis_level="subject",
        image_digest="sha256:deadbeef",
        schema_version="0.5",
        descriptor={"name": "MRIQC", "schema-version": "0.5"},
        version=1,
        created_at=ts,
        updated_at=ts,
    )
    dto = AnalysisPipelineDTO.model_validate(pipeline)
    assert dto.slug == "mriqc"
    assert dto.work_unit == "subject"
    assert dto.descriptor["name"] == "MRIQC"
    assert dto.image_digest == "sha256:deadbeef"


def test_run_model_dto_round_trip():
    ts = _now()
    run = AnalysisPipelineRun(
        id=9,
        pipeline_id=5,
        input_source=InputSource.DB_SUBSET.value,
        stack_ids=[3, 1, 2],
        external_path=None,
        params={"flag": True},
        repo_sha="b" * 40,
        image_digest="sha256:cafe",
        input_path="/scratch/run-9/bids",
        output_path="/scratch/run-9/output",
        work_dir="/scratch/run-9/work",
        retain_input=True,
        prefect_run_id="stub-1234",
        provenance={"stack_ids": [3, 1, 2], "repo_sha": "b" * 40},
        status=AnalysisPipelineRunStatus.PENDING.value,
        last_error=None,
        job_id=42,
        created_at=ts,
        updated_at=ts,
    )
    dto = AnalysisPipelineRunDTO.model_validate(run)
    assert dto.input_source == "db_subset"
    assert dto.stack_ids == [3, 1, 2]
    assert dto.params == {"flag": True}
    assert dto.repo_sha == "b" * 40
    assert dto.status == "pending"
    assert dto.job_id == 42
    assert dto.provenance["repo_sha"] == "b" * 40


def test_run_defaults_for_optional_fields():
    # SQLAlchemy column defaults (params={}, retain_input=True, provenance={})
    # are applied at FLUSH, not on a transient instance — so on a transient run
    # those attrs are None and would fail the non-Optional DTO fields. The DTO's
    # own field defaults fill them when the source omits the keys. Validate from
    # a dict that mirrors a minimal run to exercise exactly those DTO defaults.
    minimal = {
        "id": 10,
        "pipeline_id": 5,
        "input_source": InputSource.EXTERNAL_PATH.value,
        "external_path": "/data/bids",
        "repo_sha": "c" * 40,
        "status": AnalysisPipelineRunStatus.RUNNING.value,
        "created_at": _now(),
        "updated_at": _now(),
    }
    dto = AnalysisPipelineRunDTO.model_validate(minimal)
    assert dto.external_path == "/data/bids"
    assert dto.stack_ids is None
    assert dto.params == {}
    assert dto.retain_input is True
    assert dto.provenance == {}
    assert dto.input_source == "external_path"
    assert dto.status == "running"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
