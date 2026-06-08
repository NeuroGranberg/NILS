"""Analysis-pipeline subsystem (Part 2 backend foundation).

This package implements the *analysis* pipeline integration (Boutiques+x-nils
descriptors → registry → run orchestration → derivative ingest). It is a
DIFFERENT subsystem from ``nils_dataset_pipeline`` (the cohort STAGE tracker:
anonymize → extract → sort → bids). The ``AnalysisPipeline*`` naming and the
separate module exist precisely to avoid that name collision (spec §0.5(1)).

The public surface re-exported here is the contract other layers (routes, CLI,
tests) build against:

* **descriptor** — ``parse_descriptor`` / ``validate_descriptor`` + ``Descriptor``
  and its errors (§2).
* **results** — ``parse_results_json`` + ``RunResults`` / ``UnitResult`` (§3).
* **models** — the three ORM tables + their DTOs + the enums (§4).
* **config** — scratch-path builders (§6.3).
* **needs** — ``apply_needs`` + the handler registry (§6.2).
* **bridge** — the ``PipelineBridge`` seam + ``StubBridge`` substrate (§6.1).
* **registry** — ``AnalysisPipelineRegistry`` + the ``analysis_pipeline_registry``
  singleton + the repo-fetch seam (§5).
* **runner** — ``AnalysisPipelineRunner`` lifecycle owner (§6.4).
* **ingest** — ``plan_ingest`` + the gated ``ingest_derivatives`` job (§6.5).
"""

from __future__ import annotations

from analysis_pipeline.descriptor import (
    SUPPORTED_SCHEMA_VERSION_RANGE,
    Descriptor,
    DescriptorError,
    UnsupportedSchemaVersionError,
    parse_descriptor,
    slugify,
    validate_descriptor,
    work_unit_of,
)
from analysis_pipeline.results import (
    ResultsError,
    RunResults,
    UnitResult,
    UnitStatus,
    parse_results_json,
)
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
from analysis_pipeline.config import (
    DEFAULT_SCRATCH_ROOT,
    get_scratch_root,
    run_bids_dir,
    run_output_dir,
    run_scratch_dir,
    run_work_dir,
)
from analysis_pipeline.needs import (
    NEEDS_HANDLERS,
    NeedsContext,
    apply_needs,
)
from analysis_pipeline.bridge import (
    BridgeRunResult,
    BridgeRunState,
    PipelineBridge,
    StubBridge,
)
from analysis_pipeline.registry import (
    DESCRIPTOR_FILENAME,
    AnalysisPipelineRegistry,
    FetchedRepo,
    GitRepoFetcher,
    LocalFixtureRepoFetcher,
    PipelineSpec,
    RegistryError,
    RepoFetcher,
    analysis_pipeline_registry,
    build_pipeline_specs,
    discover_descriptors,
    next_repo_version,
)
from analysis_pipeline.runner import (
    RUN_JOB_STAGE,
    AnalysisPipelineRunner,
)
from analysis_pipeline.ingest import (
    INGEST_DB_WRITE_ENABLED,
    IngestPlan,
    plan_ingest,
    run_ingest_derivatives_job,
)

__all__ = [
    # descriptor
    "SUPPORTED_SCHEMA_VERSION_RANGE",
    "Descriptor",
    "DescriptorError",
    "UnsupportedSchemaVersionError",
    "parse_descriptor",
    "validate_descriptor",
    "slugify",
    "work_unit_of",
    # results
    "ResultsError",
    "RunResults",
    "UnitResult",
    "UnitStatus",
    "parse_results_json",
    # models
    "AnalysisPipeline",
    "AnalysisPipelineDTO",
    "AnalysisPipelineRepo",
    "AnalysisPipelineRepoDTO",
    "AnalysisPipelineRun",
    "AnalysisPipelineRunDTO",
    "AnalysisPipelineRunStatus",
    "InputSource",
    # config
    "DEFAULT_SCRATCH_ROOT",
    "get_scratch_root",
    "run_bids_dir",
    "run_output_dir",
    "run_scratch_dir",
    "run_work_dir",
    # needs
    "NEEDS_HANDLERS",
    "NeedsContext",
    "apply_needs",
    # bridge
    "BridgeRunResult",
    "BridgeRunState",
    "PipelineBridge",
    "StubBridge",
    # registry
    "DESCRIPTOR_FILENAME",
    "AnalysisPipelineRegistry",
    "FetchedRepo",
    "GitRepoFetcher",
    "LocalFixtureRepoFetcher",
    "PipelineSpec",
    "RegistryError",
    "RepoFetcher",
    "analysis_pipeline_registry",
    "build_pipeline_specs",
    "discover_descriptors",
    "next_repo_version",
    # runner
    "RUN_JOB_STAGE",
    "AnalysisPipelineRunner",
    # ingest
    "INGEST_DB_WRITE_ENABLED",
    "IngestPlan",
    "plan_ingest",
    "run_ingest_derivatives_job",
]
