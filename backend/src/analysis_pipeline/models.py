"""ORM models + DTOs for the analysis-pipeline subsystem (frozen design §4).

Three tables, all on the **APP-DB** ``cohorts.models.Base`` (the same Base the
cohort STAGE tracker ``nils_dataset_pipeline`` attaches to — control plane, NOT
the metadata DB):

* :class:`AnalysisPipelineRepo`  → ``analysis_pipeline_repos``  — a pinned
  ``(url, sha)`` repository version; bumps ``version`` on refresh (§17.10).
* :class:`AnalysisPipeline`      → ``analysis_pipelines``       — one detected
  descriptor inside a repo version. Identity = ``(repo, sha, slug)`` (sha lives
  on the repo row, §9.1).
* :class:`AnalysisPipelineRun`   → ``analysis_pipeline_runs``   — one execution.
  Pins ``repo_sha`` + ``image_digest`` at launch (immutable-per-run, §17.10),
  stores ``stack_ids`` as DATA (JSONB, never a FK — stacks live in the metadata
  DB, a different database), and mirrors a ``jobs.id`` via a cross-Base
  ``use_alter`` FK with **no** ORM relationship (Job lives on a different Base;
  access it through ``job_service.get_job(run.job_id)``).

Schema creation is ``create_all(checkfirst=True)`` — NO Alembic. The wiring
point is ``db/backup.py:_ensure_application_schema()`` (importing this module
before the ``CohortsBase.metadata.create_all`` call registers these tables).
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

# JSONB on PostgreSQL (production); JSON on SQLite (unit-test engines) so
# ``create_all`` compiles under both dialects. SQLite has no ``visit_JSONB``,
# and these models register on the shared app-DB ``Base.metadata`` — without the
# variant, importing this module would break any SQLite ``create_all`` test that
# runs afterwards. Mirrors the cross-dialect ``JSON`` used in ``jobs.models``.
_JSONB = JSONB().with_variant(JSON(), "sqlite")

# Models attach to the APP-DB Base (same Base nils_dataset_pipeline uses). The
# cross-Base FK to jobs.id works because both Bases live in the SAME database.
from cohorts.models import Base
from jobs.models import Job as _Job

# The job_id FK is cross-Base (jobs.models.Base != cohorts.models.Base). Reference
# the actual jobs.id COLUMN OBJECT (not the "jobs.id" string): a string ref is
# resolved against this table's own MetaData, where `jobs` does not live, so
# create_all() cannot compile the use_alter FK on a fresh DB. The column object
# needs no metadata lookup; use_alter still defers the ALTER until after jobs
# exists (it is created first by _ensure_application_schema).
_JOBS_ID_COLUMN = _Job.__table__.c.id

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass


def _utc_now() -> datetime:
    """Current UTC timestamp (mirrors nils_dataset_pipeline/models.py)."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Enums (stored as their ``.value`` via String columns — no hard PG enum type,
# for forward-compat, mirroring how nils_dataset_pipeline stores status strings)
# ---------------------------------------------------------------------------


class InputSource(str, enum.Enum):
    """Where a run's input data comes from.

    ``run_output`` (chaining one pipeline's output into another) is a documented
    forward-compat seam (§9.2 / §14.5) and is intentionally NOT added in v1.
    """

    DB_SUBSET = "db_subset"
    EXTERNAL_PATH = "external_path"


class AnalysisPipelineRunStatus(str, enum.Enum):
    """Lifecycle of a single analysis-pipeline run."""

    PENDING = "pending"
    MATERIALIZING = "materializing"
    LAUNCHING = "launching"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_WARNINGS = "completed_with_warnings"
    FAILED = "failed"
    CANCELED = "canceled"


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class AnalysisPipelineRepo(Base):
    """A pinned ``(url, sha)`` repository version (§4.1).

    One row per ``(url, sha)``. ``version`` bumps on refresh when the resolved
    sha changes; an in-flight run keeps its pinned sha (immutable-per-run,
    §17.10), so a new repo version never mutates an older one.
    """

    __tablename__ = "analysis_pipeline_repos"
    __table_args__ = (
        UniqueConstraint(
            "url", "sha", name="uq_analysis_pipeline_repo_url_sha"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(500), nullable=False)
    sha: Mapped[str] = mapped_column(String(64), nullable=False)
    default_branch: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utc_now,
        onupdate=_utc_now,
    )

    pipelines: Mapped[list["AnalysisPipeline"]] = relationship(
        "AnalysisPipeline",
        back_populates="repo",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:  # pragma: no cover - debug repr
        return (
            f"<AnalysisPipelineRepo(id={self.id}, "
            f"url={self.url!r}, sha={self.sha[:8] if self.sha else None!r}, "
            f"version={self.version})>"
        )


class AnalysisPipeline(Base):
    """One detected descriptor inside a repo version (§4.2).

    Identity = ``(repo, sha, slug)``; the sha lives on the repo row, so at the
    table level the uniqueness is ``(repo_id, slug)``. The full parsed descriptor
    is snapshotted as JSONB so a pipeline is self-describing without re-fetching
    the repo.
    """

    __tablename__ = "analysis_pipelines"
    __table_args__ = (
        UniqueConstraint(
            "repo_id", "slug", name="uq_analysis_pipeline_repo_slug"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("analysis_pipeline_repos.id", ondelete="CASCADE"),
        nullable=False,
    )

    slug: Mapped[str] = mapped_column(String(120), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Resolved internal work unit + raw BIDS-Apps analysis level.
    work_unit: Mapped[str] = mapped_column(String(20), nullable=False)
    analysis_level: Mapped[str] = mapped_column(String(20), nullable=False)

    image_digest: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    schema_version: Mapped[str] = mapped_column(String(20), nullable=False)

    # Full parsed descriptor (Descriptor.model_dump(by_alias=True)).
    descriptor: Mapped[dict[str, Any]] = mapped_column(_JSONB, nullable=False)

    # Mirrors the repo version at registration time.
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utc_now,
        onupdate=_utc_now,
    )

    repo: Mapped["AnalysisPipelineRepo"] = relationship(
        "AnalysisPipelineRepo", back_populates="pipelines"
    )
    # No cascade-delete of runs: runs retain provenance even when a pipeline is
    # removed (registry.remove does a soft unregister, §5). pipeline_id stays
    # NOT NULL on the run side; hard-delete of a pipeline with runs is forbidden.
    runs: Mapped[list["AnalysisPipelineRun"]] = relationship(
        "AnalysisPipelineRun", back_populates="pipeline"
    )

    def __repr__(self) -> str:  # pragma: no cover - debug repr
        return (
            f"<AnalysisPipeline(id={self.id}, repo_id={self.repo_id}, "
            f"slug={self.slug!r}, work_unit={self.work_unit!r}, "
            f"version={self.version})>"
        )


class AnalysisPipelineRun(Base):
    """One execution of an :class:`AnalysisPipeline` (§4.3).

    ``repo_sha`` + ``image_digest`` are pinned once at launch and never mutated
    (immutable-per-run, §17.10). ``stack_ids`` is stored as DATA (JSONB), never a
    FK — the stacks live in the metadata DB, a different database. ``job_id`` is a
    cross-Base ``use_alter`` FK to ``jobs.id`` with **no** ORM relationship; reach
    the job via ``job_service.get_job(run.job_id)``.
    """

    __tablename__ = "analysis_pipeline_runs"
    __table_args__ = (
        Index("ix_analysis_pipeline_runs_pipeline", "pipeline_id"),
        Index("ix_analysis_pipeline_runs_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("analysis_pipelines.id", ondelete="RESTRICT"),
        nullable=False,
    )

    # Input selection.
    input_source: Mapped[str] = mapped_column(String(20), nullable=False)
    # DATA, never FK — metadata-DB stack ids.
    stack_ids: Mapped[Optional[list[int]]] = mapped_column(_JSONB, nullable=True)
    external_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    # Resolved config-form values.
    params: Mapped[dict[str, Any]] = mapped_column(
        _JSONB, nullable=False, default=dict
    )

    # Pinned-at-launch immutable refs (§17.10).
    repo_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    image_digest: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    # Materialized scratch paths.
    input_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    output_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    work_dir: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    # GC policy (default: keep the materialized input).
    retain_input: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )

    # External orchestrator id (stub uses a synthetic "stub-<uuid>").
    prefect_run_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Provenance snapshot: stack_ids, repo_sha, image_digest, work_unit,
    # results counts, metrics.
    provenance: Mapped[dict[str, Any]] = mapped_column(
        _JSONB, nullable=False, default=dict
    )

    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default=AnalysisPipelineRunStatus.PENDING.value,
    )
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Cross-Base FK to jobs.id. use_alter defers FK creation so the two Bases'
    # metadata can be created independently. NO ORM relationship to Job (it's on
    # jobs.models.Base — a different Base, same DB). Mirrors
    # nils_dataset_pipeline/models.py:105-109 exactly.
    job_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey(_JOBS_ID_COLUMN, ondelete="SET NULL", use_alter=True),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utc_now,
        onupdate=_utc_now,
    )

    pipeline: Mapped["AnalysisPipeline"] = relationship(
        "AnalysisPipeline", back_populates="runs"
    )
    # Note: no relationship to Job (different Base). Use
    # job_service.get_job(self.job_id) to resolve the mirrored job.

    def __repr__(self) -> str:  # pragma: no cover - debug repr
        return (
            f"<AnalysisPipelineRun(id={self.id}, "
            f"pipeline_id={self.pipeline_id}, "
            f"input_source={self.input_source!r}, status={self.status!r})>"
        )


# ---------------------------------------------------------------------------
# Pydantic DTOs (from_attributes=True for ORM → JSON serialization on routes)
# ---------------------------------------------------------------------------


class AnalysisPipelineRepoDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    url: str
    sha: str
    default_branch: Optional[str] = None
    version: int
    created_at: datetime
    updated_at: datetime


class AnalysisPipelineDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    repo_id: int
    slug: str
    name: str
    description: Optional[str] = None
    work_unit: str
    analysis_level: str
    image_digest: Optional[str] = None
    schema_version: str
    descriptor: dict[str, Any]
    version: int
    created_at: datetime
    updated_at: datetime


class AnalysisPipelineRunDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    pipeline_id: int
    input_source: str
    stack_ids: Optional[list[int]] = None
    external_path: Optional[str] = None
    params: dict[str, Any] = {}
    repo_sha: str
    image_digest: Optional[str] = None
    input_path: Optional[str] = None
    output_path: Optional[str] = None
    work_dir: Optional[str] = None
    retain_input: bool = True
    prefect_run_id: Optional[str] = None
    provenance: dict[str, Any] = {}
    status: str
    last_error: Optional[str] = None
    job_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime


__all__ = [
    # enums
    "InputSource",
    "AnalysisPipelineRunStatus",
    # ORM
    "AnalysisPipelineRepo",
    "AnalysisPipeline",
    "AnalysisPipelineRun",
    # DTOs
    "AnalysisPipelineRepoDTO",
    "AnalysisPipelineDTO",
    "AnalysisPipelineRunDTO",
]
