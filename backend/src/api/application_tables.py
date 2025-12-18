"""Helpers for application database table exploration endpoints."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

from sqlalchemy import Column

from cohorts.models import Cohort
from jobs.models import Job, JobRun
from nils_dataset_pipeline.models import NilsDatasetPipelineStep


@dataclass(frozen=True)
class TableDefinition:
    """Definition of a table available for exploration."""
    name: str
    label: str
    columns: Sequence[Column]
    default_order: Sequence[Column]
    category: str = "other"
    description: str = ""


@dataclass(frozen=True)
class TableCategory:
    """Category grouping for tables."""
    id: str
    label: str
    description: str
    table_names: tuple[str, ...] = field(default_factory=tuple)


# Table categories for organization in the UI
TABLE_CATEGORIES: dict[str, TableCategory] = {
    "cohorts": TableCategory(
        id="cohorts",
        label="Cohorts & Pipeline",
        description="Cohort definitions and pipeline execution state",
        table_names=("cohorts", "nils_dataset_pipeline_steps"),
    ),
    "jobs": TableCategory(
        id="jobs",
        label="Jobs",
        description="Job execution history and run logs",
        table_names=("jobs", "job_runs"),
    ),
    "anonymization": TableCategory(
        id="anonymization",
        label="Anonymization",
        description="PHI removal audit logs and processing summaries",
        table_names=("anonymize_study_audit", "anonymize_leaf_summary"),
    ),
}


_TABLES: dict[str, TableDefinition] = {}


def _register(
    model,
    *,
    label: str,
    category: str = "other",
    description: str = "",
    default_order: Sequence[str] | None = None,
) -> None:
    """Register a model's table for exploration."""
    table = model.__table__
    ordered = []
    if default_order:
        for column_name in default_order:
            column = table.c.get(column_name)
            if column is not None:
                ordered.append(column)
    definition = TableDefinition(
        name=table.name,
        label=label,
        columns=tuple(table.c),
        default_order=tuple(ordered),
        category=category,
        description=description,
    )
    _TABLES[table.name] = definition


def _register_anonymize_tables() -> None:
    """Register anonymization tables if available."""
    try:
        from anonymize.store import AnonymizeLeafSummary, AnonymizeStudyAudit
        _register(
            AnonymizeStudyAudit,
            label="Study Audits",
            category="anonymization",
            description="Per-study anonymization audit records",
            default_order=["id"],
        )
        _register(
            AnonymizeLeafSummary,
            label="Leaf Summaries",
            category="anonymization",
            description="Per-folder anonymization summaries",
            default_order=["id"],
        )
    except ImportError:
        pass


# Core application tables
_register(
    Cohort,
    label="Cohorts",
    category="cohorts",
    description="Dataset cohort definitions",
    default_order=["id"],
)
_register(
    NilsDatasetPipelineStep,
    label="Pipeline Steps",
    category="cohorts",
    description="Pipeline execution state per cohort/stage/step",
    default_order=["cohort_id", "sort_order"],
)
_register(
    Job,
    label="Jobs",
    category="jobs",
    description="Background job records",
    default_order=["id"],
)
_register(
    JobRun,
    label="Job Runs",
    category="jobs",
    description="Individual job execution runs",
    default_order=["id"],
)

# Register anonymize tables (may not be available in all environments)
_register_anonymize_tables()


def list_tables() -> Iterable[TableDefinition]:
    """Return all registered application tables."""
    return _TABLES.values()


def get_table(name: str) -> TableDefinition:
    """Get a table definition by name."""
    definition = _TABLES.get(name)
    if definition is None:
        raise KeyError(name)
    return definition


def list_categories() -> Iterable[TableCategory]:
    """Return all table categories."""
    return TABLE_CATEGORIES.values()


def get_category(category_id: str) -> TableCategory:
    """Get a category by ID."""
    category = TABLE_CATEGORIES.get(category_id)
    if category is None:
        raise KeyError(category_id)
    return category


def get_tables_by_category(category_id: str) -> list[TableDefinition]:
    """Get all tables in a category."""
    category = TABLE_CATEGORIES.get(category_id)
    if category is None:
        return []
    return [_TABLES[name] for name in category.table_names if name in _TABLES]
