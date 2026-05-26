"""M10 — Sort step3 must not overwrite ``body_part`` for QC-protected stacks.

Tests the ``Step3Classification._apply_body_part_qc_protection``
pre-flight: any value row whose ``series_stack_id`` is in the cohort's
QC-protected set has its ``body_part`` swapped with whatever's currently
in ``series_classification_cache`` so the upsert is a no-op on that
column.

Failure modes that must NEVER block step3 (defensive):
- Missing app-DB cohort.
- Missing QC state row.
- Empty QC state.
- Exceptions inside the helper (logged + skipped).
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import (
    Column, Integer, MetaData, String, Table, Text,
    create_engine, text,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Force ``sort`` to resolve to ``src/sort`` (see test_step3_pipeline_build.py
# for the rationale — a tests/sort/ subpackage shadows the source package).
import importlib.util as _ilu
import sys as _sys
from pathlib import Path as _P
_SORT_INIT = (
    _P(__file__).resolve().parents[2] / "src" / "sort" / "__init__.py"
)
if _SORT_INIT.exists() and "sort" not in _sys.modules:
    _spec = _ilu.spec_from_file_location(
        "sort", _SORT_INIT, submodule_search_locations=[str(_SORT_INIT.parent)],
    )
    _mod = _ilu.module_from_spec(_spec)
    _sys.modules["sort"] = _mod
    _spec.loader.exec_module(_mod)
del _ilu, _sys, _P, _SORT_INIT

from cohorts.models import (
    Base, CohortBodyPartQCState, CreateCohortPayload,
)
from cohorts.service import cohort_service
from nils_dataset_pipeline import models as _nils_models  # noqa: F401

from sort.steps.step3_classification import Step3Classification


# ---------------------------------------------------------------------------
# Fixture — app DB + a fake metadata DB connection (just the columns we read).
# ---------------------------------------------------------------------------


@pytest.fixture
def step3_env(tmp_path: Path, monkeypatch):
    # App DB — owns CohortBodyPartQCState.
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(
        bind=engine, autoflush=False, autocommit=False,
        expire_on_commit=False, future=True,
    )
    tables = [t for t in Base.metadata.tables.values()
              if t.name != "nils_dataset_pipeline_steps"]
    Base.metadata.create_all(engine, tables=tables)

    @contextmanager
    def _scope():
        s = SessionLocal()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    monkeypatch.setattr("cohorts.service.engine", engine)
    monkeypatch.setattr("cohorts.service.session_scope", _scope)
    monkeypatch.setattr("db.session.session_scope", _scope)
    # ``protection.py`` imports session_scope at module load time, so a
    # patch on ``db.session.session_scope`` doesn't reach it. Patch the
    # already-imported reference too.
    import qc.body_part.protection as _p
    monkeypatch.setattr(_p, "session_scope", _scope)

    monkeypatch.setattr(cohort_service, "_initialize_pipeline_steps", lambda *a, **kw: None)
    monkeypatch.setattr(cohort_service, "_reinitialize_pipeline_steps", lambda *a, **kw: None)
    monkeypatch.setattr(cohort_service, "get_stages_from_pipeline", lambda *a, **kw: [])
    original = cohort_service._initialized
    cohort_service._initialized = True

    src = tmp_path / "src"
    src.mkdir()
    cohort = cohort_service.create_cohort(
        CreateCohortPayload(name="alpha-cohort", source_path=str(src))
    )

    # Fake metadata DB — a single table with just the columns the
    # protection helper reads from.
    meta_engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    meta_md = MetaData()
    Table(
        "series_classification_cache", meta_md,
        Column("series_stack_id", Integer, primary_key=True),
        Column("body_part", String(64), nullable=True),
    )
    meta_md.create_all(meta_engine)

    yield {
        "app_cohort_id": cohort.id,
        "cohort_name": cohort.name,
        "scope": _scope,
        "meta_engine": meta_engine,
    }

    cohort_service._initialized = original


def _seed_meta_rows(meta_engine, rows):
    """Insert (stack_id, body_part) tuples into the fake metadata DB."""
    with meta_engine.begin() as conn:
        for sid, bp in rows:
            conn.execute(
                text(
                    "INSERT INTO series_classification_cache "
                    "(series_stack_id, body_part) VALUES (:sid, :bp)"
                ),
                {"sid": sid, "bp": bp},
            )


def _set_qc_state(scope, cohort_id, protected_stack_ids):
    """Plant a CohortBodyPartQCState row whose current_picks list
    references ``protected_stack_ids``."""
    with scope() as db:
        state = CohortBodyPartQCState(
            cohort_id=cohort_id,
            categories=["Brain", "Spine"],
            training_samples=[],
            current_summary={},
            current_picks=[
                {
                    "study_id": 1, "subject_id": 1, "subject_code": "S001",
                    "session_date": "2024-01-01",
                    "stacks": [
                        {"stack_id": sid, "label": "Brain",
                         "confidence": 0.9, "probs": {},
                         "is_override": False, "needs_check": False,
                         "previous_label": None, "prior_source": None,
                         "changed": False, "series_instance_uid": "u",
                         "technique": None, "orientation": None}
                        for sid in protected_stack_ids
                    ],
                    "session_combo": ["Brain"], "session_combo_key": "Brain",
                    "session_prev_combo_key": "Brain",
                    "session_changed": False, "stacks_changed": 0,
                    "low_conf_count": 0, "needs_check": False,
                }
            ],
            current_profile={},
        )
        db.add(state)


def _values(rows):
    """Build a minimal upsert-values list from (stack_id, kw_body_part)."""
    return [
        {"series_stack_id": sid, "body_part": bp}
        for sid, bp in rows
    ]


# ---------------------------------------------------------------------------
# _apply_body_part_qc_protection — happy paths
# ---------------------------------------------------------------------------


def test_protection_overrides_keyword_with_existing_db_value(step3_env):
    _set_qc_state(step3_env["scope"], step3_env["app_cohort_id"], [101, 102])
    _seed_meta_rows(step3_env["meta_engine"], [(101, "Brain"), (102, "Brain-Neck")])

    values = _values([
        (101, "spine"),       # keyword detector said spine — must be overridden
        (102, "brain-neck"),  # case differs — must be overridden to DB value
        (200, "spine"),       # not protected — keyword value preserved
    ])

    with step3_env["meta_engine"].connect() as conn:
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, step3_env["cohort_name"],
        )

    by_id = {v["series_stack_id"]: v["body_part"] for v in values}
    assert by_id[101] == "Brain"          # overridden
    assert by_id[102] == "Brain-Neck"     # overridden
    assert by_id[200] == "spine"          # untouched


def test_protection_no_op_when_keyword_already_matches_db(step3_env):
    _set_qc_state(step3_env["scope"], step3_env["app_cohort_id"], [101])
    _seed_meta_rows(step3_env["meta_engine"], [(101, "Brain")])

    values = _values([(101, "Brain")])
    with step3_env["meta_engine"].connect() as conn:
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, step3_env["cohort_name"],
        )
    assert values[0]["body_part"] == "Brain"


def test_protection_clears_keyword_value_when_db_is_null(step3_env):
    """If QC ran but the cache row's body_part is NULL (e.g. an
    earlier partial run), we still preserve NULL rather than letting
    the keyword detector backfill it."""
    _set_qc_state(step3_env["scope"], step3_env["app_cohort_id"], [101])
    _seed_meta_rows(step3_env["meta_engine"], [(101, None)])

    values = _values([(101, "spine")])
    with step3_env["meta_engine"].connect() as conn:
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, step3_env["cohort_name"],
        )
    assert values[0]["body_part"] is None


def test_protection_skips_protected_stack_missing_from_cache(step3_env):
    """If a stack is QC-protected but for some reason isn't in the
    metadata cache yet, we keep the keyword value (defensive)."""
    _set_qc_state(step3_env["scope"], step3_env["app_cohort_id"], [101])
    # No row in the fake cache.

    values = _values([(101, "spine")])
    with step3_env["meta_engine"].connect() as conn:
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, step3_env["cohort_name"],
        )
    assert values[0]["body_part"] == "spine"


# ---------------------------------------------------------------------------
# _apply_body_part_qc_protection — graceful no-ops
# ---------------------------------------------------------------------------


def test_protection_no_qc_state_is_no_op(step3_env):
    _seed_meta_rows(step3_env["meta_engine"], [(101, "Brain")])
    values = _values([(101, "spine")])
    with step3_env["meta_engine"].connect() as conn:
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, step3_env["cohort_name"],
        )
    assert values[0]["body_part"] == "spine"


def test_protection_unknown_cohort_name_is_no_op(step3_env):
    values = _values([(101, "spine")])
    with step3_env["meta_engine"].connect() as conn:
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, "does-not-exist",
        )
    assert values[0]["body_part"] == "spine"


def test_protection_empty_values_is_no_op(step3_env):
    _set_qc_state(step3_env["scope"], step3_env["app_cohort_id"], [101])
    values: list[dict] = []
    with step3_env["meta_engine"].connect() as conn:
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, step3_env["cohort_name"],
        )
    assert values == []


def test_protection_swallows_internal_exceptions(step3_env, monkeypatch):
    """Any unhandled error in the helper must be logged and ignored —
    step3 must keep working even if the QC tables are corrupt."""
    _set_qc_state(step3_env["scope"], step3_env["app_cohort_id"], [101])
    _seed_meta_rows(step3_env["meta_engine"], [(101, "Brain")])

    # Force the protection helper to raise.
    import qc.body_part.protection as protection_mod
    def _boom(_):
        raise RuntimeError("simulated failure")
    monkeypatch.setattr(protection_mod, "get_qc_protected_stack_ids", _boom)

    values = _values([(101, "spine")])
    with step3_env["meta_engine"].connect() as conn:
        # Must not raise.
        Step3Classification()._apply_body_part_qc_protection(
            conn, values, step3_env["cohort_name"],
        )
    # Keyword value preserved (we couldn't apply protection).
    assert values[0]["body_part"] == "spine"
