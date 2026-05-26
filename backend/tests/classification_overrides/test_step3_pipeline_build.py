"""Regression tests for ``Step3Classification._build_pipeline``.

These guard the bridging contract between the two databases:

  * ``StepContext.cohort_id`` is the *metadata DB* cohort id.
  * ``cohort_classification_overrides.cohort_id`` is the *application DB*
    cohort id.

The two IDs are independent auto-increment sequences and almost never match
in practice. ``_build_pipeline`` therefore must resolve the application-DB
cohort by **name**, not by ID, or the override deltas silently fail to reach
the classifier.

Before the fix, ``_build_pipeline(cohort_id=<metadata-id>)`` queried
``load_override_map`` with the wrong ID, got ``{}``, and silently fell back
to the global YAML defaults — with no exception, no warning, no log line.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# The ``tests/sort/`` subdirectory is a package (has __init__.py); once
# pytest has seen it, a bare ``import sort`` resolves to the tests package
# instead of ``src/sort/``. Force resolution to ``src/sort/`` BEFORE any
# other imports pull ``sort`` in.
import importlib.util as _ilu
import sys as _sys
from pathlib import Path as _P
_SORT_INIT = _P(__file__).resolve().parents[2] / "src" / "sort" / "__init__.py"
if _SORT_INIT.exists():
    _spec = _ilu.spec_from_file_location(
        "sort", _SORT_INIT, submodule_search_locations=[str(_SORT_INIT.parent)],
    )
    _mod = _ilu.module_from_spec(_spec)
    _sys.modules["sort"] = _mod
    _spec.loader.exec_module(_mod)
del _ilu, _sys, _P, _SORT_INIT

from cohorts import keyword_override_service
from cohorts.models import Base, CreateCohortPayload
from cohorts.service import cohort_service

# Register NilsDatasetPipelineStep mapper required by Cohort.pipeline_steps.
from nils_dataset_pipeline import models as _nils_models  # noqa: F401

from sort.steps.step3_classification import Step3Classification


@pytest.fixture
def step3_env(tmp_path: Path, monkeypatch):
    """In-memory app DB with one cohort whose app-DB id != any plausible
    metadata-DB id. Patched into every place the service reads the app DB."""
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(
        bind=engine, autoflush=False, autocommit=False,
        expire_on_commit=False, future=True,
    )
    tables = [
        t for t in Base.metadata.tables.values()
        if t.name != "nils_dataset_pipeline_steps"
    ]
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
    monkeypatch.setattr("cohorts.keyword_override_service.session_scope", _scope)
    # Step3 imports ``db.session.session_scope`` lazily inside
    # ``_build_pipeline``; patch it there so the app-DB lookup hits the same
    # in-memory engine as ``keyword_override_service``.
    monkeypatch.setattr("db.session.session_scope", _scope)

    monkeypatch.setattr(
        cohort_service, "_initialize_pipeline_steps", lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        cohort_service, "_reinitialize_pipeline_steps", lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        cohort_service, "get_stages_from_pipeline", lambda *a, **kw: [],
    )
    original_init = cohort_service._initialized
    cohort_service._initialized = True

    src = tmp_path / "src"
    src.mkdir()
    cohort = cohort_service.create_cohort(
        CreateCohortPayload(name="broms", source_path=str(src))
    )

    yield {"app_cohort_id": cohort.id, "cohort_name": cohort.name}

    cohort_service._initialized = original_init


def test_build_pipeline_resolves_cohort_by_name_not_id(step3_env):
    """Simulates the production bug: a metadata-DB cohort_id that does NOT
    match the app-DB id. The override must still reach the pipeline.

    We stage an override for the **real** app-DB id and then call
    ``_build_pipeline`` with only the cohort *name*. The merged config must
    contain the added keyword.
    """
    app_id = step3_env["app_cohort_id"]
    name = step3_env["cohort_name"]

    keyword_override_service.update_bucket(
        app_id, "provenance",
        "provenances.Localizer.detection.keywords",
        added=["mst"], removed=[],
    )

    step = Step3Classification()
    pipeline = step._build_pipeline(name)

    # The Provenance detector must have received the merged YAML with "mst"
    # appended to Localizer keywords.
    prov = pipeline.provenance_detector._provenances["Localizer"]
    assert "mst" in prov["detection"]["keywords"]


def test_build_pipeline_returns_defaults_for_unknown_cohort_name():
    """A cohort name with no row in the app DB must fall back to defaults
    without raising."""
    step = Step3Classification()
    pipeline = step._build_pipeline("does-not-exist")
    # Defaults: 'mst' is NOT in the shipped Localizer keywords.
    prov = pipeline.provenance_detector._provenances["Localizer"]
    assert "mst" not in prov["detection"]["keywords"]


def test_build_pipeline_no_override_rows_returns_defaults(step3_env):
    """Cohort exists in app DB but has no override rows → defaults."""
    step = Step3Classification()
    pipeline = step._build_pipeline(step3_env["cohort_name"])
    prov = pipeline.provenance_detector._provenances["Localizer"]
    assert "mst" not in prov["detection"]["keywords"]
    # Ships with the canonical localizer keyword.
    assert "localizer" in prov["detection"]["keywords"]
