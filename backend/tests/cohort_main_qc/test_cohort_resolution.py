"""Regression tests guarding the bridging contract between the two databases.

Background — the pre-existing 'broms' bug:

  * URL ``/api/cohorts/{id}/...`` carries the *application DB* cohort id.
  * ``cohort_main_qc_state.cohort_id`` FKs to ``cohorts.id`` (app DB).
  * ``series_classification_cache.dicom_origin_cohort`` is the cohort *name*
    (string).
  * The two databases use independent auto-increment sequences and almost
    never share ids in production.

These tests prove that ``CohortMainQCService``:
  1. Reads the cohort name from the **app DB** by id, then
  2. Uses **the name** (not the id) for every metadata-DB query, and
  3. Uses the **app-DB id** for every app-DB row (state snapshot).

Without these two guarantees, snapshot apply silently writes nothing — same
class of bug as the keyword-overrides 'broms' regression.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cohorts.models import Base, CreateCohortPayload, CohortMainQCState
from cohorts.service import cohort_service
# Register NilsDatasetPipelineStep mapper required by Cohort.pipeline_steps.
from nils_dataset_pipeline import models as _nils_models  # noqa: F401

from qc.cohort_main_qc_service import CohortMainQCService


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
    """In-memory app DB with one cohort whose app-DB id is unlikely to match
    any plausible metadata-DB id."""
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(
        bind=engine, autoflush=False, autocommit=False,
        expire_on_commit=False, future=True,
    )
    tables = [
        t for t in Base.metadata.tables.values()
        if t.name != "nils_dataset_pipeline_steps"
        and not t.name.startswith("analysis_pipeline")
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
    # The service imports session_scope from db.session at call time.
    monkeypatch.setattr("qc.cohort_main_qc_service.session_scope", _scope)

    monkeypatch.setattr(cohort_service, "_initialize_pipeline_steps", lambda *a, **kw: None)
    monkeypatch.setattr(cohort_service, "_reinitialize_pipeline_steps", lambda *a, **kw: None)
    monkeypatch.setattr(cohort_service, "get_stages_from_pipeline", lambda *a, **kw: [])
    original_init = cohort_service._initialized
    cohort_service._initialized = True

    src = tmp_path / "src"
    src.mkdir()
    cohort = cohort_service.create_cohort(
        CreateCohortPayload(name="alpha-cohort", source_path=str(src))
    )

    yield {"app_cohort_id": cohort.id, "cohort_name": cohort.name, "scope": _scope}

    cohort_service._initialized = original_init


# ----------------------------------------------------------------------------
# Helpers — capture every metadata-DB SQL execution and what params it used
# ----------------------------------------------------------------------------


class _FakeMetadataResult:
    """Stand-in for SQLAlchemy Result; supports .fetchall() and .fetchone()."""
    def __init__(self, rows=()):
        self._rows = list(rows)
    def fetchall(self): return list(self._rows)
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._rows[0] if self._rows else None


class _FakeMetadataSession:
    """Records every (sql_text, params) tuple it sees and returns empty rows."""
    def __init__(self, log: list):
        self._log = log
    def __enter__(self): return self
    def __exit__(self, *exc): return False
    def execute(self, stmt, params=None):
        try:
            sql_text = str(stmt.text) if hasattr(stmt, "text") else str(stmt)
        except Exception:
            sql_text = repr(stmt)
        self._log.append({"sql": sql_text, "params": dict(params or {})})
        return _FakeMetadataResult([])
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass


@pytest.fixture
def captured_meta(monkeypatch):
    """Patch MetadataSessionLocal so every metadata-DB hit is captured."""
    log: list = []
    factory = lambda: _FakeMetadataSession(log)
    monkeypatch.setattr("qc.cohort_main_qc_service.MetadataSessionLocal", factory)
    return log


# ----------------------------------------------------------------------------
# The actual contract assertions
# ----------------------------------------------------------------------------


def test_apply_uses_cohort_name_for_every_metadata_query(env, captured_meta):
    """When apply() is called with the app-DB id, EVERY metadata-DB query
    must carry the cohort *name* (not the id) in its params."""
    svc = CohortMainQCService()
    svc.apply(env["app_cohort_id"])

    assert captured_meta, "apply() didn't hit the metadata DB at all"

    cohort_name = env["cohort_name"]
    bad_app_id = env["app_cohort_id"]

    # Every metadata-DB query that scopes by cohort must use the NAME,
    # not the app-DB id. The integer must NEVER appear as a cohort param.
    for entry in captured_meta:
        params = entry["params"]
        # If the query carries any cohort-scoping param, it must be a string
        # equal to cohort_name (or contain cohort_name).
        for key, val in params.items():
            if "cohort" in key.lower():
                assert val == cohort_name or (
                    isinstance(val, str) and cohort_name in val
                ), (
                    f"metadata query params={params} carries a non-name "
                    f"value for key={key!r}: {val!r} "
                    f"(expected the string {cohort_name!r})"
                )
                # The integer app-DB id must never be a cohort scoping param.
                assert val != bad_app_id, (
                    f"metadata query mistakenly scoped by the app-DB id "
                    f"{bad_app_id} for key={key!r}: {entry}"
                )


def test_apply_persists_state_row_under_app_db_cohort_id(env, captured_meta):
    """The persisted snapshot row must be keyed by the *app-DB* cohort id
    (the FK target), not by anything from the metadata DB."""
    svc = CohortMainQCService()
    svc.apply(env["app_cohort_id"])

    with env["scope"]() as session:
        row = session.get(CohortMainQCState, env["app_cohort_id"])
        assert row is not None
        assert row.cohort_id == env["app_cohort_id"]


def test_apply_unknown_cohort_id_raises_lookup_error(captured_meta):
    """An app-DB id with no cohort row must raise LookupError (not silently
    fall through and try to scope the metadata DB by None or by the int)."""
    svc = CohortMainQCService()
    with pytest.raises(LookupError):
        svc.apply(999999)
    # And no metadata query should have leaked through.
    assert captured_meta == []


def test_get_state_for_unknown_cohort_id_raises(env):
    svc = CohortMainQCService()
    with pytest.raises(LookupError):
        svc.get_state(999999)


def test_get_state_returns_empty_when_cohort_exists_but_no_snapshot(env):
    svc = CohortMainQCService()
    state = svc.get_state(env["app_cohort_id"])
    assert state.cohort_id == env["app_cohort_id"]
    assert state.has_current is False
    assert state.has_previous is False
    assert state.picks == []


def test_restore_previous_resolves_cohort_by_id_then_uses_name(env, captured_meta, monkeypatch):
    """restore_previous must:
       * accept the app-DB id from the URL
       * resolve to a name via the app DB
       * scope the metadata DB by name only
       * persist back to the app-DB row by id
    """
    svc = CohortMainQCService()
    # First Apply seeds current; second seeds previous.
    svc.apply(env["app_cohort_id"])
    captured_meta.clear()
    svc.apply(env["app_cohort_id"])
    captured_meta.clear()

    svc.restore_previous(env["app_cohort_id"])

    cohort_name = env["cohort_name"]
    bad_app_id = env["app_cohort_id"]
    for entry in captured_meta:
        for key, val in entry["params"].items():
            if "cohort" in key.lower():
                assert val == cohort_name or (
                    isinstance(val, str) and cohort_name in val
                ), entry
                assert val != bad_app_id, entry
