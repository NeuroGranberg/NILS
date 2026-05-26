"""M7 — get_changes paginated query + service integration with the
metadata-DB token writer.

Reuses the M6 fixture; mocks ``apply_picks_to_metadata`` to record
calls without touching a real metadata DB.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import (
    Base, BodyPartSampleOp, CohortBodyPartQCState, CreateCohortPayload,
)
from cohorts.service import cohort_service
from nils_dataset_pipeline import models as _nils_models  # noqa: F401

from qc.body_part import service as service_mod
from qc.body_part.candidates import CandidateStack
from qc.body_part.service import CohortBodyPartQCService

# Reuse the same builders as M6.
from tests.cohort_body_part_qc.test_m6_service_apply import (
    _stub_apply_worker,
    _three_session_cohort,
    _train_a_classifier,
)


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
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
    monkeypatch.setattr("qc.body_part.service.session_scope", _scope)

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
    yield {"engine": engine, "scope": _scope,
           "cohort_id": cohort.id, "cohort_name": cohort.name}
    cohort_service._initialized = original


def _stub_token_writer(monkeypatch):
    """Replace the token writer with a recorder so we don't hit metadata DB."""
    calls = {"n": 0, "picks": []}
    def fake_apply(picks, session_factory=None):
        calls["n"] += 1
        calls["picks"].append([dict(p) for p in picks])
        return {"body_part_updated": 0, "low_conf_added": 0, "low_conf_removed": 0}
    monkeypatch.setattr(service_mod.token_writer, "apply_picks_to_metadata", fake_apply)
    return calls


# ---------------------------------------------------------------------------
# Service hooks: apply / restore_previous / override / reset all call
# the token writer.
# ---------------------------------------------------------------------------


def test_apply_invokes_token_writer(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    calls = _stub_token_writer(monkeypatch)

    svc.apply(cid)
    assert calls["n"] == 1
    # All 3 sessions / 4 stacks made it to the writer.
    flat = [s for picks in calls["picks"] for sess in picks for s in sess["stacks"]]
    assert {s["stack_id"] for s in flat} == {101, 102, 201, 301}


def test_override_invokes_token_writer_for_one_session(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    _stub_token_writer(monkeypatch)
    svc.apply(cid)

    calls = _stub_token_writer(monkeypatch)
    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Brain-Neck",
    )
    assert calls["n"] == 1
    sess_passed = calls["picks"][0]
    assert len(sess_passed) == 1
    assert sess_passed[0]["study_id"] == 1


def test_reset_session_invokes_token_writer(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    _stub_token_writer(monkeypatch)
    svc.apply(cid)
    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Spine",
    )
    _stub_apply_worker(monkeypatch)
    calls = _stub_token_writer(monkeypatch)
    svc.reset_session(cid, subject_id=1, session_date="2024-01-01")
    assert calls["n"] == 1


def test_restore_previous_invokes_token_writer(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    _stub_token_writer(monkeypatch)
    svc.apply(cid)
    _stub_apply_worker(monkeypatch, predictions={301: ("Spine", 0.92)})
    _stub_token_writer(monkeypatch)
    svc.apply(cid)

    calls = _stub_token_writer(monkeypatch)
    svc.restore_previous(cid)
    assert calls["n"] == 1


def test_apply_does_not_fail_when_token_writer_errors(env, monkeypatch):
    """Token-write errors must not block the QC snapshot."""
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    def boom(*a, **kw):
        raise RuntimeError("metadata DB down")
    monkeypatch.setattr(
        service_mod.token_writer, "apply_picks_to_metadata", boom,
    )
    out = svc.apply(cid)
    assert out.has_current is True
    # State row was still written.
    with env["scope"]() as s:
        st = s.get(CohortBodyPartQCState, cid)
        assert st.current_picks


# ---------------------------------------------------------------------------
# get_changes
# ---------------------------------------------------------------------------


def _setup_with_changes(env, monkeypatch):
    """Run apply with predictions that yield 3 changed stacks."""
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    # 101: prior 'brain' → Brain (changed because case differs)
    # 102: prior 'brain' → Brain (changed, same reason)
    # 201: prior None → Brain (changed: addition)
    # 301: prior 'spine' → Brain (changed: relabeled)
    _stub_apply_worker(monkeypatch, predictions={
        101: ("Brain", 0.95),
        102: ("Brain", 0.85),
        201: ("Brain", 0.50),  # low conf
        301: ("Brain", 0.92),
    })
    _stub_token_writer(monkeypatch)
    svc.apply(cid)
    return svc, cid


def test_get_changes_returns_only_changed_rows(env, monkeypatch):
    svc, cid = _setup_with_changes(env, monkeypatch)
    page = svc.get_changes(cid)
    # All 4 stacks 'changed' (prior labels differ from new label).
    assert page.total == 4
    assert len(page.rows) == 4
    # Sorted by ascending confidence — 201 (0.50) should be first.
    assert page.rows[0].stack_id == 201
    assert page.rows[0].previous_label is None
    assert page.rows[0].new_label == "Brain"
    assert page.rows[0].needs_check is True


def test_get_changes_filter_from_label(env, monkeypatch):
    svc, cid = _setup_with_changes(env, monkeypatch)
    page = svc.get_changes(cid, from_label="spine")
    assert page.total == 1
    assert page.rows[0].stack_id == 301


def test_get_changes_filter_to_label(env, monkeypatch):
    svc, cid = _setup_with_changes(env, monkeypatch)
    page = svc.get_changes(cid, to_label="Brain")
    assert page.total == 4


def test_get_changes_filter_prior_source(env, monkeypatch):
    svc, cid = _setup_with_changes(env, monkeypatch)
    # 201 has prior_source=None; the rest have 'text_keyword'.
    page = svc.get_changes(cid, prior_source="text_keyword")
    assert page.total == 3
    assert {r.stack_id for r in page.rows} == {101, 102, 301}


def test_get_changes_filter_from_label_none_marker(env, monkeypatch):
    svc, cid = _setup_with_changes(env, monkeypatch)
    # Row 201 has prior=None → from_label='(none)' should match.
    page = svc.get_changes(cid, from_label="(none)")
    assert page.total == 1
    assert page.rows[0].stack_id == 201


def test_get_changes_pagination(env, monkeypatch):
    svc, cid = _setup_with_changes(env, monkeypatch)
    page1 = svc.get_changes(cid, offset=0, limit=2)
    page2 = svc.get_changes(cid, offset=2, limit=2)
    assert page1.total == 4 and page2.total == 4
    assert len(page1.rows) == 2 and len(page2.rows) == 2
    assert {r.stack_id for r in page1.rows}.isdisjoint(
        {r.stack_id for r in page2.rows},
    )


def test_get_changes_empty_when_no_apply(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    page = svc.get_changes(cid)
    assert page.total == 0
    assert page.rows == []


def test_get_changes_unknown_cohort(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(LookupError):
        svc.get_changes(99999)
