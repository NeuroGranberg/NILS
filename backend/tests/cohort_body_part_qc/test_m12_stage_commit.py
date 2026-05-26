"""M12 — stage→commit gate + override preservation on re-Apply.

Covers:
* Apply leaves stage_status='staged' and does NOT write to the metadata DB.
* Commit pushes labels + tokens through tokens.apply_picks_to_metadata.
* Restore-previous re-stages without auto-committing.
* Override on a staged cohort stays staged; override on a committed
  cohort flips to ``dirty`` and writes through.
* Re-Apply preserves prior overrides; flags conflicts when the new
  model strongly disagrees.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import (
    Base, BodyPartSampleOp, CreateCohortPayload,
)
from cohorts.service import cohort_service
from nils_dataset_pipeline import models as _nils_models  # noqa: F401

from qc.body_part import service as service_mod
from qc.body_part import tokens as tokens_mod
from qc.body_part.candidates import CandidateStack
from qc.body_part.service import CohortBodyPartQCService


# ---------------------------------------------------------------------------
# Fixtures (same shape as test_m6_service_apply but with token_writer spy)
# ---------------------------------------------------------------------------


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


def _three_session_cohort() -> list[CandidateStack]:
    return [
        CandidateStack(
            stack_id=101, slice_index=10, series_instance_uid="1.1.1",
            study_id=1, subject_id=1,
            subject_code="sub-001", session_date="2024-01-01",
            technique="MPRAGE", orientation="axial",
            body_part="brain", num_slices=11,
        ),
        CandidateStack(
            stack_id=102, slice_index=10, series_instance_uid="1.1.2",
            study_id=1, subject_id=1,
            subject_code="sub-001", session_date="2024-01-01",
            technique="FLAIR", orientation="axial",
            body_part="brain", num_slices=11,
        ),
        CandidateStack(
            stack_id=201, slice_index=10, series_instance_uid="2.1.1",
            study_id=2, subject_id=2,
            subject_code="sub-002", session_date="2024-01-02",
            technique="MPRAGE", orientation="axial",
            body_part=None, num_slices=11,
        ),
        CandidateStack(
            stack_id=301, slice_index=10, series_instance_uid="3.1.1",
            study_id=3, subject_id=3,
            subject_code="sub-003", session_date="2024-01-03",
            technique="MPRAGE", orientation="sagittal",
            body_part="spine", num_slices=11,
        ),
    ]


def _stub_worker(monkeypatch, *, predictions=None):
    def fake_embed(items, *, mode):
        return {"encoder_chain": ["biomedclip", "siglip2"], "dim_total": 6,
                "n_items": len(items), "n_cached": 0,
                "n_computed": len(items), "cached_keys": []}

    default_preds = {
        101: ("Brain", 0.95),
        102: ("Brain", 0.90),
        201: ("Brain", 0.85),
        301: ("Brain", 0.80),
    }
    pred_map = {**default_preds, **(predictions or {})}

    def fake_infer(*, classifier_path, classifier_sha256, classes,
                   encoder_chain, items, manual_review_below=None,
                   n_slices=None):
        results = []
        for it in items:
            sid = it["stack_id"]
            label, conf = pred_map.get(sid, (None, 0.0))
            results.append({
                "stack_id": sid, "label": label,
                "confidence": float(conf),
                "probs": {label: float(conf)} if label else {},
                "needs_check": (label is None) or (conf < (manual_review_below or 0.7)),
                "n_slices_used": 3 if label else 0,
            })
        return {"results": results}

    monkeypatch.setattr(service_mod.worker_client, "embed", fake_embed)
    monkeypatch.setattr(service_mod.worker_client, "infer", fake_infer)


def _train_classifier(svc, cid, monkeypatch):
    from datetime import datetime, timezone
    from cohorts.models import BodyPartModel, CohortBodyPartQCState
    import qc.body_part.service as _svc_mod

    svc.update_categories(cid, ["Brain", "Brain-Neck", "Spine", "Chest"])

    with _svc_mod.session_scope() as db:
        model = BodyPartModel(
            name="test-model",
            classes=["Brain", "Spine"],
            label_remap={},
            artifact_path="/fake/test_model.joblib",
            artifact_sha256="deadbeef" * 8,
            meta={
                "encoder_chain": [
                    {"name": "biomedclip", "version": "v1.0.0", "dim": 4},
                    {"name": "siglip2", "version": "v1.0.0", "dim": 2},
                ],
                "classes": ["Brain", "Spine"],
                "trained_at": "2026-04-26T10:00:00+00:00",
            },
            accuracy=0.94,
            n_samples=10,
            trained_at=datetime.now(timezone.utc),
            is_default=False,
        )
        db.add(model)
        db.flush()
        model_id = model.id

        state = db.get(CohortBodyPartQCState, cid)
        if state is not None:
            state.selected_model_id = model_id
            db.flush()


@pytest.fixture
def token_spy(monkeypatch):
    """Replace ``apply_picks_to_metadata`` with a spy so we can assert
    when (if at all) the metadata DB write was triggered."""
    calls: list[list[dict]] = []

    def fake_apply(picks, **kwargs):
        # Deep-copy via dict() so mutations after the call don't leak.
        calls.append([dict(p) for p in picks])
        return {"body_part_updated": 0, "low_conf_added": 0, "low_conf_removed": 0}

    monkeypatch.setattr(tokens_mod, "apply_picks_to_metadata", fake_apply)
    monkeypatch.setattr(service_mod.token_writer, "apply_picks_to_metadata", fake_apply)
    return calls


# ---------------------------------------------------------------------------
# Stage / Commit
# ---------------------------------------------------------------------------


def test_apply_does_not_write_metadata(env, monkeypatch, token_spy):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)

    out = svc.apply(cid)

    assert out.stage_status == "staged"
    assert out.last_committed_at is None
    assert out.pending_changes_count > 0
    # The metadata DB writer must not have been invoked.
    assert token_spy == []


def test_commit_writes_metadata_and_marks_committed(env, monkeypatch, token_spy):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)
    assert token_spy == []

    out = svc.commit(cid)

    assert out.stage_status == "committed"
    assert out.last_committed_at is not None
    assert out.pending_changes_count == 0
    assert len(token_spy) == 1
    # All 3 sessions were written.
    assert len(token_spy[0]) == 3


def test_commit_with_no_picks_raises(env, monkeypatch, token_spy):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    with pytest.raises(LookupError):
        svc.commit(cid)
    assert token_spy == []


def test_override_while_staged_does_not_write(env, monkeypatch, token_spy):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)

    sess = svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Brain-Neck",
    )
    by_id = {s.stack_id: s for s in sess.stacks}
    assert by_id[101].label == "Brain-Neck"
    # Still staged → no write.
    assert token_spy == []
    state = svc.get_state(cid)
    assert state.stage_status == "staged"


def test_override_while_committed_writes_and_marks_dirty(
    env, monkeypatch, token_spy,
):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)
    svc.commit(cid)
    assert len(token_spy) == 1  # the commit

    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Brain-Neck",
    )
    # The override wrote through.
    assert len(token_spy) == 2
    state = svc.get_state(cid)
    assert state.stage_status == "dirty"


def test_restore_previous_re_stages(env, monkeypatch, token_spy):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)
    svc.commit(cid)
    _stub_worker(monkeypatch, predictions={301: ("Spine", 0.92)})
    svc.apply(cid)
    # Apply leaves stage='staged'. Token spy unchanged from the commit.
    assert len(token_spy) == 1

    out = svc.restore_previous(cid)
    # Restored into staged, NOT auto-committed.
    assert out.stage_status == "staged"
    assert len(token_spy) == 1


# ---------------------------------------------------------------------------
# Override preservation on re-Apply (Milestone B)
# ---------------------------------------------------------------------------


def test_reapply_preserves_override_low_conf(env, monkeypatch, token_spy):
    """Override survives re-apply when the new model is not confident."""
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)

    # User overrides 101 → "Spine".
    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Spine",
    )
    state = svc.get_state(cid)
    by_id = {s.stack_id: s for sess in state.picks for s in sess.stacks}
    assert by_id[101].label == "Spine"
    assert by_id[101].is_override is True

    # Re-apply with model NOT strongly confident.
    _stub_worker(monkeypatch, predictions={101: ("Brain", 0.60)})
    out2 = svc.apply(cid)
    by_id2 = {s.stack_id: s for sess in out2.picks for s in sess.stacks}
    # Override preserved.
    assert by_id2[101].label == "Spine"
    assert by_id2[101].is_override is True
    assert by_id2[101].override_conflict is None


def test_reapply_preserves_override_high_conf_same_label(
    env, monkeypatch, token_spy,
):
    """Override survives re-apply when the new model agrees."""
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)
    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Brain",
    )

    _stub_worker(monkeypatch, predictions={101: ("Brain", 0.99)})
    out2 = svc.apply(cid)
    by_id = {s.stack_id: s for sess in out2.picks for s in sess.stacks}
    assert by_id[101].label == "Brain"
    assert by_id[101].is_override is True
    assert by_id[101].override_conflict is None


def test_reapply_flags_override_conflict_high_conf_disagree(
    env, monkeypatch, token_spy,
):
    """Override is preserved but flagged when the model strongly
    disagrees with the user."""
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)
    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Spine",
    )

    _stub_worker(monkeypatch, predictions={101: ("Brain", 0.97)})
    out2 = svc.apply(cid)
    by_id = {s.stack_id: s for sess in out2.picks for s in sess.stacks}
    assert by_id[101].label == "Spine", "override label preserved"
    assert by_id[101].is_override is True
    assert by_id[101].override_conflict is not None
    assert by_id[101].override_conflict["label"] == "Brain"
    assert by_id[101].override_conflict["prob"] >= 0.95
    # Cohort summary counts the conflict.
    assert out2.summary.override_conflicts_count == 1


def test_reapply_clears_conflict_when_user_accepts_via_override(
    env, monkeypatch, token_spy,
):
    """If a stack carries an override-conflict and the user issues a
    fresh override (e.g. accepting the model's suggestion), the conflict
    flag is cleared."""
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_worker(monkeypatch)
    svc.apply(cid)
    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Spine",
    )
    _stub_worker(monkeypatch, predictions={101: ("Brain", 0.97)})
    svc.apply(cid)

    sess = svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Brain",
    )
    by_id = {s.stack_id: s for s in sess.stacks}
    assert by_id[101].label == "Brain"
    assert by_id[101].is_override is True
    assert by_id[101].override_conflict is None
