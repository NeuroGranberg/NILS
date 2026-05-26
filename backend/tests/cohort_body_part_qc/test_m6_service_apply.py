"""M6 — service.apply / restore_previous / override_stack / reset_session.

The worker is fully mocked: ``embed`` is a no-op (returns counts) and
``infer`` returns a deterministic prediction map keyed on stack_id.
``enumerate_candidates`` is mocked to return a synthetic cohort.
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


# ---------------------------------------------------------------------------
# Test fixtures: synthetic cohort
# ---------------------------------------------------------------------------


def _three_session_cohort() -> list[CandidateStack]:
    """Three sessions, each with ≥1 stack, varied prior body_part labels."""
    return [
        # Session 1: 2 stacks (Brain pair)
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
        # Session 2: 1 stack with no prior label (will be a "diff" addition)
        CandidateStack(
            stack_id=201, slice_index=10, series_instance_uid="2.1.1",
            study_id=2, subject_id=2,
            subject_code="sub-002", session_date="2024-01-02",
            technique="MPRAGE", orientation="axial",
            body_part=None, num_slices=11,
        ),
        # Session 3: 1 stack labeled "spine" prior — we'll predict Brain
        # to exercise the change_matrix.
        CandidateStack(
            stack_id=301, slice_index=10, series_instance_uid="3.1.1",
            study_id=3, subject_id=3,
            subject_code="sub-003", session_date="2024-01-03",
            technique="MPRAGE", orientation="sagittal",
            body_part="spine", num_slices=11,
        ),
    ]


def _stub_apply_worker(monkeypatch, *, predictions=None):
    embed_calls: dict = {"n": 0, "items": None, "mode": None}
    infer_calls: dict = {"n": 0, "items": None}

    def fake_embed(items, *, mode):
        embed_calls["n"] += 1
        embed_calls["items"] = list(items)
        embed_calls["mode"] = mode
        return {"encoder_chain": ["biomedclip", "siglip2"], "dim_total": 6,
                "n_items": len(items), "n_cached": 0,
                "n_computed": len(items), "cached_keys": []}

    default_preds = {
        101: ("Brain", 0.95),
        102: ("Brain", 0.90),
        201: ("Brain", 0.85),
        301: ("Brain", 0.80),  # was spine → now brain (change!)
    }
    pred_map = {**default_preds, **(predictions or {})}

    def fake_infer(*, classifier_path, classifier_sha256, classes,
                   encoder_chain, items, manual_review_below=None,
                   n_slices=None):
        infer_calls["n"] += 1
        infer_calls["items"] = list(items)
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
        return {"n_items": len(items),
                "n_predicted": sum(1 for r in results if r["label"]),
                "n_low_conf": sum(1 for r in results if r["needs_check"]),
                "n_no_embeddings": sum(1 for r in results if r["n_slices_used"] == 0),
                "results": results}

    monkeypatch.setattr(service_mod.worker_client, "embed", fake_embed)
    monkeypatch.setattr(service_mod.worker_client, "infer", fake_infer)
    return embed_calls, infer_calls


def _train_a_classifier(svc, cid, monkeypatch):
    """Quick stub: create a model in the registry and select it for the cohort."""
    from datetime import datetime, timezone
    from cohorts.models import BodyPartModel, CohortBodyPartQCState
    import qc.body_part.service as _svc_mod

    svc.update_categories(cid, ["Brain", "Brain-Neck", "Spine", "Chest"])

    # Use the monkeypatched session_scope (writes to the test in-memory DB)
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
                "dim_in": 6, "pca_components": 4,
                "classes": ["Brain", "Spine"],
                "per_class_counts": {"Brain": 5, "Spine": 5},
                "n_samples": 10, "accuracy": 0.94,
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


# ---------------------------------------------------------------------------
# apply()
# ---------------------------------------------------------------------------


def test_apply_requires_trained_classifier(env, monkeypatch):
    svc = CohortBodyPartQCService()
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    with pytest.raises(ValueError, match="No model selected"):
        svc.apply(env["cohort_id"])


def test_apply_full_flow(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)

    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    embed_calls, infer_calls = _stub_apply_worker(monkeypatch)

    out = svc.apply(cid)

    # Worker called once each.
    assert embed_calls["n"] == 1
    assert infer_calls["n"] == 1
    # 3 axial × 5 slices + 1 sagittal × 3 slices = 18 embed items.
    assert len(embed_calls["items"]) == 18
    # 4 stacks → 4 infer items.
    assert len(infer_calls["items"]) == 4

    # State has current snapshot, no previous yet.
    assert out.has_current is True
    assert out.has_previous is False
    # 3 sessions, summary fields populated.
    assert out.summary.total_sessions == 3
    assert out.summary.total_stacks == 4

    # Stack 301: prior=spine, new=Brain → changed=True.
    by_id = {s.stack_id: s for sess in out.picks for s in sess.stacks}
    assert by_id[301].previous_label == "spine"
    assert by_id[301].label == "Brain"
    assert by_id[301].changed is True
    assert by_id[301].prior_source == "text_keyword"
    # Stack 201: no prior → changed=True (None != Brain).
    assert by_id[201].previous_label is None
    assert by_id[201].changed is True
    assert by_id[201].prior_source is None
    # Stack 101: prior=brain ≠ Brain (case-sensitive match) → changed=True.
    # Our prior is lowercase 'brain', new is 'Brain' → after normalize,
    # both 'brain'/'Brain'. They're stored as-is so 'brain' != 'Brain' → changed.
    assert by_id[101].previous_label == "brain"
    assert by_id[101].label == "Brain"

    # change_matrix references prior labels (or "(none)") and new labels.
    cm = out.summary.change_matrix
    assert "spine" in cm and cm["spine"].get("Brain") == 1
    assert "(none)" in cm and cm["(none)"].get("Brain") == 1


def test_apply_rotates_snapshot_on_second_run(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    svc.apply(cid)

    # Second apply with new predictions — old current → previous.
    _stub_apply_worker(monkeypatch, predictions={301: ("Spine", 0.92)})
    out2 = svc.apply(cid)
    assert out2.has_current is True
    assert out2.has_previous is True
    # Stack 301 is now Spine again.
    by_id = {s.stack_id: s for sess in out2.picks for s in sess.stacks}
    assert by_id[301].label == "Spine"


def test_apply_low_confidence_flags(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    # Force stack 201 below the threshold.
    _stub_apply_worker(monkeypatch, predictions={201: ("Brain", 0.50)})
    out = svc.apply(cid)
    s2 = next(s for s in out.picks if s.subject_id == 2)
    assert s2.needs_check is True
    assert s2.low_conf_count == 1


def test_apply_no_eligible_stacks(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates", lambda name: [])
    with pytest.raises(ValueError, match="No eligible stacks"):
        svc.apply(cid)


def test_apply_unknown_cohort(env, monkeypatch):
    svc = CohortBodyPartQCService()
    monkeypatch.setattr(service_mod, "enumerate_candidates", lambda name: [])
    with pytest.raises(LookupError):
        svc.apply(99999)


# ---------------------------------------------------------------------------
# restore_previous
# ---------------------------------------------------------------------------


def test_restore_previous_swaps_snapshots(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    svc.apply(cid)
    _stub_apply_worker(monkeypatch, predictions={301: ("Spine", 0.92)})
    svc.apply(cid)

    out = svc.restore_previous(cid)
    assert out.has_previous is False
    by_id = {s.stack_id: s for sess in out.picks for s in sess.stacks}
    # Restored to the FIRST apply's predictions.
    assert by_id[301].label == "Brain"


def test_restore_previous_no_previous(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    with pytest.raises(LookupError, match="No previous"):
        svc.restore_previous(cid)


def test_restore_previous_unknown_cohort(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(LookupError):
        svc.restore_previous(99999)


# ---------------------------------------------------------------------------
# override_stack
# ---------------------------------------------------------------------------


def test_override_stack_sets_label_and_recomputes_summary(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    svc.apply(cid)

    sess = svc.override_stack(
        cohort_id=cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Brain-Neck",
    )
    assert sess.subject_id == 1
    assert sess.session_date == "2024-01-01"
    assert 1 in sess.study_ids
    by_id = {s.stack_id: s for s in sess.stacks}
    assert by_id[101].label == "Brain-Neck"
    assert by_id[101].is_override is True
    assert by_id[101].confidence == 1.0
    assert by_id[101].needs_check is False

    # Cohort summary updated.
    state = svc.get_state(cid)
    assert "Brain+Brain-Neck" in state.summary.by_combo


def test_override_stack_unknown_label(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    svc.apply(cid)
    with pytest.raises(ValueError, match="not a configured category"):
        svc.override_stack(
            cid, subject_id=1, session_date="2024-01-01",
            stack_id=101, label="Liver",
        )


def test_override_stack_unknown_stack(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    svc.apply(cid)
    with pytest.raises(LookupError):
        svc.override_stack(
            cid, subject_id=1, session_date="2024-01-01",
            stack_id=9999, label="Brain",
        )


# ---------------------------------------------------------------------------
# reset_session
# ---------------------------------------------------------------------------


def test_reset_session_re_runs_one_session(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    svc.apply(cid)

    # User overrides session 1, then resets it — predictions come back fresh.
    svc.override_stack(
        cid, subject_id=1, session_date="2024-01-01",
        stack_id=101, label="Spine",
    )
    _stub_apply_worker(monkeypatch)  # re-stub; reset will call worker again.
    sess = svc.reset_session(cid, subject_id=1, session_date="2024-01-01")
    by_id = {s.stack_id: s for s in sess.stacks}
    assert by_id[101].label == "Brain"
    assert by_id[101].is_override is False


def test_reset_session_unknown_session(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    _train_a_classifier(svc, cid, monkeypatch)
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    _stub_apply_worker(monkeypatch)
    svc.apply(cid)
    with pytest.raises(LookupError):
        svc.reset_session(cid, subject_id=999, session_date="9999-01-01")


def test_reset_session_no_classifier(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    # No trained classifier yet.
    monkeypatch.setattr(service_mod, "enumerate_candidates",
                        lambda name: _three_session_cohort())
    with pytest.raises(ValueError, match="No model selected"):
        svc.reset_session(cid, subject_id=1, session_date="2024-01-01")
