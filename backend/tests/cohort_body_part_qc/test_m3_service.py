"""M3 — service tests for update_categories / seed / update_samples.

The worker is mocked at ``qc.body_part.service.worker_client`` so no
torch / network is required. ``enumerate_candidates`` is also mocked
to return a synthetic candidate pool.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import (
    Base,
    BodyPartSampleOp,
    CohortBodyPartQCState,
    CreateCohortPayload,
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
    monkeypatch.setattr("qc.body_part.service.session_scope", _scope)

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

    yield {
        "engine": engine, "scope": _scope,
        "cohort_id": cohort.id, "cohort_name": cohort.name,
    }
    cohort_service._initialized = original_init


# ---------------------------------------------------------------------------
# update_categories
# ---------------------------------------------------------------------------


def test_update_categories_replaces_list(env):
    svc = CohortBodyPartQCService()
    out = svc.update_categories(env["cohort_id"], ["Brain", "Spine"])
    assert out.categories == ["Brain", "Spine"]
    # training_summary keys reflect the new categories.
    assert set(out.training_summary.keys()) == {"Brain", "Spine"}


def test_update_categories_dedups_case_insensitive(env):
    svc = CohortBodyPartQCService()
    out = svc.update_categories(env["cohort_id"], ["Brain", "BRAIN", "Spine"])
    # First spelling wins; duplicates dropped.
    assert out.categories == ["Brain", "Spine"]


def test_update_categories_strips_whitespace(env):
    svc = CohortBodyPartQCService()
    out = svc.update_categories(env["cohort_id"], ["  Brain  ", "Spine"])
    assert out.categories == ["Brain", "Spine"]


def test_update_categories_rejects_empty(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(ValueError):
        svc.update_categories(env["cohort_id"], [])


def test_update_categories_rejects_blank_string(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(ValueError):
        svc.update_categories(env["cohort_id"], ["Brain", "   "])


def test_update_categories_drops_orphaned_samples(env):
    svc = CohortBodyPartQCService()
    # Seed some samples.
    svc.update_categories(env["cohort_id"], ["Brain", "Spine"])
    svc.update_samples(env["cohort_id"], [
        BodyPartSampleOp(op="approve", stack_id=1, slice_index=10, label="Brain"),
        BodyPartSampleOp(op="approve", stack_id=2, slice_index=10, label="Spine"),
    ])
    # Drop "Spine" → Spine sample should disappear.
    out = svc.update_categories(env["cohort_id"], ["Brain"])
    with env["scope"]() as s:
        state = s.get(CohortBodyPartQCState, env["cohort_id"])
        labels = [t["label"] for t in state.training_samples]
        assert labels == ["Brain"]
    assert out.categories == ["Brain"]


# ---------------------------------------------------------------------------
# update_samples
# ---------------------------------------------------------------------------


def test_update_samples_approve_then_remove(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=10, slice_index=5, label="Brain"),
        BodyPartSampleOp(op="approve", stack_id=20, slice_index=5, label="Spine"),
    ])
    state = svc.get_state(cid)
    assert state.training_summary["Brain"]["total"] == 1
    assert state.training_summary["Spine"]["total"] == 1

    svc.update_samples(cid, [
        BodyPartSampleOp(op="remove", stack_id=10, slice_index=5),
    ])
    state2 = svc.get_state(cid)
    assert state2.training_summary["Brain"]["total"] == 0
    assert state2.training_summary["Spine"]["total"] == 1


def test_update_samples_replace_overwrites_label(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=10, slice_index=5, label="Brain"),
        BodyPartSampleOp(op="replace", stack_id=10, slice_index=5, label="Spine"),
    ])
    with env["scope"]() as s:
        state = s.get(CohortBodyPartQCState, cid)
        assert len(state.training_samples) == 1
        assert state.training_samples[0]["label"] == "Spine"


def test_update_samples_move_changes_label(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=10, slice_index=5, label="Brain"),
    ])
    svc.update_samples(cid, [
        BodyPartSampleOp(
            op="move", stack_id=10, slice_index=5,
            label="Brain", new_label="Brain-Neck",
        ),
    ])
    with env["scope"]() as s:
        state = s.get(CohortBodyPartQCState, cid)
        assert state.training_samples[0]["label"] == "Brain-Neck"


def test_update_samples_move_unknown_label(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=10, slice_index=5, label="Brain"),
    ])
    with pytest.raises(ValueError):
        svc.update_samples(cid, [
            BodyPartSampleOp(
                op="move", stack_id=10, slice_index=5,
                label="Brain", new_label="Liver",
            ),
        ])


def test_update_samples_move_missing_entry(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    with pytest.raises(ValueError):
        svc.update_samples(cid, [
            BodyPartSampleOp(
                op="move", stack_id=99, slice_index=5,
                label="Brain", new_label="Spine",
            ),
        ])


def test_update_samples_unknown_op(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(ValueError):
        svc.update_samples(env["cohort_id"], [
            BodyPartSampleOp(op="poke", stack_id=1, slice_index=1, label="Brain"),
        ])


def test_update_samples_approve_unknown_label(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(ValueError):
        svc.update_samples(env["cohort_id"], [
            BodyPartSampleOp(op="approve", stack_id=1, slice_index=1, label="Liver"),
        ])


def test_update_samples_approve_missing_label(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(ValueError):
        svc.update_samples(env["cohort_id"], [
            BodyPartSampleOp(op="approve", stack_id=1, slice_index=1),
        ])


# ---------------------------------------------------------------------------
# seed — uses worker stub + candidate stub
# ---------------------------------------------------------------------------


def _fake_candidates() -> list[CandidateStack]:
    return [
        CandidateStack(
            stack_id=100 + i, slice_index=10,
            series_instance_uid=f"1.2.3.{i}",
            study_id=200 + i, subject_id=300 + i,
            subject_code=f"sub-{i:03d}",
            session_date="2024-01-01", technique="MPRAGE",
            orientation="axial", body_part=None, num_slices=200,
        )
        for i in range(5)
    ]


def test_seed_returns_hydrated_candidates(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]

    # Make sure the cohort has the requested category.
    svc.update_categories(cid, ["Brain", "Spine"])

    monkeypatch.setattr(service_mod, "enumerate_candidates", lambda name: _fake_candidates())

    # Worker returns 3 candidates from the 5 we offered.
    def fake_seed(category, items, n_target=100, p_min=None, m_min=None):
        assert category == "Brain"
        assert len(items) == 5
        return {
            "category": category,
            "n_input": 5, "n_kept": 5, "n_picked": 3,
            "encoder_name": "biomedclip", "encoder_version": "v1.0.0",
            "candidates": [
                {"stack_id": 100, "slice_index": 10, "zs_prob": 0.92, "margin": 0.40},
                {"stack_id": 102, "slice_index": 10, "zs_prob": 0.85, "margin": 0.30},
                {"stack_id": 104, "slice_index": 10, "zs_prob": 0.71, "margin": 0.10},
            ],
        }
    monkeypatch.setattr(service_mod.worker_client, "seed", fake_seed)

    out = svc.seed(cid, "Brain", n_target=10)
    assert len(out) == 3
    by_id = {c.stack_id: c for c in out}
    assert by_id[100].zs_prob == pytest.approx(0.92)
    assert by_id[100].subject_code == "sub-000"
    assert by_id[100].orientation == "axial"
    assert by_id[100].thumbnail_url.startswith("/api/qc/dicom/stack/100/slice/10")


def test_seed_unknown_category_raises(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    monkeypatch.setattr(service_mod, "enumerate_candidates", lambda name: _fake_candidates())
    with pytest.raises(ValueError):
        svc.seed(cid, "Liver", n_target=10)


def test_seed_no_candidates_returns_empty(env, monkeypatch):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    monkeypatch.setattr(service_mod, "enumerate_candidates", lambda name: [])
    out = svc.seed(cid, "Brain", n_target=10)
    assert out == []


def test_seed_n_target_must_be_positive(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(ValueError):
        svc.seed(env["cohort_id"], "Brain", n_target=0)


def test_seed_unknown_cohort_raises(env, monkeypatch):
    svc = CohortBodyPartQCService()
    monkeypatch.setattr(service_mod, "enumerate_candidates", lambda name: [])
    with pytest.raises(LookupError):
        svc.seed(99999, "Brain", n_target=10)


def test_seed_drops_unknown_pairs_from_worker(env, monkeypatch):
    """If the worker returns a (stack_id, slice_index) we don't know
    about (e.g. metadata changed mid-flight), it's silently dropped."""
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]

    monkeypatch.setattr(service_mod, "enumerate_candidates", lambda name: _fake_candidates())
    def fake_seed(category, items, n_target=100, p_min=None, m_min=None):
        return {
            "category": category, "n_input": 5, "n_kept": 5, "n_picked": 2,
            "encoder_name": "biomedclip", "encoder_version": "v1.0.0",
            "candidates": [
                {"stack_id": 100, "slice_index": 10, "zs_prob": 0.9, "margin": 0.3},
                {"stack_id": 999, "slice_index": 10, "zs_prob": 0.8, "margin": 0.2},
            ],
        }
    monkeypatch.setattr(service_mod.worker_client, "seed", fake_seed)

    out = svc.seed(cid, "Brain", n_target=5)
    assert [c.stack_id for c in out] == [100]


# ---------------------------------------------------------------------------
# list_samples — hydrated training set view (used by the frontend
# SamplesGrid).
# ---------------------------------------------------------------------------


def test_list_samples_returns_hydrated_with_thumbnails(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=10, slice_index=5, label="Brain"),
        BodyPartSampleOp(op="approve", stack_id=20, slice_index=7, label="Spine"),
    ])
    out = svc.list_samples(cid)
    assert len(out) == 2
    by_id = {s.stack_id: s for s in out}
    assert by_id[10].label == "Brain"
    assert by_id[10].thumbnail_url == "/api/qc/dicom/stack/10/slice/5/thumbnail"
    assert by_id[20].thumbnail_url == "/api/qc/dicom/stack/20/slice/7/thumbnail"


def test_list_samples_filter_by_label(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=10, slice_index=5, label="Brain"),
        BodyPartSampleOp(op="approve", stack_id=20, slice_index=7, label="Spine"),
        BodyPartSampleOp(op="approve", stack_id=30, slice_index=3, label="Brain"),
    ])
    out = svc.list_samples(cid, label="Brain")
    assert {s.stack_id for s in out} == {10, 30}


def test_list_samples_empty_for_fresh_cohort(env):
    svc = CohortBodyPartQCService()
    out = svc.list_samples(env["cohort_id"])
    assert out == []


def test_list_samples_unknown_cohort_raises(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(LookupError):
        svc.list_samples(99999)


def test_list_samples_orders_by_approved_at_desc(env):
    svc = CohortBodyPartQCService()
    cid = env["cohort_id"]
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=10, slice_index=5, label="Brain"),
    ])
    svc.update_samples(cid, [
        BodyPartSampleOp(op="approve", stack_id=20, slice_index=5, label="Brain"),
    ])
    out = svc.list_samples(cid)
    # Newest first.
    assert out[0].stack_id == 20
    assert out[1].stack_id == 10
