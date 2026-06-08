"""Shared fixtures and stubs for the cohort Body Part QC test suite.

Extracted from M3 / M6 / M10 tests so M11's end-to-end smoke test (and
any future tests) can reuse the boilerplate without copy-pasting.

Importing this module is side-effect free; everything is exposed as
plain functions / pytest fixtures via ``@pytest.fixture``.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import Base, BodyPartSampleOp, CreateCohortPayload
from cohorts.service import cohort_service
# Register NilsDatasetPipelineStep mapper required by Cohort.pipeline_steps.
from nils_dataset_pipeline import models as _nils_models  # noqa: F401

from qc.body_part import service as service_mod
from qc.body_part.candidates import CandidateStack
from qc.body_part.service import CohortBodyPartQCService


# ---------------------------------------------------------------------------
# In-memory app DB
# ---------------------------------------------------------------------------


def make_app_db_env(tmp_path: Path, monkeypatch, cohort_name: str = "alpha-cohort"):
    """Build an in-memory SQLite app DB and a freshly created cohort.

    Returns a dict with::

        engine, scope, cohort_id, cohort_name

    The fixture patches ``cohorts.service.engine`` /
    ``cohorts.service.session_scope`` and ``qc.body_part.service.session_scope``
    so the cohort + body-part-qc services hit this DB. Pipeline-step
    initialization is no-op'd to keep the test light.

    Caller is responsible for unwinding ``cohort_service._initialized``
    by capturing it from the returned dict.
    """
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
    monkeypatch.setattr("qc.body_part.service.session_scope", _scope)

    monkeypatch.setattr(cohort_service, "_initialize_pipeline_steps", lambda *a, **kw: None)
    monkeypatch.setattr(cohort_service, "_reinitialize_pipeline_steps", lambda *a, **kw: None)
    monkeypatch.setattr(cohort_service, "get_stages_from_pipeline", lambda *a, **kw: [])
    original = cohort_service._initialized
    cohort_service._initialized = True

    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    cohort = cohort_service.create_cohort(
        CreateCohortPayload(name=cohort_name, source_path=str(src))
    )
    return {
        "engine": engine,
        "scope": _scope,
        "cohort_id": cohort.id,
        "cohort_name": cohort.name,
        "_original_initialized": original,
    }


@pytest.fixture
def app_db_env(tmp_path: Path, monkeypatch) -> Iterator[dict]:
    """pytest fixture wrapper for :func:`make_app_db_env`."""
    env = make_app_db_env(tmp_path, monkeypatch)
    try:
        yield env
    finally:
        cohort_service._initialized = env["_original_initialized"]


# ---------------------------------------------------------------------------
# Synthetic candidates
# ---------------------------------------------------------------------------


def synthetic_candidates() -> list[CandidateStack]:
    """Three sessions, varied prior body_part labels.

    Used by the M6 / M11 apply tests to exercise the diff/change-matrix
    machinery against a deterministic, easy-to-reason-about cohort.

    Sessions::

        1: 2 stacks (101, 102) — prior "brain"
        2: 1 stack  (201)      — no prior label
        3: 1 stack  (301)      — prior "spine"
    """
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


# ---------------------------------------------------------------------------
# Worker stubs — embed / infer / train / seed
# ---------------------------------------------------------------------------


def stub_embed_worker(monkeypatch) -> dict:
    """Replace ``worker_client.embed`` with a no-op that records calls."""
    calls: dict = {"n": 0, "items": None, "mode": None}

    def fake_embed(items, *, mode):
        calls["n"] += 1
        calls["items"] = list(items)
        calls["mode"] = mode
        return {
            "encoder_chain": ["biomedclip", "siglip2"],
            "dim_total": 6,
            "n_items": len(items),
            "n_cached": 0,
            "n_computed": len(items),
            "cached_keys": [],
        }

    monkeypatch.setattr(service_mod.worker_client, "embed", fake_embed)
    return calls


def stub_infer_worker(monkeypatch, predictions: dict | None = None) -> dict:
    """Replace ``worker_client.infer`` with a deterministic stub.

    ``predictions`` is a ``{stack_id: (label, confidence)}`` map. Stacks
    not present default to ``(None, 0.0)`` (= no embeddings / no prediction).
    """
    calls: dict = {"n": 0, "items": None}
    pred_map = dict(predictions or {})

    def fake_infer(*, classifier_path, classifier_sha256, classes,
                   encoder_chain, items, manual_review_below=None,
                   n_slices=None):
        calls["n"] += 1
        calls["items"] = list(items)
        results = []
        for it in items:
            sid = it["stack_id"]
            label, conf = pred_map.get(sid, (None, 0.0))
            results.append({
                "stack_id": sid, "label": label,
                "confidence": float(conf),
                "probs": {label: float(conf)} if label else {},
                "needs_check": (label is None)
                    or (conf < (manual_review_below or 0.7)),
                "n_slices_used": 3 if label else 0,
            })
        return {
            "n_items": len(items),
            "n_predicted": sum(1 for r in results if r["label"]),
            "n_low_conf": sum(1 for r in results if r["needs_check"]),
            "n_no_embeddings": sum(1 for r in results if r["n_slices_used"] == 0),
            "results": results,
        }

    monkeypatch.setattr(service_mod.worker_client, "infer", fake_infer)
    return calls


def stub_train_worker(monkeypatch, *, accuracy: float = 1.0) -> dict:
    """Replace ``worker_client.train`` with a no-op trainer."""
    calls: dict = {"n": 0, "samples": None}

    def fake_train(cohort_id, samples, *, mode, **kw):
        calls["n"] += 1
        calls["samples"] = list(samples)
        # ``samples`` may be a list of dicts or of BodyPartSampleOp DTOs
        # depending on the call site — accept both.
        def _label(s):
            return s["label"] if isinstance(s, dict) else s.label
        classes = sorted({_label(s) for s in samples})
        return {
            "classifier_path": f"/fake/cohort_{cohort_id}.joblib",
            "classifier_sha256": "deadbeef" * 8,
            "classifier_meta": {
                "cohort_id": cohort_id,
                "encoder_chain": [
                    {"name": "biomedclip", "version": "v1.0.0", "dim": 4},
                    {"name": "siglip2", "version": "v1.0.0", "dim": 2},
                ],
                "dim_in": 6, "pca_components": 4,
                "classes": classes,
                "per_class_counts": {c: 5 for c in classes},
                "n_samples": len(samples), "accuracy": accuracy,
                "trained_at": "2026-04-26T10:00:00+00:00",
            },
        }

    monkeypatch.setattr(service_mod.worker_client, "train", fake_train)
    return calls


def stub_seed_worker(monkeypatch, picked: list[tuple[int, int, float, float]]) -> dict:
    """Replace ``worker_client.seed`` with a stub returning ``picked``.

    ``picked`` is a list of ``(stack_id, slice_index, zs_prob, margin)``.
    """
    calls: dict = {"n": 0, "category": None, "items": None}

    def fake_seed(category, items, n_target=100, p_min=None, m_min=None):
        calls["n"] += 1
        calls["category"] = category
        calls["items"] = list(items)
        return {
            "category": category,
            "n_input": len(items),
            "n_kept": len(items),
            "n_picked": len(picked),
            "encoder_name": "biomedclip",
            "encoder_version": "v1.0.0",
            "candidates": [
                {"stack_id": sid, "slice_index": idx,
                 "zs_prob": p, "margin": m}
                for (sid, idx, p, m) in picked
            ],
        }

    monkeypatch.setattr(service_mod.worker_client, "seed", fake_seed)
    return calls


def stub_token_writer(monkeypatch) -> dict:
    """Replace ``token_writer.apply_picks_to_metadata`` with a recorder."""
    calls: dict = {"n": 0, "picks": []}

    def fake_apply(picks, session_factory=None):
        calls["n"] += 1
        calls["picks"].append([dict(p) for p in picks])
        return {"body_part_updated": 0, "low_conf_added": 0,
                "low_conf_removed": 0}

    monkeypatch.setattr(
        service_mod.token_writer, "apply_picks_to_metadata", fake_apply,
    )
    return calls


# ---------------------------------------------------------------------------
# Convenience: training data + classifier in one call
# ---------------------------------------------------------------------------


def quick_train(svc: CohortBodyPartQCService, cohort_id: int, monkeypatch,
                *, categories: list[str] | None = None,
                samples_per_class: int = 5) -> None:
    """Set up categories, approve enough samples, and train a stub classifier.

    By the time this returns the cohort has a row with
    ``classifier_path`` / ``classifier_sha256`` / ``classifier_meta``
    populated and is ready to call ``apply``.
    """
    cats = categories or ["Brain", "Spine"]
    svc.update_categories(cohort_id, cats)
    ops = []
    sid = 1
    for label in cats:
        for _ in range(samples_per_class):
            ops.append(BodyPartSampleOp(
                op="approve", stack_id=sid, slice_index=10, label=label,
            ))
            sid += 1
    svc.update_samples(cohort_id, ops)

    stub_embed_worker(monkeypatch)
    stub_train_worker(monkeypatch)
    svc.train(cohort_id, mode="concat")
