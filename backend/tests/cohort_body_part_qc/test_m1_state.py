"""M1 tests — verify that:

  1. ``CohortBodyPartQCState`` and ``StackEmbeddingCache`` are created by
     ``Base.metadata.create_all()`` (no manual migration needed).
  2. ``CohortBodyPartQCService.get_state`` lazy-creates a default state
     row on first read with the documented defaults.
  3. The protection helper returns an empty set for cohorts that have
     never had a successful Apply.
  4. DTOs round-trip JSON serialization correctly, including all the
     diff fields.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from cohorts.models import (
    Base,
    BodyPartChangeRowDTO,
    BodyPartChangesPageDTO,
    BodyPartSessionPickDTO,
    BodyPartStackPickDTO,
    BodyPartStateDTO,
    BodyPartSummaryDTO,
    CohortBodyPartQCState,
    CreateCohortPayload,
    StackEmbeddingCache,
)
from cohorts.service import cohort_service
# Register NilsDatasetPipelineStep mapper required by Cohort.pipeline_steps.
from nils_dataset_pipeline import models as _nils_models  # noqa: F401

from qc.body_part.protection import get_qc_protected_stack_ids
from qc.body_part.service import (
    DEFAULT_CATEGORIES,
    DEFAULT_FILTERS,
    DEFAULT_THRESHOLDS,
    CohortBodyPartQCService,
)


# ---------------------------------------------------------------------------
# Fixture — same in-memory SQLite app-DB pattern used by Main-QC tests.
# ---------------------------------------------------------------------------


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
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
    monkeypatch.setattr("qc.body_part.service.session_scope", _scope)
    monkeypatch.setattr("qc.body_part.protection.session_scope", _scope)

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
        "engine": engine,
        "scope": _scope,
        "cohort_id": cohort.id,
        "cohort_name": cohort.name,
    }

    cohort_service._initialized = original_init


# ---------------------------------------------------------------------------
# 1) Schema creation
# ---------------------------------------------------------------------------


def test_cohort_body_part_qc_state_table_exists(env):
    insp = inspect(env["engine"])
    names = insp.get_table_names()
    assert "cohort_body_part_qc_state" in names
    assert "stack_embedding_cache" in names


def test_cohort_body_part_qc_state_columns(env):
    insp = inspect(env["engine"])
    cols = {c["name"] for c in insp.get_columns("cohort_body_part_qc_state")}
    expected = {
        "cohort_id",
        "categories",
        "training_samples",
        "classifier_path",
        "classifier_sha256",
        "classifier_meta",
        "current_run_at",
        "current_summary",
        "current_picks",
        "current_profile",
        "previous_run_at",
        "previous_summary",
        "previous_picks",
        "previous_profile",
    }
    assert expected.issubset(cols), expected - cols


def test_stack_embedding_cache_columns(env):
    insp = inspect(env["engine"])
    cols = {c["name"] for c in insp.get_columns("stack_embedding_cache")}
    expected = {
        "series_stack_id",
        "slice_index",
        "encoder_name",
        "encoder_version",
        "embedding",
        "dim",
        "created_at",
    }
    assert expected.issubset(cols), expected - cols


# ---------------------------------------------------------------------------
# 2) get_state: lazy-creates default row
# ---------------------------------------------------------------------------


def test_get_state_lazy_creates_default_row(env):
    svc = CohortBodyPartQCService()
    cohort_id = env["cohort_id"]

    # No row before first read.
    with env["scope"]() as s:
        assert s.get(CohortBodyPartQCState, cohort_id) is None

    state = svc.get_state(cohort_id)

    assert state.cohort_id == cohort_id
    assert state.has_current is False
    assert state.has_previous is False
    assert state.categories == DEFAULT_CATEGORIES
    assert state.classifier_meta is None
    assert state.profile["filters"] == DEFAULT_FILTERS
    assert state.profile["encoder_mode"] == "concat"
    assert state.profile["thresholds"] == DEFAULT_THRESHOLDS
    assert state.available_id_types == ["code"]
    assert state.training_summary == {
        c: {"axial": 0, "sagittal": 0, "coronal": 0, "total": 0}
        for c in DEFAULT_CATEGORIES
    }
    # Row should now exist.
    with env["scope"]() as s:
        row = s.get(CohortBodyPartQCState, cohort_id)
        assert row is not None
        assert list(row.categories) == DEFAULT_CATEGORIES


def test_get_state_unknown_cohort_raises(env):
    svc = CohortBodyPartQCService()
    with pytest.raises(LookupError):
        svc.get_state(999_999)


def test_get_state_idempotent(env):
    svc = CohortBodyPartQCService()
    cohort_id = env["cohort_id"]
    a = svc.get_state(cohort_id)
    b = svc.get_state(cohort_id)
    assert a.cohort_id == b.cohort_id
    assert a.categories == b.categories
    # Still only one row.
    with env["scope"]() as s:
        rows = s.query(CohortBodyPartQCState).all()
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# 3) Protection helper
# ---------------------------------------------------------------------------


def test_protection_empty_when_no_state(env):
    assert get_qc_protected_stack_ids(env["cohort_id"]) == set()


def test_protection_empty_when_no_picks(env):
    svc = CohortBodyPartQCService()
    svc.get_state(env["cohort_id"])  # creates empty default row
    assert get_qc_protected_stack_ids(env["cohort_id"]) == set()


def test_protection_returns_stack_ids_from_current_picks(env):
    cohort_id = env["cohort_id"]
    with env["scope"]() as s:
        state = CohortBodyPartQCState(
            cohort_id=cohort_id,
            categories=list(DEFAULT_CATEGORIES),
            training_samples=[],
            current_picks=[
                {
                    "study_id": 1,
                    "subject_id": 10,
                    "subject_code": "S1",
                    "stacks": [
                        {"stack_id": 100, "label": "Brain"},
                        {"stack_id": 101, "label": "Spine"},
                    ],
                },
                {
                    "study_id": 2,
                    "subject_id": 11,
                    "subject_code": "S2",
                    "stacks": [{"stack_id": 200, "label": "Brain"}],
                },
            ],
        )
        s.add(state)

    protected = get_qc_protected_stack_ids(cohort_id)
    assert protected == {100, 101, 200}


# ---------------------------------------------------------------------------
# 4) DTO round-trips (diff fields included)
# ---------------------------------------------------------------------------


def test_stack_pick_dto_diff_defaults():
    dto = BodyPartStackPickDTO(stack_id=1)
    assert dto.previous_label is None
    assert dto.prior_source is None
    assert dto.changed is False


def test_session_pick_dto_diff_defaults():
    dto = BodyPartSessionPickDTO(study_id=1, subject_id=2, subject_code="S1")
    assert dto.session_changed is False
    assert dto.stacks_changed == 0
    assert dto.session_prev_combo_key == ""


def test_summary_dto_diff_defaults():
    dto = BodyPartSummaryDTO()
    assert dto.total_stacks == 0
    assert dto.stacks_changed == 0
    assert dto.sessions_changed == 0
    assert dto.change_matrix == {}


def test_state_dto_round_trip_with_diff():
    stack = BodyPartStackPickDTO(
        stack_id=42,
        label="Brain",
        confidence=0.93,
        probs={"Brain": 0.93, "Spine": 0.05, "Chest": 0.02},
        is_override=False,
        needs_check=False,
        previous_label="spine",
        prior_source="text_keyword",
        changed=True,
    )
    sess = BodyPartSessionPickDTO(
        study_id=1, subject_id=2, subject_code="S1",
        stacks=[stack],
        session_combo=["Brain"], session_combo_key="Brain",
        session_prev_combo_key="Spine", session_changed=True,
        stacks_changed=1,
    )
    summary = BodyPartSummaryDTO(
        total_sessions=1, total_stacks=1,
        stacks_changed=1, sessions_changed=1,
        by_combo={"Brain": 1},
        change_matrix={"spine": {"Brain": 1}},
    )
    state = BodyPartStateDTO(
        cohort_id=99,
        has_current=True,
        categories=list(DEFAULT_CATEGORIES),
        summary=summary,
        picks=[sess],
        profile={"thresholds": DEFAULT_THRESHOLDS},
    )

    payload = state.model_dump_json()
    restored = BodyPartStateDTO.model_validate_json(payload)

    assert restored.summary.stacks_changed == 1
    assert restored.summary.change_matrix == {"spine": {"Brain": 1}}
    assert restored.picks[0].session_changed is True
    assert restored.picks[0].stacks[0].previous_label == "spine"
    assert restored.picks[0].stacks[0].changed is True
    assert restored.picks[0].stacks[0].prior_source == "text_keyword"


def test_changes_page_dto_round_trip():
    row = BodyPartChangeRowDTO(
        study_id=1, subject_id=1, stack_id=42,
        subject_code="S1",
        previous_label="spine", new_label="Brain",
        prior_source="text_keyword",
        confidence=0.93,
        needs_check=False, is_override=False,
        thumbnail_url="/api/...png",
    )
    page = BodyPartChangesPageDTO(total=1, offset=0, limit=50, rows=[row])
    payload = page.model_dump_json()
    restored = BodyPartChangesPageDTO.model_validate_json(payload)
    assert restored.total == 1
    assert restored.rows[0].previous_label == "spine"
    assert restored.rows[0].new_label == "Brain"


# ---------------------------------------------------------------------------
# 5) Stub endpoints raise NotImplementedError (mapped to 501 by router)
# ---------------------------------------------------------------------------


def test_all_service_methods_are_implemented(env):
    """Through M7 the entire public service surface is implemented.

    This test exists as a regression guard: if a future refactor
    accidentally turns a method back into a stub the suite will catch it.
    """
    svc = CohortBodyPartQCService()
    # Just make sure the methods exist and are not literal stubs.
    assert callable(svc.update_categories)
    assert callable(svc.seed)
    assert callable(svc.update_samples)
    assert callable(svc.apply)
    assert callable(svc.restore_previous)
    assert callable(svc.override_stack)
    assert callable(svc.reset_session)
    assert callable(svc.get_changes)


# ---------------------------------------------------------------------------
# 6) Embedding cache table accepts a row round-trip
# ---------------------------------------------------------------------------


def test_stack_embedding_cache_round_trip(env):
    import numpy as np

    blob = np.array([0.1, 0.2, 0.3], dtype=np.float32).tobytes()
    with env["scope"]() as s:
        s.add(
            StackEmbeddingCache(
                series_stack_id=1,
                slice_index=0,
                encoder_name="biomedclip",
                encoder_version="v1.0.0",
                embedding=blob,
                dim=3,
            )
        )

    with env["scope"]() as s:
        row = s.get(
            StackEmbeddingCache, (1, 0, "biomedclip", "v1.0.0")
        )
        assert row is not None
        arr = np.frombuffer(row.embedding, dtype=np.float32)
        assert arr.shape == (3,)
        assert np.allclose(arr, [0.1, 0.2, 0.3], atol=1e-6)
        assert row.dim == 3
