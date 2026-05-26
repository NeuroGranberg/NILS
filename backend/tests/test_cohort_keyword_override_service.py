"""Service + DB-level tests for cohort keyword overrides."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from cohorts import keyword_override_service
from cohorts.models import (
    Base,
    CohortClassificationOverride,
    CreateCohortPayload,
)
from cohorts.service import cohort_service

# Importing this module registers the NilsDatasetPipelineStep mapper which
# is referenced by Cohort.pipeline_steps. Without it SQLAlchemy can't
# configure the Cohort mapper.
from nils_dataset_pipeline import models as _nils_models  # noqa: F401


@pytest.fixture
def cohort_env(tmp_path: Path, monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )
    # Skip the pipeline_steps table (it uses JSONB which SQLite can't compile)
    # and is orthogonal to what we're testing.
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

    # cohort_service uses its own session_scope; patch both places.
    monkeypatch.setattr("cohorts.service.engine", engine)
    monkeypatch.setattr("cohorts.service.session_scope", _scope)
    monkeypatch.setattr("cohorts.keyword_override_service.session_scope", _scope)

    # The pipeline initializer talks to Postgres for the pipeline_steps
    # table (which we intentionally skipped above). Stub it out.
    monkeypatch.setattr(
        cohort_service, "_initialize_pipeline_steps",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        cohort_service, "_reinitialize_pipeline_steps",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        cohort_service, "get_stages_from_pipeline",
        lambda *a, **kw: [],
    )

    original_init = cohort_service._initialized
    cohort_service._initialized = True  # skip auto-init (we built tables)

    source = tmp_path / "src"
    source.mkdir()
    cohort = cohort_service.create_cohort(
        CreateCohortPayload(name="CohortA", source_path=str(source))
    )

    yield {"engine": engine, "SessionLocal": SessionLocal, "cohort_id": cohort.id, "scope": _scope}

    cohort_service._initialized = original_init


def test_get_config_returns_full_catalog_with_empty_deltas(cohort_env):
    config = keyword_override_service.get_config(cohort_env["cohort_id"])
    axes = {a.axis for a in config.axes}
    assert "base" in axes
    assert "contrast" in axes
    # Every bucket starts with empty deltas. Effective is a case-insensitive
    # dedup of defaults (preserving the first occurrence verbatim).
    for axis in config.axes:
        for bucket in axis.buckets:
            assert bucket.added == []
            assert bucket.removed == []
            effective_keys = [k.strip().lower() for k in bucket.effective]
            expected_keys: list[str] = []
            seen: set[str] = set()
            for k in bucket.defaults:
                key = k.strip().lower() if isinstance(k, str) else None
                if not key or key in seen:
                    continue
                seen.add(key)
                expected_keys.append(key)
            assert effective_keys == expected_keys


def test_update_bucket_persists_delta(cohort_env):
    cohort_id = cohort_env["cohort_id"]
    view = keyword_override_service.update_bucket(
        cohort_id, "contrast", "negative_keywords",
        added=["sem contraste"], removed=[" -c"],
    )
    assert "sem contraste" in view.effective
    assert all(k.lower() != "-c" for k in view.effective if k.strip().lower() == "-c")

    # Row exists in DB.
    with cohort_env["scope"]() as s:
        row = s.scalar(
            select(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id,
            )
        )
        assert row is not None
        assert row.added == ["sem contraste"]
        assert row.removed == [" -c"]


def test_update_bucket_empty_delta_removes_row(cohort_env):
    cohort_id = cohort_env["cohort_id"]
    keyword_override_service.update_bucket(
        cohort_id, "contrast", "negative_keywords",
        added=["tmp"], removed=[],
    )
    keyword_override_service.update_bucket(
        cohort_id, "contrast", "negative_keywords",
        added=[], removed=[],
    )
    with cohort_env["scope"]() as s:
        count = s.scalar(
            select(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id,
            )
        )
        assert count is None


def test_update_bucket_rejects_unknown_axis(cohort_env):
    with pytest.raises(ValueError, match="Unknown axis"):
        keyword_override_service.update_bucket(
            cohort_env["cohort_id"], "nonsense", "whatever",
            added=["a"], removed=[],
        )


def test_update_bucket_rejects_unknown_bucket_path(cohort_env):
    with pytest.raises(ValueError, match="Unknown bucket path"):
        keyword_override_service.update_bucket(
            cohort_env["cohort_id"], "base", "bases.Imaginary.keywords",
            added=["x"], removed=[],
        )


def test_update_bucket_rejects_overlap(cohort_env):
    with pytest.raises(ValueError, match="both added and removed"):
        keyword_override_service.update_bucket(
            cohort_env["cohort_id"], "contrast", "negative_keywords",
            added=["FOO"], removed=["foo"],
        )


def test_update_bucket_normalizes_dedup_case_insensitive(cohort_env):
    view = keyword_override_service.update_bucket(
        cohort_env["cohort_id"], "contrast", "negative_keywords",
        added=["foo", "Foo", "  ", "FOO"], removed=[],
    )
    # Only one entry stored (case-insensitive dedup); whitespace-only ignored.
    assert len(view.added) == 1
    assert view.added[0].lower() == "foo"


def test_reset_bucket_deletes_row(cohort_env):
    cohort_id = cohort_env["cohort_id"]
    keyword_override_service.update_bucket(
        cohort_id, "contrast", "negative_keywords",
        added=["tmp"], removed=[],
    )
    keyword_override_service.reset_bucket(cohort_id, "contrast", "negative_keywords")
    with cohort_env["scope"]() as s:
        count = s.scalar(
            select(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id,
            )
        )
        assert count is None


def test_reset_all_wipes_every_row(cohort_env):
    cohort_id = cohort_env["cohort_id"]
    keyword_override_service.update_bucket(
        cohort_id, "contrast", "negative_keywords", added=["a"], removed=[],
    )
    keyword_override_service.update_bucket(
        cohort_id, "contrast", "positive_keywords", added=["b"], removed=[],
    )
    keyword_override_service.reset_all(cohort_id)
    with cohort_env["scope"]() as s:
        rows = s.scalars(
            select(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id,
            )
        ).all()
        assert rows == []


def test_load_override_map_is_empty_for_fresh_cohort(cohort_env):
    out = keyword_override_service.load_override_map(cohort_env["cohort_id"])
    assert out == {}


def test_load_override_map_roundtrip(cohort_env):
    cohort_id = cohort_env["cohort_id"]
    keyword_override_service.update_bucket(
        cohort_id, "base", "bases.T1w.keywords",
        added=["custom_t1"], removed=["t1w"],
    )
    out = keyword_override_service.load_override_map(cohort_id)
    assert out == {
        ("base", "bases.T1w.keywords"): {
            "added": ["custom_t1"],
            "removed": ["t1w"],
        },
    }


def test_get_global_catalog_has_no_cohort_deltas(cohort_env):
    # Even with overrides present, the global catalog shows defaults only.
    keyword_override_service.update_bucket(
        cohort_env["cohort_id"], "contrast", "negative_keywords",
        added=["should_not_appear"], removed=[],
    )
    axes = keyword_override_service.get_global_catalog()
    for axis in axes:
        for bucket in axis.buckets:
            assert bucket.added == []
            assert bucket.removed == []
            assert bucket.effective == bucket.defaults
