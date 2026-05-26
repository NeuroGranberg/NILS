"""M11 — End-to-end smoke test.

Walks the **entire user flow** end-to-end through the service layer:

  Setup ▸ Seed ▸ Approve samples ▸ Train ▸ Apply ▸ Diff ▸
  Override ▸ Reset ▸ Restore-previous

The point isn't to re-test every branch (per-milestone tests already
do that) but to make sure the milestones compose. If anyone breaks
the end-to-end happy path, this test will catch it.

Worker calls are stubbed via the helpers in ``_fixtures.py`` so this
test runs in < 100 ms with no GPU / no network / no DICOM I/O.
"""

from __future__ import annotations

import pytest

from cohorts.models import (
    BodyPartChangesPageDTO, BodyPartStateDTO, CohortBodyPartQCState,
)
from qc.body_part import service as service_mod
from qc.body_part.service import CohortBodyPartQCService

from tests.cohort_body_part_qc._fixtures import (
    app_db_env,            # pytest fixture (re-exported)
    quick_train,
    stub_embed_worker,
    stub_infer_worker,
    stub_seed_worker,
    stub_token_writer,
    synthetic_candidates,
)

# Re-export so pytest can find the fixture by name.
__all__ = ["app_db_env"]


# ---------------------------------------------------------------------------
# End-to-end: setup → seed → approve → train → apply → diff →
#             override → reset → restore-previous
# ---------------------------------------------------------------------------


def test_full_lifecycle_smoke(app_db_env, monkeypatch):
    """Exercise every public service method in order and verify
    invariants at each step. Read top-to-bottom for the user story."""
    svc = CohortBodyPartQCService()
    cid = app_db_env["cohort_id"]

    # ── Stub everything that crosses a process boundary ───────────────
    monkeypatch.setattr(
        service_mod, "enumerate_candidates",
        lambda name: synthetic_candidates(),
    )
    token_calls = stub_token_writer(monkeypatch)

    # ── 1. Setup categories ───────────────────────────────────────────
    state0 = svc.update_categories(cid, ["Brain", "Spine", "Chest"])
    assert state0.categories == ["Brain", "Spine", "Chest"]
    assert state0.has_current is False
    assert state0.classifier_meta is None

    # ── 2. Seed candidates for "Brain" ────────────────────────────────
    # Worker picks 3 of the 4 cohort stacks; service hydrates each
    # candidate with subject_code / orientation / thumbnail URL.
    stub_seed_worker(monkeypatch, picked=[
        (101, 10, 0.95, 0.40),
        (102, 10, 0.90, 0.30),
        (201, 10, 0.70, 0.10),
    ])
    candidates = svc.seed(cid, "Brain", n_target=10)
    assert {c.stack_id for c in candidates} == {101, 102, 201}
    assert all(c.thumbnail_url.startswith("/api/qc/dicom/stack/") for c in candidates)

    # ── 3. Approve a training set (≥5 per class) ──────────────────────
    # Use quick_train to get a valid trained classifier on the books.
    # (Note: this overrides the previous svc.update_categories with a
    # 2-category set, which is fine for the smoke flow.)
    quick_train(svc, cid, monkeypatch, categories=["Brain", "Spine"])
    state1 = svc.get_state(cid)
    assert state1.classifier_meta is not None
    assert state1.classifier_meta["accuracy"] == 1.0
    assert state1.classifier_meta["n_samples"] == 10
    assert state1.training_summary["Brain"]["total"] == 5
    assert state1.training_summary["Spine"]["total"] == 5

    # ── 4. Apply across the cohort ────────────────────────────────────
    # 101/102: prior "brain" → predict "Brain"  (case-changed → diff)
    # 201    : prior None    → predict "Brain"  (added → diff, low-conf)
    # 301    : prior "spine" → predict "Brain"  (relabel → diff)
    stub_embed_worker(monkeypatch)
    stub_infer_worker(monkeypatch, predictions={
        101: ("Brain", 0.92),
        102: ("Brain", 0.85),
        201: ("Brain", 0.55),  # below 0.70 → low-conf
        301: ("Brain", 0.80),
    })
    state2 = svc.apply(cid)
    assert state2.has_current is True
    assert state2.has_previous is False  # first apply
    assert state2.summary.total_sessions == 3
    assert state2.summary.total_stacks == 4
    assert state2.summary.stacks_changed == 4
    assert state2.summary.sessions_changed == 3
    assert state2.summary.needs_check == 1  # the 0.55 stack
    # Token writer was called once with all 3 sessions.
    assert token_calls["n"] == 1
    assert len(token_calls["picks"][-1]) == 3
    # Change matrix records the "spine → Brain" relabel and the
    # "(none) → Brain" addition.
    cm = state2.summary.change_matrix
    assert cm.get("spine", {}).get("Brain") == 1
    assert cm.get("(none)", {}).get("Brain") == 1

    # ── 5. Inspect the changes view ───────────────────────────────────
    page: BodyPartChangesPageDTO = svc.get_changes(cid)
    assert page.total == 4
    # Sorted by ascending confidence; 201 (0.55) first.
    assert page.rows[0].stack_id == 201
    assert page.rows[0].needs_check is True
    # Filter by prior "spine" → 1 row.
    spine_page = svc.get_changes(cid, from_label="spine")
    assert spine_page.total == 1
    assert spine_page.rows[0].stack_id == 301

    # ── 6. Override the low-conf stack to a different category ────────
    # User decides 201 is actually "Spine".
    token_calls["n"] = 0  # reset for this assertion
    sess = svc.override_stack(
        cid, subject_id=2, session_date="2024-01-02",
        stack_id=201, label="Spine",
    )
    assert sess.subject_id == 2
    assert sess.session_date == "2024-01-02"
    overridden = next(s for s in sess.stacks if s.stack_id == 201)
    assert overridden.label == "Spine"
    assert overridden.is_override is True
    # Override clears needs_check (user-confirmed labels are trusted).
    assert overridden.needs_check is False
    # Token writer was called for just the affected session.
    assert token_calls["n"] == 1
    assert len(token_calls["picks"][-1]) == 1

    # ── 7. Reset session 2 — should drop the override and re-infer ────
    stub_embed_worker(monkeypatch)
    stub_infer_worker(monkeypatch, predictions={
        201: ("Brain", 0.95),  # this time confident
    })
    token_calls["n"] = 0
    reset_sess = svc.reset_session(cid, subject_id=2, session_date="2024-01-02")
    re_inferred = next(s for s in reset_sess.stacks if s.stack_id == 201)
    assert re_inferred.label == "Brain"
    assert re_inferred.is_override is False
    assert re_inferred.confidence == pytest.approx(0.95)
    # Token writer was called for the reset session.
    assert token_calls["n"] == 1

    # ── 8. Re-apply (rotates the snapshot) ────────────────────────────
    # Different predictions this time so we can see the rotation work.
    stub_embed_worker(monkeypatch)
    stub_infer_worker(monkeypatch, predictions={
        101: ("Brain", 0.99),
        102: ("Brain", 0.98),
        201: ("Spine", 0.95),  # diverges from the cached current
        301: ("Brain", 0.97),
    })
    state3 = svc.apply(cid)
    assert state3.has_previous is True
    # The new run says 201 is Spine.
    sess2 = next(p for p in state3.picks if p.study_id == 2)
    stk201 = next(s for s in sess2.stacks if s.stack_id == 201)
    assert stk201.label == "Spine"

    # ── 9. Restore-previous — undo the third apply ────────────────────
    state4 = svc.restore_previous(cid)
    assert state4.has_current is True
    assert state4.has_previous is False  # consumed by restore
    sess2_after = next(p for p in state4.picks if p.study_id == 2)
    stk201_after = next(s for s in sess2_after.stacks if s.stack_id == 201)
    # We restored to the post-reset state where 201 was Brain @ 0.95.
    assert stk201_after.label == "Brain"
    assert stk201_after.confidence == pytest.approx(0.95)


def test_full_lifecycle_persists_to_app_db(app_db_env, monkeypatch):
    """Mirror of the smoke test focused on **DB persistence**.

    After running the lifecycle the ``CohortBodyPartQCState`` row must
    contain non-empty ``categories`` / ``training_samples`` /
    ``classifier_path`` / ``current_picks`` so a subsequent process
    re-loading from disk would see the same picture.
    """
    svc = CohortBodyPartQCService()
    cid = app_db_env["cohort_id"]
    scope = app_db_env["scope"]

    monkeypatch.setattr(
        service_mod, "enumerate_candidates",
        lambda name: synthetic_candidates(),
    )
    stub_token_writer(monkeypatch)
    quick_train(svc, cid, monkeypatch, categories=["Brain", "Spine"])

    stub_embed_worker(monkeypatch)
    stub_infer_worker(monkeypatch, predictions={
        101: ("Brain", 0.92),
        102: ("Brain", 0.85),
        201: ("Brain", 0.95),
        301: ("Brain", 0.80),
    })
    svc.apply(cid)

    with scope() as db:
        row = db.get(CohortBodyPartQCState, cid)
        assert row is not None
        assert row.categories == ["Brain", "Spine"]
        assert len(row.training_samples) == 10
        assert row.classifier_path is not None
        assert row.classifier_sha256 is not None
        assert row.classifier_meta is not None
        assert len(row.current_picks) == 3
        # All 4 stacks present across the 3 sessions.
        all_stack_ids = {
            stk["stack_id"]
            for sess in row.current_picks
            for stk in sess["stacks"]
        }
        assert all_stack_ids == {101, 102, 201, 301}


def test_full_lifecycle_state_dto_round_trip(app_db_env, monkeypatch):
    """The :class:`BodyPartStateDTO` returned at the end of the
    lifecycle must round-trip JSON cleanly so the API layer can serve it
    without any custom encoder gymnastics."""
    svc = CohortBodyPartQCService()
    cid = app_db_env["cohort_id"]

    monkeypatch.setattr(
        service_mod, "enumerate_candidates",
        lambda name: synthetic_candidates(),
    )
    stub_token_writer(monkeypatch)
    quick_train(svc, cid, monkeypatch, categories=["Brain", "Spine"])
    stub_embed_worker(monkeypatch)
    stub_infer_worker(monkeypatch, predictions={
        101: ("Brain", 0.92),
        102: ("Brain", 0.85),
        201: ("Brain", 0.55),
        301: ("Brain", 0.80),
    })
    state = svc.apply(cid)

    payload = state.model_dump_json()
    parsed = BodyPartStateDTO.model_validate_json(payload)
    assert parsed.cohort_id == state.cohort_id
    assert parsed.has_current is True
    assert parsed.summary.total_sessions == state.summary.total_sessions
    assert len(parsed.picks) == len(state.picks)


def test_full_lifecycle_reproducibility(app_db_env, monkeypatch):
    """Two ``apply`` calls with identical worker output must produce
    identical summaries (up to ``current_run_at``).

    Catches accidental sources of nondeterminism — sorting on
    ``set()`` iteration order, dict-key shuffles, etc.
    """
    svc = CohortBodyPartQCService()
    cid = app_db_env["cohort_id"]

    monkeypatch.setattr(
        service_mod, "enumerate_candidates",
        lambda name: synthetic_candidates(),
    )
    stub_token_writer(monkeypatch)
    quick_train(svc, cid, monkeypatch, categories=["Brain", "Spine"])

    def _do_apply():
        stub_embed_worker(monkeypatch)
        stub_infer_worker(monkeypatch, predictions={
            101: ("Brain", 0.92),
            102: ("Brain", 0.85),
            201: ("Spine", 0.85),
            301: ("Brain", 0.80),
        })
        return svc.apply(cid)

    a = _do_apply()
    b = _do_apply()
    assert a.summary.total_sessions == b.summary.total_sessions
    assert a.summary.total_stacks == b.summary.total_stacks
    assert a.summary.stacks_changed == b.summary.stacks_changed
    assert a.summary.sessions_changed == b.summary.sessions_changed
    assert a.summary.change_matrix == b.summary.change_matrix
    assert a.summary.by_combo == b.summary.by_combo
    # Picks are equal up to the run timestamp, which we don't assert on.
    a_combos = sorted(p.session_combo_key for p in a.picks)
    b_combos = sorted(p.session_combo_key for p in b.picks)
    assert a_combos == b_combos
