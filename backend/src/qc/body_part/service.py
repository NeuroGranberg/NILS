"""Cohort-wide Body Part QC service (orchestration).

This module is intentionally thin — heavy compute (encoder inference,
training, embedding extraction) lives in the ``body-part-qc-worker``
Docker service and is called over HTTP. The methods below own:

- DB I/O (``cohort_body_part_qc_state``, ``stack_embedding_cache``,
  ``series_classification_cache``)
- DTO construction
- Snapshot rotation (Apply / Restore Previous)
- Pre-QC diff capture (``previous_label`` / ``prior_source``)
- Manual review token bookkeeping
- Worker invocation

Behavior matches :class:`qc.cohort_main_qc_service.CohortMainQCService`
for everything that is not body-part-specific.

The full implementation is delivered across milestones M1–M7 (see the
spec at ``/home/nima/.factory/specs/2026-04-26-body-part-qc-cohort-module-v1.md``).
M1 ships the public surface and a working ``get_state`` so the route
module can be wired up; later milestones fill in the rest.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from sqlalchemy.orm import Session

from cohorts.models import (
    BodyPartCandidateDTO,
    BodyPartChangesPageDTO,
    BodyPartModel,
    BodyPartSampleOp,
    BodyPartSessionPickDTO,
    BodyPartStackPickDTO,
    BodyPartStateDTO,
    BodyPartSummaryDTO,
    BodyPartTrainingSampleDTO,
    CohortBodyPartQCState,
)
from cohorts.repository import get_cohort
from db.session import session_scope
from qc.body_part import tokens as token_writer
from qc.body_part import worker_client
from collections import defaultdict

from qc.body_part.candidates import (
    CandidateStack,
    enumerate_candidates,
    partition_by_category,
)
from qc.body_part.preprocess import central_slice_indices, orientation_slice_indices

logger = logging.getLogger(__name__)


# Default categories that ship with V1. Users can rename / add / remove
# via PUT /api/cohorts/{id}/body-part-qc/categories.
DEFAULT_CATEGORIES: list[str] = ["Brain", "Brain-Neck", "Spine", "Chest"]

# Default thresholds (frontend reads these from the state DTO so ops can
# tune without a deploy). Mirrored in worker thresholds.yaml.
DEFAULT_THRESHOLDS: dict[str, float] = {
    "manual_review_below": 0.70,
    "ambiguous_margin_below": 0.15,
    "apply_warn_below": 0.70,
    # Milestone B: when re-Apply happens, an existing override is preserved
    # unless the new model's top prediction is at or above this confidence
    # AND assigns a different label. In that case the override is *kept*
    # but flagged as a conflict for user review.
    "override_conflict_prob": 0.95,
}

# Default cohort filter applied at Apply time. RawRecon + anat,
# excluding localizer. The user may widen this in the UI.
DEFAULT_FILTERS: dict = {
    "provenance": ["RawRecon"],
    "intent": ["anat"],
    "exclude_localizer": True,
}

# Manual-review token added to ``manual_review_reasons_csv`` when QC
# confidence falls below ``manual_review_below``.
LOW_CONF_REVIEW_TOKEN = "bodypart_qc:low_confidence"
COMMITTED_BODY_PART_LABELS_PROFILE_KEY = "committed_body_part_labels"


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class CohortBodyPartQCService:
    """Orchestration for the cohort-wide Body Part QC module.

    The service is **stateless** — every public method opens its own DB
    session via :func:`db.session.session_scope`. This matches the
    pattern used by :class:`qc.cohort_main_qc_service.CohortMainQCService`.
    """

    # ---- M1: read-only state surface ----

    def get_state(self, cohort_id: int) -> BodyPartStateDTO:
        """Return the current Body Part QC state for a cohort.

        For cohorts with no row in ``cohort_body_part_qc_state``, returns
        a default state populated with :data:`DEFAULT_CATEGORIES` and the
        default profile so the UI can render the "setup" view immediately.
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")

            state = db.get(CohortBodyPartQCState, cohort_id)
            if state is None:
                # Lazy-create on first GET so subsequent edits have a row
                # to update. Match the pattern in cohort_main_qc_service.
                state = CohortBodyPartQCState(
                    cohort_id=cohort_id,
                    categories=list(DEFAULT_CATEGORIES),
                    training_samples=[],
                    classifier_path=None,
                    classifier_sha256=None,
                    classifier_meta=None,
                    current_summary={},
                    current_picks=[],
                    current_profile=self._default_profile(),
                )
                db.add(state)
                db.commit()
                db.refresh(state)

            return self._to_state_dto(state)

    # ---- M2–M7: stubs (NotImplementedError until milestone lands) ----

    def update_categories(
        self, cohort_id: int, categories: list[str]
    ) -> BodyPartStateDTO:
        """Replace the cohort's Body Part QC categories.

        Validates that the new list is non-empty, contains only strings
        with no duplicates after normalisation, and preserves user order.
        Categories that have no remaining training samples after the
        change are dropped from ``training_samples`` automatically.
        """
        clean = self._validate_categories(categories)

        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            valid = set(clean)
            kept_samples = [
                s for s in (state.training_samples or [])
                if s.get("label") in valid
            ]
            state.categories = clean
            state.training_samples = kept_samples
            db.flush()
            db.refresh(state)
            return self._to_state_dto(state)

    def seed(
        self,
        cohort_id: int,
        category: str,
        n_target: int = 100,
        n_keyword_prior: int | None = None,
        n_zero_shot: int | None = None,
    ) -> list[BodyPartCandidateDTO]:
        """Run seeding for one category via the worker.

        Two-stage pipeline:
          1. **Keyword-prior stage**: candidates whose ``body_part``
             matches ``category`` via the keyword detector are selected
             purely by embedding diversity (FPS) — no zero-shot scoring.
          2. **Zero-shot stage**: candidates with ``body_part=None`` are
             subsampled with technique-weighted stratification, then
             scored and selected by the existing zero-shot pipeline.

        The split is controllable via ``n_keyword_prior`` and
        ``n_zero_shot``. Defaults: 50/50 of ``n_target``.
        """
        if n_target <= 0:
            raise ValueError("n_target must be > 0")

        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = self._ensure_state(db, cohort_id)
            if category not in (state.categories or []):
                raise ValueError(
                    f"Category {category!r} is not configured for this cohort."
                )
            category_keyword_map = (
                (state.current_profile or {}).get("category_keyword_map")
            )

        candidates = enumerate_candidates(cohort.name)
        if not candidates:
            return []

        # Partition into keyword-prior pool vs unlabeled pool.
        prior_pool, null_pool = partition_by_category(
            candidates, category, category_keyword_map,
        )

        # Budget split.
        if n_keyword_prior is None:
            n_keyword_prior = min(50, n_target // 2)
        if n_zero_shot is None:
            n_zero_shot = n_target - n_keyword_prior
        # If prior pool is small, roll budget over to zero-shot.
        actual_prior_budget = min(n_keyword_prior, len(prior_pool))
        n_zero_shot += (n_keyword_prior - actual_prior_budget)
        n_keyword_prior = actual_prior_budget

        idx = {(c.stack_id, c.slice_index): c for c in candidates}
        out: list[BodyPartCandidateDTO] = []

        # ── Stage 1: keyword-prior diversity selection ──
        if n_keyword_prior > 0 and prior_pool:
            prior_items = [
                {"stack_id": c.stack_id, "slice_index": c.slice_index}
                for c in prior_pool
            ]
            prior_result = worker_client.seed_prior(
                prior_items, n_target=n_keyword_prior,
            )
            for cand in prior_result.get("candidates", []):
                key = (int(cand["stack_id"]), int(cand["slice_index"]))
                meta = idx.get(key)
                if meta is None:
                    continue
                out.append(
                    BodyPartCandidateDTO(
                        stack_id=meta.stack_id,
                        slice_index=meta.slice_index,
                        orientation=(meta.orientation or "unknown").lower(),
                        zs_prob=1.0,
                        margin=1.0,
                        top2_label=None,
                        series_instance_uid=meta.series_instance_uid,
                        study_id=meta.study_id,
                        subject_code=meta.subject_code,
                        session_date=meta.session_date,
                        thumbnail_url=self._thumbnail_url(
                            meta.stack_id, meta.slice_index,
                        ),
                        source="keyword_prior",
                        keyword_label=meta.body_part,
                    )
                )
            logger.info(
                "seed: keyword-prior stage returned %d/%d "
                "(cohort=%s, category=%s, prior_pool=%d)",
                len([o for o in out if o.source == "keyword_prior"]),
                n_keyword_prior, cohort.name, category, len(prior_pool),
            )

        # ── Stage 2: zero-shot on unlabeled pool ──
        if n_zero_shot > 0 and null_pool:
            scored_pool = self._stratified_subsample(
                null_pool, n_zero_shot, cohort.name, category,
            )

            items = [
                {"stack_id": c.stack_id, "slice_index": c.slice_index}
                for c in scored_pool
            ]
            result = worker_client.seed(
                category=category,
                items=items,
                n_target=n_zero_shot,
            )

            for cand in result.get("candidates", []):
                key = (int(cand["stack_id"]), int(cand["slice_index"]))
                meta = idx.get(key)
                if meta is None:
                    continue
                top2 = None
                zs = float(cand["zs_prob"])
                margin = float(cand["margin"])
                if zs - margin > 0.05:
                    top2 = "(other body part)"
                out.append(
                    BodyPartCandidateDTO(
                        stack_id=meta.stack_id,
                        slice_index=meta.slice_index,
                        orientation=(meta.orientation or "unknown").lower(),
                        zs_prob=zs,
                        margin=margin,
                        top2_label=top2,
                        series_instance_uid=meta.series_instance_uid,
                        study_id=meta.study_id,
                        subject_code=meta.subject_code,
                        session_date=meta.session_date,
                        thumbnail_url=self._thumbnail_url(
                            meta.stack_id, meta.slice_index,
                        ),
                        source="zero_shot",
                    )
                )
            logger.info(
                "seed: zero-shot stage returned %d/%d "
                "(cohort=%s, category=%s, null_pool=%d, scored_pool=%d)",
                len([o for o in out if o.source == "zero_shot"]),
                n_zero_shot, cohort.name, category,
                len(null_pool), len(scored_pool),
            )

        return out

    @staticmethod
    def _stratified_subsample(
        candidates: list[CandidateStack],
        pool_cap: int,
        cohort_name: str,
        category: str,
    ) -> list[CandidateStack]:
        """Technique-weighted stratified subsample of the candidate pool.

        Groups candidates by ``(technique, body_part)`` and allocates
        proportional shares (minimum 1 per cell) up to ``pool_cap``.
        Deterministic per ``(cohort_name, category)``.
        """
        if pool_cap <= 0 or not candidates:
            return []
        if len(candidates) <= pool_cap:
            return candidates

        env_cap = os.environ.get("BODY_PART_SEED_POOL_CAP")
        if env_cap is not None:
            pool_cap = int(env_cap) or pool_cap

        rng = random.Random(f"{cohort_name}:{category}")
        cells: dict[tuple[str, str], list[CandidateStack]] = defaultdict(list)
        for c in candidates:
            key = (c.technique or "Unknown", c.body_part or "unlabeled")
            cells[key].append(c)

        total = len(candidates)
        out: list[CandidateStack] = []
        for key, items in cells.items():
            share = max(1, round(pool_cap * len(items) / total))
            rng.shuffle(items)
            out.extend(items[:share])

        rng.shuffle(out)
        return out[:pool_cap]

    def update_samples(
        self, cohort_id: int, ops: list[BodyPartSampleOp],
    ) -> BodyPartStateDTO:
        """Apply approve / remove / replace / move ops to the labeled set.

        Operations:
            - ``approve``: add (stack_id, slice_index, label) if absent.
            - ``remove``:  drop the entry for (stack_id, slice_index).
            - ``replace``: alias for approve when overwriting label is
              fine. Same identity key, may rewrite ``label``.
            - ``move``:    change ``label`` to ``new_label`` for
              (stack_id, slice_index).

        All ops are applied transactionally; an invalid op aborts the
        whole batch.
        """
        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            valid_labels = set(state.categories or [])
            # Deep copy so mutations don't accidentally alias the JSON
            # column's stored value (SQLAlchemy doesn't auto-detect
            # nested mutations on JSON columns).
            samples = [dict(s) for s in (state.training_samples or [])]
            by_key: dict[tuple[int, int], int] = {
                (s["stack_id"], s["slice_index"]): i
                for i, s in enumerate(samples)
            }

            for op in ops:
                key = (op.stack_id, op.slice_index)
                if op.op == "approve" or op.op == "replace":
                    if op.label is None:
                        raise ValueError("approve/replace requires `label`.")
                    if op.label not in valid_labels:
                        raise ValueError(
                            f"label {op.label!r} is not a configured category."
                        )
                    entry = {
                        "stack_id": op.stack_id,
                        "slice_index": op.slice_index,
                        "label": op.label,
                        "orientation": "unknown",  # hydrated at train time
                        "approved_at": datetime.now(timezone.utc).isoformat(),
                    }
                    if key in by_key:
                        samples[by_key[key]] = entry
                    else:
                        by_key[key] = len(samples)
                        samples.append(entry)
                elif op.op == "remove":
                    if key in by_key:
                        del samples[by_key[key]]
                        # Rebuild index after a deletion to keep positions sane.
                        by_key = {
                            (s["stack_id"], s["slice_index"]): i
                            for i, s in enumerate(samples)
                        }
                elif op.op == "move":
                    if op.new_label is None:
                        raise ValueError("move requires `new_label`.")
                    if op.new_label not in valid_labels:
                        raise ValueError(
                            f"new_label {op.new_label!r} is not a configured category."
                        )
                    if key not in by_key:
                        raise ValueError(
                            f"Cannot move (stack {op.stack_id}, "
                            f"slice {op.slice_index}): not in training set."
                        )
                    samples[by_key[key]]["label"] = op.new_label
                else:
                    raise ValueError(f"Unknown op {op.op!r}")

            state.training_samples = samples
            db.flush()
            db.refresh(state)
            return self._to_state_dto(state)

    def list_samples(
        self, cohort_id: int, *, label: Optional[str] = None,
    ) -> list[BodyPartTrainingSampleDTO]:
        """Return the approved-samples list for the cohort.

        Hydrates each entry with a thumbnail URL so the frontend can
        render a grid without an extra round-trip. Optional ``label``
        filters to a single category.

        Sort order: by approved_at descending (newest first), with
        ties broken by ``(stack_id, slice_index)`` for determinism.
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = db.get(CohortBodyPartQCState, cohort_id)
            entries = list((state.training_samples or [])) if state else []

        if label is not None:
            entries = [e for e in entries if e.get("label") == label]

        def _sort_key(s: dict) -> tuple:
            return (
                str(s.get("approved_at") or ""),
                int(s.get("stack_id") or 0),
                int(s.get("slice_index") or 0),
            )

        entries.sort(key=_sort_key, reverse=True)

        out: list[BodyPartTrainingSampleDTO] = []
        for s in entries:
            try:
                out.append(BodyPartTrainingSampleDTO(
                    stack_id=int(s["stack_id"]),
                    slice_index=int(s["slice_index"]),
                    label=str(s["label"]),
                    orientation=str(s.get("orientation") or "unknown"),
                    approved_at=s.get("approved_at") or datetime.now(timezone.utc),
                    thumbnail_url=self._thumbnail_url(
                        int(s["stack_id"]), int(s["slice_index"]),
                    ),
                    subject_code=None,
                ))
            except (KeyError, ValueError, TypeError):
                logger.warning("Skipping malformed sample %r", s)
        return out

    def apply(self, cohort_id: int) -> BodyPartStateDTO:
        """Run cohort inference, capture diff vs prior body_part, and
        rotate the snapshot.

        Steps:
            1. Load classifier metadata; require a trained classifier.
            2. Enumerate eligible cohort stacks (anat / non-localizer /
               RawRecon-class) with their **prior body_part** read from
               ``series_classification_cache``.
            3. Embed the 3 central slices of every stack via ``/embed``.
            4. Run ``/infer``.
            5. Build session-grouped picks with diff fields:
               - ``previous_label`` = prior body_part (may be None)
               - ``prior_source`` = "text_keyword" if previous_label
                 came from the existing keyword-based detector,
                 otherwise None.
               - ``changed`` = predicted_label != previous_label.
            6. Build cohort-level summary including ``change_matrix``.
            7. Rotate snapshot (current → previous; new → current).
            8. Return updated state DTO.

        Token writes to the metadata DB (``manual_review_reasons_csv``
        / ``body_part`` column) land in M7.
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = self._ensure_state(db, cohort_id)

            # Resolve the classifier from the model registry.
            model = self._resolve_model(db, state)
            if model is None:
                raise ValueError(
                    "No model selected — train and select a model in "
                    "the Models tab before applying."
                )
            classifier_meta = model.meta or {}
            classifier_path = model.artifact_path
            classifier_sha256 = model.artifact_sha256
            categories = list(model.classes or [])

            mode = (state.current_profile or {}).get("encoder_mode") or "concat"
            thresholds = (state.current_profile or {}).get(
                "thresholds", DEFAULT_THRESHOLDS,
            )
            cohort_name = cohort.name

        candidates = enumerate_candidates(cohort_name)
        if not candidates:
            raise ValueError(
                "No eligible stacks found for this cohort. "
                "Body Part QC requires anat / non-localizer / RawRecon stacks."
            )

        # Build the slice list: orientation-aware slice selection.
        embed_items: list[dict[str, int]] = []
        infer_items: list[dict[str, Any]] = []
        for c in candidates:
            slice_idxs = orientation_slice_indices(c.num_slices, c.orientation)
            for idx in slice_idxs:
                embed_items.append({"stack_id": c.stack_id, "slice_index": idx})
            infer_items.append({
                "stack_id": c.stack_id,
                "num_slices": c.num_slices,
                "orientation": c.orientation,
            })

        # 3) Embed (idempotent — cache hits skip recomputation).
        # Chunk to avoid HTTP timeout on large cohorts (87K+ items).
        _EMBED_CHUNK = 5000
        for i in range(0, len(embed_items), _EMBED_CHUNK):
            chunk = embed_items[i : i + _EMBED_CHUNK]
            logger.info(
                "apply: embedding chunk %d/%d (%d items)",
                i // _EMBED_CHUNK + 1,
                (len(embed_items) + _EMBED_CHUNK - 1) // _EMBED_CHUNK,
                len(chunk),
            )
            worker_client.embed(chunk, mode=mode)

        # 4) Infer.
        encoder_chain = classifier_meta.get("encoder_chain") or []
        if not encoder_chain:
            raise ValueError(
                "classifier_meta.encoder_chain is missing — re-train."
            )
        infer_resp = worker_client.infer(
            classifier_path=classifier_path,
            classifier_sha256=classifier_sha256,
            classes=classifier_meta.get("classes", categories),
            encoder_chain=encoder_chain,
            items=infer_items,
            manual_review_below=float(thresholds.get(
                "manual_review_below",
                DEFAULT_THRESHOLDS["manual_review_below"],
            )),
        )

        # 5) Build per-stack diff + session picks.
        cand_by_id = {c.stack_id: c for c in candidates}
        results = infer_resp.get("results", [])
        stack_diffs = self._build_stack_diffs(results, cand_by_id)

        # 5b) Milestone B: carry forward existing overrides so a re-Apply
        # doesn't blow away user corrections. The new model's prediction
        # is kept as-is unless it strongly disagrees, in which case we
        # flag the row as an override-conflict for review.
        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            prior_picks = list(state.current_picks or [])
        override_conflict_prob = float(thresholds.get(
            "override_conflict_prob",
            DEFAULT_THRESHOLDS["override_conflict_prob"],
        ))
        stack_diffs = self._merge_overrides(
            stack_diffs,
            prior_picks=prior_picks,
            override_conflict_prob=override_conflict_prob,
        )

        # 6) Group by session and compute session-level diff aggregates.
        picks, summary = self._build_session_picks_and_summary(
            stack_diffs, cand_by_id,
            categories=categories,
        )

        # 7) Rotate snapshot. Apply NO LONGER writes to the metadata DB
        # (Milestone C). The user must explicitly Commit. We move
        # current → previous so single-level undo still works.
        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            now = datetime.now(timezone.utc)
            if state.current_picks:
                state.previous_run_at = state.current_run_at
                state.previous_summary = state.current_summary
                state.previous_picks = state.current_picks
                state.previous_profile = state.current_profile
            state.current_run_at = now
            state.current_summary = summary
            state.current_picks = picks
            profile = dict(state.current_profile or self._default_profile())
            profile["encoder_mode"] = mode
            profile["thresholds"] = dict(thresholds)
            state.current_profile = profile
            # Newly applied picks are staged until the user commits.
            state.stage_status = "staged"
            db.flush()
            db.refresh(state)

        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            return self._to_state_dto(state)

    def commit(
        self,
        cohort_id: int,
        *,
        stack_ids: list[int] | None = None,
        min_confidence: float | None = None,
        from_label: str | None = None,
        to_label: str | None = None,
    ) -> BodyPartStateDTO:
        """Write staged ``current_picks`` to the metadata DB.

        Supports multiple filter modes (combined with AND):
        - **Full commit** (no args): writes all picks. Back-compat.
        - **By stack_ids**: writes only the specified stacks.
        - **By min_confidence**: writes all picks with confidence ≥
          threshold.
        - **By from_label**: only stacks whose ``previous_label``
          matches (use ``(none)`` for null).
        - **By to_label**: only stacks whose ``label`` matches.

        When ``stack_ids`` is provided it takes precedence over all
        other filters.

        After a partial commit the remaining picks stay in
        ``current_picks`` with ``stage_status='dirty'``.

        Raises ``LookupError`` if there are no picks to commit.
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = self._ensure_state(db, cohort_id)
            picks = list(state.current_picks or [])
            categories = list(state.categories or [])
            if not picks:
                raise LookupError(
                    "Nothing to commit — run Apply first."
                )

        # Determine which stacks to commit.
        has_filter = (
            stack_ids is not None
            or min_confidence is not None
            or from_label is not None
            or to_label is not None
        )
        target_ids: set[int] | None = None
        if stack_ids is not None:
            target_ids = set(stack_ids)
        elif has_filter:
            target_ids = set()
            for sess in picks:
                for stk in sess.get("stacks", []):
                    if min_confidence is not None:
                        if float(stk.get("confidence") or 0) < min_confidence:
                            continue
                    if from_label is not None:
                        prev = stk.get("previous_label") or "(none)"
                        if prev != from_label:
                            continue
                    if to_label is not None:
                        if stk.get("label") != to_label:
                            continue
                    target_ids.add(int(stk["stack_id"]))

        # Split picks into committed vs remaining.
        if target_ids is not None:
            committed_picks, remaining_picks = self._split_picks(
                picks, target_ids,
            )
        else:
            committed_picks = picks
            remaining_picks = []

        if not committed_picks:
            raise LookupError(
                "No stacks match the commit criteria."
            )

        # Write through.
        token_writer.apply_picks_to_metadata(committed_picks)

        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            now = datetime.now(timezone.utc)
            state.last_committed_at = now
            profile = dict(state.current_profile or self._default_profile())
            committed_labels = self._merge_committed_body_part_labels(
                profile.get(COMMITTED_BODY_PART_LABELS_PROFILE_KEY),
                committed_picks,
            )
            profile[COMMITTED_BODY_PART_LABELS_PROFILE_KEY] = committed_labels
            state.current_profile = profile

            if remaining_picks:
                state.current_picks = remaining_picks
                state.current_summary = self._recompute_summary(
                    remaining_picks, categories=categories,
                )
                state.stage_status = "dirty"
                state.last_commit_signature = (
                    self._compute_commit_signature(remaining_picks)
                )
            else:
                state.stage_status = "committed"
                state.last_commit_signature = (
                    self._compute_commit_signature(committed_picks)
                )
            db.flush()
            db.refresh(state)
            return self._to_state_dto(state)

    def destage(
        self,
        cohort_id: int,
        stack_ids: list[int],
    ) -> BodyPartStateDTO:
        """Remove stacks from ``current_picks`` without writing to the
        metadata DB. The metadata ``body_part`` column retains whatever
        the keyword detector (or a previous commit) wrote.
        """
        if not stack_ids:
            raise ValueError("stack_ids must not be empty")

        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = self._ensure_state(db, cohort_id)
            picks = list(state.current_picks or [])
            categories = list(state.categories or [])
            if not picks:
                raise LookupError("Nothing staged to destage.")

            remove_set = set(stack_ids)
            _, remaining = self._split_picks(picks, remove_set)

            if remaining:
                state.current_picks = remaining
                state.current_summary = self._recompute_summary(
                    remaining, categories=categories,
                )
                state.stage_status = "dirty"
                state.last_commit_signature = (
                    self._compute_commit_signature(remaining)
                )
            else:
                state.current_picks = []
                state.current_summary = {}
                state.stage_status = "none"
                state.last_commit_signature = None
            db.flush()
            db.refresh(state)
            return self._to_state_dto(state)

    @staticmethod
    def _split_picks(
        picks: list[dict], target_ids: set[int],
    ) -> tuple[list[dict], list[dict]]:
        """Split session-grouped picks into two lists: matching target_ids
        and the remainder. Empty sessions are dropped."""
        matched: list[dict] = []
        remaining: list[dict] = []
        for sess in picks:
            m_stacks = []
            r_stacks = []
            for stk in sess.get("stacks", []):
                if int(stk["stack_id"]) in target_ids:
                    m_stacks.append(stk)
                else:
                    r_stacks.append(stk)
            if m_stacks:
                m_sess = {**sess, "stacks": m_stacks}
                matched.append(m_sess)
            if r_stacks:
                r_sess = {**sess, "stacks": r_stacks}
                remaining.append(r_sess)
        return matched, remaining

    def restore_previous(self, cohort_id: int) -> BodyPartStateDTO:
        """Single-level undo: swap ``current_*`` ↔ ``previous_*``.

        After restore, ``previous_*`` is cleared (a 2nd Restore is a
        no-op until the next Apply). The metadata DB is **not**
        written automatically — the restored snapshot lands in the
        ``staged`` state and requires an explicit Commit.
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = db.get(CohortBodyPartQCState, cohort_id)
            if state is None or state.previous_picks is None:
                raise LookupError("No previous snapshot to restore")
            state.current_run_at = state.previous_run_at or state.current_run_at
            state.current_summary = state.previous_summary or {}
            state.current_picks = state.previous_picks or []
            state.current_profile = state.previous_profile or self._default_profile()
            state.previous_run_at = None
            state.previous_summary = None
            state.previous_picks = None
            state.previous_profile = None
            # Restored snapshot is staged; user must Commit to write it
            # back to the metadata DB.
            state.stage_status = "staged"
            db.flush()
            db.refresh(state)

        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            return self._to_state_dto(state)

    def reset(self, cohort_id: int) -> dict:
        """Wipe all QC state for the cohort.

        Removes the ``CohortBodyPartQCState`` row and clears the
        ``bodypart_qc:low_confidence`` review token from every stack in
        the cohort. Leaves embedding cache, classifier artefact on disk,
        and the ``body_part`` column untouched — the next resort will
        overwrite ``body_part`` with keyword-detector values (since QC
        protection will no longer exist).

        Returns:
            {"tokens_removed": int, "had_state": bool}
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            cohort_name = cohort.name
            state = db.get(CohortBodyPartQCState, cohort_id)
            had_state = state is not None
            if had_state:
                db.delete(state)
                db.flush()

        tokens_removed = token_writer.wipe_low_conf_tokens_for_cohort(cohort_name)
        return {"tokens_removed": tokens_removed, "had_state": had_state}

    def select_model(
        self, cohort_id: int, model_id: Optional[int],
    ) -> BodyPartStateDTO:
        """Set which global model this cohort uses for Apply.

        Pass ``model_id=None`` to clear the selection (falls back to
        the per-cohort legacy classifier or the default model).
        """
        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            if model_id is not None:
                model = db.get(BodyPartModel, model_id)
                if model is None:
                    raise LookupError(f"Model {model_id} not found")
            state.selected_model_id = model_id
            db.flush()
            db.refresh(state)
            return self._to_state_dto(state)

    def _resolve_model(
        self, db, state: CohortBodyPartQCState,
    ) -> Optional[BodyPartModel]:
        """Resolve the effective model for a cohort.

        Priority: selected_model_id → is_default model → None (use legacy).
        """
        if state.selected_model_id:
            model = db.get(BodyPartModel, state.selected_model_id)
            if model is not None:
                return model
        # Fall back to default model
        from sqlalchemy import select
        default = db.scalar(
            select(BodyPartModel).where(BodyPartModel.is_default == True)
        )
        return default

    def override_stack(
        self,
        cohort_id: int,
        subject_id: int,
        session_date: str,
        stack_id: int,
        label: str,
        note: Optional[str] = None,
    ) -> BodyPartSessionPickDTO:
        """Manual override for one stack within a session.

        Sessions are identified by ``(subject_id, session_date)``. A
        session can span multiple StudyInstanceUIDs (e.g. brain+spine);
        this method overrides the specific ``stack_id`` regardless of
        which underlying study owns it.
        """
        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            if label not in (state.categories or []):
                raise ValueError(
                    f"Label {label!r} is not a configured category."
                )
            picks = [dict(p) for p in (state.current_picks or [])]
            target_idx = None
            stack_idx = None
            for i, p in enumerate(picks):
                if (
                    p.get("subject_id") == subject_id
                    and p.get("session_date") == session_date
                ):
                    for j, st in enumerate(p["stacks"]):
                        if st["stack_id"] == stack_id:
                            target_idx = i
                            stack_idx = j
                            break
                if target_idx is not None:
                    break
            if target_idx is None or stack_idx is None:
                raise LookupError(
                    f"stack {stack_id} not found in session "
                    f"(subject_id={subject_id}, date={session_date})"
                )

            # Apply override.
            sess = picks[target_idx]
            stacks = [dict(s) for s in sess["stacks"]]
            stk = stacks[stack_idx]
            stk["label"] = label
            stk["is_override"] = True
            stk["needs_check"] = False  # user has reviewed it
            stk["confidence"] = 1.0
            # Probs collapse to a one-hot for consistency.
            stk["probs"] = {c: (1.0 if c == label else 0.0)
                            for c in (state.categories or [])}
            stk["changed"] = (stk.get("previous_label") != label)
            # Resolving an override-conflict by setting the label clears
            # the conflict marker.
            stk["override_conflict"] = None
            stacks[stack_idx] = stk
            sess["stacks"] = stacks

            # Recompute session-level aggregates.
            self._recompute_session_aggregates(sess)
            picks[target_idx] = sess

            # Recompute cohort-level summary.
            categories = list(state.categories or [])
            summary = self._recompute_summary(picks, categories=categories)

            state.current_picks = picks
            state.current_summary = summary
            # Milestone C: if the cohort was already committed, this
            # override creates drift between current_picks and the
            # metadata DB — flip to ``dirty`` until the user re-commits.
            # If we're already staged, stay staged.
            if state.stage_status == "committed":
                state.stage_status = "dirty"
            db.flush()
            db.refresh(state)
            session_for_tokens = picks[target_idx]
            stage_status = state.stage_status

        # Persist the override on the metadata DB only if the cohort is
        # in ``dirty`` state — i.e. the prior committed snapshot is
        # already on disk and the user is editing on top of it. In
        # ``staged`` state nothing has been committed yet, so we hold
        # the override in current_picks until the user commits.
        if stage_status == "dirty":
            try:
                token_writer.apply_picks_to_metadata([session_for_tokens])
            except Exception:
                logger.exception("override_stack: token write failed")

        return self._session_pick_dto(session_for_tokens)

    def reset_session(
        self, cohort_id: int, subject_id: int, session_date: str,
    ) -> BodyPartSessionPickDTO:
        """Re-run inference for one session, clearing overrides.

        Sessions identified by ``(subject_id, session_date)``. The session
        may span multiple StudyInstanceUIDs; we re-infer over every
        eligible stack across all of them.
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = self._ensure_state(db, cohort_id)
            model = self._resolve_model(db, state)
            if model is None:
                raise ValueError("No model selected — select a model in the Models tab.")
            classifier_path = model.artifact_path
            classifier_sha256 = model.artifact_sha256
            classifier_meta = model.meta or {}
            categories = list(model.classes or [])
            mode = (state.current_profile or {}).get("encoder_mode") or "concat"
            thresholds = (state.current_profile or {}).get(
                "thresholds", DEFAULT_THRESHOLDS,
            )
            picks = [dict(p) for p in (state.current_picks or [])]
            target_idx = next(
                (
                    i for i, p in enumerate(picks)
                    if p.get("subject_id") == subject_id
                    and p.get("session_date") == session_date
                ),
                None,
            )
            if target_idx is None:
                raise LookupError(
                    f"session (subject_id={subject_id}, date={session_date}) "
                    "not found in current picks"
                )
            cohort_name = cohort.name

        all_candidates = enumerate_candidates(cohort_name)
        sess_candidates = [
            c for c in all_candidates
            if c.subject_id == subject_id and c.session_date == session_date
        ]
        if not sess_candidates:
            raise LookupError(
                f"No eligible stacks for session "
                f"(subject_id={subject_id}, date={session_date})"
            )

        # Embed + infer just this session.
        embed_items: list[dict[str, int]] = []
        infer_items: list[dict[str, Any]] = []
        for c in sess_candidates:
            for idx in orientation_slice_indices(c.num_slices, c.orientation):
                embed_items.append({"stack_id": c.stack_id, "slice_index": idx})
            infer_items.append({
                "stack_id": c.stack_id,
                "num_slices": c.num_slices,
                "orientation": c.orientation,
            })
        worker_client.embed(embed_items, mode=mode)
        infer_resp = worker_client.infer(
            classifier_path=classifier_path,
            classifier_sha256=classifier_sha256,
            classes=classifier_meta.get("classes", categories),
            encoder_chain=classifier_meta.get("encoder_chain", []),
            items=infer_items,
            manual_review_below=float(thresholds.get(
                "manual_review_below",
                DEFAULT_THRESHOLDS["manual_review_below"],
            )),
        )

        cand_by_id = {c.stack_id: c for c in sess_candidates}
        diffs = self._build_stack_diffs(
            infer_resp.get("results", []), cand_by_id,
        )
        new_picks, _ = self._build_session_picks_and_summary(
            diffs, cand_by_id, categories=categories,
        )
        if not new_picks:
            raise LookupError(
                f"Inference returned no picks for session "
                f"(subject_id={subject_id}, date={session_date})"
            )

        # Persist.
        with session_scope() as db:
            state = self._ensure_state(db, cohort_id)
            picks_list = [dict(p) for p in (state.current_picks or [])]
            target_idx = next(
                (
                    i for i, p in enumerate(picks_list)
                    if p.get("subject_id") == subject_id
                    and p.get("session_date") == session_date
                ),
                None,
            )
            if target_idx is None:
                picks_list.append(new_picks[0])
            else:
                picks_list[target_idx] = new_picks[0]
            summary = self._recompute_summary(picks_list, categories=categories)
            state.current_picks = picks_list
            state.current_summary = summary
            # Reset clears any override on this session. If the cohort
            # was already committed, this creates drift → dirty.
            if state.stage_status == "committed":
                state.stage_status = "dirty"
            db.flush()
            db.refresh(state)
            session_for_tokens = new_picks[0]
            stage_status = state.stage_status

        # Same staged/dirty logic as override_stack.
        if stage_status == "dirty":
            try:
                token_writer.apply_picks_to_metadata([session_for_tokens])
            except Exception:
                logger.exception("reset_session: token write failed")

        return self._session_pick_dto(session_for_tokens)

    def get_changes(
        self,
        cohort_id: int,
        *,
        from_label: Optional[str] = None,
        to_label: Optional[str] = None,
        prior_source: Optional[str] = None,
        min_confidence: Optional[float] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> BodyPartChangesPageDTO:
        """Paginated change list (only stacks where ``changed = true``).

        The data source is the in-memory ``current_picks`` JSON snapshot
        (no metadata-DB join needed). Filtering happens in Python; for
        cohorts with > ~50k stacks consider materializing this view if
        it becomes a hot path.
        """
        from cohorts.models import BodyPartChangeRowDTO

        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            state = db.get(CohortBodyPartQCState, cohort_id)
            if state is None or not state.current_picks:
                return BodyPartChangesPageDTO(
                    total=0, offset=offset, limit=limit, rows=[],
                )
            picks = list(state.current_picks)

        # Collect all stack_ids first to batch-lookup slices_count.
        all_stack_ids: list[int] = []
        for sess in picks:
            for stk in sess.get("stacks", []):
                all_stack_ids.append(int(stk["stack_id"]))
        slices_map = self._fetch_slices_count(all_stack_ids)

        rows: list[BodyPartChangeRowDTO] = []
        for sess in picks:
            for stk in sess.get("stacks", []):
                # Show stacks that changed vs. their prior label, plus
                # any stack flagged as an override-conflict (Milestone B)
                # so the user can review model disagreements even when
                # the kept label matches the prior body_part.
                if not stk.get("changed") and not stk.get("override_conflict"):
                    continue
                if from_label is not None and (
                    (stk.get("previous_label") or "(none)") != from_label
                ):
                    continue
                if to_label is not None and (
                    (stk.get("label") or "(none)") != to_label
                ):
                    continue
                if prior_source is not None and (
                    stk.get("prior_source") != prior_source
                ):
                    continue
                conf = float(stk.get("confidence") or 0.0)
                if min_confidence is not None and conf < min_confidence:
                    continue

                stack_id = int(stk["stack_id"])
                n_slices = slices_map.get(stack_id, 0)
                middle_idx = n_slices // 2 if n_slices > 0 else 0

                rows.append(BodyPartChangeRowDTO(
                    study_id=int(stk.get("study_id") or sess.get("primary_study_id") or 0),
                    subject_id=int(sess.get("subject_id") or 0),
                    stack_id=stack_id,
                    subject_code=sess.get("subject_code", ""),
                    session_date=sess.get("session_date"),
                    series_description=None,  # populated lazily by FE
                    technique=stk.get("technique"),
                    orientation=stk.get("orientation"),
                    previous_label=stk.get("previous_label"),
                    new_label=stk.get("label") or "(none)",
                    prior_source=stk.get("prior_source"),
                    confidence=conf,
                    needs_check=bool(stk.get("needs_check", False)),
                    is_override=bool(stk.get("is_override", False)),
                    thumbnail_url=self._thumbnail_url(stack_id, 0),
                    middle_slice_url=self._thumbnail_url(
                        stack_id, middle_idx,
                    ),
                    override_conflict=stk.get("override_conflict"),
                ))

        # Stable sort: lowest confidence first (most likely to need
        # review), then by stack_id for determinism.
        rows.sort(key=lambda r: (r.confidence, r.stack_id))

        total = len(rows)
        page = rows[offset:offset + limit]
        return BodyPartChangesPageDTO(
            total=total, offset=offset, limit=limit, rows=page,
        )

    @staticmethod
    def _fetch_slices_count(stack_ids: list[int]) -> dict[int, int]:
        """Batch-fetch ``slices_count`` from metadata DB for middle-slice URLs."""
        if not stack_ids:
            return {}
        from sqlalchemy import text
        from metadata_db.session import SessionLocal as MetadataSessionLocal
        sql = text(
            "SELECT series_stack_id, slices_count "
            "FROM series_classification_cache "
            "WHERE series_stack_id = ANY(:ids)"
        )
        with MetadataSessionLocal() as db:
            rows = db.execute(sql, {"ids": stack_ids}).fetchall()
        return {int(r.series_stack_id): int(r.slices_count or 0) for r in rows}

    # ---- Helpers ----

    @staticmethod
    def _validate_categories(categories: list[str]) -> list[str]:
        """Strip + dedupe (case-insensitive) while preserving order."""
        if not isinstance(categories, list):
            raise ValueError("categories must be a list of strings")
        if not categories:
            raise ValueError("at least one category is required")
        seen: set[str] = set()
        out: list[str] = []
        for raw in categories:
            if not isinstance(raw, str):
                raise ValueError(f"category {raw!r} is not a string")
            cat = raw.strip()
            if not cat:
                raise ValueError("category cannot be empty or whitespace-only")
            key = cat.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(cat)
        return out

    def _ensure_state(
        self, db: Session, cohort_id: int,
    ) -> CohortBodyPartQCState:
        """Return the state row, creating a defaults row if absent.

        Mirrors :meth:`get_state` but keeps the row attached to the
        caller's session so updates within the same transaction are
        atomic.
        """
        cohort = get_cohort(db, cohort_id)
        if cohort is None:
            raise LookupError(f"Cohort {cohort_id} not found")
        state = db.get(CohortBodyPartQCState, cohort_id)
        if state is None:
            state = CohortBodyPartQCState(
                cohort_id=cohort_id,
                categories=list(DEFAULT_CATEGORIES),
                training_samples=[],
                classifier_path=None,
                classifier_sha256=None,
                classifier_meta=None,
                current_summary={},
                current_picks=[],
                current_profile=self._default_profile(),
            )
            db.add(state)
            db.flush()
            db.refresh(state)
        return state

    # ---- Apply / picks construction ----

    # ---- Stage / commit helpers (Milestone C) ----

    @staticmethod
    def _compute_commit_signature(picks: list[dict]) -> str:
        """SHA-256 of the (stack_id, label, is_override) tuples that
        define what gets committed to the metadata DB.

        Used to detect drift between ``current_picks`` and the most
        recent commit so the UI can show an accurate ``stage_status``
        without scanning the metadata DB.
        """
        triples: list[tuple[int, Optional[str], bool]] = []
        for sess in picks or []:
            for stk in sess.get("stacks", []):
                triples.append((
                    int(stk.get("stack_id", 0)),
                    stk.get("label"),
                    bool(stk.get("is_override", False)),
                ))
        triples.sort()
        h = hashlib.sha256()
        h.update(json.dumps(triples).encode("utf-8"))
        return h.hexdigest()

    @staticmethod
    def _normalise_body_part_label(label: object) -> str | None:
        if not isinstance(label, str):
            return None
        out = label.strip().lower()
        return out or None

    def _merge_committed_body_part_labels(
        self,
        existing: object,
        committed_picks: list[dict],
    ) -> dict[str, str]:
        """Merge newly committed picks into the durable protection map.

        ``current_picks`` is a staging surface and partial commits remove
        committed rows from it. This map is the durable record used by resort
        protection to keep QC-written body_part labels across step3 reruns.
        """
        out: dict[str, str] = {}
        if isinstance(existing, dict):
            for raw_sid, raw_label in existing.items():
                label = self._normalise_body_part_label(raw_label)
                if label is None:
                    continue
                try:
                    out[str(int(raw_sid))] = label
                except (TypeError, ValueError):
                    continue

        for sess in committed_picks or []:
            for stk in sess.get("stacks", []):
                sid = stk.get("stack_id")
                label = self._normalise_body_part_label(stk.get("label"))
                if sid is None or label is None:
                    continue
                out[str(int(sid))] = label
        return out

    @staticmethod
    def _count_pending_changes(picks: list[dict]) -> int:
        """Number of stacks where the model/UI label disagrees with the
        prior label (or where a needs_check / override flag has changed)
        relative to the on-disk metadata. The cheap proxy: every stack
        with a label flagged ``changed`` plus every override.
        """
        n = 0
        for sess in picks or []:
            for stk in sess.get("stacks", []):
                if stk.get("changed") or stk.get("is_override"):
                    n += 1
        return n

    # ---- Override merge (Milestone B) ----

    @staticmethod
    def _index_overrides(picks: list[dict]) -> dict[int, dict]:
        """Flatten prior session picks into ``{stack_id: stack_dict}``
        for stacks that carry ``is_override=True``."""
        out: dict[int, dict] = {}
        for sess in picks or []:
            for stk in sess.get("stacks", []):
                if stk.get("is_override"):
                    out[int(stk["stack_id"])] = stk
        return out

    def _merge_overrides(
        self,
        stack_diffs: list[dict],
        *,
        prior_picks: list[dict],
        override_conflict_prob: float,
    ) -> list[dict]:
        """Carry forward prior overrides into a fresh inference result.

        - If the stack had ``is_override=True`` previously and the new
          model's top probability is < ``override_conflict_prob`` (or
          agrees with the override), the override is preserved as-is.
        - If the new model strongly disagrees, the override is still
          preserved (label/probs/is_override unchanged) but the stack
          is annotated with an ``override_conflict`` payload so the
          UI can surface a "model thinks differently" badge.
        """
        if not prior_picks:
            return stack_diffs
        prior_overrides = self._index_overrides(prior_picks)
        if not prior_overrides:
            return stack_diffs

        merged: list[dict] = []
        for d in stack_diffs:
            sid = int(d["stack_id"])
            prev = prior_overrides.get(sid)
            if prev is None:
                # No prior override — use the new prediction as-is.
                d["override_conflict"] = None
                merged.append(d)
                continue

            # Carry the override forward. Keep the prior label / probs
            # / is_override / confidence intact; refresh diff fields
            # against the latest ``previous_label`` from this run.
            new_label = d.get("label")
            new_top = float(max(d.get("probs", {}).values(), default=0.0) or 0.0)
            carried = {
                **d,
                "label": prev.get("label"),
                "probs": dict(prev.get("probs") or {}),
                "confidence": float(prev.get("confidence") or 1.0),
                "is_override": True,
                "needs_check": False,
                "changed": (
                    self._normalize_prior(prev.get("label"))
                    != d.get("previous_label")
                ),
            }
            if (
                new_label is not None
                and new_label != prev.get("label")
                and new_top >= override_conflict_prob
            ):
                carried["override_conflict"] = {
                    "label": new_label,
                    "prob": new_top,
                }
            else:
                carried["override_conflict"] = None
            merged.append(carried)
        return merged

    @staticmethod
    def _normalize_prior(label: Optional[str]) -> Optional[str]:
        """Lowercase + strip the prior body_part for diff comparison.

        The keyword detector emits values like 'brain', 'spine',
        'brain-neck' (always lowercase). The QC categories are
        title-case ('Brain', 'Spine', ...). We compare on the
        case-insensitive form so the diff is meaningful.
        """
        if label is None:
            return None
        s = str(label).strip().lower()
        return s if s else None

    @staticmethod
    def _classify_prior_source(prior: Optional[str]) -> Optional[str]:
        """Return ``'text_keyword'`` if a prior label was set by the
        keyword-based detector, else ``None``."""
        return "text_keyword" if prior else None

    def _build_stack_diffs(
        self,
        infer_results: list[dict],
        cand_by_id: dict[int, "CandidateStack"],
    ) -> list[dict]:
        """One row per stack with QC prediction + prior body_part diff."""
        out: list[dict] = []
        for r in infer_results:
            stack_id = int(r["stack_id"])
            cand = cand_by_id.get(stack_id)
            if cand is None:
                continue
            prior = self._normalize_prior(cand.body_part)
            new_label = r.get("label")
            changed = bool(new_label) and (
                self._normalize_prior(new_label) != prior
            )
            out.append({
                "stack_id": stack_id,
                "study_id": cand.study_id,
                "subject_id": cand.subject_id,
                "subject_code": cand.subject_code,
                "session_date": cand.session_date,
                "series_instance_uid": cand.series_instance_uid,
                "technique": cand.technique,
                "orientation": cand.orientation,
                "label": new_label,
                "confidence": float(r.get("confidence", 0.0) or 0.0),
                "probs": dict(r.get("probs", {}) or {}),
                "needs_check": bool(r.get("needs_check", False)),
                "is_override": False,
                "previous_label": prior,
                "prior_source": self._classify_prior_source(prior),
                "changed": changed,
                "n_slices_used": int(r.get("n_slices_used", 0) or 0),
                "override_conflict": None,
            })
        return out

    @staticmethod
    def _combo_key(labels: Iterable[Optional[str]]) -> str:
        """Stable key for a session's set of body-part labels.

        Sorted, deduped, '+'-joined. ``None`` labels are treated as
        '(none)' so sessions with unlabeled stacks have a distinct key.
        """
        clean = sorted({(l or "(none)") for l in labels})
        return "+".join(clean) if clean else "(empty)"

    def _build_session_picks_and_summary(
        self,
        stack_diffs: list[dict],
        cand_by_id: dict[int, "CandidateStack"],
        *,
        categories: list[str],
    ) -> tuple[list[dict], dict]:
        """Group per-stack diffs into per-session picks and compute the
        cohort-level summary including ``change_matrix``.

        Sessions are keyed by ``(subject_id, session_date)``. PACS often
        splits a calendar visit into multiple StudyInstanceUIDs (brain +
        spine etc.); we aggregate them under one session and record
        ``study_ids`` + ``primary_study_id`` for downstream consumers.
        """
        by_session: dict[tuple[int, Optional[str]], list[dict]] = {}
        for d in stack_diffs:
            key = (d["subject_id"], d.get("session_date"))
            by_session.setdefault(key, []).append(d)

        picks: list[dict] = []
        for (subject_id, session_date), stacks in by_session.items():
            first = stacks[0]
            study_ids = sorted({int(s["study_id"]) for s in stacks if s.get("study_id")})
            # Pick the study that owns the most stacks as primary; tie → min.
            study_counts: dict[int, int] = {}
            for s in stacks:
                sid = int(s.get("study_id") or 0)
                if sid:
                    study_counts[sid] = study_counts.get(sid, 0) + 1
            if study_counts:
                top = max(study_counts.values())
                primary_study_id = min(
                    sid for sid, c in study_counts.items() if c == top
                )
            else:
                primary_study_id = 0
            sess = {
                "subject_id": subject_id,
                "session_date": session_date,
                "subject_code": first["subject_code"],
                "study_ids": study_ids,
                "primary_study_id": primary_study_id,
                "stacks": stacks,
                "subject_other_ids": {},
            }
            self._recompute_session_aggregates(sess)
            picks.append(sess)

        summary = self._recompute_summary(picks, categories=categories)
        return picks, summary

    def _recompute_session_aggregates(self, sess: dict) -> None:
        """Set the per-session combo / changed / counts in-place."""
        labels = [s.get("label") for s in sess["stacks"]]
        sess["session_combo"] = sorted({l for l in labels if l})
        sess["session_combo_key"] = self._combo_key(labels)

        prev_labels = [s.get("previous_label") for s in sess["stacks"]]
        sess["session_prev_combo_key"] = self._combo_key(prev_labels)
        sess["session_changed"] = (
            sess["session_combo_key"] != sess["session_prev_combo_key"]
        )
        sess["stacks_changed"] = sum(
            1 for s in sess["stacks"] if s.get("changed")
        )
        sess["low_conf_count"] = sum(
            1 for s in sess["stacks"]
            if s.get("needs_check") and s.get("label") is not None
        )
        sess["needs_check"] = (
            sess["low_conf_count"] > 0
            or any(s.get("label") is None for s in sess["stacks"])
        )

    def _recompute_summary(
        self, picks: list[dict], *, categories: list[str],
    ) -> dict:
        by_combo: dict[str, int] = {}
        change_matrix: dict[str, dict[str, int]] = {}
        total_stacks = 0
        stacks_changed = 0
        sessions_changed = 0
        needs_check = 0
        override_conflicts_count = 0

        for sess in picks:
            key = sess.get("session_combo_key", "")
            by_combo[key] = by_combo.get(key, 0) + 1
            if sess.get("needs_check"):
                needs_check += 1
            if sess.get("session_changed"):
                sessions_changed += 1
            for stk in sess.get("stacks", []):
                total_stacks += 1
                if stk.get("changed"):
                    stacks_changed += 1
                    prior = stk.get("previous_label") or "(none)"
                    new = stk.get("label") or "(none)"
                    change_matrix.setdefault(prior, {})
                    change_matrix[prior][new] = (
                        change_matrix[prior].get(new, 0) + 1
                    )
                if stk.get("override_conflict"):
                    override_conflicts_count += 1

        return {
            "total_sessions": len(picks),
            "by_combo": by_combo,
            "needs_check": needs_check,
            "total_stacks": total_stacks,
            "stacks_changed": stacks_changed,
            "sessions_changed": sessions_changed,
            "change_matrix": change_matrix,
            "override_conflicts_count": override_conflicts_count,
        }

    def _session_pick_dto(self, sess: dict) -> BodyPartSessionPickDTO:
        return BodyPartSessionPickDTO(
            subject_id=sess.get("subject_id", 0),
            session_date=sess.get("session_date"),
            subject_code=sess.get("subject_code", ""),
            study_ids=list(sess.get("study_ids") or []),
            primary_study_id=int(sess.get("primary_study_id") or 0),
            stacks=[
                BodyPartStackPickDTO(
                    stack_id=s["stack_id"],
                    label=s.get("label"),
                    confidence=s.get("confidence"),
                    probs=s.get("probs", {}),
                    is_override=s.get("is_override", False),
                    needs_check=s.get("needs_check", False),
                    series_instance_uid=s.get("series_instance_uid"),
                    technique=s.get("technique"),
                    orientation=s.get("orientation"),
                    previous_label=s.get("previous_label"),
                    prior_source=s.get("prior_source"),
                    changed=s.get("changed", False),
                    override_conflict=s.get("override_conflict"),
                )
                for s in sess.get("stacks", [])
            ],
            session_combo=sess.get("session_combo", []),
            session_combo_key=sess.get("session_combo_key", ""),
            session_prev_combo_key=sess.get("session_prev_combo_key", ""),
            session_changed=sess.get("session_changed", False),
            stacks_changed=sess.get("stacks_changed", 0),
            low_conf_count=sess.get("low_conf_count", 0),
            needs_check=sess.get("needs_check", False),
            subject_other_ids=sess.get("subject_other_ids", {}),
        )

    @staticmethod
    def _thumbnail_url(stack_id: int, slice_index: int) -> str:
        """Frontend thumbnail URL for one (stack, slice) pair.

        Backed by the existing DICOM file route (frame-aware in the
        renderer). Kept as a method so future changes (e.g. a dedicated
        thumbnail endpoint) only touch one place.
        """
        return f"/api/qc/dicom/stack/{stack_id}/slice/{slice_index}/thumbnail"

    @staticmethod
    def _default_profile() -> dict:
        return {
            "filters": dict(DEFAULT_FILTERS),
            "encoder_mode": "concat",
            "thresholds": dict(DEFAULT_THRESHOLDS),
            "compute_device": None,  # filled in by /train and /apply
        }

    def _to_state_dto(self, state: CohortBodyPartQCState) -> BodyPartStateDTO:
        """Build a public DTO from a SQLAlchemy state row."""
        has_current = bool(state.current_picks)
        has_previous = state.previous_run_at is not None

        # Training summary — group by (category, orientation) and total.
        training_summary: dict[str, dict[str, int]] = {}
        for c in state.categories or []:
            training_summary[c] = {
                "axial": 0, "sagittal": 0, "coronal": 0, "total": 0,
            }
        for s in state.training_samples or []:
            cat = s.get("label")
            ori = s.get("orientation")
            if cat not in training_summary:
                training_summary[cat] = {
                    "axial": 0, "sagittal": 0, "coronal": 0, "total": 0,
                }
            if ori in ("axial", "sagittal", "coronal"):
                training_summary[cat][ori] = (
                    training_summary[cat].get(ori, 0) + 1
                )
            training_summary[cat]["total"] = (
                training_summary[cat].get("total", 0) + 1
            )

        # Picks. Stale snapshots from before (subject_id, session_date)
        # keying are missing the new keys; treat them as absent so the
        # user re-Applies. (We log once per cohort to make the cause
        # visible.)
        raw_picks = list(state.current_picks or [])
        if raw_picks and any(
            ("subject_id" not in p) or ("session_date" not in p) or ("study_ids" not in p)
            for p in raw_picks
        ):
            logger.info(
                "cohort_body_part_qc state for cohort_id=%s pre-dates "
                "session keying — returning empty picks to force re-Apply.",
                state.cohort_id,
            )
            raw_picks = []

        picks: list[BodyPartSessionPickDTO] = []
        for sess in raw_picks:
            stacks = [
                BodyPartStackPickDTO(**stk) for stk in sess.get("stacks", [])
            ]
            picks.append(
                BodyPartSessionPickDTO(
                    subject_id=sess["subject_id"],
                    session_date=sess.get("session_date"),
                    subject_code=sess["subject_code"],
                    study_ids=list(sess.get("study_ids") or []),
                    primary_study_id=int(sess.get("primary_study_id") or 0),
                    stacks=stacks,
                    session_combo=sess.get("session_combo", []),
                    session_combo_key=sess.get("session_combo_key", ""),
                    session_prev_combo_key=sess.get(
                        "session_prev_combo_key", ""
                    ),
                    session_changed=sess.get("session_changed", False),
                    stacks_changed=sess.get("stacks_changed", 0),
                    low_conf_count=sess.get("low_conf_count", 0),
                    needs_check=sess.get("needs_check", False),
                    subject_other_ids=sess.get("subject_other_ids", {}),
                )
            )

        summary_dict = state.current_summary or {}
        summary = BodyPartSummaryDTO(
            total_sessions=summary_dict.get("total_sessions", 0),
            by_combo=summary_dict.get("by_combo", {}),
            needs_check=summary_dict.get("needs_check", 0),
            total_stacks=summary_dict.get("total_stacks", 0),
            stacks_changed=summary_dict.get("stacks_changed", 0),
            sessions_changed=summary_dict.get("sessions_changed", 0),
            change_matrix=summary_dict.get("change_matrix", {}),
            override_conflicts_count=summary_dict.get(
                "override_conflicts_count", 0,
            ),
        )

        # Stage / commit projection.
        current_picks = list(state.current_picks or [])
        committed_signature = state.last_commit_signature
        if state.stage_status == "committed" and committed_signature is not None:
            # If on-record signature differs from current, treat as dirty
            # at projection time so the UI reflects edits that happened
            # without a full DB write (e.g. via legacy code paths).
            if self._compute_commit_signature(current_picks) != committed_signature:
                stage_status = "dirty"
            else:
                stage_status = "committed"
        else:
            stage_status = state.stage_status or "none"

        if stage_status in ("staged", "dirty"):
            pending_changes_count = self._count_pending_changes(current_picks)
        else:
            pending_changes_count = 0

        # Resolve selected model name for the DTO.
        selected_model_name: Optional[str] = None
        if state.selected_model_id:
            try:
                from sqlalchemy.orm import object_session
                _db = object_session(state)
                if _db:
                    _m = _db.get(BodyPartModel, state.selected_model_id)
                    if _m:
                        selected_model_name = _m.name
            except Exception:
                pass

        return BodyPartStateDTO(
            cohort_id=state.cohort_id,
            has_current=has_current,
            has_previous=has_previous,
            current_run_at=state.current_run_at if has_current else None,
            previous_run_at=state.previous_run_at,
            categories=list(state.categories or DEFAULT_CATEGORIES),
            training_summary=training_summary,
            classifier_meta=state.classifier_meta,
            summary=summary,
            picks=picks,
            profile=state.current_profile or self._default_profile(),
            available_id_types=["code"],
            stage_status=stage_status,
            last_committed_at=state.last_committed_at,
            pending_changes_count=pending_changes_count,
            selected_model_id=state.selected_model_id,
            selected_model_name=selected_model_name,
        )
