"""Sort-step3 protection helper.

The keyword-based ``BodyPartDetector`` runs every time Sort step3 reclassifies
a cohort and writes ``body_part`` into ``series_classification_cache``. Once a
cohort has been processed by the Body Part QC module, those QC labels must
not be silently overwritten.

This helper exposes a single call that step3 uses to skip the ``body_part``
column update for stacks already labeled by an active QC state.
"""

from __future__ import annotations

import logging
from typing import Iterable

from cohorts.models import CohortBodyPartQCState
from db.session import session_scope

logger = logging.getLogger(__name__)


COMMITTED_LABELS_PROFILE_KEY = "committed_body_part_labels"


def _normalise_label(label: object) -> str | None:
    if not isinstance(label, str):
        return None
    out = label.strip().lower()
    return out or None


def get_qc_protected_body_part_labels(cohort_id: int) -> dict[int, str]:
    """Return ``series_stack_id -> committed body_part`` for this cohort.

    Step3 must NOT overwrite ``body_part`` for these stacks during
    reclassification. The keyword detector is still allowed to RUN and
    write its detection to other (unrelated) columns or to the metadata
    audit trail; only the final ``body_part`` value is preserved.

    An empty set is returned if no QC state row exists or if the cohort
    has never had a successful Commit.
    """
    try:
        with session_scope() as db:
            state = db.get(CohortBodyPartQCState, cohort_id)
            if state is None:
                return {}

            profile = state.current_profile or {}
            committed = profile.get(COMMITTED_LABELS_PROFILE_KEY) or {}
            if isinstance(committed, dict) and committed:
                out: dict[int, str] = {}
                for raw_sid, raw_label in committed.items():
                    label = _normalise_label(raw_label)
                    if label is None:
                        continue
                    try:
                        out[int(raw_sid)] = label
                    except (TypeError, ValueError):
                        continue
                if out:
                    return out

            # Legacy fallback: older states only had current_picks. This is
            # insufficient for partial commits, but preserves compatibility for
            # full-commit states created before the committed-label registry.
            if not state.current_picks:
                return {}
            out = {}
            for sess in state.current_picks:
                for stk in sess.get("stacks", []):
                    sid = stk.get("stack_id")
                    label = _normalise_label(stk.get("label"))
                    if sid is not None and label is not None:
                        out[int(sid)] = label
            return out
    except Exception:  # pragma: no cover — defensive: never block step3
        logger.exception(
            "get_qc_protected_body_part_labels failed for cohort_id=%s; "
            "returning empty set so step3 proceeds normally.",
            cohort_id,
        )
        return {}


def get_qc_protected_stack_ids(cohort_id: int) -> set[int]:
    """Return stack ids protected by committed Body Part QC labels."""
    return set(get_qc_protected_body_part_labels(cohort_id).keys())


def filter_unprotected_stacks(
    cohort_id: int, stack_ids: Iterable[int]
) -> set[int]:
    """Convenience: return the subset of ``stack_ids`` NOT under QC protection."""
    protected = get_qc_protected_stack_ids(cohort_id)
    return set(int(s) for s in stack_ids) - protected
