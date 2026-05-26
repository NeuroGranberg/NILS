"""Metadata-DB writes for Body Part QC.

The QC service is the source of truth for the review tokens and the
final ``body_part`` label. This module is a thin SQL layer that:

- Sets ``series_classification_cache.body_part`` for a list of
  (stack_id, label) pairs.
- Adds / removes the ``bodypart_qc:low_confidence`` review token in
  ``series_classification_cache.manual_review_reasons_csv``.

All updates run in a single transaction per call. The helper accepts a
``MetadataSession`` factory so tests can inject an in-memory DB without
touching the real Postgres metadata schema.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Callable, Iterable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from metadata_db.session import SessionLocal as MetadataSessionLocal
from qc.cache_tokens import add_token, has_token, remove_token

logger = logging.getLogger(__name__)


# Review token written when a stack falls below ``manual_review_below``.
LOW_CONF_REVIEW_TOKEN: str = "bodypart_qc:low_confidence"


# Default factory; tests override.
def _default_session_factory() -> Session:
    return MetadataSessionLocal()


SessionFactory = Callable[[], Session]


# ---------------------------------------------------------------------------
# body_part column writer
# ---------------------------------------------------------------------------


def _spinal_cord_from_body_part(bp: Optional[str]) -> Optional[int]:
    """Derive ``spinal_cord`` (backward-compat binary) from ``body_part``."""
    if not bp:
        return None
    low = bp.lower()
    if low in ("spine", "neck"):
        return 1
    if low in ("brain", "brain-neck"):
        return 0
    return None


def write_body_part_labels(
    pairs: Iterable[tuple[int, Optional[str]]],
    *,
    session_factory: SessionFactory = _default_session_factory,
) -> int:
    """Set ``body_part`` and ``spinal_cord`` for the supplied pairs.

    Labels are normalised to **lowercase** before writing so that QC
    commits match the keyword detector's convention. ``spinal_cord`` is
    derived automatically (1 for spine/neck, 0 for brain/brain-neck,
    NULL otherwise).

    ``label=None`` clears the column. Returns the number of rows updated.
    Rows whose current value already matches are skipped to keep the
    audit trail clean.
    """
    items = list(pairs)
    if not items:
        return 0

    select_sql = text(
        "SELECT series_stack_id, body_part, spinal_cord "
        "FROM series_classification_cache "
        "WHERE series_stack_id = ANY(:ids)"
    )
    update_sql = text(
        "UPDATE series_classification_cache "
        "SET body_part = :bp, spinal_cord = :sc "
        "WHERE series_stack_id = :sid"
    )

    target = {sid: (label.lower() if label else None) for sid, label in items}
    n_updates = 0
    with session_factory() as db:
        rows = db.execute(select_sql, {"ids": list(target.keys())}).fetchall()
        for r in rows:
            new_label = target.get(int(r.series_stack_id))
            new_sc = _spinal_cord_from_body_part(new_label)
            if new_label == r.body_part and new_sc == r.spinal_cord:
                continue
            db.execute(update_sql, {
                "bp": new_label,
                "sc": new_sc,
                "sid": int(r.series_stack_id),
            })
            n_updates += 1
        db.commit()
    return n_updates


# ---------------------------------------------------------------------------
# Low-confidence review token
# ---------------------------------------------------------------------------


def set_low_conf_tokens(
    add_ids: Iterable[int],
    remove_ids: Iterable[int],
    *,
    session_factory: SessionFactory = _default_session_factory,
) -> tuple[int, int]:
    """Add / remove ``bodypart_qc:low_confidence`` for the given stacks.

    Returns ``(n_added, n_removed)``. Idempotent: a stack that already
    has the token is skipped.
    """
    add_set = list({int(i) for i in add_ids})
    rm_set = list({int(i) for i in remove_ids})
    if not add_set and not rm_set:
        return (0, 0)

    select_sql = text(
        "SELECT series_stack_id, manual_review_reasons_csv "
        "FROM series_classification_cache "
        "WHERE series_stack_id = ANY(:ids)"
    )
    update_sql = text(
        "UPDATE series_classification_cache "
        "SET manual_review_reasons_csv = :reasons "
        "WHERE series_stack_id = :sid"
    )
    n_added = 0
    n_removed = 0
    with session_factory() as db:
        ids = list(set(add_set + rm_set))
        rows = db.execute(select_sql, {"ids": ids}).fetchall()
        for r in rows:
            sid = int(r.series_stack_id)
            current = r.manual_review_reasons_csv
            new_value = current
            if sid in add_set and not has_token(current, LOW_CONF_REVIEW_TOKEN):
                new_value = add_token(current, LOW_CONF_REVIEW_TOKEN)
                n_added += 1
            elif sid in rm_set and has_token(current, LOW_CONF_REVIEW_TOKEN):
                new_value = remove_token(current, LOW_CONF_REVIEW_TOKEN)
                n_removed += 1
            if new_value != current:
                db.execute(update_sql, {"reasons": new_value, "sid": sid})
        db.commit()
    return (n_added, n_removed)


# ---------------------------------------------------------------------------
# Cohort-wide wipe (used by restore_previous when there is nothing to
# restore — we just clear our footprint).
# ---------------------------------------------------------------------------


def wipe_low_conf_tokens_for_cohort(
    cohort_name: str,
    *,
    session_factory: SessionFactory = _default_session_factory,
) -> int:
    """Remove ``bodypart_qc:low_confidence`` from every stack in a cohort."""
    select_sql = text(
        "SELECT series_stack_id, manual_review_reasons_csv "
        "FROM series_classification_cache "
        "WHERE LOWER(dicom_origin_cohort) = LOWER(:cohort_name) "
        "  AND POSITION(:tok IN COALESCE(manual_review_reasons_csv, '')) > 0"
    )
    update_sql = text(
        "UPDATE series_classification_cache "
        "SET manual_review_reasons_csv = :reasons "
        "WHERE series_stack_id = :sid"
    )
    n = 0
    with session_factory() as db:
        rows = db.execute(select_sql, {
            "cohort_name": cohort_name, "tok": LOW_CONF_REVIEW_TOKEN,
        }).fetchall()
        for r in rows:
            new_value = remove_token(
                r.manual_review_reasons_csv, LOW_CONF_REVIEW_TOKEN,
            )
            if new_value != r.manual_review_reasons_csv:
                db.execute(update_sql, {
                    "reasons": new_value,
                    "sid": int(r.series_stack_id),
                })
                n += 1
        db.commit()
    return n


# ---------------------------------------------------------------------------
# Apply-time orchestrator: from picks JSON → metadata DB writes
# ---------------------------------------------------------------------------


def apply_picks_to_metadata(
    picks: list[dict],
    *,
    session_factory: SessionFactory = _default_session_factory,
) -> dict[str, int]:
    """Translate a ``current_picks`` JSON snapshot into metadata-DB writes.

    For each stack:
        - ``body_part`` := pick['label'] (only when it's a configured
          category and the prediction succeeded; otherwise leave the
          column alone — the keyword detector's value still lives there).
        - low-conf token added when ``needs_check=True AND label is not
          None AND is_override=False`` (overrides are user-trusted).
        - token removed otherwise.

    Returns a small stats dict for logging / API responses.
    """
    body_part_pairs: list[tuple[int, Optional[str]]] = []
    add_ids: list[int] = []
    remove_ids: list[int] = []

    for sess in picks:
        for stk in sess.get("stacks", []):
            sid = int(stk["stack_id"])
            label = stk.get("label")
            if label:
                body_part_pairs.append((sid, label))
            needs_check = bool(stk.get("needs_check"))
            is_override = bool(stk.get("is_override"))
            label_present = label is not None
            if needs_check and label_present and not is_override:
                add_ids.append(sid)
            else:
                remove_ids.append(sid)

    n_body_part = write_body_part_labels(
        body_part_pairs, session_factory=session_factory,
    )
    n_added, n_removed = set_low_conf_tokens(
        add_ids, remove_ids, session_factory=session_factory,
    )
    return {
        "body_part_updated": n_body_part,
        "low_conf_added": n_added,
        "low_conf_removed": n_removed,
    }
