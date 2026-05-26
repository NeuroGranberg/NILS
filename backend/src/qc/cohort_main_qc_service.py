"""Cohort-wide Main Acquisition QC service.

Computes an algorithmic main-acquisition pick per session per axis (T1w,
T2w-FLAIR) for an entire cohort, surfaces the result via API for the
heatmap UI, and writes the same ``main_acquisition`` token used by the
per-session Main Acquisition QC.

Design contract: see
``/home/nima/.factory/specs/2026-04-25-cohort-main-acquisition-qc-auto-pick-heatmap-rev-3-2d-3d-dixon-family-aware.md``.

Key knobs live in ``backend/src/qc/main_qc_weights.yaml`` and can be tuned
without code changes.
"""

from __future__ import annotations

import logging
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml
from sqlalchemy import text
from sqlalchemy.orm import Session

from cohorts.models import (
    CohortMainQCAck,
    CohortMainQCState,
    MainQCCandidateDTO,
    MainQCSessionPickDTO,
    MainQCStateDTO,
    MainQCSummaryDTO,
)
from cohorts.repository import get_cohort_by_name, get_cohort
from db.session import session_scope
from metadata_db.session import SessionLocal as MetadataSessionLocal
from qc.cache_tokens import add_token, has_token, remove_token
from qc import cohort_main_qc_cache

logger = logging.getLogger(__name__)


MAIN_TOKEN = "main_acquisition"

WEIGHTS_PATH = Path(__file__).with_name("main_qc_weights.yaml")

# Reason tokens that are informational only — they describe what the algorithm
# did but do NOT mean the user has to review the session. The frontend renders
# them in a separate "info" banner; `needs_check` is computed off the residual
# (review-level) reasons.
INFO_ONLY_REASONS: frozenset[str] = frozenset({
    "dropped_short_partial_volume",
    "acknowledged",
})


# --------------------------------------------------------------------------- #
# Config loader
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class WeightsConfig:
    weights: dict
    provenance_penalty: dict
    dim_score: dict
    technique_tiers: dict
    modifier_score: dict
    dixon_tier_bonus: float
    waterexc_tier_bonus: float
    share_weight: dict
    border_thresholds: dict
    dixon_canonical_priority: list[str]
    mp2rage_canonical_priority: list[str]
    non_canonical_constructs: list[str]


def load_weights(path: Optional[Path] = None) -> WeightsConfig:
    p = path or WEIGHTS_PATH
    raw = yaml.safe_load(p.read_text())
    return WeightsConfig(
        weights=raw["weights"],
        provenance_penalty=raw["provenance_penalty"],
        dim_score=raw["dim_score"],
        technique_tiers=raw["technique_tiers"],
        modifier_score=raw["modifier_score"],
        dixon_tier_bonus=float(raw["dixon_tier_bonus"]),
        waterexc_tier_bonus=float(raw["waterexc_tier_bonus"]),
        share_weight=raw["share_weight"],
        border_thresholds=raw["border_thresholds"],
        dixon_canonical_priority=list(raw["dixon_canonical_priority"]),
        mp2rage_canonical_priority=list(raw["mp2rage_canonical_priority"]),
        non_canonical_constructs=list(raw["non_canonical_constructs"]),
    )


# --------------------------------------------------------------------------- #
# Internal types
# --------------------------------------------------------------------------- #


@dataclass
class StackRow:
    """Single eligible stack row pulled from the metadata DB."""
    series_stack_id: int
    study_id: int
    subject_id: int
    subject_code: str
    session_date: Optional[str]
    base: Optional[str]
    technique: Optional[str]
    modifier_csv: Optional[str]
    construct_csv: Optional[str]
    acceleration_csv: Optional[str]
    directory_type: Optional[str]
    orientation: Optional[str]
    body_part: Optional[str]
    spinal_cord: Optional[int]
    provenance: Optional[str]
    post_contrast: Optional[int]
    mr_acquisition_type: Optional[str]   # "2D" / "3D" / NULL
    slices_count: Optional[int]
    fov_x_mm: Optional[float]
    stack_echo_time: Optional[float]
    stack_repetition_time: Optional[float]
    stack_inversion_time: Optional[float]
    stack_flip_angle: Optional[float]
    # Image addressing — used by the cohort Main QC modal to render previews.
    series_instance_uid: Optional[str] = None
    stack_index: int = 0
    # Resolution / FOV display fields.
    fov_y_mm: Optional[float] = None
    pixsp_row_mm: Optional[float] = None
    pixsp_col_mm: Optional[float] = None
    slice_thickness_mm: Optional[float] = None


@dataclass
class Bundle:
    """A logical bundle (after Stage-1 keying and optional Stage-2 family merge)."""
    key: tuple
    stacks: list[StackRow]
    is_dixon_family: bool = False        # Stage 2 merged
    technique: Optional[str] = None
    dim: Optional[str] = None
    modifier_csv: Optional[str] = None   # canonical (sorted, lowercased) modifier
    construct_csvs: list[str] = field(default_factory=list)  # all variants in family
    canonical_construct: Optional[str] = None
    score: float = 0.0
    score_breakdown: dict = field(default_factory=dict)
    # Set by _select_canonical_stack_ids when one or more stacks were
    # auto-demoted because their slice count is far below the bundle's max
    # (likely partial-volume helper / aborted scan, not a real retake).
    short_stacks_demoted: bool = False


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _norm_csv(value: Optional[str]) -> str:
    if not value:
        return ""
    return ",".join(sorted({p.strip() for p in value.split(",") if p.strip()}))


def _modifier_minus_dixon(value: Optional[str]) -> str:
    if not value:
        return ""
    parts = [p.strip() for p in value.split(",") if p.strip() and p.strip().lower() != "dixon"]
    return ",".join(sorted(parts))


def _csv_contains(value: Optional[str], token: str) -> bool:
    if not value:
        return False
    return any(p.strip().lower() == token.lower() for p in value.split(","))


def _csv_contains_any(value: Optional[str], tokens: Iterable[str]) -> bool:
    if not value:
        return False
    pieces = {p.strip().lower() for p in value.split(",") if p.strip()}
    return any(t.lower() in pieces for t in tokens)


def _construct_has_non_canonical(construct_csv: Optional[str], banned: list[str]) -> bool:
    """True if construct_csv contains any of the banned tokens (e.g. MIP, Synthetic)."""
    if not construct_csv:
        return False
    pieces = [p.strip() for p in construct_csv.split(",") if p.strip()]
    for piece in pieces:
        for ban in banned:
            if ban.lower() in piece.lower():
                return True
    return False


def _dim_key(dim: Optional[str]) -> str:
    if dim == "3D":
        return "3d"
    if dim == "2D":
        return "2d"
    return "unknown"


def _percentiles(values: list[float]) -> Optional[dict]:
    """Compute 5/25/50/75/95 percentiles. None if too few samples."""
    pure = [float(v) for v in values if v is not None]
    if len(pure) < 3:
        return None
    pure.sort()
    return {
        "p5":  _pct(pure, 0.05),
        "p25": _pct(pure, 0.25),
        "p50": _pct(pure, 0.50),
        "p75": _pct(pure, 0.75),
        "p95": _pct(pure, 0.95),
    }


def _pct(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = max(0, min(len(sorted_vals) - 1, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def _pctile_lookup(value: Optional[float], pctiles: Optional[dict]) -> float:
    """Map ``value`` into a [0,1] score against ``pctiles`` dict from _percentiles()."""
    if value is None:
        return 0.40
    if not pctiles:
        return 0.60   # neutral when we can't bucket
    v = float(value)
    if v <= pctiles["p5"]:
        return 0.20
    if v <= pctiles["p25"]:
        return 0.40
    if v <= pctiles["p50"]:
        return 0.60
    if v <= pctiles["p75"]:
        return 0.80
    if v <= pctiles["p95"]:
        return 1.0
    # well above 95th: still high but not infinity
    return 1.0


def _classify_family(modifier_csv: Optional[str]) -> str:
    """Map a winning bundle's modifier_csv to a coarse 'family' category for legend chips."""
    if _csv_contains(modifier_csv, "Dixon"):
        return "dixon"
    if _csv_contains(modifier_csv, "WaterExc"):
        return "waterexc"
    return "plain"


def _classify_slice_bucket(slices: Optional[int]) -> Optional[str]:
    """Map a winning bundle's slice count to a coarse resolution bucket."""
    if not slices:
        return None
    if slices >= 176:
        return "hi"
    if slices >= 100:
        return "std"
    return "lo"


def _build_pick_content(winner_bundle, max_slices: int) -> dict:
    """Return the compact content descriptor for a winning bundle."""
    return {
        "technique": winner_bundle.technique,
        "dim": winner_bundle.dim,
        "family": _classify_family(winner_bundle.modifier_csv),
        "slice_bucket": _classify_slice_bucket(max_slices),
        "slices": int(max_slices) if max_slices else None,
    }


# --------------------------------------------------------------------------- #
# Service
# --------------------------------------------------------------------------- #


class CohortMainQCService:
    """Cohort-level Main Acquisition QC service."""

    def __init__(self, weights: Optional[WeightsConfig] = None) -> None:
        self.weights = weights or load_weights()

    # ---- Public API ----------------------------------------------------- #

    def get_state(self, cohort_id: int) -> MainQCStateDTO:
        with session_scope() as app_db:
            cohort = get_cohort(app_db, cohort_id)
            if not cohort:
                raise LookupError(f"Cohort id={cohort_id} not found")
            row = app_db.get(CohortMainQCState, cohort_id)
            if not row:
                return MainQCStateDTO(cohort_id=cohort_id)
            # Detach all values we need; the ORM row goes out of scope below.
            profile = row.current_profile or {}
            raw_picks = list(row.current_picks or [])
            raw_summary = dict(row.current_summary or {})
            current_run_at = row.current_run_at
            previous_run_at = row.previous_run_at
            has_previous = row.previous_picks is not None
            # Acks live in their own table; load them once here so the picks
            # we return reflect "user reviewed and confirmed" even after a
            # cohort Apply rewrote current_picks.
            ack_set = {
                (a.subject_id, a.session_date, a.axis)
                for a in app_db.query(CohortMainQCAck)
                .filter(CohortMainQCAck.cohort_id == cohort_id)
                .all()
            }

        # Stale-snapshot guard: snapshots from before the (subject_id,
        # session_date) keying era are missing one of those fields. Treat
        # them as absent so the user re-Applies and gets the new shape.
        if raw_picks and any(
            ("subject_id" not in p) or ("session_date" not in p)
            for p in raw_picks
        ):
            logger.info(
                "cohort_main_qc state for cohort_id=%s pre-dates session "
                "keying — returning empty state to force re-Apply.",
                cohort_id,
            )
            return MainQCStateDTO(cohort_id=cohort_id)

        # Always discover the available id types live, regardless of whether the
        # snapshot pre-dates this feature. Same contract as MainAcquisitionService.
        # Cached for 60 s per cohort to keep page-open responsive on big cohorts.
        cached_ids = cohort_main_qc_cache.get(cohort_id)
        if cached_ids is not None:
            id_map, available_id_types = cached_ids
        else:
            subject_ids = {p.get("subject_id") for p in raw_picks if p.get("subject_id")}
            id_map, available_id_types = self._fetch_subject_other_ids(
                subject_ids=subject_ids,
            )
            cohort_main_qc_cache.set(cohort_id, (id_map, available_id_types))

        # Back-fill subject_other_ids on picks persisted before this feature
        # and overlay the acknowledgement state from the ack table.
        enriched_picks: list[dict] = []
        for p in raw_picks:
            sid = p.get("subject_id")
            existing = p.get("subject_other_ids") or {}
            if (not existing) and sid in id_map:
                p = {**p, "subject_other_ids": dict(id_map[sid])}
            p = self._overlay_ack(p, ack_set)
            enriched_picks.append(p)

        return MainQCStateDTO(
            cohort_id=cohort_id,
            has_current=True,
            has_previous=has_previous,
            current_run_at=current_run_at,
            previous_run_at=previous_run_at,
            summary={
                k: MainQCSummaryDTO(**v) for k, v in raw_summary.items()
            },
            picks=[MainQCSessionPickDTO(**p) for p in enriched_picks],
            profile=profile,
            available_id_types=available_id_types,
        )

    @staticmethod
    def _overlay_ack(
        pick: dict,
        ack_set: set[tuple[int, Optional[str], str]],
    ) -> dict:
        """If ``(subject_id, session_date, axis)`` is in ``ack_set``, mark
        the pick acknowledged.

        Sets ``acknowledged=True``, clears ``needs_check`` (an ack overrides
        any review-level reasons), and adds ``"acknowledged"`` to
        ``needs_check_reasons`` so the modal can render the info banner.
        Idempotent — safe even if the picks JSON already contained the token.
        """
        subject_id = pick.get("subject_id")
        session_date = pick.get("session_date")
        axis = pick.get("axis")
        if subject_id is None or axis is None:
            return pick
        if (subject_id, session_date, axis) not in ack_set:
            return pick
        reasons = list(pick.get("needs_check_reasons") or [])
        if "acknowledged" not in reasons:
            reasons.append("acknowledged")
        return {
            **pick,
            "needs_check": False,
            "needs_check_reasons": reasons,
            "acknowledged": True,
        }

    def apply(self, cohort_id: int) -> MainQCStateDTO:
        """Run the algorithm cohort-wide, wipe & rewrite tokens, persist snapshot."""
        with session_scope() as app_db:
            cohort = get_cohort(app_db, cohort_id)
            if not cohort:
                raise LookupError(f"Cohort id={cohort_id} not found")
            cohort_name = cohort.name

        logger.info("Cohort Main QC: applying for cohort_id=%s name=%s", cohort_id, cohort_name)

        # 1. Load identifier map once (used by both axes).
        id_map, available_id_types = self._fetch_subject_other_ids(cohort_name)
        # The state for this cohort is about to change; drop any cached id-map
        # so the next get_state re-reads. (Cheap; tiny cache.)
        cohort_main_qc_cache.invalidate(cohort_id)

        # 2. Compute fresh result for both axes.
        result_t1w = self._compute_axis(cohort_name, "t1w", id_map)
        result_flair = self._compute_axis(cohort_name, "flair", id_map)
        all_picks = result_t1w["picks"] + result_flair["picks"]
        profile = {
            "t1w": result_t1w["profile"],
            "flair": result_flair["profile"],
            "available_id_types": available_id_types,
        }
        summary = {
            "t1w": result_t1w["summary"],
            "flair": result_flair["summary"],
        }

        # 2. Wipe existing main_acquisition tokens cohort-wide.
        self._wipe_main_tokens_for_cohort(cohort_name)

        # 3. Write tokens for new picks.
        new_main_stack_ids: list[int] = []
        for pick in all_picks:
            new_main_stack_ids.extend(pick["winning_stack_ids"])
        if new_main_stack_ids:
            self._write_main_tokens(new_main_stack_ids)

        # 4. Persist snapshot (rotate current -> previous).
        with session_scope() as app_db:
            row = app_db.get(CohortMainQCState, cohort_id)
            now = datetime.now(timezone.utc)
            if row is None:
                row = CohortMainQCState(
                    cohort_id=cohort_id,
                    current_run_at=now,
                    current_summary=summary,
                    current_picks=all_picks,
                    current_profile=profile,
                )
                app_db.add(row)
            else:
                # rotate current -> previous
                row.previous_run_at = row.current_run_at
                row.previous_summary = row.current_summary
                row.previous_picks = row.current_picks
                row.previous_profile = row.current_profile
                row.current_run_at = now
                row.current_summary = summary
                row.current_picks = all_picks
                row.current_profile = profile

        return self.get_state(cohort_id)

    def restore_previous(self, cohort_id: int) -> MainQCStateDTO:
        """Restore the previous snapshot. Wipes current tokens and replays previous picks.

        Single-level undo: previous becomes current; the run before previous is lost.
        """
        with session_scope() as app_db:
            cohort = get_cohort(app_db, cohort_id)
            if not cohort:
                raise LookupError(f"Cohort id={cohort_id} not found")
            row = app_db.get(CohortMainQCState, cohort_id)
            if not row or row.previous_picks is None:
                raise LookupError("No previous snapshot to restore")
            cohort_name = cohort.name
            prev_picks = row.previous_picks
            prev_summary = row.previous_summary
            prev_profile = row.previous_profile
            prev_run_at = row.previous_run_at

        # 1. Wipe current cohort-wide.
        self._wipe_main_tokens_for_cohort(cohort_name)

        # 2. Replay previous picks.
        replay_ids: list[int] = []
        for pick in prev_picks:
            replay_ids.extend(pick.get("winning_stack_ids", []))
        if replay_ids:
            self._write_main_tokens(replay_ids)

        # 2a. Drop cached id-map for this cohort.
        cohort_main_qc_cache.invalidate(cohort_id)

        # 3. Swap snapshots in-place: previous becomes current; previous cleared.
        with session_scope() as app_db:
            row = app_db.get(CohortMainQCState, cohort_id)
            row.current_run_at = prev_run_at or datetime.now(timezone.utc)
            row.current_summary = prev_summary or {}
            row.current_picks = prev_picks or []
            row.current_profile = prev_profile or {}
            row.previous_run_at = None
            row.previous_summary = None
            row.previous_picks = None
            row.previous_profile = None

        return self.get_state(cohort_id)

    def update_session_pick(
        self,
        cohort_id: int,
        subject_id: int,
        session_date: str,
        axis: str,
        stack_ids: list[int],
    ) -> MainQCSessionPickDTO:
        """Manually override the auto-pick for a single session+axis.

        A session is identified by ``(subject_id, session_date)`` and may
        span multiple StudyInstanceUIDs (brain + spine on the same date).
        This method wipes the main token from any eligible stack in any of
        the session's studies for the axis, then writes the token to the
        supplied stack_ids. Cohort scope is preserved end-to-end.
        """
        with session_scope() as app_db:
            cohort = get_cohort(app_db, cohort_id)
            if not cohort:
                raise LookupError(f"Cohort id={cohort_id} not found")
            cohort_name = cohort.name

        # Validate the stacks belong to this session (subject + date) and to
        # this cohort. The cohort_origin filter prevents a stack that lives
        # in another cohort but belongs to the same subject from being chosen.
        with MetadataSessionLocal() as meta_db:
            rows = meta_db.execute(
                text("""
                    SELECT scc.series_stack_id,
                           s.study_id, s.subject_id,
                           st.study_date::text AS session_date,
                           scc.dicom_origin_cohort
                    FROM series_classification_cache scc
                    JOIN series s ON s.series_instance_uid = scc.series_instance_uid
                    JOIN study st ON st.study_id = s.study_id
                    WHERE scc.series_stack_id = ANY(:ids)
                """),
                {"ids": list(stack_ids)},
            ).fetchall()
        for r in rows:
            if r.subject_id != subject_id or (r.session_date or "") != session_date:
                raise ValueError(
                    f"stack {r.series_stack_id} does not belong to session "
                    f"(subject_id={subject_id}, date={session_date})"
                )
            if (r.dicom_origin_cohort or "").lower() != cohort_name.lower():
                raise ValueError(
                    f"stack {r.series_stack_id} does not belong to cohort {cohort_name!r}"
                )

        # Resolve the full set of study_ids for this session (cohort-scoped).
        session_study_ids = self._resolve_session_study_ids(
            cohort_name, subject_id, session_date,
        )

        # Wipe existing main token for this session+axis (across all its
        # studies), then set on supplied ids.
        self._wipe_main_tokens_for_session_axis(cohort_name, session_study_ids, axis)
        if stack_ids:
            self._write_main_tokens(stack_ids)

        # Patch the state JSON.
        with session_scope() as app_db:
            row = app_db.get(CohortMainQCState, cohort_id)
            if row is None:
                raise LookupError("No current snapshot — run Apply first")
            picks = list(row.current_picks or [])
            updated: Optional[dict] = None
            for entry in picks:
                if (
                    entry.get("subject_id") == subject_id
                    and entry.get("session_date") == session_date
                    and entry.get("axis") == axis
                ):
                    entry["winning_stack_ids"] = list(stack_ids)
                    entry["needs_check"] = False
                    entry["needs_check_reasons"] = ["manual_override"]
                    # Update is_main flags in candidate_summary.
                    for cand in entry.get("candidate_summary", []):
                        cand["is_main"] = cand.get("stack_id") in stack_ids
                    updated = entry
                    break
            if updated is None:
                # Session had no algo entry; create a minimal one.
                updated = {
                    "subject_id": subject_id,
                    "session_date": session_date,
                    "subject_code": "",
                    "axis": axis,
                    "study_ids": session_study_ids,
                    "primary_study_id": session_study_ids[0] if session_study_ids else 0,
                    "winning_stack_ids": list(stack_ids),
                    "score": None,
                    "needs_check": False,
                    "needs_check_reasons": ["manual_override"],
                    "candidate_summary": [],
                }
                picks.append(updated)
            row.current_picks = picks

        return MainQCSessionPickDTO(**updated)

    def reset_session_to_auto(
        self,
        cohort_id: int,
        subject_id: int,
        session_date: str,
        axis: str,
    ) -> MainQCSessionPickDTO:
        """Re-run the algorithm just for this session+axis and write tokens."""
        with session_scope() as app_db:
            cohort = get_cohort(app_db, cohort_id)
            if not cohort:
                raise LookupError(f"Cohort id={cohort_id} not found")
            cohort_name = cohort.name
            state_row = app_db.get(CohortMainQCState, cohort_id)
            if state_row is None:
                raise LookupError("No current snapshot — run Apply first")
            profile = (state_row.current_profile or {}).get(axis, {})

        # Pull eligible stacks for THIS session.
        rows = self._fetch_eligible_rows(
            cohort_name, axis, session_filter=(subject_id, session_date),
        )
        session_study_ids = self._resolve_session_study_ids(
            cohort_name, subject_id, session_date,
        )

        if not rows:
            pick = MainQCSessionPickDTO(
                subject_id=subject_id,
                session_date=session_date,
                subject_code="",
                axis=axis,
                study_ids=session_study_ids,
                primary_study_id=session_study_ids[0] if session_study_ids else 0,
                winning_stack_ids=[],
                score=None,
                needs_check=True,
                needs_check_reasons=["no_eligible_stacks"],
                candidate_summary=[],
            )
        else:
            pick_dict = self._pick_for_session(rows, profile, axis)
            pick_dict["study_ids"] = session_study_ids
            winners = pick_dict.get("winning_stack_ids") or []
            if winners:
                winner_study_ids = sorted({
                    r.study_id for r in rows
                    if r.series_stack_id in winners
                })
                pick_dict["primary_study_id"] = (
                    winner_study_ids[0]
                    if winner_study_ids
                    else (session_study_ids[0] if session_study_ids else 0)
                )
            else:
                pick_dict["primary_study_id"] = (
                    session_study_ids[0] if session_study_ids else 0
                )
            pick = MainQCSessionPickDTO(**pick_dict)

        # Wipe tokens for this session+axis and write new ones.
        self._wipe_main_tokens_for_session_axis(cohort_name, session_study_ids, axis)
        if pick.winning_stack_ids:
            self._write_main_tokens(pick.winning_stack_ids)

        # Patch state row + drop any prior acknowledgement for this session+axis
        # (Reset to auto means "I want a fresh algorithmic decision").
        with session_scope() as app_db:
            row = app_db.get(CohortMainQCState, cohort_id)
            picks = list(row.current_picks or [])
            replaced = False
            for i, entry in enumerate(picks):
                if (
                    entry.get("subject_id") == subject_id
                    and entry.get("session_date") == session_date
                    and entry.get("axis") == axis
                ):
                    picks[i] = pick.model_dump(mode="json")
                    replaced = True
                    break
            if not replaced:
                picks.append(pick.model_dump(mode="json"))
            row.current_picks = picks
            ack = app_db.get(
                CohortMainQCAck, (cohort_id, subject_id, session_date, axis),
            )
            if ack is not None:
                app_db.delete(ack)

        return pick

    def acknowledge_session_pick(
        self,
        cohort_id: int,
        subject_id: int,
        session_date: str,
        axis: str,
    ) -> MainQCSessionPickDTO:
        """Mark a session+axis pick as user-reviewed-and-confirmed.

        Idempotent: re-acknowledging is a no-op other than refreshing the
        timestamp. Acknowledgements live in ``cohort_main_qc_ack`` so they
        survive a cohort Apply (which rewrites ``current_picks``).
        """
        normalized_axis = (axis or "").strip().lower()
        if normalized_axis not in {"t1w", "flair"}:
            raise ValueError(f"Invalid axis: {axis!r}")

        with session_scope() as app_db:
            cohort = get_cohort(app_db, cohort_id)
            if not cohort:
                raise LookupError(f"Cohort id={cohort_id} not found")
            state_row = app_db.get(CohortMainQCState, cohort_id)
            if state_row is None:
                raise LookupError("No current snapshot — run Apply first")

            ack_pk = (cohort_id, subject_id, session_date, normalized_axis)
            ack = app_db.get(CohortMainQCAck, ack_pk)
            if ack is None:
                ack = CohortMainQCAck(
                    cohort_id=cohort_id,
                    subject_id=subject_id,
                    session_date=session_date,
                    axis=normalized_axis,
                    acknowledged_at=datetime.now(timezone.utc),
                )
                app_db.add(ack)
            else:
                ack.acknowledged_at = datetime.now(timezone.utc)

            # Patch the live picks JSON so the pick we return reflects the new
            # state without depending on the read-side overlay.
            picks = list(state_row.current_picks or [])
            patched: Optional[dict] = None
            for i, entry in enumerate(picks):
                if (
                    entry.get("subject_id") == subject_id
                    and entry.get("session_date") == session_date
                    and entry.get("axis") == normalized_axis
                ):
                    picks[i] = self._overlay_ack(
                        entry, {(subject_id, session_date, normalized_axis)},
                    )
                    patched = picks[i]
                    break
            if patched is None:
                raise LookupError(
                    f"No pick for subject_id={subject_id} "
                    f"session_date={session_date} axis={normalized_axis} "
                    f"in cohort {cohort_id}"
                )
            state_row.current_picks = picks

        return MainQCSessionPickDTO(**patched)

    # ---- helpers ------------------------------------------------------- #

    def _resolve_session_study_ids(
        self,
        cohort_name: str,
        subject_id: int,
        session_date: str,
    ) -> list[int]:
        """Return all study_ids in the cohort for a (subject, date)."""
        sql = """
            SELECT DISTINCT s.study_id
            FROM series s
            JOIN study st ON st.study_id = s.study_id
            JOIN series_classification_cache scc
                 ON scc.series_instance_uid = s.series_instance_uid
            WHERE LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)
              AND s.subject_id = :subject_id
              AND st.study_date::text = :session_date
            ORDER BY s.study_id
        """
        with MetadataSessionLocal() as meta_db:
            res = meta_db.execute(
                text(sql),
                {
                    "cohort_name": cohort_name,
                    "subject_id": subject_id,
                    "session_date": session_date,
                },
            ).fetchall()
        return [int(r.study_id) for r in res]

    # ---- Algorithm core ------------------------------------------------- #

    def _compute_axis(
        self,
        cohort_name: str,
        axis: str,
        subject_other_ids_map: Optional[dict[int, dict[str, str]]] = None,
    ) -> dict:
        rows = self._fetch_eligible_rows(cohort_name, axis)
        profile = self._compute_cohort_profile(rows, axis)

        # Group rows by session = (subject_id, session_date). PACS often splits
        # one calendar visit into multiple StudyInstanceUIDs (e.g. brain study
        # + spine study), and those should appear as a single QC session.
        sessions: dict[tuple[int, Optional[str]], list[StackRow]] = defaultdict(list)
        for r in rows:
            sessions[(r.subject_id, r.session_date)].append(r)

        # Also enumerate all sessions in cohort (some may have NO eligible stacks → red).
        all_sessions = self._fetch_all_sessions(cohort_name)

        id_map = subject_other_ids_map or {}

        picks: list[dict] = []
        green = amber = red = needs_check = 0
        for sess in all_sessions:
            sess_key = (sess["subject_id"], sess["session_date"])
            session_rows = sessions.get(sess_key, [])
            study_ids = list(sess["study_ids"])
            primary_study_id = study_ids[0] if study_ids else 0
            if not session_rows:
                pick = {
                    "subject_id": sess["subject_id"],
                    "session_date": sess["session_date"],
                    "subject_code": sess["subject_code"] or "",
                    "axis": axis,
                    "study_ids": study_ids,
                    "primary_study_id": primary_study_id,
                    "winning_stack_ids": [],
                    "score": None,
                    "needs_check": False,
                    "needs_check_reasons": ["no_eligible_stacks"],
                    "candidate_summary": [],
                    "content": None,
                    "subject_other_ids": dict(id_map.get(sess["subject_id"], {})),
                }
                picks.append(pick)
                red += 1
                continue
            pick = self._pick_for_session(session_rows, profile, axis)
            pick["study_ids"] = study_ids
            # primary = study owning the winning stack(s) when available;
            # otherwise the lowest study_id.
            winners = pick.get("winning_stack_ids") or []
            if winners:
                winner_study_ids = sorted({
                    r.study_id for r in session_rows
                    if r.series_stack_id in winners
                })
                pick["primary_study_id"] = (
                    winner_study_ids[0] if winner_study_ids else primary_study_id
                )
            else:
                pick["primary_study_id"] = primary_study_id
            pick["subject_other_ids"] = dict(id_map.get(pick["subject_id"], {}))
            picks.append(pick)
            score = pick.get("score") or 0.0
            if pick.get("winning_stack_ids"):
                if score >= 0.80:
                    green += 1
                elif score >= 0.60:
                    amber += 1
                else:
                    red += 1
            else:
                red += 1
            if pick.get("needs_check"):
                needs_check += 1

        return {
            "picks": picks,
            "profile": profile,
            "summary": {
                "green": green,
                "amber": amber,
                "red": red,
                "needs_check": needs_check,
                "total_sessions": len(all_sessions),
            },
        }

    def _fetch_subject_other_ids(
        self,
        cohort_name: Optional[str] = None,
        subject_ids: Optional[Iterable[int]] = None,
    ) -> tuple[dict[int, dict[str, str]], list[str]]:
        """Return (subject_id → {id_type_name: value}, ["code", ...sorted unique types]).

        Works in two modes (use whichever produces a non-empty subject set):
          - by ``cohort_name``: scopes through series_classification_cache.
          - by explicit ``subject_ids``: leaner; matches the MainAcquisitionService
            pattern (used by ``get_state`` to enrich a previously-persisted snapshot).
        """
        ids_list: list[int] = list({int(s) for s in (subject_ids or []) if s})

        if not ids_list and cohort_name:
            # Resolve subject_ids from the cohort first (single round-trip).
            sql_ids = """
                SELECT DISTINCT s.subject_id
                FROM series s
                JOIN series_classification_cache scc
                  ON scc.series_instance_uid = s.series_instance_uid
                WHERE LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)
                  AND s.subject_id IS NOT NULL
            """
            with MetadataSessionLocal() as meta_db:
                ids_list = [
                    int(r.subject_id)
                    for r in meta_db.execute(text(sql_ids), {"cohort_name": cohort_name}).fetchall()
                ]

        if not ids_list:
            return {}, ["code"]

        sql = """
            SELECT soi.subject_id, it.id_type_name, soi.other_identifier
            FROM subject_other_identifiers soi
            JOIN id_types it ON it.id_type_id = soi.id_type_id
            WHERE soi.subject_id = ANY(:subject_ids)
        """
        with MetadataSessionLocal() as meta_db:
            rows = meta_db.execute(text(sql), {"subject_ids": ids_list}).fetchall()

        id_map: dict[int, dict[str, str]] = {}
        types: set[str] = set()
        for r in rows:
            sid = int(r.subject_id)
            tn = r.id_type_name
            val = r.other_identifier
            if not tn or val is None:
                continue
            id_map.setdefault(sid, {})[tn] = str(val)
            types.add(tn)
        return id_map, ["code"] + sorted(types)

    # ---- Eligibility query --------------------------------------------- #

    def _fetch_eligible_rows(
        self,
        cohort_name: str,
        axis: str,
        session_filter: Optional[tuple[int, str]] = None,
    ) -> list[StackRow]:
        """Fetch all eligible stacks for ``axis`` in ``cohort_name``.

        ``session_filter``: optional ``(subject_id, session_date)`` tuple to
        restrict the query to a single session (used by Reset to auto). The
        cohort filter is always applied; a session filter further narrows it.
        """
        wb = self.weights.non_canonical_constructs

        if axis == "t1w":
            base_clause = "scc.base = 'T1w'"
            modifier_clause = ""  # any
        elif axis == "flair":
            base_clause = "scc.base = 'T2w'"
            modifier_clause = " AND scc.modifier_csv ILIKE '%FLAIR%'"
        else:
            raise ValueError(f"unknown axis {axis!r}")

        # Banned construct substrings — translate to ILIKE OR-list.
        banned_clauses = " AND ".join(
            f"COALESCE(scc.construct_csv,'') NOT ILIKE '%{b}%'" for b in wb
        )

        study_filter = ""
        params: dict[str, Any] = {"cohort_name": cohort_name}
        if session_filter is not None:
            study_filter = (
                " AND s.subject_id = :subject_id"
                " AND st.study_date::text = :session_date"
            )
            params["subject_id"] = session_filter[0]
            params["session_date"] = session_filter[1]

        sql = f"""
            SELECT
                scc.series_stack_id,
                scc.series_instance_uid,
                s.study_id,
                s.subject_id,
                subj.subject_code,
                st.study_date::text AS session_date,
                scc.base,
                scc.technique,
                scc.modifier_csv,
                scc.construct_csv,
                scc.acceleration_csv,
                scc.directory_type,
                scc.orientation_patient AS orientation,
                scc.body_part,
                scc.spinal_cord,
                scc.provenance,
                scc.post_contrast,
                sf.mr_acquisition_type,
                scc.slices_count,
                scc.fov_x_mm,
                scc.fov_y_mm,
                scc.pixsp_row_mm,
                scc.pixsp_col_mm,
                scc.slice_thickness_mm,
                ss.stack_echo_time,
                ss.stack_repetition_time,
                ss.stack_inversion_time,
                ss.stack_flip_angle,
                ss.stack_index
            FROM series_classification_cache scc
            JOIN series s ON s.series_instance_uid = scc.series_instance_uid
            JOIN study st ON s.study_id = st.study_id
            LEFT JOIN subject subj ON s.subject_id = subj.subject_id
            LEFT JOIN series_stack ss ON ss.series_stack_id = scc.series_stack_id
            LEFT JOIN stack_fingerprint sf ON sf.series_stack_id = scc.series_stack_id
            WHERE LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)
              AND scc.directory_type = 'anat'
              AND COALESCE(scc.localizer, 0) = 0
              AND {base_clause}
              {modifier_clause}
              AND COALESCE(scc.provenance, '') NOT IN ('SyMRI', 'ProjectionDerived', 'SubtractionDerived')
              AND {banned_clauses}
              {study_filter}
        """

        with MetadataSessionLocal() as meta_db:
            res = meta_db.execute(text(sql), params).fetchall()

        out: list[StackRow] = []
        for r in res:
            out.append(StackRow(
                series_stack_id=int(r.series_stack_id),
                study_id=int(r.study_id),
                subject_id=int(r.subject_id) if r.subject_id is not None else 0,
                subject_code=r.subject_code or "",
                session_date=r.session_date,
                base=r.base,
                technique=r.technique,
                modifier_csv=r.modifier_csv,
                construct_csv=r.construct_csv,
                acceleration_csv=r.acceleration_csv,
                directory_type=r.directory_type,
                orientation=r.orientation,
                body_part=r.body_part,
                spinal_cord=r.spinal_cord,
                provenance=r.provenance,
                post_contrast=r.post_contrast,
                mr_acquisition_type=r.mr_acquisition_type,
                slices_count=int(r.slices_count) if r.slices_count is not None else None,
                fov_x_mm=float(r.fov_x_mm) if r.fov_x_mm is not None else None,
                stack_echo_time=r.stack_echo_time,
                stack_repetition_time=r.stack_repetition_time,
                stack_inversion_time=r.stack_inversion_time,
                stack_flip_angle=r.stack_flip_angle,
                series_instance_uid=r.series_instance_uid,
                stack_index=int(r.stack_index) if r.stack_index is not None else 0,
                fov_y_mm=float(r.fov_y_mm) if r.fov_y_mm is not None else None,
                pixsp_row_mm=float(r.pixsp_row_mm) if r.pixsp_row_mm is not None else None,
                pixsp_col_mm=float(r.pixsp_col_mm) if r.pixsp_col_mm is not None else None,
                slice_thickness_mm=float(r.slice_thickness_mm) if r.slice_thickness_mm is not None else None,
            ))
        return out

    def _fetch_all_sessions(self, cohort_name: str) -> list[dict]:
        # Sessions are keyed by (subject_id, session_date). We aggregate the
        # set of study_ids under each session so token wipes can target every
        # constituent study (e.g. brain + spine on the same date).
        # Cohort-scope is preserved through the cohort-filtered join.
        sql = """
            SELECT s.subject_id,
                   MIN(subj.subject_code) AS subject_code,
                   st.study_date::text AS session_date,
                   array_agg(DISTINCT s.study_id ORDER BY s.study_id) AS study_ids
            FROM series s
            JOIN study st ON s.study_id = st.study_id
            LEFT JOIN subject subj ON s.subject_id = subj.subject_id
            JOIN series_classification_cache scc ON scc.series_instance_uid = s.series_instance_uid
            WHERE LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)
            GROUP BY s.subject_id, st.study_date
            ORDER BY MIN(subj.subject_code) NULLS LAST, st.study_date NULLS LAST
        """
        with MetadataSessionLocal() as meta_db:
            res = meta_db.execute(text(sql), {"cohort_name": cohort_name}).fetchall()
        return [
            {
                "subject_id": int(r.subject_id) if r.subject_id else 0,
                "subject_code": r.subject_code or "",
                "session_date": r.session_date,
                "study_ids": [int(sid) for sid in (r.study_ids or [])],
            }
            for r in res
        ]

    # ---- Profile -------------------------------------------------------- #

    def _compute_cohort_profile(self, rows: list[StackRow], axis: str) -> dict:
        techniques = Counter((r.technique or "Unknown") for r in rows)
        techniques_by_dim = Counter(((r.technique or "Unknown"), r.mr_acquisition_type or "unknown") for r in rows)
        dim_share_counter = Counter((r.mr_acquisition_type or None) for r in rows)
        modifier_combos = Counter((r.modifier_csv or "") for r in rows)
        construct_combos = Counter((r.construct_csv or "") for r in rows)

        # Slice percentiles per dim bucket.
        slice_3d = [r.slices_count for r in rows if r.mr_acquisition_type == "3D" and r.slices_count]
        slice_2d = [r.slices_count for r in rows if r.mr_acquisition_type == "2D" and r.slices_count]
        slice_unk = [r.slices_count for r in rows if not r.mr_acquisition_type and r.slices_count]
        fov = [r.fov_x_mm for r in rows if r.fov_x_mm]

        total = max(1, sum(dim_share_counter.values()))
        dim_share = {str(k or "unknown"): v / total for k, v in dim_share_counter.items()}

        return {
            "axis": axis,
            "techniques": dict(techniques),
            "techniques_by_dim": {f"{k[0]}|{k[1]}": v for k, v in techniques_by_dim.items()},
            "dim_share": dim_share,
            "modifier_combos": dict(modifier_combos),
            "construct_combos": dict(construct_combos),
            "slice_pctiles_3d": _percentiles(slice_3d),
            "slice_pctiles_2d": _percentiles(slice_2d),
            "slice_pctiles_unknown": _percentiles(slice_unk),
            "fov_pctiles": _percentiles(fov),
            "dominant_technique": techniques.most_common(1)[0][0] if techniques else None,
            "dominant_share": (techniques.most_common(1)[0][1] / sum(techniques.values())) if techniques else 0.0,
            "dominant_dim": dim_share_counter.most_common(1)[0][0] if dim_share_counter else None,
            "has_dixon_family": any("dixon" in (m or "").lower() for m in modifier_combos),
            "total_eligible_stacks": len(rows),
        }

    # ---- Bundling ------------------------------------------------------- #

    def _stage1_bundle_key(self, r: StackRow) -> tuple:
        """Same fingerprint as MainAcquisitionService._bundle_key_fields."""
        return (
            r.body_part or "",
            r.spinal_cord or 0,
            r.orientation or "",
            r.base or "",
            r.mr_acquisition_type or "",
            _norm_csv(r.modifier_csv),
            r.technique or "",
            _norm_csv(r.acceleration_csv),
            _norm_csv(r.construct_csv),
            r.directory_type or "",
            r.stack_echo_time,
            r.stack_repetition_time,
            r.stack_inversion_time,
            r.stack_flip_angle,
            r.post_contrast,
        )

    def _stage2_family_key(self, r: StackRow) -> tuple:
        """Dixon family key: same physics minus Dixon/construct_csv."""
        return (
            r.body_part or "",
            r.spinal_cord or 0,
            r.orientation or "",
            r.base or "",
            r.mr_acquisition_type or "",
            _modifier_minus_dixon(r.modifier_csv),  # drop Dixon
            r.technique or "",
            _norm_csv(r.acceleration_csv),
            r.directory_type or "",
            r.post_contrast,
            # No construct_csv; that's the variant axis we merge over.
            # No TR/TE/TI/FA — Dixon outputs may differ slightly per construct.
        )

    def _bundle_session(self, session_rows: list[StackRow]) -> list[Bundle]:
        """Stage 1: key by full fingerprint. Stage 2: merge Dixon families."""
        # Stage 1: each unique fingerprint -> one Bundle.
        groups: dict[tuple, list[StackRow]] = defaultdict(list)
        for r in session_rows:
            groups[self._stage1_bundle_key(r)].append(r)

        bundles: list[Bundle] = []
        for key, stacks in groups.items():
            bundles.append(Bundle(
                key=key,
                stacks=stacks,
                technique=stacks[0].technique,
                dim=stacks[0].mr_acquisition_type,
                modifier_csv=_norm_csv(stacks[0].modifier_csv),
                construct_csvs=[_norm_csv(s.construct_csv) for s in stacks],
            ))

        # Stage 2: merge Dixon families.
        # Family condition: modifier contains 'Dixon' and at least 2 stacks
        # share the family key with differing construct_csv.
        family_groups: dict[tuple, list[Bundle]] = defaultdict(list)
        non_family: list[Bundle] = []
        for b in bundles:
            r0 = b.stacks[0]
            if not _csv_contains(r0.modifier_csv, "Dixon"):
                non_family.append(b)
                continue
            fam_key = self._stage2_family_key(r0)
            family_groups[fam_key].append(b)

        merged: list[Bundle] = list(non_family)
        for fam_key, sub_bundles in family_groups.items():
            if len(sub_bundles) == 1:
                merged.append(sub_bundles[0])
                continue
            # Merge into one family bundle.
            all_stacks: list[StackRow] = []
            constructs: list[str] = []
            for sb in sub_bundles:
                all_stacks.extend(sb.stacks)
                constructs.extend(sb.construct_csvs)
            r0 = all_stacks[0]
            fam = Bundle(
                key=fam_key,
                stacks=all_stacks,
                is_dixon_family=True,
                technique=r0.technique,
                dim=r0.mr_acquisition_type,
                modifier_csv=_norm_csv(r0.modifier_csv),
                construct_csvs=constructs,
            )
            merged.append(fam)
        return merged

    # ---- Canonical pick within a bundle -------------------------------- #

    def _select_canonical_stack_ids(self, bundle: Bundle) -> tuple[list[int], Optional[str]]:
        """Pick the canonical-output stacks within a bundle.

        Returns (stack_ids, canonical_construct_label).
        - For Dixon families: priority InPhase → Water; if neither present,
          drop bundle (return ([], None)).
        - For MP2RAGE bundles (technique=MP2RAGE): Denoised,Uniform → Uniform → all.
        - Otherwise: all stacks in the bundle.
        """
        cfg = self.weights

        if bundle.is_dixon_family:
            for canonical in cfg.dixon_canonical_priority:
                matched = [s for s in bundle.stacks if _csv_contains(s.construct_csv, canonical)]
                # Filter: prefer those with ONLY the canonical construct (no MIP, MPR siblings).
                pure = [s for s in matched
                        if not _construct_has_non_canonical(s.construct_csv, cfg.non_canonical_constructs)]
                if pure:
                    return ([int(s.series_stack_id) for s in pure], canonical)
            return ([], None)  # Family with only Fat/OutPhase/Phase: drop.

        if (bundle.technique or "").upper() == "MP2RAGE":
            for canonical in cfg.mp2rage_canonical_priority:
                matched = [s for s in bundle.stacks if _norm_csv(s.construct_csv) == canonical]
                if matched:
                    return ([int(s.series_stack_id) for s in matched], canonical)
            # Fall through: tag all (cohort with bare MP2RAGE, no labeled sister).
            return ([int(s.series_stack_id) for s in bundle.stacks], None)

        # Generic bundle: tag every stack as canonical, but auto-demote stacks
        # whose slice count is far below the bundle's max — those are almost
        # certainly partial-volume helpers / aborted scans, not real retakes.
        # The demoted stacks still appear in candidate_summary (so the user can
        # see them in the modal) with is_main=False.
        stacks = list(bundle.stacks)
        if len(stacks) >= 2:
            slices = [int(s.slices_count or 0) for s in stacks]
            max_slices = max(slices)
            pv_pct = float(cfg.border_thresholds.get("partial_volume_pct", 0.50))
            pv_min = int(cfg.border_thresholds.get("partial_volume_min_slices", 60))
            if max_slices >= pv_min:
                cutoff = pv_pct * max_slices
                kept = [s for s, n in zip(stacks, slices) if n >= cutoff]
                if kept and len(kept) < len(stacks):
                    bundle.short_stacks_demoted = True
                    return ([int(s.series_stack_id) for s in kept], None)

        return ([int(s.series_stack_id) for s in stacks], None)

    # ---- Scoring ------------------------------------------------------- #

    def _score_bundle(self, bundle: Bundle, profile: dict, axis: str) -> tuple[float, dict]:
        cfg = self.weights
        w = cfg.weights
        r0 = bundle.stacks[0]

        # 1. Dim
        dim = bundle.dim
        if dim == "3D":
            dim_s = float(cfg.dim_score["3d"])
        elif dim == "2D":
            cohort_3d_share = float(profile.get("dim_share", {}).get("3D", 0.0))
            dim_s = float(cfg.dim_score["2d_base"]) - float(cfg.dim_score["2d_3d_penalty"]) * cohort_3d_share
            dim_s = max(0.0, min(1.0, dim_s))
        else:
            dim_s = float(cfg.dim_score["unknown"])

        # 2. Technique tier (with Dixon family / WaterExc bonuses)
        tech_tiers = cfg.technique_tiers.get(axis, {}) or {}
        tier = float(tech_tiers.get(bundle.technique or "Unknown", tech_tiers.get("Unknown", 0.30)))
        is_dixon_or_we = False
        if _csv_contains(bundle.modifier_csv, "Dixon"):
            # Only apply Dixon tier bonus if a canonical construct exists.
            has_canonical = False
            for canonical in cfg.dixon_canonical_priority:
                if any(_csv_contains(s.construct_csv, canonical) for s in bundle.stacks):
                    has_canonical = True
                    break
            if has_canonical:
                tier = min(1.0, tier + float(cfg.dixon_tier_bonus))
            is_dixon_or_we = True
        if _csv_contains(bundle.modifier_csv, "WaterExc"):
            tier = min(1.0, tier + float(cfg.waterexc_tier_bonus))
            is_dixon_or_we = True
        tech_s = tier

        # 3. Modifier score (additive on 0.5 base)
        mod_table = cfg.modifier_score.get(axis, {}) or {}
        mod_delta = 0.0
        for token, delta in mod_table.items():
            if _csv_contains(bundle.modifier_csv, token):
                mod_delta += float(delta)
        mod_s = max(0.0, min(1.0, 0.5 + mod_delta))

        # 4. Slice count (in dim-bucket percentile)
        max_slices = max((s.slices_count or 0) for s in bundle.stacks)
        pctiles_key = f"slice_pctiles_{_dim_key(dim)}"
        slice_s = _pctile_lookup(max_slices, profile.get(pctiles_key))

        # 5. FOV (cohort-wide percentile)
        fov_v = max((s.fov_x_mm or 0) for s in bundle.stacks)
        fov_s = _pctile_lookup(fov_v if fov_v > 0 else None, profile.get("fov_pctiles"))

        # 6. Cohort-share (via bundle-shape population share)
        share_cfg = cfg.share_weight["dixon_or_waterexc" if is_dixon_or_we else "default"]
        techniques = profile.get("techniques") or {}
        total = max(1, sum(techniques.values()))
        tech_share = (techniques.get(bundle.technique or "Unknown", 0)) / total
        share_s = (
            float(share_cfg["share_coef"]) * min(1.0, tech_share / 0.5)   # tops out at 50% share
            + float(share_cfg["floor_coef"]) * float(share_cfg["floor_value"])
        )
        share_s = max(0.0, min(1.0, share_s))

        # 7. Orientation confidence (binary-ish: known orientation = 1.0, unknown = 0.4)
        orient_s = 1.0 if r0.orientation else 0.40

        # 8. Completeness (slices_count present, fov present)
        complete_s = 0.5
        if max_slices:
            complete_s += 0.25
        if fov_v:
            complete_s += 0.25
        complete_s = min(1.0, complete_s)

        score = (
            w["dim"] * dim_s
            + w["tech"] * tech_s
            + w["mod"] * mod_s
            + w["slices"] * slice_s
            + w["fov"] * fov_s
            + w["share"] * share_s
            + w["orient"] * orient_s
            + w["complete"] * complete_s
        )
        # EPIMix penalty
        prov = (r0.provenance or "RawRecon")
        prov_mult = float(cfg.provenance_penalty.get(prov, 1.0))
        score *= prov_mult
        score = max(0.0, min(1.0, score))

        breakdown = {
            "dim": round(dim_s, 4),
            "tech": round(tech_s, 4),
            "mod": round(mod_s, 4),
            "slices": round(slice_s, 4),
            "fov": round(fov_s, 4),
            "share": round(share_s, 4),
            "orient": round(orient_s, 4),
            "complete": round(complete_s, 4),
            "provenance_mult": prov_mult,
        }
        return score, breakdown

    # ---- Per-session pick ---------------------------------------------- #

    def _pick_for_session(self, session_rows: list[StackRow], profile: dict, axis: str) -> dict:
        cfg = self.weights
        bundles = self._bundle_session(session_rows)

        # Score every bundle.
        for b in bundles:
            score, breakdown = self._score_bundle(b, profile, axis)
            b.score = score
            b.score_breakdown = breakdown

        # Drop bundles whose canonical pick fails (Dixon family with only Fat/Phase).
        viable: list[tuple[Bundle, list[int], Optional[str]]] = []
        non_viable: list[Bundle] = []
        for b in bundles:
            stack_ids, canon = self._select_canonical_stack_ids(b)
            if stack_ids:
                viable.append((b, stack_ids, canon))
            else:
                non_viable.append(b)

        # Build candidate_summary spanning ALL bundles so user can see rejects too.
        candidate_summary: list[dict] = []
        all_winning_ids: set[int] = set()

        # Local helper — keeps the three call sites below in sync.
        def _cand(s: StackRow, b: Bundle, *, is_main: bool, in_family: bool, is_canon: bool) -> dict:
            return {
                "stack_id": int(s.series_stack_id),
                "score": round(b.score, 4),
                "technique": b.technique,
                "modifier_csv": b.modifier_csv,
                "construct_csv": s.construct_csv,
                "dim": b.dim,
                "slices": s.slices_count,
                "fov_x_mm": s.fov_x_mm,
                "post_contrast": s.post_contrast,
                "provenance": s.provenance,
                "is_main": is_main,
                "in_winning_family": in_family,
                "is_canonical_in_family": is_canon,
                "series_instance_uid": s.series_instance_uid,
                "stack_index": int(s.stack_index or 0),
                "orientation": s.orientation,
                "base": s.base,
                "fov_y_mm": s.fov_y_mm,
                "pixsp_row_mm": s.pixsp_row_mm,
                "pixsp_col_mm": s.pixsp_col_mm,
                "slice_thickness_mm": s.slice_thickness_mm,
            }

        if not viable:
            # No bundle can be tagged main.
            for b in bundles:
                for s in b.stacks:
                    candidate_summary.append(
                        _cand(s, b, is_main=False, in_family=False, is_canon=False)
                    )
            r0 = session_rows[0]
            return {
                "subject_id": r0.subject_id,
                "session_date": r0.session_date,
                "subject_code": r0.subject_code,
                "axis": axis,
                "study_ids": [],
                "primary_study_id": 0,
                "winning_stack_ids": [],
                "score": 0.0,
                "needs_check": True,
                "needs_check_reasons": ["no_canonical_construct"],
                "candidate_summary": candidate_summary,
                "content": None,
                "subject_other_ids": {},
            }

        # Top bundle by score.
        viable.sort(key=lambda triple: triple[0].score, reverse=True)
        winner_bundle, winner_ids, winner_canon = viable[0]
        all_winning_ids.update(winner_ids)

        # Pre/post twin: any bundle ≥ pre_post_twin_pct * winner.score AND different post_contrast.
        pre_post_thresh = float(cfg.border_thresholds["pre_post_twin_pct"])
        winner_pcs = {s.post_contrast for s in winner_bundle.stacks}
        twin_bundle: Optional[Bundle] = None
        twin_ids: list[int] = []
        for b, ids, canon in viable[1:]:
            if b.score < pre_post_thresh * winner_bundle.score:
                break
            other_pcs = {s.post_contrast for s in b.stacks}
            if other_pcs.isdisjoint(winner_pcs):
                twin_bundle = b
                twin_ids = ids
                all_winning_ids.update(twin_ids)
                break

        # Compose candidate_summary for ALL bundles (viable + non_viable), labeling
        # winning members.
        winning_family_keys = {winner_bundle.key}
        if twin_bundle is not None:
            winning_family_keys.add(twin_bundle.key)
        for b, ids, canon in viable:
            for s in b.stacks:
                is_winner = int(s.series_stack_id) in all_winning_ids
                candidate_summary.append(
                    _cand(
                        s, b,
                        is_main=is_winner,
                        in_family=b.key in winning_family_keys,
                        is_canon=is_winner,
                    )
                )
        for b in non_viable:
            for s in b.stacks:
                candidate_summary.append(
                    _cand(s, b, is_main=False, in_family=False, is_canon=False)
                )

        # Border / needs_check reasons.
        reasons: list[str] = []
        bt = cfg.border_thresholds

        # Retake: ≥2 stacks in winning bundle that are NOT family sisters.
        if winner_bundle.is_dixon_family:
            # within Dixon family, sisters are expected — count canonical winners only.
            if len(winner_ids) > 1 and len({_norm_csv(s.construct_csv) for s in winner_bundle.stacks if int(s.series_stack_id) in winner_ids}) > 1:
                # multiple sisters tagged main — fine, no retake flag.
                pass
            elif len([s for s in winner_bundle.stacks if int(s.series_stack_id) in winner_ids]) > 1:
                reasons.append("retake_dixon_canonical")
        elif (winner_bundle.technique or "").upper() == "MP2RAGE":
            if len(winner_ids) > 2:
                reasons.append("retake_mp2rage")
        else:
            if len(winner_bundle.stacks) > 1:
                reasons.append("retake")

        # Close runner-up.
        if len(viable) >= 2:
            runner_score = viable[1][0].score
            if winner_bundle.score > 0 and (winner_bundle.score - runner_score) / winner_bundle.score < float(bt["runner_up_within_pct"]):
                reasons.append("close_runner_up")

        # Unknown dim.
        if not winner_bundle.dim:
            reasons.append("unknown_dim")

        # Slice outside 5-95th of dim bin.
        pctiles = profile.get(f"slice_pctiles_{_dim_key(winner_bundle.dim)}")
        max_slices = max((s.slices_count or 0) for s in winner_bundle.stacks)
        if pctiles and max_slices:
            if max_slices < pctiles["p5"] or max_slices > pctiles["p95"]:
                reasons.append("slice_count_outlier")

        # Pre/post twin.
        if twin_bundle is not None:
            reasons.append("pre_post_twin")

        # EPIMix fallback.
        if any((s.provenance or "") == "EPIMix" for s in winner_bundle.stacks):
            reasons.append("epimix_fallback")

        # Rare technique pick.
        techniques = profile.get("techniques") or {}
        total = max(1, sum(techniques.values()))
        winning_share = (techniques.get(winner_bundle.technique or "Unknown", 0)) / total
        if winning_share < float(bt["rare_technique_share"]):
            reasons.append("rare_technique")

        # Dixon vs plain border.
        if winner_bundle.is_dixon_family:
            for b, _ids, _canon in viable[1:]:
                if not _csv_contains(b.modifier_csv, "Dixon") and not _csv_contains(b.modifier_csv, "WaterExc"):
                    if winner_bundle.score > 0 and (winner_bundle.score - b.score) / winner_bundle.score <= float(bt["dixon_vs_plain_pct"]):
                        reasons.append("dixon_vs_plain")
                        break

        # Informational only (algorithm transparency — not a review trigger):
        # the canonical-stack selector demoted one or more short stacks within
        # the winning bundle.
        if winner_bundle.short_stacks_demoted:
            reasons.append("dropped_short_partial_volume")

        # `needs_check` only fires when at least one review-level reason is
        # present. Informational tokens listed in INFO_ONLY_REASONS show up
        # in needs_check_reasons (so the modal can render them) but don't
        # paint the heatmap cell as "needs_check".
        review_reasons = [r for r in reasons if r not in INFO_ONLY_REASONS]

        r0 = session_rows[0]
        max_slices_winner = max((s.slices_count or 0) for s in winner_bundle.stacks)
        return {
            "subject_id": r0.subject_id,
            "session_date": r0.session_date,
            "subject_code": r0.subject_code,
            "axis": axis,
            # study_ids / primary_study_id are filled by the caller (it knows
            # the full set of studies in the session even when some have no
            # eligible stacks).
            "study_ids": [],
            "primary_study_id": 0,
            "winning_stack_ids": sorted(all_winning_ids),
            "score": round(winner_bundle.score, 4),
            "needs_check": bool(review_reasons),
            "needs_check_reasons": reasons,
            "candidate_summary": candidate_summary,
            "content": _build_pick_content(winner_bundle, max_slices_winner),
            "subject_other_ids": {},
        }

    # ---- Token wipe / write -------------------------------------------- #

    def _wipe_main_tokens_for_cohort(self, cohort_name: str) -> None:
        sql_select = """
            SELECT scc.series_stack_id, scc.classification_string
            FROM series_classification_cache scc
            WHERE LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)
              AND POSITION(:tok IN COALESCE(scc.classification_string, '')) > 0
        """
        sql_update = """
            UPDATE series_classification_cache
            SET classification_string = :cs
            WHERE series_stack_id = :sid
        """
        with MetadataSessionLocal() as meta_db:
            rows = meta_db.execute(
                text(sql_select),
                {"cohort_name": cohort_name, "tok": MAIN_TOKEN},
            ).fetchall()
            for r in rows:
                if has_token(r.classification_string, MAIN_TOKEN):
                    new_cs = remove_token(r.classification_string, MAIN_TOKEN)
                    meta_db.execute(
                        text(sql_update),
                        {"cs": new_cs, "sid": r.series_stack_id},
                    )
            meta_db.commit()

    def _wipe_main_tokens_for_session_axis(
        self,
        cohort_name: str,
        study_ids: list[int],
        axis: str,
    ) -> None:
        """Wipe the main_acquisition token from every eligible stack in the
        session, across **all** ``study_ids`` that belong to this session.

        A session can span multiple StudyInstanceUIDs (brain study + spine
        study on the same date), so a per-session wipe must touch all of
        them. Cohort scope is enforced via ``dicom_origin_cohort``.
        """
        if not study_ids:
            return
        if axis == "t1w":
            base_clause = "scc.base = 'T1w'"
            modifier_clause = ""
        elif axis == "flair":
            base_clause = "scc.base = 'T2w'"
            modifier_clause = " AND scc.modifier_csv ILIKE '%FLAIR%'"
        else:
            raise ValueError(f"unknown axis {axis!r}")

        sql_select = f"""
            SELECT scc.series_stack_id, scc.classification_string
            FROM series_classification_cache scc
            JOIN series s ON s.series_instance_uid = scc.series_instance_uid
            WHERE LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)
              AND s.study_id = ANY(:study_ids)
              AND scc.directory_type = 'anat'
              AND {base_clause}
              {modifier_clause}
              AND POSITION(:tok IN COALESCE(scc.classification_string, '')) > 0
        """
        sql_update = """
            UPDATE series_classification_cache
            SET classification_string = :cs
            WHERE series_stack_id = :sid
        """
        with MetadataSessionLocal() as meta_db:
            rows = meta_db.execute(
                text(sql_select),
                {
                    "cohort_name": cohort_name,
                    "study_ids": list(study_ids),
                    "tok": MAIN_TOKEN,
                },
            ).fetchall()
            for r in rows:
                if has_token(r.classification_string, MAIN_TOKEN):
                    new_cs = remove_token(r.classification_string, MAIN_TOKEN)
                    meta_db.execute(
                        text(sql_update),
                        {"cs": new_cs, "sid": r.series_stack_id},
                    )
            meta_db.commit()

    def _write_main_tokens(self, stack_ids: Iterable[int]) -> None:
        ids = list(stack_ids)
        if not ids:
            return
        sql_select = """
            SELECT series_stack_id, classification_string
            FROM series_classification_cache
            WHERE series_stack_id = ANY(:ids)
        """
        sql_update = """
            UPDATE series_classification_cache
            SET classification_string = :cs
            WHERE series_stack_id = :sid
        """
        with MetadataSessionLocal() as meta_db:
            rows = meta_db.execute(text(sql_select), {"ids": ids}).fetchall()
            for r in rows:
                new_cs = add_token(r.classification_string, MAIN_TOKEN)
                if new_cs != r.classification_string:
                    meta_db.execute(
                        text(sql_update),
                        {"cs": new_cs, "sid": r.series_stack_id},
                    )
            meta_db.commit()
