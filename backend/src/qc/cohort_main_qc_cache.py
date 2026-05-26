"""Tiny in-process TTL cache for the Cohort Main QC subject-other-ids lookup.

``CohortMainQCService.get_state`` runs a SQL roundtrip on every page open
to enrich previously-persisted snapshots with the live ``subject_other_ids``
map (so newly-added id types like BROMS show up without rerunning Apply).
That query is small but never cached. This module memoises it for 60 s
keyed by cohort_id.

Stdlib only; mirrors the ``main_acquisition_cache`` design.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Optional


_TTL_SECONDS = 60.0
_MAX_ENTRIES = 64

_lock = threading.Lock()
_store: dict[int, tuple[float, Any]] = {}


def get(cohort_id: int) -> Optional[Any]:
    now = time.monotonic()
    with _lock:
        entry = _store.get(int(cohort_id))
        if not entry:
            return None
        expires_at, value = entry
        if expires_at <= now:
            _store.pop(int(cohort_id), None)
            return None
        return value


def set(cohort_id: int, value: Any) -> None:  # noqa: A001 - mirrors dict API
    now = time.monotonic()
    with _lock:
        if len(_store) >= _MAX_ENTRIES:
            for k, (exp, _) in list(_store.items()):
                if exp <= now:
                    _store.pop(k, None)
            if len(_store) >= _MAX_ENTRIES:
                victims = sorted(_store.items(), key=lambda kv: kv[1][0])
                for k, _ in victims[: len(_store) - _MAX_ENTRIES + 1]:
                    _store.pop(k, None)
        _store[int(cohort_id)] = (now + _TTL_SECONDS, value)


def invalidate(cohort_id: int) -> None:
    with _lock:
        _store.pop(int(cohort_id), None)


def clear_all() -> None:
    with _lock:
        _store.clear()
