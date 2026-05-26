"""Tiny in-process TTL cache for the MASQC session-list query.

The session-list query (``MainAcquisitionService.get_sessions``) is the
dominant cost when a reviewer clicks "Next session": it scales with the
cohort, not with the single session being shown. Rather than rewriting
the SQL or adding indexes (deferred to a later pass), we keep the result
in process for a short window. The same call repeated within the window
becomes a dict lookup.

Design choices:

* Stdlib only — no external deps.
* Keyed by ``(cohort_id, filters_hash, display_id_type, only_multi_stack)``.
* TTL of 60 s — a stack role toggle invalidates the cohort entries
  immediately via :func:`invalidate_cohort`, so the TTL is mostly a safety
  net for non-MASQC writes (e.g. pipeline imports).
* Bounded at 256 entries; eviction is "drop everything that's already
  expired, then drop the soonest-to-expire" to keep the implementation
  trivial and lock-time short.

Thread-safe via a single module-level lock; writes are rare and small.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from typing import Any, Optional


_TTL_SECONDS = 60.0
_MAX_ENTRIES = 256

_lock = threading.Lock()
_store: dict[tuple, tuple[float, Any]] = {}


def _filters_hash(filters_dict: dict) -> str:
    """Stable hash of a FilterState dict (sorted keys + sorted lists)."""
    canonical: dict[str, Optional[list[str]]] = {}
    for k, v in sorted(filters_dict.items()):
        if v is None:
            canonical[k] = None
        elif isinstance(v, list):
            canonical[k] = sorted(str(x) for x in v)
        else:
            canonical[k] = [str(v)]
    blob = json.dumps(canonical, separators=(",", ":"), sort_keys=True)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def make_key(
    cohort_id: int,
    filters_dict: dict,
    display_id_type: Optional[str],
    only_multi_stack: bool,
) -> tuple:
    return (
        int(cohort_id),
        _filters_hash(filters_dict),
        display_id_type or "code",
        bool(only_multi_stack),
    )


def get(key: tuple) -> Optional[Any]:
    """Return a cached value if still fresh, else None."""
    now = time.monotonic()
    with _lock:
        entry = _store.get(key)
        if not entry:
            return None
        expires_at, value = entry
        if expires_at <= now:
            _store.pop(key, None)
            return None
        return value


def set(key: tuple, value: Any) -> None:  # noqa: A001 - mirrors dict API
    """Store ``value`` under ``key`` with the module TTL."""
    now = time.monotonic()
    with _lock:
        # Lazy housekeeping: drop expired entries when we're about to grow.
        if len(_store) >= _MAX_ENTRIES:
            for k, (exp, _) in list(_store.items()):
                if exp <= now:
                    _store.pop(k, None)
            # Still over budget? Drop the soonest-to-expire entries.
            if len(_store) >= _MAX_ENTRIES:
                victims = sorted(_store.items(), key=lambda kv: kv[1][0])
                for k, _ in victims[: len(_store) - _MAX_ENTRIES + 1]:
                    _store.pop(k, None)
        _store[key] = (now + _TTL_SECONDS, value)


def invalidate_cohort(cohort_id: int) -> None:
    """Drop every cache entry for the given cohort."""
    cid = int(cohort_id)
    with _lock:
        for k in [k for k in _store if k[0] == cid]:
            _store.pop(k, None)


def clear_all() -> None:
    """Test helper: wipe the cache."""
    with _lock:
        _store.clear()
