"""Persistent cache of frozen-encoder embeddings.

Backed by ``stack_embedding_cache`` in the application DB. Avoids paying
the encoder cost more than once per (stack_id, slice_index, encoder_name,
encoder_version).

Embedding bytes are little-endian float32 (``np.float32.tobytes()``).
Reads validate ``len(bytes) == dim * 4`` and that the cached ``dim``
matches the encoder's declared output dimension.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import numpy as np
from sqlalchemy import and_, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from cohorts.models import StackEmbeddingCache
from db.session import session_scope

logger = logging.getLogger(__name__)


# Maximum number of (stack_id, slice_index) pairs in a single tuple-IN
# SELECT. Postgres parses the IN-list as a deep expression tree and blows
# past max_stack_depth (~2 MB) for cohorts with thousands of stacks. We
# chunk to keep the parse tree shallow; 1000 is comfortable on the
# default Postgres config and small enough to round-trip quickly.
_DEFAULT_READ_CHUNK = 1000


def _read_chunk_size() -> int:
    raw = os.environ.get("BODY_PART_READ_MANY_CHUNK")
    if not raw:
        return _DEFAULT_READ_CHUNK
    try:
        n = int(raw)
        return n if n > 0 else _DEFAULT_READ_CHUNK
    except ValueError:
        return _DEFAULT_READ_CHUNK


@dataclass(frozen=True)
class CacheKey:
    series_stack_id: int
    slice_index: int
    encoder_name: str
    encoder_version: str

    def as_tuple(self) -> tuple[int, int, str, str]:
        return (
            self.series_stack_id,
            self.slice_index,
            self.encoder_name,
            self.encoder_version,
        )


def _embedding_to_bytes(arr: np.ndarray) -> bytes:
    return np.ascontiguousarray(arr.astype(np.float32, copy=False)).tobytes()


def _bytes_to_embedding(b: bytes, expected_dim: int) -> np.ndarray:
    arr = np.frombuffer(b, dtype=np.float32)
    if arr.shape != (expected_dim,):
        raise ValueError(
            f"Cached embedding shape {arr.shape} does not match expected "
            f"({expected_dim},)."
        )
    return arr


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def read_one(
    db: Session, key: CacheKey, expected_dim: int,
) -> Optional[np.ndarray]:
    """Return the cached embedding if present, else ``None``.

    Raises :class:`ValueError` if the cached row's ``dim`` differs from
    ``expected_dim`` (encoder version-pinning safeguard).
    """
    row = db.get(StackEmbeddingCache, key.as_tuple())
    if row is None:
        return None
    if row.dim != expected_dim:
        raise ValueError(
            f"Cached dim {row.dim} != expected {expected_dim} for {key}. "
            "Bump encoder_version when changing models."
        )
    return _bytes_to_embedding(row.embedding, expected_dim)


def read_many(
    db: Session,
    keys: list[CacheKey],
    expected_dim: int,
) -> dict[CacheKey, np.ndarray]:
    """Bulk variant of :func:`read_one`. Returns one entry per cache hit;
    misses are simply absent from the dict.

    Performance: previously the worker did one ``db.get`` per key, which
    was hundreds of milliseconds for cohorts with thousands of stacks.
    This implementation runs a single SELECT keyed by
    ``(series_stack_id, slice_index, encoder_name, encoder_version)``.

    The query is partitioned by (encoder_name, encoder_version) and uses
    a composite IN over ``(series_stack_id, slice_index)`` per partition
    — works on both PostgreSQL and SQLite.
    """
    if not keys:
        return {}

    # Group keys by (encoder_name, encoder_version) — the typical caller
    # uses one encoder per call so this is usually a single partition.
    by_encoder: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for k in keys:
        by_encoder.setdefault(
            (k.encoder_name, k.encoder_version), [],
        ).append((k.series_stack_id, k.slice_index))

    chunk_size = _read_chunk_size()
    out: dict[CacheKey, np.ndarray] = {}
    for (enc_name, enc_ver), pairs in by_encoder.items():
        # Composite IN on (series_stack_id, slice_index) — supported by
        # both Postgres and SQLite via ``tuple_(...).in_(...)``. Chunked
        # to keep the per-statement parse tree shallow on Postgres.
        for start in range(0, len(pairs), chunk_size):
            sub = pairs[start : start + chunk_size]
            stmt = (
                select(StackEmbeddingCache)
                .where(StackEmbeddingCache.encoder_name == enc_name)
                .where(StackEmbeddingCache.encoder_version == enc_ver)
                .where(
                    tuple_(
                        StackEmbeddingCache.series_stack_id,
                        StackEmbeddingCache.slice_index,
                    ).in_(sub)
                )
            )
            for row in db.execute(stmt).scalars():
                if row.dim != expected_dim:
                    raise ValueError(
                        f"Cached dim {row.dim} != expected {expected_dim} for "
                        f"({row.series_stack_id}, {row.slice_index}, "
                        f"{enc_name}, {enc_ver}). "
                        "Bump encoder_version when changing models."
                    )
                out[CacheKey(
                    series_stack_id=int(row.series_stack_id),
                    slice_index=int(row.slice_index),
                    encoder_name=enc_name,
                    encoder_version=enc_ver,
                )] = _bytes_to_embedding(row.embedding, expected_dim)
    return out


def write_many(
    db: Session,
    items: Iterable[tuple[CacheKey, np.ndarray]],
) -> int:
    """UPSERT a batch of embeddings. Returns the number of rows written.

    Uses dialect-specific ``ON CONFLICT DO UPDATE`` so callers can safely
    overwrite stale entries (e.g. after a preprocess change with a new
    ``encoder_version``).
    """
    payload = []
    for key, arr in items:
        payload.append({
            "series_stack_id": key.series_stack_id,
            "slice_index": key.slice_index,
            "encoder_name": key.encoder_name,
            "encoder_version": key.encoder_version,
            "embedding": _embedding_to_bytes(arr),
            "dim": int(arr.shape[-1]),
        })
    if not payload:
        return 0

    # Postgres' wire protocol caps each statement at 65535 bound
    # parameters. With 6 parameters per row, an unsplit upsert blows
    # past this limit at ~10k rows. Chunk the payload to stay safely
    # under the cap.
    chunk_rows = int(
        os.environ.get("BODY_PART_WRITE_MANY_CHUNK", "5000") or "5000",
    )
    if chunk_rows <= 0:
        chunk_rows = len(payload)

    dialect = db.bind.dialect.name if db.bind is not None else "postgresql"
    n_written = 0
    for off in range(0, len(payload), chunk_rows):
        batch = payload[off:off + chunk_rows]
        if dialect == "sqlite":
            stmt = sqlite_insert(StackEmbeddingCache).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    "series_stack_id", "slice_index",
                    "encoder_name", "encoder_version",
                ],
                set_={"embedding": stmt.excluded.embedding, "dim": stmt.excluded.dim},
            )
        else:
            stmt = pg_insert(StackEmbeddingCache).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    "series_stack_id", "slice_index",
                    "encoder_name", "encoder_version",
                ],
                set_={"embedding": stmt.excluded.embedding, "dim": stmt.excluded.dim},
            )
        db.execute(stmt)
        n_written += len(batch)
    return n_written


def get_or_compute(
    key: CacheKey,
    expected_dim: int,
    compute_fn: Callable[[], np.ndarray],
) -> np.ndarray:
    """Convenience: read from cache, else compute and write."""
    with session_scope() as db:
        cached = read_one(db, key, expected_dim)
    if cached is not None:
        return cached
    arr = compute_fn().astype(np.float32, copy=False)
    if arr.shape != (expected_dim,):
        raise ValueError(
            f"compute_fn produced shape {arr.shape}, expected ({expected_dim},)"
        )
    with session_scope() as db:
        write_many(db, [(key, arr)])
    return arr


def batch_get_or_compute(
    requests: list[tuple[CacheKey, Callable[[], np.ndarray]]],
    expected_dim: int,
) -> dict[CacheKey, np.ndarray]:
    """Bulk variant: returns a dict mapping each input key to its embedding,
    computing only the misses.

    Cache writes are batched into a single transaction at the end.
    """
    out: dict[CacheKey, np.ndarray] = {}
    misses: list[tuple[CacheKey, Callable[[], np.ndarray]]] = []

    keys = [k for k, _ in requests]
    with session_scope() as db:
        cached = read_many(db, keys, expected_dim)
    for key, fn in requests:
        hit = cached.get(key)
        if hit is not None:
            out[key] = hit
        else:
            misses.append((key, fn))

    if misses:
        new_items: list[tuple[CacheKey, np.ndarray]] = []
        for key, fn in misses:
            arr = fn().astype(np.float32, copy=False)
            if arr.shape != (expected_dim,):
                raise ValueError(
                    f"compute_fn produced shape {arr.shape}, "
                    f"expected ({expected_dim},) for {key}"
                )
            out[key] = arr
            new_items.append((key, arr))
        with session_scope() as db:
            write_many(db, new_items)

    return out
