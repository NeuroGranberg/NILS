"""Scratch-path configuration for analysis-pipeline runs (frozen design §6.3).

PURE / DB-free. A run materializes its BIDS input + collects its derivatives
under a per-run scratch directory rooted at ``PIPELINE_SCRATCH_ROOT``:

    <PIPELINE_SCRATCH_ROOT>/run-<id>/
        bids/     # materialized BIDS input (db_subset) or symlink target
        output/   # derivatives root the flow writes (results.json + outputs)
        work/     # always-cleaned scratch work dir (needs.work_dir target)

The env var is read each call (no module-level cache) so a test can override
``PIPELINE_SCRATCH_ROOT`` per-case. This mirrors the ``parse_data_roots()`` env
idiom in ``api/server.py``: ``os.getenv`` with a sane default, no settings
object. There is intentionally no content-addressed cache in v1 — every run
materializes fresh (correctness over speed, §5 fallback).
"""

from __future__ import annotations

import os
from pathlib import Path

# Default scratch root when ``PIPELINE_SCRATCH_ROOT`` is unset. A world-writable
# tmp path keeps the foundation runnable without provisioning a data root; a
# real deployment overrides this via env to a path under the NILS data root.
DEFAULT_SCRATCH_ROOT = "/tmp/nils-pipeline-scratch"

_SCRATCH_ENV_VAR = "PIPELINE_SCRATCH_ROOT"


def get_scratch_root() -> Path:
    """Resolve the scratch root from ``PIPELINE_SCRATCH_ROOT`` (env or default).

    Read on every call (not cached) so tests and operators can override it.
    """
    raw = os.getenv(_SCRATCH_ENV_VAR)
    if raw and raw.strip():
        return Path(raw.strip())
    return Path(DEFAULT_SCRATCH_ROOT)


def run_scratch_dir(run_id: int, root: Path | None = None) -> Path:
    """``<root>/run-<id>`` — the per-run scratch directory."""
    base = root if root is not None else get_scratch_root()
    return base / f"run-{run_id}"


def run_bids_dir(run_id: int, root: Path | None = None) -> Path:
    """``<root>/run-<id>/bids`` — materialized BIDS input root."""
    return run_scratch_dir(run_id, root) / "bids"


def run_output_dir(run_id: int, root: Path | None = None) -> Path:
    """``<root>/run-<id>/output`` — derivatives root the flow writes into."""
    return run_scratch_dir(run_id, root) / "output"


def run_work_dir(run_id: int, root: Path | None = None) -> Path:
    """``<root>/run-<id>/work`` — always-cleaned scratch work dir."""
    return run_scratch_dir(run_id, root) / "work"


__all__ = [
    "DEFAULT_SCRATCH_ROOT",
    "get_scratch_root",
    "run_scratch_dir",
    "run_bids_dir",
    "run_output_dir",
    "run_work_dir",
]
