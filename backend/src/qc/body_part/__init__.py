"""Cohort-wide Body Part QC module (V1).

Public entry points:

- :class:`CohortBodyPartQCService` (in ``service.py``): orchestration class
  used by the API router. Handles state retrieval, category/sample edits,
  classifier training, cohort-wide inference (Apply), single-level undo
  (Restore Previous), per-stack overrides, and the diff-vs-prior view.
- :func:`get_qc_protected_stack_ids` (in ``protection.py``): used by the
  Sort step3 pipeline to skip overwriting ``body_part`` for stacks that
  this module has already labeled.

Heavy compute (encoder inference, training, embedding) lives in the
``body-part-qc-worker`` Docker service; the backend service calls it
over HTTP. See ``Dockerfile.body_part_worker`` and ``server.py``.

The full design contract is captured in the spec at
``/home/nima/.factory/specs/2026-04-26-body-part-qc-cohort-module-v1.md``.
"""

from qc.body_part.protection import get_qc_protected_stack_ids
from qc.body_part.service import CohortBodyPartQCService

__all__ = [
    "CohortBodyPartQCService",
    "get_qc_protected_stack_ids",
]
