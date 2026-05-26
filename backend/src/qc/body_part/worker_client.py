"""Thin HTTP client for the Body Part QC worker container.

The backend service uses this to delegate compute (embedding, seeding,
training, inference) to ``body-part-qc-worker``. Errors propagate as
:class:`WorkerError`; the route layer maps them to HTTP 502/504.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT_S = 1800.0  # generous — orientation-aware embed adds slices on CPU


class WorkerError(RuntimeError):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"worker error {status}: {body}")
        self.status = status
        self.body = body


def _base_url() -> str:
    return os.environ.get(
        "BODY_PART_WORKER_URL",
        "http://body-part-qc-worker:8000",
    ).rstrip("/")


def _request(
    method: str, path: str, *,
    json_body: Optional[dict[str, Any]] = None,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> dict[str, Any]:
    url = f"{_base_url()}{path}"
    try:
        resp = httpx.request(method, url, json=json_body, timeout=timeout)
    except httpx.HTTPError as exc:
        raise WorkerError(0, f"transport error: {exc}") from exc
    if resp.status_code >= 400:
        raise WorkerError(resp.status_code, resp.text)
    return resp.json()


# ---------------------------------------------------------------------------
# Public client functions — one per worker endpoint
# ---------------------------------------------------------------------------


def health() -> dict[str, Any]:
    return _request("GET", "/health", timeout=10.0)


def embed(
    items: list[dict[str, int]], *, mode: str = "concat",
) -> dict[str, Any]:
    return _request("POST", "/embed", json_body={"items": items, "mode": mode})


def seed(
    category: str,
    items: list[dict[str, int]],
    *,
    n_target: int = 100,
    p_min: Optional[float] = None,
    m_min: Optional[float] = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "category": category,
        "items": items,
        "n_target": int(n_target),
    }
    if p_min is not None:
        body["p_min"] = float(p_min)
    if m_min is not None:
        body["m_min"] = float(m_min)
    return _request("POST", "/seed", json_body=body)


def seed_prior(
    items: list[dict[str, int]],
    *,
    n_target: int = 50,
) -> dict[str, Any]:
    """Call POST /seed-prior on the worker. FPS-only diversity selection
    for keyword-prior candidates (no zero-shot scoring)."""
    body: dict[str, Any] = {
        "items": items,
        "n_target": int(n_target),
    }
    return _request("POST", "/seed-prior", json_body=body)


def train(
    cohort_id: int = 0,
    samples: list[dict[str, Any]] | None = None,
    *,
    mode: str = "concat",
    pca_components: Optional[int] = None,
    use_pca: bool = True,
    logreg_C: Optional[float] = None,
    random_state: Optional[int] = None,
    n_train_slices: int = 1,
    auto_tune: bool = False,
    estimator_kind: str = "logreg",
    num_slices_map: Optional[dict[int, int]] = None,
    label_remap: Optional[dict[str, str]] = None,
    target_classes: Optional[list[str]] = None,
    artifact_name: Optional[str] = None,
) -> dict[str, Any]:
    """Call POST /train on the worker. ``samples`` items must already be
    embedded under ``mode`` (the backend should call :func:`embed`
    first)."""
    body: dict[str, Any] = {
        "cohort_id": int(cohort_id),
        "samples": samples or [],
        "mode": mode,
        "n_train_slices": n_train_slices,
        "auto_tune": auto_tune,
        "estimator_kind": estimator_kind,
        "use_pca": use_pca,
    }
    if pca_components is not None:
        body["pca_components"] = int(pca_components)
    if logreg_C is not None:
        body["logreg_C"] = float(logreg_C)
    if random_state is not None:
        body["random_state"] = int(random_state)
    if num_slices_map is not None:
        body["num_slices_map"] = {str(k): v for k, v in num_slices_map.items()}
    if label_remap:
        body["label_remap"] = label_remap
    if target_classes:
        body["target_classes"] = target_classes
    if artifact_name:
        body["artifact_name"] = artifact_name
    return _request("POST", "/train", json_body=body)


def infer(
    *,
    classifier_path: str,
    classifier_sha256: str,
    classes: list[str],
    encoder_chain: list[dict[str, Any]],
    items: list[dict[str, Any]],
    manual_review_below: Optional[float] = None,
    n_slices: Optional[int] = None,
) -> dict[str, Any]:
    """Call POST /infer on the worker. ``items`` is a list of
    ``{stack_id, num_slices, orientation?}`` dicts; the worker reads
    embeddings from the cache and runs the persisted classifier."""
    body: dict[str, Any] = {
        "classifier_path": classifier_path,
        "classifier_sha256": classifier_sha256,
        "classes": classes,
        "encoder_chain": encoder_chain,
        "items": items,
    }
    if manual_review_below is not None:
        body["manual_review_below"] = float(manual_review_below)
    if n_slices is not None:
        body["n_slices"] = int(n_slices)
    return _request("POST", "/infer", json_body=body)
