"""Body Part QC worker — FastAPI app.

Runs in its own container (`Dockerfile.body_part_worker`) and exposes a
small, **internal** HTTP API used by the backend service:

- ``GET  /health``      → device info + which encoders are loaded
- ``POST /embed``       → batch-embed (stack_id, slice_index) requests with cache
- ``POST /seed``        → zero-shot seeding for one category    (M3)
- ``POST /train``       → train the linear probe                (M4)
- ``POST /infer``       → run cohort inference                  (M5)

The worker performs no DB writes for QC results — the backend owns
those. The worker only writes to ``stack_embedding_cache`` (its compute
is the only source of truth for those rows).
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import bindparam, text

from db.session import session_scope
from metadata_db.session import SessionLocal as MetadataSessionLocal

# Import models that participate in SQLAlchemy relationships referenced by
# string from ``cohorts.models.Cohort``. Without these imports, the first
# DB call configures mappers and fails with NameError because the related
# class never made it into SQLAlchemy's class registry.
import nils_dataset_pipeline.models  # noqa: F401  (mapper registration)

from qc.body_part.embedding_cache import (
    CacheKey,
    batch_get_or_compute,
    write_many,
)
from qc.body_part.encoders import (
    EncoderRegistry,
    build_concat_features,
    device_info,
    select_device,
)
from qc.body_part.preprocess import (
    DEFAULT_PREPROCESS,
    central_slice_indices,
    orientation_slice_indices,
    load_slice_array_from_dicom,
    preprocess_slice,
)
from qc.body_part.prompts import prompts_for_category
from qc.body_part.seed import (
    DEFAULT_SEED_CONFIG,
    SeedConfig,
    select_seed_candidates,
)
from qc.body_part.infer import (
    DEFAULT_INFER_CONFIG,
    ClassifierLoader,
    InferConfig,
    StackInferenceRequest,
    run_inference,
)
from qc.body_part.train import (
    DEFAULT_TRAIN_CONFIG,
    EncoderSpec,
    TrainConfig,
    TrainingSample,
    train_classifier,
)
from qc.dicom_service import _cached_resolve_file_path  # path resolver

logger = logging.getLogger(__name__)


app = FastAPI(title="Body Part QC Worker", version="1.0.0")


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    ok: bool = True
    device: str
    cuda_device_name: Optional[str] = None
    vram_gb: Optional[float] = None
    encoders_loaded: list[str] = []
    preprocess_version: str


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    info = device_info()
    loaded: list[str] = []
    state = EncoderRegistry._state
    if state.biomedclip is not None:
        loaded.append("biomedclip")
    if state.siglip2 is not None:
        loaded.append("siglip2")
    return HealthResponse(
        device=info["device"],
        cuda_device_name=info["cuda_device_name"],
        vram_gb=info["vram_gb"],
        encoders_loaded=loaded,
        preprocess_version=DEFAULT_PREPROCESS.version,
    )


# ---------------------------------------------------------------------------
# /embed
# ---------------------------------------------------------------------------


class EmbedRequestItem(BaseModel):
    """One slice to embed. ``slice_index`` is the position within the
    stack ordered by slice_location (NOT instance_id)."""
    stack_id: int
    slice_index: int


class EmbedPayload(BaseModel):
    """Body of POST /embed."""
    items: list[EmbedRequestItem]
    mode: str = "concat"  # 'biomedclip' | 'siglip2' | 'concat'


class EmbedResultItem(BaseModel):
    stack_id: int
    slice_index: int
    encoder_name: str
    encoder_version: str
    dim: int


class EmbedResponse(BaseModel):
    encoder_chain: list[str]   # e.g. ["biomedclip", "siglip2"] for concat
    dim_total: int
    n_items: int
    n_cached: int
    n_computed: int
    cached_keys: list[EmbedResultItem]   # everything that ended up in cache


@app.post("/embed", response_model=EmbedResponse)
def embed(payload: EmbedPayload) -> EmbedResponse:
    """Embed slices for the requested mode, populating the cache.

    Single-encoder modes write the per-encoder cache directly.
    ``concat`` mode runs each encoder independently and writes both
    per-encoder rows; the actual concatenation happens at consumer time
    (training / inference) by reading both rows back.
    """
    if not payload.items:
        return EmbedResponse(
            encoder_chain=[],
            dim_total=0, n_items=0, n_cached=0, n_computed=0,
            cached_keys=[],
        )
    try:
        encoders = EncoderRegistry.encoders_for_mode(payload.mode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Resolve dicom file paths for all (stack_id, slice_index) pairs.
    items_with_path = _resolve_paths(payload.items)
    if not items_with_path:
        raise HTTPException(
            status_code=404,
            detail="None of the requested (stack_id, slice_index) pairs were resolvable.",
        )

    n_cached = 0
    n_computed = 0
    cached_keys: list[EmbedResultItem] = []
    encoder_chain: list[str] = []
    dim_total = 0

    for enc in encoders:
        encoder_chain.append(enc.info.name)
        dim_total += enc.info.dim_image

        # Check cache, identify misses, compute misses, then write.
        keys = [
            CacheKey(
                series_stack_id=it.stack_id,
                slice_index=it.slice_index,
                encoder_name=enc.info.name,
                encoder_version=enc.info.version,
            )
            for it in items_with_path
        ]

        with session_scope() as db:
            from qc.body_part.embedding_cache import read_many
            cached_map: dict[CacheKey, np.ndarray] = read_many(
                db, keys, enc.info.dim_image,
            )

        misses = [
            (key, items_with_path[i])
            for i, key in enumerate(keys)
            if key not in cached_map
        ]
        if misses:
            slice_imgs = _parallel_load_and_preprocess(
                [it for _, it in misses]
            )
            valid_pairs = [
                (key, img) for ((key, _), img) in zip(misses, slice_imgs)
                if img is not None
            ]
            if valid_pairs:
                batch_imgs = [img for _, img in valid_pairs]
                # Chunk encoder input to bound peak GPU memory. Without
                # this, a 15k-image cohort can balloon a single forward
                # pass to >20 GiB of activations and OOM on a 48 GB
                # card. Tunable via env var.
                gpu_chunk = int(
                    os.environ.get("BODY_PART_ENCODER_BATCH", "256") or "256",
                )
                if gpu_chunk <= 0:
                    gpu_chunk = len(batch_imgs)
                feats_chunks: list[np.ndarray] = []
                for off in range(0, len(batch_imgs), gpu_chunk):
                    feats_chunks.append(
                        enc.encode_images(batch_imgs[off:off + gpu_chunk])
                    )
                feats = (
                    feats_chunks[0]
                    if len(feats_chunks) == 1
                    else np.concatenate(feats_chunks, axis=0)
                )
                new_items = list(zip([k for k, _ in valid_pairs], feats))
                with session_scope() as db:
                    write_many(db, new_items)
                n_computed += len(new_items)

        n_cached += len(cached_map)
        for key in keys:
            cached_keys.append(
                EmbedResultItem(
                    stack_id=key.series_stack_id,
                    slice_index=key.slice_index,
                    encoder_name=key.encoder_name,
                    encoder_version=key.encoder_version,
                    dim=enc.info.dim_image,
                )
            )

    return EmbedResponse(
        encoder_chain=encoder_chain,
        dim_total=dim_total,
        n_items=len(items_with_path),
        n_cached=n_cached,
        n_computed=n_computed,
        cached_keys=cached_keys,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _ResolvedItem:
    __slots__ = ("stack_id", "slice_index", "instance_id", "file_path", "frame_index")

    def __init__(
        self, stack_id: int, slice_index: int, instance_id: int,
        file_path: str, frame_index: int,
    ):
        self.stack_id = stack_id
        self.slice_index = slice_index
        self.instance_id = instance_id
        self.file_path = file_path
        self.frame_index = frame_index


def _resolve_paths(items: list[EmbedRequestItem]) -> list[_ResolvedItem]:
    """Resolve (stack_id, slice_index) → (instance_id, absolute file path,
    frame_index) using the metadata DB. Multi-frame DICOM expands frames
    in slice-order via :func:`get_series_frame_list`-style logic.

    Items that cannot be resolved (missing DICOM, deleted files, etc.) are
    silently dropped — the response reports the count.

    Performance note: previously this issued one JOIN-of-5 query per
    stack_id, which made large cohorts (~5k stacks) take ~5 minutes.
    We now fetch all stacks in a single ``IN`` query and group rows in
    Python.
    """
    if not items:
        return []
    by_stack: dict[int, list[EmbedRequestItem]] = {}
    for it in items:
        by_stack.setdefault(it.stack_id, []).append(it)

    stack_ids = list(by_stack.keys())
    out: list[_ResolvedItem] = []

    # Chunk the IN clause to avoid statement timeout on large cohorts.
    _RESOLVE_CHUNK = 2000
    rows_by_stack: dict[int, list] = {sid: [] for sid in stack_ids}
    with MetadataSessionLocal() as meta_db:
        stmt = text(
            """
            SELECT ss.series_stack_id AS stack_id,
                   i.instance_id,
                   COALESCE(i.number_of_frames, 1) AS num_frames,
                   i.dicom_file_path,
                   c.path AS cohort_path
            FROM instance i
            JOIN series_stack ss ON i.series_stack_id = ss.series_stack_id
            JOIN series s ON i.series_instance_uid = s.series_instance_uid
            LEFT JOIN series_classification_cache scc
                ON s.series_instance_uid = scc.series_instance_uid
            LEFT JOIN cohort c ON LOWER(c.name) = LOWER(scc.dicom_origin_cohort)
            WHERE ss.series_stack_id IN :sids
            ORDER BY ss.series_stack_id,
                     COALESCE(i.slice_location, i.instance_number, 0) ASC,
                     i.instance_number ASC
            """
        ).bindparams(bindparam("sids", expanding=True))
        for off in range(0, len(stack_ids), _RESOLVE_CHUNK):
            chunk = stack_ids[off : off + _RESOLVE_CHUNK]
            rows = meta_db.execute(stmt, {"sids": chunk}).fetchall()
            for r in rows:
                rows_by_stack.setdefault(int(r.stack_id), []).append(r)

    for stack_id, reqs in by_stack.items():
        expanded: list[tuple[int, int, str, Optional[str]]] = []
        for r in rows_by_stack.get(stack_id, []):
            num_frames = int(r.num_frames or 1)
            for f in range(num_frames):
                expanded.append((
                    int(r.instance_id), f,
                    r.dicom_file_path, r.cohort_path,
                ))
        for req in reqs:
            if req.slice_index < 0 or req.slice_index >= len(expanded):
                continue
            inst_id, frame, dpath, cpath = expanded[req.slice_index]
            resolved = _cached_resolve_file_path(dpath or "", cpath)
            if not resolved:
                continue
            out.append(_ResolvedItem(
                stack_id=stack_id,
                slice_index=req.slice_index,
                instance_id=inst_id,
                file_path=resolved,
                frame_index=frame,
            ))
    return out


def _load_and_preprocess(item: _ResolvedItem) -> Optional[np.ndarray]:
    res = load_slice_array_from_dicom(
        item.file_path, frame_index=item.frame_index,
    )
    if res is None:
        return None
    arr, slope, intercept = res
    return preprocess_slice(arr, rescale_slope=slope, rescale_intercept=intercept)


def _parallel_load_and_preprocess(
    items: list[_ResolvedItem],
) -> list[Optional[np.ndarray]]:
    """Parallelise the per-DICOM read+decode+preprocess pipeline.

    DICOM read is I/O-bound (ZFS / NFS) and ``pixel_array`` decompression
    releases the GIL via NumPy/ pylibjpeg, so threads scale almost
    linearly with disk bandwidth. We preserve input order so callers can
    still ``zip`` results with the original list.

    Tunable via env ``BODY_PART_DICOM_THREADS`` (default 16). Set to 1
    to disable parallelism (useful for debugging).
    """
    if not items:
        return []
    n_workers = max(1, int(os.environ.get("BODY_PART_DICOM_THREADS", "16")))
    if n_workers == 1 or len(items) == 1:
        return [_load_and_preprocess(it) for it in items]
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        return list(pool.map(_load_and_preprocess, items))


# ---------------------------------------------------------------------------
# /seed — zero-shot seeding for one category
# ---------------------------------------------------------------------------


class SeedItem(BaseModel):
    """One slice in the candidate pool offered to the seeder."""
    stack_id: int
    slice_index: int


class SeedPayload(BaseModel):
    """Body of POST /seed."""
    category: str
    items: list[SeedItem]
    n_target: int = 100
    p_min: Optional[float] = None
    m_min: Optional[float] = None


class SeedCandidateResponse(BaseModel):
    stack_id: int
    slice_index: int
    zs_prob: float
    margin: float


class SeedResponse(BaseModel):
    category: str
    n_input: int
    n_kept: int
    n_picked: int
    encoder_name: str
    encoder_version: str
    candidates: list[SeedCandidateResponse]


@app.post("/seed", response_model=SeedResponse)
def seed(payload: SeedPayload) -> SeedResponse:
    """Run zero-shot text↔image seeding for ``category`` over ``items``.

    Embeddings are produced (or fetched from cache) using BiomedCLIP only
    — SigLIP2 has no usable text tower in our setup. The candidate slices
    must already be enumerated by the caller (the backend service); this
    endpoint is purely the compute step.
    """
    if not payload.items:
        return SeedResponse(
            category=payload.category,
            n_input=0, n_kept=0, n_picked=0,
            encoder_name="biomedclip", encoder_version="",
            candidates=[],
        )

    # 1) Make sure we have BiomedCLIP image embeddings cached for everything.
    bc = EncoderRegistry.get_biomedclip()

    items_with_path = _resolve_paths(
        [EmbedRequestItem(stack_id=it.stack_id, slice_index=it.slice_index)
         for it in payload.items]
    )
    if not items_with_path:
        raise HTTPException(
            status_code=404,
            detail="None of the requested (stack_id, slice_index) pairs were resolvable.",
        )

    keys = [
        CacheKey(
            series_stack_id=it.stack_id,
            slice_index=it.slice_index,
            encoder_name=bc.info.name,
            encoder_version=bc.info.version,
        )
        for it in items_with_path
    ]

    # Cache hits + misses
    with session_scope() as db:
        from qc.body_part.embedding_cache import read_many
        cached_map: dict[CacheKey, np.ndarray] = read_many(
            db, keys, bc.info.dim_image,
        )

    # Compute misses
    miss_pairs: list[tuple[CacheKey, _ResolvedItem]] = [
        (k, items_with_path[i]) for i, k in enumerate(keys) if k not in cached_map
    ]
    if miss_pairs:
        slice_imgs = _parallel_load_and_preprocess(
            [it for _, it in miss_pairs]
        )
        valid = [
            (k, img) for ((k, _), img) in zip(miss_pairs, slice_imgs)
            if img is not None
        ]
        if valid:
            feats = bc.encode_images([img for _, img in valid])
            with session_scope() as db:
                write_many(db, list(zip([k for k, _ in valid], feats)))
            for (k, _), f in zip(valid, feats):
                cached_map[k] = f

    # Assemble matrix in the original input order.
    embedding_rows: list[np.ndarray] = []
    kept_resolved: list[_ResolvedItem] = []
    for it, key in zip(items_with_path, keys):
        emb = cached_map.get(key)
        if emb is not None:
            embedding_rows.append(emb)
            kept_resolved.append(it)

    if not embedding_rows:
        return SeedResponse(
            category=payload.category,
            n_input=len(payload.items),
            n_kept=0, n_picked=0,
            encoder_name=bc.info.name, encoder_version=bc.info.version,
            candidates=[],
        )
    image_emb = np.stack(embedding_rows, axis=0)

    # 2) Encode prompts → average positives, keep negatives separate.
    pack = prompts_for_category(payload.category)
    pos_emb = bc.encode_texts(list(pack.positive))   # (P, D)
    neg_emb = bc.encode_texts(list(pack.negative))   # (M, D)
    pos_avg = pos_emb.mean(axis=0)
    pos_avg = pos_avg / max(np.linalg.norm(pos_avg), 1e-12)

    # 3) Score + select.
    cfg = SeedConfig(
        n_target=int(payload.n_target),
        p_min=DEFAULT_SEED_CONFIG.p_min if payload.p_min is None else float(payload.p_min),
        m_min=DEFAULT_SEED_CONFIG.m_min if payload.m_min is None else float(payload.m_min),
    )
    picks = select_seed_candidates(
        image_emb, pos_avg, neg_emb, config=cfg,
    )

    out = []
    for c in picks:
        ri = kept_resolved[c.index]
        out.append(SeedCandidateResponse(
            stack_id=ri.stack_id,
            slice_index=ri.slice_index,
            zs_prob=c.zs_prob,
            margin=c.margin,
        ))

    return SeedResponse(
        category=payload.category,
        n_input=len(payload.items),
        n_kept=int(image_emb.shape[0]),
        n_picked=len(out),
        encoder_name=bc.info.name,
        encoder_version=bc.info.version,
        candidates=out,
    )


# ---------------------------------------------------------------------------
# /seed-prior — diversity-only selection for keyword-prior candidates
# ---------------------------------------------------------------------------


class SeedPriorPayload(BaseModel):
    """Body of POST /seed-prior. No zero-shot scoring — labels come
    from the keyword detector. Only FPS diversity selection."""
    items: list[SeedItem]
    n_target: int = 50


class SeedPriorCandidateResponse(BaseModel):
    stack_id: int
    slice_index: int


class SeedPriorResponse(BaseModel):
    n_input: int
    n_kept: int
    n_picked: int
    candidates: list[SeedPriorCandidateResponse]


@app.post("/seed-prior", response_model=SeedPriorResponse)
def seed_prior(payload: SeedPriorPayload) -> SeedPriorResponse:
    """Select diverse keyword-prior candidates via BiomedCLIP FPS.

    The candidates already have trusted keyword-based labels so we skip
    zero-shot scoring and only run farthest-point sampling for diversity.
    """
    if not payload.items:
        return SeedPriorResponse(
            n_input=0, n_kept=0, n_picked=0, candidates=[],
        )

    bc = EncoderRegistry.get_biomedclip()

    items_with_path = _resolve_paths(
        [EmbedRequestItem(stack_id=it.stack_id, slice_index=it.slice_index)
         for it in payload.items]
    )
    if not items_with_path:
        raise HTTPException(
            status_code=404,
            detail="None of the requested (stack_id, slice_index) pairs were resolvable.",
        )

    keys = [
        CacheKey(
            series_stack_id=it.stack_id,
            slice_index=it.slice_index,
            encoder_name=bc.info.name,
            encoder_version=bc.info.version,
        )
        for it in items_with_path
    ]

    with session_scope() as db:
        from qc.body_part.embedding_cache import read_many
        cached_map: dict[CacheKey, np.ndarray] = read_many(
            db, keys, bc.info.dim_image,
        )

    # Compute misses.
    miss_pairs: list[tuple[CacheKey, _ResolvedItem]] = [
        (k, items_with_path[i]) for i, k in enumerate(keys) if k not in cached_map
    ]
    if miss_pairs:
        slice_imgs = _parallel_load_and_preprocess(
            [it for _, it in miss_pairs]
        )
        valid = [
            (k, img) for ((k, _), img) in zip(miss_pairs, slice_imgs)
            if img is not None
        ]
        if valid:
            feats = bc.encode_images([img for _, img in valid])
            with session_scope() as db:
                write_many(db, list(zip([k for k, _ in valid], feats)))
            for (k, _), f in zip(valid, feats):
                cached_map[k] = f

    # Assemble matrix in original input order.
    embedding_rows: list[np.ndarray] = []
    kept_resolved: list[_ResolvedItem] = []
    for it, key in zip(items_with_path, keys):
        emb = cached_map.get(key)
        if emb is not None:
            embedding_rows.append(emb)
            kept_resolved.append(it)

    if not embedding_rows:
        return SeedPriorResponse(
            n_input=len(payload.items), n_kept=0, n_picked=0,
            candidates=[],
        )
    image_emb = np.stack(embedding_rows, axis=0)

    # FPS diversity selection — no scoring.
    from qc.body_part.seed import select_prior_candidates
    picks = select_prior_candidates(image_emb, payload.n_target)

    out = [
        SeedPriorCandidateResponse(
            stack_id=kept_resolved[p.index].stack_id,
            slice_index=kept_resolved[p.index].slice_index,
        )
        for p in picks
    ]

    return SeedPriorResponse(
        n_input=len(payload.items),
        n_kept=int(image_emb.shape[0]),
        n_picked=len(out),
        candidates=out,
    )


# ---------------------------------------------------------------------------
# /train — fit linear-probe classifier on approved samples
# ---------------------------------------------------------------------------


class TrainSampleItem(BaseModel):
    stack_id: int
    slice_index: int
    label: str


class TrainPayload(BaseModel):
    cohort_id: int = 0
    samples: list[TrainSampleItem]
    mode: str = "concat"           # 'biomedclip' | 'siglip2' | 'concat'
    pca_components: Optional[int] = None   # None → skip PCA (or auto-tune picks)
    logreg_C: Optional[float] = None
    random_state: Optional[int] = None
    n_train_slices: int = 1        # average N central slices per stack
    auto_tune: bool = False        # CV sweep over PCA/C
    estimator_kind: str = "logreg" # "logreg" | "rf" | "svm"
    use_pca: bool = True           # False → skip PCA entirely
    num_slices_map: Optional[dict[int, int]] = None  # stack_id → total slices
    label_remap: Optional[dict[str, str]] = None     # fold labels at train time
    target_classes: Optional[list[str]] = None        # filter to these classes
    artifact_name: Optional[str] = None               # custom .joblib filename


class TrainResponse(BaseModel):
    classifier_path: str
    classifier_sha256: str
    classifier_meta: dict


# Where the worker writes joblib artefacts. Mounted as a shared volume
# in docker-compose so the backend can read them at Apply time.
ARTEFACTS_DIR = os.environ.get(
    "BODY_PART_ARTEFACTS_DIR", "/app/resource/qc_models/body_part",
)


@app.post("/train", response_model=TrainResponse)
def train(payload: TrainPayload) -> TrainResponse:
    """Train the linear probe.

    Pre-condition: every (stack_id, slice_index) in ``payload.samples``
    has been embedded under the requested ``mode`` (i.e. cache rows
    exist for every encoder in the chain). The route enforces this by
    returning a structured 409 if any embeddings are missing.
    """
    if not payload.samples:
        raise HTTPException(status_code=400, detail="samples is empty")

    try:
        encoders = EncoderRegistry.encoders_for_mode(payload.mode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    encoder_chain = [
        EncoderSpec(
            name=e.info.name,
            version=e.info.version,
            dim=int(e.info.dim_image),
        )
        for e in encoders
    ]

    samples = [
        TrainingSample(
            stack_id=s.stack_id,
            slice_index=s.slice_index,
            label=s.label,
        )
        for s in payload.samples
    ]

    # Determine PCA: use_pca=False → None; explicit value → use it; else default
    if not payload.use_pca:
        pca_comp: Optional[int] = None
    elif payload.pca_components is not None:
        pca_comp = payload.pca_components
    else:
        pca_comp = DEFAULT_TRAIN_CONFIG.pca_components

    est_kind = payload.estimator_kind
    if est_kind not in ("logreg", "rf", "svm"):
        raise HTTPException(status_code=400, detail=f"Unknown estimator_kind: {est_kind!r}")

    cfg = TrainConfig(
        pca_components=pca_comp,
        pca_floor=DEFAULT_TRAIN_CONFIG.pca_floor,
        logreg_C=payload.logreg_C if payload.logreg_C is not None else DEFAULT_TRAIN_CONFIG.logreg_C,
        random_state=payload.random_state if payload.random_state is not None else DEFAULT_TRAIN_CONFIG.random_state,
        n_train_slices=payload.n_train_slices,
        auto_tune=payload.auto_tune,
        estimator_kind=est_kind,
    )

    try:
        with session_scope() as db:
            result = train_classifier(
                db=db,
                cohort_id=payload.cohort_id,
                samples=samples,
                encoder_chain=encoder_chain,
                artefacts_dir=ARTEFACTS_DIR,
                config=cfg,
                num_slices_map=payload.num_slices_map,
                label_remap=payload.label_remap,
                target_classes=payload.target_classes,
                artifact_name=payload.artifact_name,
            )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:  # pragma: no cover
        logger.exception("train_classifier failed")
        raise HTTPException(status_code=500, detail=str(e))

    return TrainResponse(
        classifier_path=result.classifier_path,
        classifier_sha256=result.classifier_sha256,
        classifier_meta=result.classifier_meta,
    )


# ---------------------------------------------------------------------------
# /infer — predict body part for cohort stacks
# ---------------------------------------------------------------------------


class InferStackItem(BaseModel):
    stack_id: int
    num_slices: int
    orientation: Optional[str] = None


class InferEncoderSpec(BaseModel):
    name: str
    version: str
    dim: int


class InferPayload(BaseModel):
    classifier_path: str
    classifier_sha256: str
    classes: list[str]
    encoder_chain: list[InferEncoderSpec]
    items: list[InferStackItem]
    manual_review_below: Optional[float] = None
    n_slices: Optional[int] = None


class InferResultItem(BaseModel):
    stack_id: int
    label: Optional[str] = None
    confidence: float
    probs: dict[str, float]
    needs_check: bool
    n_slices_used: int
    reasoning: Optional[dict] = None


class InferResponse(BaseModel):
    n_items: int
    n_predicted: int
    n_low_conf: int
    n_no_embeddings: int
    results: list[InferResultItem]


@app.post("/infer", response_model=InferResponse)
def infer(payload: InferPayload) -> InferResponse:
    """Run cohort inference. Pre-condition: every stack's central
    slices must already be embedded for the same encoder chain that
    trained ``classifier_path``."""
    if not payload.items:
        return InferResponse(
            n_items=0, n_predicted=0, n_low_conf=0,
            n_no_embeddings=0, results=[],
        )

    encoder_chain = [
        EncoderSpec(name=e.name, version=e.version, dim=e.dim)
        for e in payload.encoder_chain
    ]
    cfg = InferConfig(
        n_slices=payload.n_slices or DEFAULT_INFER_CONFIG.n_slices,
        manual_review_below=(
            payload.manual_review_below
            if payload.manual_review_below is not None
            else DEFAULT_INFER_CONFIG.manual_review_below
        ),
    )
    items = [
        StackInferenceRequest(
            stack_id=i.stack_id,
            num_slices=i.num_slices,
            orientation=i.orientation,
        )
        for i in payload.items
    ]

    try:
        with session_scope() as db:
            preds = run_inference(
                db=db,
                classifier_path=payload.classifier_path,
                classifier_sha256=payload.classifier_sha256,
                classes=list(payload.classes),
                encoder_chain=encoder_chain,
                items=items,
                config=cfg,
            )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:  # pragma: no cover
        logger.exception("inference failed")
        raise HTTPException(status_code=500, detail=str(e))

    n_low_conf = sum(1 for p in preds if p.needs_check and p.label is not None)
    n_no_emb = sum(1 for p in preds if p.n_slices_used == 0)
    return InferResponse(
        n_items=len(items),
        n_predicted=sum(1 for p in preds if p.label is not None),
        n_low_conf=n_low_conf,
        n_no_embeddings=n_no_emb,
        results=[
            InferResultItem(
                stack_id=p.stack_id,
                label=p.label,
                confidence=p.confidence,
                probs=p.probs,
                needs_check=p.needs_check,
                n_slices_used=p.n_slices_used,
                reasoning=p.reasoning,
            )
            for p in preds
        ],
    )
