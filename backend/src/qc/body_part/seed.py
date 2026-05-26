"""Zero-shot sample seeding for Body Part QC.

Pipeline (per category, given a candidate set of stack/slice embeddings):

1. **Score** each slice with the BiomedCLIP **text–image margin**
   ``zs_prob - max(other_zs_probs)``, where ``zs_prob`` is the softmax
   over (positive_prompts ⊕ negative_prompts) attributed to the
   positive class. Margin > 0 ⇒ confidently this category.

2. **Confidence gate**: keep slices with ``zs_prob ≥ p_min`` AND
   ``margin ≥ m_min`` (defaults 0.55 / 0.05). Falls back gracefully
   when too few survive.

3. **Diversity pick**: select up to ``n_target`` slices using a
   light-weight k-medoids++ initialization (farthest-point sampling on
   cosine distance over the BiomedCLIP image embeddings). This avoids
   100 near-duplicate central slices.

The seeder takes a callable that produces ``(image_embedding,
text_pos_embedding, text_neg_embedding)`` and is therefore easy to
unit-test with fakes — no torch dependency at import time.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tuning knobs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeedConfig:
    n_target: int = 100
    p_min: float = 0.55          # min positive softmax probability
    m_min: float = 0.05          # min margin (zs_prob - second-best)
    fallback_p_min: float = 0.40  # if not enough survive p_min/m_min
    fallback_m_min: float = 0.0
    softmax_temperature: float = 100.0  # CLIP-style logits scaling
    diversity_seed: int = 1234   # deterministic fps seed


DEFAULT_SEED_CONFIG = SeedConfig()


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _l2_normalize_rows(x: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(x, axis=1, keepdims=True).clip(min=1e-12)
    return x / n


def score_zero_shot(
    image_embeddings: np.ndarray,        # (N, D)
    pos_text_embedding: np.ndarray,      # (D,) — averaged positive prompts
    neg_text_embeddings: np.ndarray,     # (M, D)
    *,
    temperature: float = DEFAULT_SEED_CONFIG.softmax_temperature,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(zs_prob, margin)`` arrays of shape ``(N,)``.

    ``zs_prob`` is the softmax probability that an image belongs to the
    positive class given (1 positive prompt vector ⊕ M negative prompt
    vectors). ``margin`` is ``zs_prob - max(other_zs_probs)`` — for the
    binary positive vs union(negatives) decomposition this collapses to
    ``2*zs_prob - 1`` minus a small correction; we compute it from the
    full distribution to be robust if temperature scaling shifts which
    negative prompt dominates.
    """
    if image_embeddings.size == 0:
        return np.zeros((0,), dtype=np.float32), np.zeros((0,), dtype=np.float32)

    img = _l2_normalize_rows(image_embeddings.astype(np.float32))
    pos = pos_text_embedding.astype(np.float32)
    pos = pos / max(np.linalg.norm(pos), 1e-12)
    neg = _l2_normalize_rows(neg_text_embeddings.astype(np.float32))

    text = np.vstack([pos[None, :], neg])  # (1+M, D)
    logits = (img @ text.T) * float(temperature)  # (N, 1+M)

    # Numerically stable softmax along axis=1
    logits -= logits.max(axis=1, keepdims=True)
    exp = np.exp(logits)
    probs = exp / exp.sum(axis=1, keepdims=True)

    zs_prob = probs[:, 0].astype(np.float32)
    if probs.shape[1] == 1:
        margin = zs_prob.copy()
    else:
        # Margin against the strongest non-positive prompt.
        next_best = probs[:, 1:].max(axis=1).astype(np.float32)
        margin = zs_prob - next_best
    return zs_prob, margin


# ---------------------------------------------------------------------------
# Diversity selection (farthest-point on cosine distance)
# ---------------------------------------------------------------------------


def farthest_point_indices(
    embeddings: np.ndarray, n_target: int, *, seed: int = 0,
) -> list[int]:
    """Return up to ``n_target`` row indices selected by farthest-point
    sampling under cosine distance.

    Embeddings are L2-normalised; the first pick is deterministic
    (``seed % N``) and each subsequent pick maximises the minimum cosine
    distance to all previously picked rows. This is k-medoids++
    initialisation — empirically a strong baseline for small budgets and
    much cheaper than full k-medoids/PAM.

    Returns indices into ``embeddings`` in pick order.
    """
    n = embeddings.shape[0]
    if n == 0 or n_target <= 0:
        return []
    n_target = min(n_target, n)

    x = _l2_normalize_rows(embeddings.astype(np.float32))
    rng = np.random.default_rng(seed)
    first = int(rng.integers(0, n))
    picked = [first]
    # Cosine distance = 1 - dot for unit vectors.
    min_dist = 1.0 - x @ x[first]
    while len(picked) < n_target:
        nxt = int(np.argmax(min_dist))
        if min_dist[nxt] <= 0.0:
            # All remaining points are duplicates of an already-picked one.
            break
        picked.append(nxt)
        new_dist = 1.0 - x @ x[nxt]
        min_dist = np.minimum(min_dist, new_dist)
    return picked


# ---------------------------------------------------------------------------
# Top-level seed pipeline
# ---------------------------------------------------------------------------


@dataclass
class SeedCandidate:
    """One zero-shot candidate before frontend hydration."""
    index: int          # row in the input embedding matrix
    zs_prob: float
    margin: float


def select_seed_candidates(
    image_embeddings: np.ndarray,
    pos_text_embedding: np.ndarray,
    neg_text_embeddings: np.ndarray,
    *,
    config: SeedConfig = DEFAULT_SEED_CONFIG,
) -> list[SeedCandidate]:
    """Run the full zero-shot seeding pipeline.

    Args:
        image_embeddings: (N, D) — one row per candidate slice.
        pos_text_embedding: (D,) — averaged positive prompt embedding.
        neg_text_embeddings: (M, D) — one row per negative prompt.
        config: tuning knobs.

    Returns:
        Up to ``config.n_target`` candidates sorted in pick order
        (diversity-first; not by score).
    """
    n = image_embeddings.shape[0]
    if n == 0 or config.n_target <= 0:
        return []

    zs, margin = score_zero_shot(
        image_embeddings,
        pos_text_embedding,
        neg_text_embeddings,
        temperature=config.softmax_temperature,
    )

    # Primary gate.
    keep = (zs >= config.p_min) & (margin >= config.m_min)
    if int(keep.sum()) < min(20, config.n_target):
        # Relax gates if the gate left us too little material.
        keep = (zs >= config.fallback_p_min) & (margin >= config.fallback_m_min)
    if int(keep.sum()) == 0:
        return []

    kept_idx = np.where(keep)[0]
    kept_emb = image_embeddings[kept_idx]

    # Diversity-aware selection.
    pick_local = farthest_point_indices(
        kept_emb, config.n_target, seed=config.diversity_seed,
    )

    out: list[SeedCandidate] = []
    for li in pick_local:
        gi = int(kept_idx[li])
        out.append(SeedCandidate(
            index=gi,
            zs_prob=float(zs[gi]),
            margin=float(margin[gi]),
        ))
    return out


# ---------------------------------------------------------------------------
# Prior-candidate diversity selection (no zero-shot scoring)
# ---------------------------------------------------------------------------


@dataclass
class PriorCandidate:
    """One keyword-prior candidate selected via diversity FPS."""
    index: int  # row in the input embedding matrix


def select_prior_candidates(
    image_embeddings: np.ndarray,
    n_target: int,
    *,
    seed: int = DEFAULT_SEED_CONFIG.diversity_seed,
) -> list[PriorCandidate]:
    """Select diverse keyword-prior candidates via farthest-point sampling.

    These candidates already have a trusted keyword-based label so we
    skip zero-shot scoring entirely and just pick a diverse set.

    Args:
        image_embeddings: (N, D) — one row per prior candidate slice.
        n_target: Maximum number of candidates to select.
        seed: FPS seed for deterministic selection.

    Returns:
        Up to ``n_target`` candidates in FPS pick order.
    """
    n = image_embeddings.shape[0]
    if n == 0 or n_target <= 0:
        return []

    picked = farthest_point_indices(image_embeddings, n_target, seed=seed)
    return [PriorCandidate(index=i) for i in picked]
