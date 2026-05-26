"""Frozen image/text encoders for Body Part QC.

Two encoders are wired in V1:

- **BiomedCLIP** (``microsoft/BiomedCLIP-PubMedBERT_256-vit_base_patch16_224``):
  used for both image embedding and zero-shot text↔image seeding.
- **SigLIP2** (``google/siglip2-base-patch16-224``): image only.

Both are loaded lazily — importing this module is cheap and does not pull
torch / transformers. ``EncoderRegistry.get(...)`` triggers the actual load
on first call.

Combined modes:

- ``"biomedclip"`` — only BiomedCLIP image embedding (D=512)
- ``"siglip2"``    — only SigLIP2 image embedding (D=768)
- ``"concat"``     — L2-normalized BiomedCLIP ⊕ L2-normalized SigLIP2 (D=1280)

GPU detection runs at first encoder load. ``BODY_PART_DEVICE`` env var can
force ``cuda`` / ``cpu`` / ``mps``; default ``"auto"`` picks the best
available device.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional, Sequence

import numpy as np

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover
    import torch  # noqa: F401


# ---------------------------------------------------------------------------
# Device selection
# ---------------------------------------------------------------------------


def select_device() -> str:
    """Return ``"cuda"``, ``"mps"``, or ``"cpu"`` based on availability and
    the ``BODY_PART_DEVICE`` env var override."""
    forced = (os.environ.get("BODY_PART_DEVICE") or "auto").strip().lower()
    if forced in ("cuda", "cpu", "mps"):
        return forced
    try:
        import torch
    except Exception:  # pragma: no cover — torch missing in unit tests
        return "cpu"
    if torch.cuda.is_available():
        return "cuda"
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def device_info() -> dict:
    """Return ``{device, cuda_device_name, vram_gb}`` for /health endpoint."""
    info = {"device": select_device(), "cuda_device_name": None, "vram_gb": None}
    if info["device"] != "cuda":
        return info
    try:
        import torch
        info["cuda_device_name"] = torch.cuda.get_device_name(0)
        props = torch.cuda.get_device_properties(0)
        info["vram_gb"] = round(props.total_memory / (1024 ** 3), 1)
    except Exception:  # pragma: no cover
        logger.exception("Failed to query CUDA device info.")
    return info


# ---------------------------------------------------------------------------
# Encoder protocol
# ---------------------------------------------------------------------------


@dataclass
class EncoderInfo:
    name: str
    version: str
    dim_image: int
    dim_text: Optional[int] = None
    hf_id: Optional[str] = None


class BaseEncoder:
    """Common protocol every encoder honors."""
    info: EncoderInfo

    def encode_images(self, images: Sequence[np.ndarray]) -> np.ndarray:
        """Encode a batch of (H, W, 3) uint8 images. Return L2-normalized
        float32 array of shape (N, dim_image)."""
        raise NotImplementedError

    def encode_texts(self, prompts: Sequence[str]) -> np.ndarray:
        """Encode a batch of prompts. Return L2-normalized float32 array of
        shape (N, dim_text)."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# BiomedCLIP — image + text
# ---------------------------------------------------------------------------


class BiomedCLIPEncoder(BaseEncoder):
    info = EncoderInfo(
        name="biomedclip",
        version="v1.0.0",
        dim_image=512,
        dim_text=512,
        hf_id="microsoft/BiomedCLIP-PubMedBERT_256-vit_base_patch16_224",
    )

    def __init__(self, device: Optional[str] = None) -> None:
        import open_clip
        import torch

        self.device = device or select_device()
        logger.info("Loading BiomedCLIP on device=%s ...", self.device)
        model, _, preprocess = open_clip.create_model_and_transforms(
            f"hf-hub:{self.info.hf_id}"
        )
        tokenizer = open_clip.get_tokenizer(f"hf-hub:{self.info.hf_id}")
        self.model = model.to(self.device).eval()
        self.preprocess = preprocess
        self.tokenizer = tokenizer
        # Probe true output dim — defensive (HF revisions can shift).
        with torch.no_grad():
            dummy = torch.zeros(1, 3, 224, 224, device=self.device)
            feat = self.model.encode_image(dummy)
            self.info = EncoderInfo(
                **{**self.info.__dict__, "dim_image": int(feat.shape[-1])}
            )

    def encode_images(self, images: Sequence[np.ndarray]) -> np.ndarray:
        import torch
        from PIL import Image
        if not images:
            return np.zeros((0, self.info.dim_image), dtype=np.float32)
        tensors = []
        for arr in images:
            tensors.append(self.preprocess(Image.fromarray(arr)))
        batch = torch.stack(tensors).to(self.device)
        with torch.no_grad():
            feat = self.model.encode_image(batch)
            feat = feat / feat.norm(dim=-1, keepdim=True).clamp(min=1e-12)
        return feat.detach().cpu().to(torch.float32).numpy()

    def encode_texts(self, prompts: Sequence[str]) -> np.ndarray:
        import torch
        if not prompts:
            return np.zeros((0, self.info.dim_text or 0), dtype=np.float32)
        toks = self.tokenizer(list(prompts)).to(self.device)
        with torch.no_grad():
            feat = self.model.encode_text(toks)
            feat = feat / feat.norm(dim=-1, keepdim=True).clamp(min=1e-12)
        return feat.detach().cpu().to(torch.float32).numpy()


# ---------------------------------------------------------------------------
# SigLIP2 — image only (text tower available but not used in V1)
# ---------------------------------------------------------------------------


class SigLIP2Encoder(BaseEncoder):
    info = EncoderInfo(
        name="siglip2",
        version="v1.0.0",
        dim_image=768,
        dim_text=None,
        hf_id="google/siglip2-base-patch16-224",
    )

    def __init__(self, device: Optional[str] = None) -> None:
        import torch
        import transformers

        self.device = device or select_device()
        logger.info("Loading SigLIP2 on device=%s ...", self.device)
        model = transformers.AutoModel.from_pretrained(self.info.hf_id)
        processor = transformers.AutoProcessor.from_pretrained(self.info.hf_id)
        self.model = model.to(self.device).eval()
        self.processor = processor
        with torch.no_grad():
            dummy = torch.zeros(1, 3, 224, 224, device=self.device)
            # transformers >= 5.x returns BaseModelOutputWithPooling here,
            # not a raw tensor — extract the pooled vector explicitly.
            out = self.model.get_image_features(pixel_values=dummy)
            feat = self._extract_image_feat(out)
            self.info = EncoderInfo(
                **{**self.info.__dict__, "dim_image": int(feat.shape[-1])}
            )

    @staticmethod
    def _extract_image_feat(out):
        """Pull the pooled image embedding from a SigLIP2 forward output.

        Older transformers returned a tensor directly; newer versions
        return ``BaseModelOutputWithPooling``. Handle both.
        """
        if hasattr(out, "shape"):
            return out  # raw tensor (older transformers)
        pooled = getattr(out, "pooler_output", None)
        if pooled is not None:
            return pooled
        # Last-resort fallback: CLS token from last_hidden_state.
        return out.last_hidden_state[:, 0, :]

    def encode_images(self, images: Sequence[np.ndarray]) -> np.ndarray:
        import torch
        from PIL import Image
        if not images:
            return np.zeros((0, self.info.dim_image), dtype=np.float32)
        pil_imgs = [Image.fromarray(a) for a in images]
        inputs = self.processor(images=pil_imgs, return_tensors="pt").to(self.device)
        with torch.no_grad():
            out = self.model.get_image_features(**inputs)
            feat = self._extract_image_feat(out)
            feat = feat / feat.norm(dim=-1, keepdim=True).clamp(min=1e-12)
        return feat.detach().cpu().to(torch.float32).numpy()

    def encode_texts(self, prompts: Sequence[str]) -> np.ndarray:
        raise NotImplementedError("SigLIP2 text tower not used in V1.")


# ---------------------------------------------------------------------------
# Registry — lazy singleton encoders
# ---------------------------------------------------------------------------


@dataclass
class _RegistryState:
    biomedclip: Optional[BiomedCLIPEncoder] = None
    siglip2: Optional[SigLIP2Encoder] = None


class EncoderRegistry:
    """Lazy singleton holder.

    The registry is module-global; the worker process holds encoders in
    GPU memory between requests. Tests can install fakes via
    :meth:`set_for_testing`.
    """

    _state: _RegistryState = _RegistryState()

    @classmethod
    def get_biomedclip(cls) -> BaseEncoder:
        if cls._state.biomedclip is None:
            cls._state.biomedclip = BiomedCLIPEncoder()
        return cls._state.biomedclip

    @classmethod
    def get_siglip2(cls) -> BaseEncoder:
        if cls._state.siglip2 is None:
            cls._state.siglip2 = SigLIP2Encoder()
        return cls._state.siglip2

    @classmethod
    def get_by_name(cls, name: str) -> BaseEncoder:
        if name == "biomedclip":
            return cls.get_biomedclip()
        if name == "siglip2":
            return cls.get_siglip2()
        raise ValueError(f"Unknown encoder name: {name!r}")

    @classmethod
    def encoders_for_mode(cls, mode: str) -> list[BaseEncoder]:
        if mode == "biomedclip":
            return [cls.get_biomedclip()]
        if mode == "siglip2":
            return [cls.get_siglip2()]
        if mode == "concat":
            return [cls.get_biomedclip(), cls.get_siglip2()]
        raise ValueError(f"Unknown encoder mode: {mode!r}")

    @classmethod
    def set_for_testing(
        cls,
        *,
        biomedclip: Optional[BaseEncoder] = None,
        siglip2: Optional[BaseEncoder] = None,
    ) -> None:
        cls._state = _RegistryState(biomedclip=biomedclip, siglip2=siglip2)

    @classmethod
    def reset(cls) -> None:
        cls._state = _RegistryState()


def build_concat_features(
    images: Sequence[np.ndarray],
    *,
    mode: str = "concat",
) -> tuple[np.ndarray, list[EncoderInfo]]:
    """Encode ``images`` with the encoders selected by ``mode`` and return
    the concatenation along with the encoder metadata used.

    Each encoder block is L2-normalized independently before concat
    (encoders already L2-normalize their outputs, so this is a no-op,
    but the post-concat vector is **not** unit-normalized — the
    sklearn ``StandardScaler`` in the training pipeline handles that).
    """
    encoders = EncoderRegistry.encoders_for_mode(mode)
    blocks = [enc.encode_images(images) for enc in encoders]
    if not blocks:
        return np.zeros((len(images), 0), dtype=np.float32), []
    return np.concatenate(blocks, axis=1).astype(np.float32), [e.info for e in encoders]
