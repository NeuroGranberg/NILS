"""M2 — encoder registry + GPU detection tests (no torch).

We can't load the real BiomedCLIP / SigLIP2 weights in unit tests (torch
is not in the dev venv and the weights are huge). Instead we exercise
the registry contract with deterministic fakes.
"""

from __future__ import annotations

import os

import numpy as np
import pytest

from qc.body_part import encoders as enc_mod
from qc.body_part.encoders import (
    BaseEncoder,
    EncoderInfo,
    EncoderRegistry,
    build_concat_features,
    select_device,
)


class FakeBiomedCLIP(BaseEncoder):
    info = EncoderInfo(
        name="biomedclip", version="v1.0.0", dim_image=4, dim_text=4,
        hf_id="microsoft/test",
    )

    def encode_images(self, images):
        if not images:
            return np.zeros((0, 4), dtype=np.float32)
        out = np.array(
            [[float(arr.mean()), 0.0, 0.0, 1.0] for arr in images],
            dtype=np.float32,
        )
        # L2-normalize like the real encoder.
        norms = np.linalg.norm(out, axis=1, keepdims=True).clip(min=1e-12)
        return out / norms

    def encode_texts(self, prompts):
        out = np.array([[1.0, 0.0, 0.0, 0.0] for _ in prompts], dtype=np.float32)
        return out


class FakeSigLIP2(BaseEncoder):
    info = EncoderInfo(
        name="siglip2", version="v1.0.0", dim_image=2, dim_text=None,
        hf_id="google/test",
    )

    def encode_images(self, images):
        if not images:
            return np.zeros((0, 2), dtype=np.float32)
        out = np.array(
            [[float(arr.std()), 1.0] for arr in images],
            dtype=np.float32,
        )
        norms = np.linalg.norm(out, axis=1, keepdims=True).clip(min=1e-12)
        return out / norms


@pytest.fixture(autouse=True)
def _reset_registry():
    EncoderRegistry.reset()
    yield
    EncoderRegistry.reset()


# ---------------------------------------------------------------------------
# Device selection
# ---------------------------------------------------------------------------


def test_select_device_default(monkeypatch):
    monkeypatch.delenv("BODY_PART_DEVICE", raising=False)
    # No torch installed in dev venv → falls back to "cpu"
    assert select_device() in {"cpu", "cuda", "mps"}


def test_select_device_forced_cpu(monkeypatch):
    monkeypatch.setenv("BODY_PART_DEVICE", "cpu")
    assert select_device() == "cpu"


def test_select_device_forced_cuda_override(monkeypatch):
    monkeypatch.setenv("BODY_PART_DEVICE", "CUDA")
    assert select_device() == "cuda"


# ---------------------------------------------------------------------------
# Registry contract with fakes
# ---------------------------------------------------------------------------


def test_registry_install_fakes_and_get_by_name():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    assert EncoderRegistry.get_by_name("biomedclip").info.dim_image == 4
    assert EncoderRegistry.get_by_name("siglip2").info.dim_image == 2


def test_registry_unknown_name():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    with pytest.raises(ValueError):
        EncoderRegistry.get_by_name("chexnet")


def test_encoders_for_mode_concat():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    encs = EncoderRegistry.encoders_for_mode("concat")
    assert [e.info.name for e in encs] == ["biomedclip", "siglip2"]


def test_encoders_for_mode_single():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    assert [e.info.name for e in EncoderRegistry.encoders_for_mode("biomedclip")] == ["biomedclip"]
    assert [e.info.name for e in EncoderRegistry.encoders_for_mode("siglip2")] == ["siglip2"]


def test_encoders_for_mode_unknown():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    with pytest.raises(ValueError):
        EncoderRegistry.encoders_for_mode("siglip3")


def test_build_concat_features_dimensions():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    images = [
        np.full((224, 224, 3), 100, dtype=np.uint8),
        np.full((224, 224, 3), 200, dtype=np.uint8),
    ]
    feats, infos = build_concat_features(images, mode="concat")
    assert feats.shape == (2, 4 + 2)
    assert [i.name for i in infos] == ["biomedclip", "siglip2"]
    assert feats.dtype == np.float32


def test_build_concat_features_single_mode():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    images = [np.zeros((224, 224, 3), dtype=np.uint8)]
    feats, infos = build_concat_features(images, mode="biomedclip")
    assert feats.shape == (1, 4)
    assert [i.name for i in infos] == ["biomedclip"]


def test_build_concat_features_empty():
    EncoderRegistry.set_for_testing(
        biomedclip=FakeBiomedCLIP(), siglip2=FakeSigLIP2(),
    )
    feats, infos = build_concat_features([], mode="concat")
    # Each encoder still reports its dim; only N=0 rows.
    assert feats.shape == (0, 4 + 2)
    assert [i.name for i in infos] == ["biomedclip", "siglip2"]
