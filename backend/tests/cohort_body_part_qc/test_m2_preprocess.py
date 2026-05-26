"""M2 — preprocess + central-slice extraction tests.

These tests use only numpy/PIL/pydicom (no torch). The DICOM file path is
synthesized via pydicom into a tmp_path so the loader can be exercised
end-to-end.
"""

from __future__ import annotations

import numpy as np
import pytest

from qc.body_part.preprocess import (
    DEFAULT_PREPROCESS,
    PreprocessConfig,
    central_slice_indices,
    load_slice_array_from_dicom,
    preprocess_slice,
)


# ---------------------------------------------------------------------------
# central_slice_indices
# ---------------------------------------------------------------------------


def test_central_slice_indices_normal():
    # 11 slices → middle = 5, picks 4,5,6
    assert central_slice_indices(11, 3) == [4, 5, 6]


def test_central_slice_indices_even_count():
    # 10 slices → middle = 5, picks 4,5,6
    assert central_slice_indices(10, 3) == [4, 5, 6]


def test_central_slice_indices_two_slices():
    # 2 slices → mid=1, raw [0,1,2] → clamped {0,1}
    out = central_slice_indices(2, 3)
    assert out == [0, 1]


def test_central_slice_indices_one_slice():
    assert central_slice_indices(1, 3) == [0]


def test_central_slice_indices_zero():
    assert central_slice_indices(0, 3) == []


def test_central_slice_indices_dedup():
    # Single slice with n=5 → all clamp to 0, dedup to [0]
    assert central_slice_indices(1, 5) == [0]


# ---------------------------------------------------------------------------
# preprocess_slice
# ---------------------------------------------------------------------------


def test_preprocess_slice_output_shape_and_dtype():
    arr = np.random.randint(0, 4096, size=(256, 192), dtype=np.uint16)
    out = preprocess_slice(arr)
    assert out.shape == (224, 224, 3)
    assert out.dtype == np.uint8


def test_preprocess_slice_letterbox_preserves_aspect():
    # Tall slice — should be letterboxed centred horizontally with zero pads.
    arr = np.full((512, 256), 1000, dtype=np.uint16)
    out = preprocess_slice(arr)
    # Top/bottom rows might be inside content (square fits), but the side
    # padding pixels should be uint8 0.
    side = out[:, 0, 0]
    # At least some pad columns are zero on a tall input.
    assert (side == 0).any()


def test_preprocess_slice_deterministic():
    arr = np.arange(64 * 64, dtype=np.uint16).reshape(64, 64)
    out1 = preprocess_slice(arr)
    out2 = preprocess_slice(arr)
    np.testing.assert_array_equal(out1, out2)


def test_preprocess_slice_handles_constant_image():
    arr = np.full((64, 64), 1000, dtype=np.uint16)
    out = preprocess_slice(arr)
    assert out.shape == (224, 224, 3)
    # Constant slice → all zeros after normalize.
    assert (out == 0).all()


def test_preprocess_slice_rejects_3d():
    arr = np.zeros((2, 64, 64), dtype=np.uint16)
    with pytest.raises(ValueError):
        preprocess_slice(arr)


def test_preprocess_slice_applies_rescale():
    arr = np.full((64, 64), 100, dtype=np.int16)
    out_a = preprocess_slice(arr)
    # Different scale → still constant, still normalises to zero.
    out_b = preprocess_slice(arr, rescale_slope=10, rescale_intercept=-50)
    assert (out_a == 0).all() and (out_b == 0).all()


def test_preprocess_config_version():
    cfg = PreprocessConfig()
    assert cfg.version == "v1"
    assert cfg.size == 224
    assert cfg.clip_pct == (1.0, 99.0)
    assert DEFAULT_PREPROCESS.version == "v1"


# ---------------------------------------------------------------------------
# load_slice_array_from_dicom — uses real pydicom on a synthesized file
# ---------------------------------------------------------------------------


def _write_minimal_dicom(path, pixel_array: np.ndarray, *, slope=1, intercept=0):
    import pydicom
    from pydicom.dataset import Dataset, FileMetaDataset
    from pydicom.uid import ExplicitVRLittleEndian, generate_uid

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.4"
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = generate_uid()

    ds = Dataset()
    ds.file_meta = file_meta
    ds.is_little_endian = True
    ds.is_implicit_VR = False
    ds.SOPClassUID = file_meta.MediaStorageSOPClassUID
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.PatientName = "Test"
    ds.PatientID = "1"
    ds.Modality = "MR"
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.Rows, ds.Columns = pixel_array.shape
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelData = pixel_array.astype(np.uint16).tobytes()
    ds.RescaleSlope = slope
    ds.RescaleIntercept = intercept
    ds.save_as(str(path), write_like_original=False)


def test_load_slice_array_from_dicom_classic(tmp_path):
    src = np.arange(64 * 32, dtype=np.uint16).reshape(64, 32)
    dcm = tmp_path / "single.dcm"
    _write_minimal_dicom(dcm, src, slope=2, intercept=10)

    res = load_slice_array_from_dicom(str(dcm))
    assert res is not None
    arr, slope, intercept = res
    assert arr.shape == (64, 32)
    assert slope == 2.0
    assert intercept == 10.0


def test_load_slice_array_from_dicom_missing(tmp_path):
    res = load_slice_array_from_dicom(str(tmp_path / "does_not_exist.dcm"))
    assert res is None
