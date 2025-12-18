"""Unit tests for stack_utils module."""

import pytest
from extract.stack_utils import (
    compute_orientation,
    compute_stack_signature,
    build_stack_row,
    signature_from_stack_record,
    _round_or_none,
)


class TestComputeOrientation:
    """Tests for compute_orientation function."""

    def test_pure_axial(self):
        """Pure axial orientation: row=(1,0,0), col=(0,1,0) -> normal=(0,0,1)"""
        iop = "1\\0\\0\\0\\1\\0"
        orientation, confidence = compute_orientation(iop)
        assert orientation == "Axial"
        assert confidence == pytest.approx(1.0, abs=0.01)

    def test_pure_sagittal(self):
        """Pure sagittal orientation: row=(0,1,0), col=(0,0,1) -> normal=(1,0,0)"""
        iop = "0\\1\\0\\0\\0\\1"
        orientation, confidence = compute_orientation(iop)
        assert orientation == "Sagittal"
        assert confidence == pytest.approx(1.0, abs=0.01)

    def test_pure_coronal(self):
        """Pure coronal orientation: row=(1,0,0), col=(0,0,1) -> normal=(0,-1,0)"""
        iop = "1\\0\\0\\0\\0\\1"
        orientation, confidence = compute_orientation(iop)
        assert orientation == "Coronal"
        assert confidence == pytest.approx(1.0, abs=0.01)

    def test_none_input(self):
        """None input returns default Axial with low confidence."""
        orientation, confidence = compute_orientation(None)
        assert orientation == "Axial"
        assert confidence == 0.5

    def test_empty_string(self):
        """Empty string returns default Axial with low confidence."""
        orientation, confidence = compute_orientation("")
        assert orientation == "Axial"
        assert confidence == 0.5

    def test_insufficient_components(self):
        """Less than 6 components returns default."""
        orientation, confidence = compute_orientation("1\\0\\0")
        assert orientation == "Axial"
        assert confidence == 0.5

    def test_oblique_orientation(self):
        """Oblique orientation has lower confidence."""
        # 45 degree rotation
        iop = "0.707\\0.707\\0\\0\\0\\1"
        orientation, confidence = compute_orientation(iop)
        # Should still categorize as Coronal (Y-dominant normal)
        assert orientation in ["Axial", "Coronal", "Sagittal"]
        assert 0.5 < confidence < 1.0

    def test_with_brackets(self):
        """Input with brackets is handled correctly."""
        iop = "[1\\0\\0\\0\\1\\0]"
        orientation, confidence = compute_orientation(iop)
        assert orientation == "Axial"
        assert confidence == pytest.approx(1.0, abs=0.01)

    def test_with_quotes(self):
        """Input with quotes is handled correctly."""
        iop = "'1\\0\\0\\0\\1\\0'"
        orientation, confidence = compute_orientation(iop)
        assert orientation == "Axial"
        assert confidence == pytest.approx(1.0, abs=0.01)

    def test_invalid_floats(self):
        """Invalid float values return default."""
        iop = "abc\\def\\ghi\\jkl\\mno\\pqr"
        orientation, confidence = compute_orientation(iop)
        assert orientation == "Axial"
        assert confidence == 0.5


class TestRoundOrNone:
    """Tests for _round_or_none helper."""

    def test_round_float(self):
        assert _round_or_none(10.567, 2) == 10.57

    def test_round_to_int(self):
        assert _round_or_none(10.567, 0) == 11.0

    def test_none_input(self):
        assert _round_or_none(None, 2) is None

    def test_string_number(self):
        assert _round_or_none("10.567", 2) == 10.57

    def test_invalid_string(self):
        assert _round_or_none("invalid", 2) is None


class TestComputeStackSignature:
    """Tests for compute_stack_signature function."""

    def test_mr_instance(self):
        """MR instance produces signature with MR fields."""
        series_uid = "1.2.3.4.5"
        instance_fields = {
            "echo_time": 10.567,
            "inversion_time": 2500.123,
            "echo_numbers": "1",
            "echo_train_length": 4,
            "repetition_time": 3000.456,
            "flip_angle": 90.5,
            "receive_coil_name": "HeadCoil",
            "image_orientation_patient": "1\\0\\0\\0\\1\\0",
            "image_type": "ORIGINAL\\PRIMARY",
            # CT/PET fields should be None
            "xray_exposure": None,
            "kvp": None,
            "tube_current": None,
            "pet_bed_index": None,
            "pet_frame_type": None,
        }
        
        sig = compute_stack_signature(series_uid, instance_fields)
        
        # Check structure
        assert sig[0] == series_uid
        assert sig[1] == 10.57  # echo_time rounded to 2 decimals
        assert sig[2] == 2500.1  # inversion_time rounded to 1 decimal
        assert sig[3] == "1"  # echo_numbers
        assert sig[4] == 4  # echo_train_length
        assert sig[13] == "Axial"  # orientation category
        assert sig[14] == "ORIGINAL\\PRIMARY"  # image_type

    def test_ct_instance(self):
        """CT instance produces signature with CT fields."""
        series_uid = "1.2.3.4.6"
        instance_fields = {
            "echo_time": None,
            "inversion_time": None,
            "echo_numbers": None,
            "echo_train_length": None,
            "repetition_time": None,
            "flip_angle": None,
            "receive_coil_name": None,
            "image_orientation_patient": "1\\0\\0\\0\\1\\0",
            "image_type": "ORIGINAL\\PRIMARY",
            "xray_exposure": 250,
            "kvp": 120.5,
            "tube_current": 300.7,
            "pet_bed_index": None,
            "pet_frame_type": None,
        }
        
        sig = compute_stack_signature(series_uid, instance_fields)
        
        assert sig[0] == series_uid
        assert sig[1] is None  # echo_time
        assert sig[8] == 250  # xray_exposure
        assert sig[9] == 120.0  # kvp rounded to 0 decimals (banker's rounding: 120.5 -> 120)
        assert sig[10] == 301.0  # tube_current rounded

    def test_same_params_same_signature(self):
        """Two instances with same params produce same signature."""
        series_uid = "1.2.3.4.5"
        fields1 = {
            "echo_time": 10.0,
            "image_orientation_patient": "1\\0\\0\\0\\1\\0",
        }
        fields2 = {
            "echo_time": 10.0,
            "image_orientation_patient": "1\\0\\0\\0\\1\\0",
        }
        
        sig1 = compute_stack_signature(series_uid, fields1)
        sig2 = compute_stack_signature(series_uid, fields2)
        
        assert sig1 == sig2

    def test_different_params_different_signature(self):
        """Different echo times produce different signatures."""
        series_uid = "1.2.3.4.5"
        fields1 = {"echo_time": 10.0}
        fields2 = {"echo_time": 20.0}
        
        sig1 = compute_stack_signature(series_uid, fields1)
        sig2 = compute_stack_signature(series_uid, fields2)
        
        assert sig1 != sig2


class TestBuildStackRow:
    """Tests for build_stack_row function."""

    def test_mr_stack_row(self):
        """Build stack row for MR instance."""
        row = build_stack_row(
            series_id=123,
            stack_index=0,
            modality="MR",
            instance_fields={
                "echo_time": 10.567,
                "inversion_time": 2500.123,
                "image_orientation_patient": "1\\0\\0\\0\\1\\0",
                "image_type": "ORIGINAL\\PRIMARY",
            },
        )
        
        assert row["series_id"] == 123
        assert row["stack_index"] == 0
        assert row["stack_modality"] == "MR"
        assert row["stack_echo_time"] == 10.57
        assert row["stack_inversion_time"] == 2500.1
        assert row["stack_image_orientation"] == "Axial"
        assert row["stack_orientation_confidence"] == pytest.approx(1.0, abs=0.01)
        assert row["stack_image_type"] == "ORIGINAL\\PRIMARY"
        assert row["stack_n_instances"] is None  # Not set during extraction

    def test_ct_stack_row(self):
        """Build stack row for CT instance."""
        row = build_stack_row(
            series_id=456,
            stack_index=1,
            modality="CT",
            instance_fields={
                "kvp": 120.5,
                "tube_current": 300.7,
                "xray_exposure": 250,
                "image_orientation_patient": "1\\0\\0\\0\\1\\0",
            },
        )
        
        assert row["series_id"] == 456
        assert row["stack_index"] == 1
        assert row["stack_modality"] == "CT"
        assert row["stack_kvp"] == 120.0  # Rounded to 0 decimals (banker's rounding)
        assert row["stack_tube_current"] == 301.0
        assert row["stack_xray_exposure"] == 250
        # MR fields should be None
        assert row["stack_echo_time"] is None
        assert row["stack_inversion_time"] is None


class TestSignatureFromStackRecord:
    """Tests for signature_from_stack_record function."""

    def test_roundtrip(self):
        """Signature from build_stack_row matches compute_stack_signature."""
        series_uid = "1.2.3.4.5"
        instance_fields = {
            "echo_time": 10.57,  # Already rounded
            "inversion_time": 2500.1,
            "echo_numbers": "1",
            "echo_train_length": 4,
            "repetition_time": 3000.5,
            "flip_angle": 90.5,
            "receive_coil_name": "HeadCoil",
            "image_orientation_patient": "1\\0\\0\\0\\1\\0",
            "image_type": "ORIGINAL\\PRIMARY",
            "xray_exposure": None,
            "kvp": None,
            "tube_current": None,
            "pet_bed_index": None,
            "pet_frame_type": None,
        }
        
        # Compute original signature
        original_sig = compute_stack_signature(series_uid, instance_fields)
        
        # Build a stack row
        row = build_stack_row(
            series_id=123,
            stack_index=0,
            modality="MR",
            instance_fields=instance_fields,
        )
        
        # Reconstruct signature from row
        reconstructed_sig = signature_from_stack_record(
            series_uid,
            row["stack_echo_time"],
            row["stack_inversion_time"],
            row["stack_echo_numbers"],
            row["stack_echo_train_length"],
            row["stack_repetition_time"],
            row["stack_flip_angle"],
            row["stack_receive_coil_name"],
            row["stack_xray_exposure"],
            row["stack_kvp"],
            row["stack_tube_current"],
            row["stack_pet_bed_index"],
            row["stack_pet_frame_type"],
            row["stack_image_orientation"],
            row["stack_image_type"],
        )
        
        assert original_sig == reconstructed_sig
