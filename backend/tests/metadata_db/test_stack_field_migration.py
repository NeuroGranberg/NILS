"""Tests for instance stack field migration and DICOM field converters."""

from __future__ import annotations

import pytest

from extract.dicom_mappings import (
    INSTANCE_FIELD_MAP,
    _to_float,
    _to_int,
    _to_backslash_str,
)


class TestInstanceStackFieldMappings:
    """Tests for the new stack-defining field mappings in INSTANCE_FIELD_MAP."""

    def test_mr_stack_fields_present(self):
        """Test that MR stack-defining fields are in INSTANCE_FIELD_MAP."""
        mr_fields = [
            "inversion_time",
            "echo_time",
            "echo_numbers",  # Changed from echo_number to echo_numbers (VM=1-n)
            "echo_train_length",
            "repetition_time",
            "flip_angle",
            "receive_coil_name",
            "image_orientation_patient",
            "image_type",
        ]
        for field in mr_fields:
            assert field in INSTANCE_FIELD_MAP, f"Missing MR field: {field}"

    def test_ct_stack_fields_present(self):
        """Test that CT stack-defining fields are in INSTANCE_FIELD_MAP."""
        ct_fields = [
            "xray_exposure",
            "kvp",
            "tube_current",
        ]
        for field in ct_fields:
            assert field in INSTANCE_FIELD_MAP, f"Missing CT field: {field}"

    def test_pet_stack_fields_present(self):
        """Test that PET stack-defining fields are in INSTANCE_FIELD_MAP."""
        pet_fields = [
            "pet_bed_index",
            "pet_frame_type",
        ]
        for field in pet_fields:
            assert field in INSTANCE_FIELD_MAP, f"Missing PET field: {field}"

    def test_echo_time_mapping(self):
        """Test echo_time field mapping."""
        mapping = INSTANCE_FIELD_MAP["echo_time"]
        assert mapping.keyword == "EchoTime"
        assert mapping.converter == _to_float

    def test_echo_numbers_mapping(self):
        """Test echo_numbers field mapping (VM=1-n, backslash-separated)."""
        mapping = INSTANCE_FIELD_MAP["echo_numbers"]
        assert mapping.keyword == "EchoNumbers"
        assert mapping.converter == _to_backslash_str

    def test_repetition_time_mapping(self):
        """Test repetition_time field mapping."""
        mapping = INSTANCE_FIELD_MAP["repetition_time"]
        assert mapping.keyword == "RepetitionTime"
        assert mapping.converter == _to_float

    def test_flip_angle_mapping(self):
        """Test flip_angle field mapping."""
        mapping = INSTANCE_FIELD_MAP["flip_angle"]
        assert mapping.keyword == "FlipAngle"
        assert mapping.converter == _to_float

    def test_image_type_mapping(self):
        """Test image_type field mapping uses backslash string converter."""
        mapping = INSTANCE_FIELD_MAP["image_type"]
        assert mapping.keyword == "ImageType"
        assert mapping.converter == _to_backslash_str

    def test_image_orientation_patient_mapping(self):
        """Test image_orientation_patient field mapping."""
        mapping = INSTANCE_FIELD_MAP["image_orientation_patient"]
        assert mapping.keyword == "ImageOrientationPatient"
        assert mapping.converter == _to_backslash_str

    def test_kvp_mapping(self):
        """Test kvp field mapping."""
        mapping = INSTANCE_FIELD_MAP["kvp"]
        assert mapping.keyword == "KVP"
        assert mapping.converter == _to_float

    def test_tube_current_mapping(self):
        """Test tube_current field mapping."""
        mapping = INSTANCE_FIELD_MAP["tube_current"]
        assert mapping.keyword == "XRayTubeCurrent"
        assert mapping.converter == _to_float

    def test_xray_exposure_mapping(self):
        """Test xray_exposure field mapping."""
        mapping = INSTANCE_FIELD_MAP["xray_exposure"]
        assert mapping.keyword == "Exposure"
        assert mapping.converter == _to_float

    def test_pet_bed_index_mapping(self):
        """Test pet_bed_index field mapping."""
        mapping = INSTANCE_FIELD_MAP["pet_bed_index"]
        assert mapping.keyword == "NumberOfSlices"
        assert mapping.converter == _to_int

    def test_pet_frame_type_mapping(self):
        """Test pet_frame_type field mapping."""
        mapping = INSTANCE_FIELD_MAP["pet_frame_type"]
        assert mapping.keyword == "SeriesType"
        assert mapping.converter == _to_backslash_str


class TestConverterFunctions:
    """Tests for converter functions with stack-related values."""

    def test_to_float_echo_time(self):
        """Test _to_float with typical echo time values."""
        assert _to_float(25.5) == 25.5
        assert _to_float("25.5") == 25.5
        assert _to_float(None) is None

    def test_to_backslash_str_echo_numbers(self):
        """Test _to_backslash_str with typical echo numbers values (VM=1-n)."""
        # Single echo number
        assert _to_backslash_str(1) == "1"
        # Multiple echo numbers
        assert _to_backslash_str([1, 2]) == "1\\2"
        # MultiValue with empty string (edge case from DICOM)
        assert _to_backslash_str([1, '']) == "1\\"
        assert _to_backslash_str(None) is None

    def test_to_backslash_str_image_type(self):
        """Test _to_backslash_str with typical ImageType values."""
        # Multi-valued ImageType
        assert _to_backslash_str(["ORIGINAL", "PRIMARY", "M", "ND"]) == "ORIGINAL\\PRIMARY\\M\\ND"
        # Single value
        assert _to_backslash_str("DERIVED") == "DERIVED"
        assert _to_backslash_str(None) is None

    def test_to_backslash_str_image_orientation(self):
        """Test _to_backslash_str with ImageOrientationPatient values."""
        # Typical 6-element direction cosines
        orientation = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0]
        result = _to_backslash_str(orientation)
        assert result == "1.0\\0.0\\0.0\\0.0\\1.0\\0.0"
