"""Tests for Polars-based orientation computation."""

import polars as pl
import pytest

from src.sort.polars_bulk import create_stacks_from_instances


class TestOrientationComputation:
    """Test orientation category and confidence calculation."""

    def test_perfect_axial_orientation(self):
        """Test perfect axial orientation (confidence = 1.0)."""
        instances = [
            {
                "instance_id": 1,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": "1\\0\\0\\0\\1\\0",  # Perfect axial
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": 30.0,
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": "HeadNeck_64",
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            }
        ]
        
        series_modalities = {1: "MR"}
        
        result = create_stacks_from_instances(instances, series_modalities)
        
        assert result.height == 1
        assert result["stack_image_orientation"][0] == "Axial"
        assert abs(result["stack_orientation_confidence"][0] - 1.0) < 0.001
    
    def test_perfect_coronal_orientation(self):
        """Test perfect coronal orientation."""
        instances = [
            {
                "instance_id": 1,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": "1\\0\\0\\0\\0\\-1",  # Perfect coronal
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": 30.0,
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": None,
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            }
        ]
        
        series_modalities = {1: "MR"}
        
        result = create_stacks_from_instances(instances, series_modalities)
        
        assert result["stack_image_orientation"][0] == "Coronal"
        assert abs(result["stack_orientation_confidence"][0] - 1.0) < 0.001
    
    def test_perfect_sagittal_orientation(self):
        """Test perfect sagittal orientation."""
        instances = [
            {
                "instance_id": 1,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": "0\\1\\0\\0\\0\\-1",  # Perfect sagittal
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": 30.0,
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": None,
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            }
        ]
        
        series_modalities = {1: "MR"}
        
        result = create_stacks_from_instances(instances, series_modalities)
        
        assert result["stack_image_orientation"][0] == "Sagittal"
        assert abs(result["stack_orientation_confidence"][0] - 1.0) < 0.001
    
    def test_float_precision_merging(self):
        """Test that similar orientations due to float precision are merged."""
        instances = [
            {
                "instance_id": 1,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": "0.9997427\\-0.02221026\\-0.004605665\\-0.007560507\\-0.1348471\\-0.9908376",
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": 30.0,
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": None,
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            },
            {
                "instance_id": 2,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": "0.9997427\\-0.02221027\\-0.004605665\\-0.007560507\\-0.1348471\\-0.9908376",  # Tiny difference
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": 30.0,
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": None,
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            },
        ]
        
        series_modalities = {1: "MR"}
        
        result = create_stacks_from_instances(instances, series_modalities)
        
        # Should create only 1 stack (not 2) because orientations are categorically the same
        assert result.height == 1
        assert result["stack_n_instances"][0] == 2
        assert result["stack_image_orientation"][0] in ["Axial", "Coronal", "Sagittal"]
    
    def test_multi_echo_creates_multiple_stacks(self):
        """Test that different echo times create separate stacks."""
        instances = [
            {
                "instance_id": i,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": "1\\0\\0\\0\\1\\0",
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": float(echo_time),
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": None,
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            }
            for i, echo_time in enumerate([5.0, 10.0, 15.0], start=1)
        ]
        
        series_modalities = {1: "MR"}
        
        result = create_stacks_from_instances(instances, series_modalities)
        
        # Should create 3 stacks (one per echo time)
        assert result.height == 3
        assert result["stack_key"][0] == "multi_echo"
        assert all(result["stack_image_orientation"] == "Axial")
    
    def test_null_orientation_handling(self):
        """Test that null/invalid orientations are handled gracefully."""
        instances = [
            {
                "instance_id": 1,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": None,  # NULL orientation
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": 30.0,
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": None,
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            }
        ]
        
        series_modalities = {1: "MR"}
        
        result = create_stacks_from_instances(instances, series_modalities)
        
        # Should not crash, returns default orientation
        assert result.height == 1
        assert result["stack_image_orientation"][0] in ["Axial", "Coronal", "Sagittal"]
        assert 0.0 <= result["stack_orientation_confidence"][0] <= 1.0
    
    def test_empty_instances(self):
        """Test that empty instance list returns empty DataFrame."""
        result = create_stacks_from_instances([], {})
        
        assert result.height == 0
        assert "stack_image_orientation" in result.columns
        assert "stack_orientation_confidence" in result.columns
    
    def test_confidence_range(self):
        """Test that confidence is always in [0, 1] range."""
        instances = [
            {
                "instance_id": 1,
                "series_id": 1,
                "series_instance_uid": "1.2.3",
                "image_orientation_patient": "0.707\\0.707\\0\\-0.707\\0.707\\0",  # 45-degree in-plane rotation
                "image_type": "ORIGINAL\\PRIMARY",
                "inversion_time": None,
                "echo_time": 30.0,
                "echo_numbers": None,
                "echo_train_length": None,
                "repetition_time": 500.0,
                "flip_angle": 90.0,
                "receive_coil_name": None,
                "xray_exposure": None,
                "kvp": None,
                "tube_current": None,
                "pet_bed_index": None,
                "pet_frame_type": None,
            }
        ]
        
        series_modalities = {1: "MR"}
        
        result = create_stacks_from_instances(instances, series_modalities)
        
        confidence = result["stack_orientation_confidence"][0]
        assert 0.0 <= confidence <= 1.0
        # This is actually still axial (row=[0.707,0.707,0], col=[-0.707,0.707,0])
        # Cross product = [0,0,1] so confidence should be 1.0
        # (It's a 45-degree in-plane rotation, not oblique)
        assert result["stack_image_orientation"][0] == "Axial"
        assert abs(confidence - 1.0) < 0.001
