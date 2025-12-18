"""Tests for Step 2: Stack Discovery."""

import pytest

from src.sort.stack_key import generate_stack_key, find_varying_columns
from src.sort.models import StackRecord


class TestStackKeyGeneration:
    """Test stack key generation logic."""

    def test_single_stack_returns_none(self):
        """Single-stack series should return None."""
        stacks = [
            StackRecord(
                series_id=1,
                series_instance_uid="1.2.3",
                image_orientation_patient="1\\0\\0\\0\\1\\0",
                image_type="ORIGINAL\\PRIMARY",
                inversion_time=None,
                echo_time=30.0,
                echo_numbers=None,
                echo_train_length=None,
                repetition_time=500.0,
                flip_angle=90.0,
                receive_coil_name="HeadNeck_64",
                xray_exposure=None,
                kvp=None,
                tube_current=None,
                pet_bed_index=None,
                pet_frame_type=None,
                stack_n_instances=100,
                first_instance_id=1,
            )
        ]
        
        assert generate_stack_key(stacks) is None

    def test_multi_echo_detection(self):
        """Multi-echo series should return 'multi_echo'."""
        stacks = [
            StackRecord(
                series_id=1,
                series_instance_uid="1.2.3",
                image_orientation_patient="1\\0\\0\\0\\1\\0",
                image_type="ORIGINAL\\PRIMARY",
                inversion_time=None,
                echo_time=30.0,  # Different echo times
                echo_numbers=None,
                echo_train_length=None,
                repetition_time=500.0,
                flip_angle=90.0,
                receive_coil_name="HeadNeck_64",
                xray_exposure=None,
                kvp=None,
                tube_current=None,
                pet_bed_index=None,
                pet_frame_type=None,
                stack_n_instances=50,
                first_instance_id=1,
            ),
            StackRecord(
                series_id=1,
                series_instance_uid="1.2.3",
                image_orientation_patient="1\\0\\0\\0\\1\\0",
                image_type="ORIGINAL\\PRIMARY",
                inversion_time=None,
                echo_time=60.0,  # Different echo time
                echo_numbers=None,
                echo_train_length=None,
                repetition_time=500.0,
                flip_angle=90.0,
                receive_coil_name="HeadNeck_64",
                xray_exposure=None,
                kvp=None,
                tube_current=None,
                pet_bed_index=None,
                pet_frame_type=None,
                stack_n_instances=50,
                first_instance_id=51,
            ),
        ]
        
        assert generate_stack_key(stacks) == "multi_echo"

    def test_multi_orientation_detection(self):
        """Multi-orientation series should return 'multi_orientation'."""
        stacks = [
            StackRecord(
                series_id=1,
                series_instance_uid="1.2.3",
                image_orientation_patient="1\\0\\0\\0\\1\\0",  # Axial
                image_type="ORIGINAL\\PRIMARY",
                inversion_time=None,
                echo_time=30.0,
                echo_numbers=None,
                echo_train_length=None,
                repetition_time=500.0,
                flip_angle=90.0,
                receive_coil_name="HeadNeck_64",
                xray_exposure=None,
                kvp=None,
                tube_current=None,
                pet_bed_index=None,
                pet_frame_type=None,
                stack_n_instances=50,
                first_instance_id=1,
            ),
            StackRecord(
                series_id=1,
                series_instance_uid="1.2.3",
                image_orientation_patient="0\\1\\0\\0\\0\\-1",  # Sagittal
                image_type="ORIGINAL\\PRIMARY",
                inversion_time=None,
                echo_time=30.0,
                echo_numbers=None,
                echo_train_length=None,
                repetition_time=500.0,
                flip_angle=90.0,
                receive_coil_name="HeadNeck_64",
                xray_exposure=None,
                kvp=None,
                tube_current=None,
                pet_bed_index=None,
                pet_frame_type=None,
                stack_n_instances=50,
                first_instance_id=51,
            ),
        ]
        
        assert generate_stack_key(stacks) == "multi_orientation"

    def test_find_varying_columns(self):
        """Test identification of varying columns."""
        stacks = [
            StackRecord(
                series_id=1,
                series_instance_uid="1.2.3",
                image_orientation_patient="1\\0\\0\\0\\1\\0",
                image_type="ORIGINAL\\PRIMARY",
                inversion_time=None,
                echo_time=30.0,
                echo_numbers=None,
                echo_train_length=None,
                repetition_time=500.0,
                flip_angle=90.0,
                receive_coil_name="HeadNeck_64",
                xray_exposure=None,
                kvp=None,
                tube_current=None,
                pet_bed_index=None,
                pet_frame_type=None,
                stack_n_instances=50,
                first_instance_id=1,
            ),
            StackRecord(
                series_id=1,
                series_instance_uid="1.2.3",
                image_orientation_patient="1\\0\\0\\0\\1\\0",
                image_type="ORIGINAL\\PRIMARY",
                inversion_time=None,
                echo_time=60.0,  # Only this varies
                echo_numbers=None,
                echo_train_length=None,
                repetition_time=500.0,
                flip_angle=90.0,
                receive_coil_name="HeadNeck_64",
                xray_exposure=None,
                kvp=None,
                tube_current=None,
                pet_bed_index=None,
                pet_frame_type=None,
                stack_n_instances=50,
                first_instance_id=51,
            ),
        ]
        
        varying = find_varying_columns(stacks)
        assert varying == {"echo_time"}


# Note: Integration tests would require database setup
# These would test:
# - query_unique_stacks() with real database
# - Step2StackDiscovery.execute() with mock handover
# - Series with single vs multiple stacks
# - Various modality combinations (MR, CT, PT)
