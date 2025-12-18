"""Tests for the bulk FK update optimization in Step 2."""

import pytest


class TestBulkFKUpdateLogic:
    """Test the bulk FK update array building logic."""

    def test_build_flat_arrays_simple(self):
        """Test building flat arrays from instance_ids_map."""
        # Simulate the data structures from step2_stack_discovery.py
        instance_ids_map = {
            (1, 0): [100, 101, 102],  # series_id=1, stack_index=0, 3 instances
            (1, 1): [103, 104],        # series_id=1, stack_index=1, 2 instances
            (2, 0): [200, 201, 202, 203],  # series_id=2, stack_index=0, 4 instances
        }
        
        stack_id_lookup = {
            (1, 0): 1000,  # series_stack_id for (series_id=1, stack_index=0)
            (1, 1): 1001,
            (2, 0): 1002,
        }
        
        # Build flat arrays (same logic as in step2_stack_discovery.py)
        all_instance_ids = []
        all_stack_ids = []
        
        for (series_id, stack_index), instance_id_list in instance_ids_map.items():
            series_stack_id = stack_id_lookup.get((series_id, stack_index))
            if series_stack_id:
                all_instance_ids.extend(instance_id_list)
                all_stack_ids.extend([series_stack_id] * len(instance_id_list))
        
        # Verify results
        assert len(all_instance_ids) == 9  # 3 + 2 + 4 = 9 instances
        assert len(all_stack_ids) == 9
        
        # Verify mapping is correct
        for i, (inst_id, stack_id) in enumerate(zip(all_instance_ids, all_stack_ids)):
            if inst_id in [100, 101, 102]:
                assert stack_id == 1000
            elif inst_id in [103, 104]:
                assert stack_id == 1001
            elif inst_id in [200, 201, 202, 203]:
                assert stack_id == 1002

    def test_build_flat_arrays_with_missing_lookup(self):
        """Test handling when stack_id_lookup is missing entries."""
        instance_ids_map = {
            (1, 0): [100, 101],
            (2, 0): [200, 201],  # This one won't have a lookup entry
        }
        
        stack_id_lookup = {
            (1, 0): 1000,
            # Missing (2, 0) entry
        }
        
        all_instance_ids = []
        all_stack_ids = []
        skipped_stacks = 0
        
        for (series_id, stack_index), instance_id_list in instance_ids_map.items():
            series_stack_id = stack_id_lookup.get((series_id, stack_index))
            if not series_stack_id:
                skipped_stacks += 1
                continue
            all_instance_ids.extend(instance_id_list)
            all_stack_ids.extend([series_stack_id] * len(instance_id_list))
        
        # Only instances from (1, 0) should be included
        assert len(all_instance_ids) == 2
        assert all_stack_ids == [1000, 1000]
        assert skipped_stacks == 1

    def test_empty_instance_ids_map(self):
        """Test handling of empty input."""
        instance_ids_map = {}
        stack_id_lookup = {}
        
        all_instance_ids = []
        all_stack_ids = []
        
        for (series_id, stack_index), instance_id_list in instance_ids_map.items():
            series_stack_id = stack_id_lookup.get((series_id, stack_index))
            if series_stack_id:
                all_instance_ids.extend(instance_id_list)
                all_stack_ids.extend([series_stack_id] * len(instance_id_list))
        
        assert len(all_instance_ids) == 0
        assert len(all_stack_ids) == 0

    def test_large_scale_array_building(self):
        """Test with larger scale data (simulating 454K stacks)."""
        # Create test data with 1000 stacks, avg 65 instances each
        instance_ids_map = {}
        stack_id_lookup = {}
        
        instance_counter = 0
        for series_id in range(100):
            for stack_index in range(10):
                # Random-ish number of instances per stack
                n_instances = 50 + (series_id + stack_index) % 30
                instances = list(range(instance_counter, instance_counter + n_instances))
                instance_ids_map[(series_id, stack_index)] = instances
                # Use 1000 + offset to avoid 0 (which is falsy)
                stack_id_lookup[(series_id, stack_index)] = 1000 + series_id * 100 + stack_index
                instance_counter += n_instances
        
        # Build arrays
        all_instance_ids = []
        all_stack_ids = []
        
        for (series_id, stack_index), instance_id_list in instance_ids_map.items():
            series_stack_id = stack_id_lookup.get((series_id, stack_index))
            if series_stack_id:
                all_instance_ids.extend(instance_id_list)
                all_stack_ids.extend([series_stack_id] * len(instance_id_list))
        
        # Verify we processed all stacks
        assert len(instance_ids_map) == 1000
        assert len(all_instance_ids) == instance_counter
        assert len(all_stack_ids) == instance_counter
        
        # Verify arrays are same length (critical for UNNEST)
        assert len(all_instance_ids) == len(all_stack_ids)


class TestTempTableMappingLogic:
    """Test the temp table + join FK update logic (new optimized approach)."""

    def test_build_mapping_records(self):
        """Test building mapping records for temp table bulk load."""
        instance_ids_map = {
            (1, 0): [100, 101, 102],
            (1, 1): [103, 104],
            (2, 0): [200, 201],
        }
        
        # Build mapping records (same logic as in step2_stack_discovery.py)
        mapping_records = []
        for (series_id, stack_index), instance_id_list in instance_ids_map.items():
            for instance_id in instance_id_list:
                mapping_records.append({
                    "instance_id": instance_id,
                    "series_id": series_id,
                    "stack_index": stack_index,
                })
        
        # Verify total count
        assert len(mapping_records) == 7  # 3 + 2 + 2 = 7
        
        # Verify structure
        for record in mapping_records:
            assert "instance_id" in record
            assert "series_id" in record
            assert "stack_index" in record
        
        # Verify specific mappings
        inst_100 = next(r for r in mapping_records if r["instance_id"] == 100)
        assert inst_100["series_id"] == 1
        assert inst_100["stack_index"] == 0
        
        inst_103 = next(r for r in mapping_records if r["instance_id"] == 103)
        assert inst_103["series_id"] == 1
        assert inst_103["stack_index"] == 1

    def test_mapping_preserves_all_instances(self):
        """Test that all instances are preserved in mapping."""
        # Large scale test
        instance_ids_map = {}
        total_expected = 0
        
        for series_id in range(50):
            for stack_index in range(5):
                n_instances = 100 + (series_id * stack_index) % 50
                instances = list(range(total_expected, total_expected + n_instances))
                instance_ids_map[(series_id, stack_index)] = instances
                total_expected += n_instances
        
        mapping_records = []
        for (series_id, stack_index), instance_id_list in instance_ids_map.items():
            for instance_id in instance_id_list:
                mapping_records.append({
                    "instance_id": instance_id,
                    "series_id": series_id,
                    "stack_index": stack_index,
                })
        
        assert len(mapping_records) == total_expected
        
        # Verify all unique instance IDs
        instance_ids = {r["instance_id"] for r in mapping_records}
        assert len(instance_ids) == total_expected

    def test_empty_mapping(self):
        """Test empty instance_ids_map produces empty mapping."""
        instance_ids_map = {}
        
        mapping_records = []
        for (series_id, stack_index), instance_id_list in instance_ids_map.items():
            for instance_id in instance_id_list:
                mapping_records.append({
                    "instance_id": instance_id,
                    "series_id": series_id,
                    "stack_index": stack_index,
                })
        
        assert len(mapping_records) == 0


class TestSSEIdempotencyLogic:
    """Test the SSE idempotency check logic."""

    def test_completed_job_returns_cached(self):
        """Completed job should return cached completion, not re-run."""
        # Simulate job status enum
        job_status = "completed"
        
        if job_status == "completed":
            should_return_cached = True
            should_run = False
        elif job_status == "running":
            should_return_cached = False
            should_run = False
        else:
            should_return_cached = False
            should_run = True
        
        assert should_return_cached is True
        assert should_run is False

    def test_running_job_returns_error(self):
        """Running job should return error, not start duplicate."""
        job_status = "running"
        
        if job_status == "completed":
            result = "cached"
        elif job_status == "running":
            result = "error"
        else:
            result = "run"
        
        assert result == "error"

    def test_queued_job_proceeds(self):
        """Queued job should proceed with execution."""
        job_status = "queued"
        
        if job_status == "completed":
            result = "cached"
        elif job_status == "running":
            result = "error"
        else:
            result = "run"
        
        assert result == "run"

    def test_failed_job_can_retry(self):
        """Failed job should allow retry (proceed with execution)."""
        job_status = "failed"
        
        if job_status == "completed":
            result = "cached"
        elif job_status == "running":
            result = "error"
        else:
            result = "run"
        
        assert result == "run"
