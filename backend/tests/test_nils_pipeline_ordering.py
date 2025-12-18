"""Tests for nils_dataset_pipeline.ordering module."""

from nils_dataset_pipeline.ordering import (
    PIPELINE_STAGES,
    get_pipeline_items,
    get_stage_ids,
    get_step_ids_for_stage,
    get_stage_config,
    is_multi_step_stage,
    get_next_step_in_stage,
    get_previous_step_in_stage,
    get_default_stage_config,
)


class TestPipelineStructure:
    """Tests for pipeline structure definition."""

    def test_pipeline_stages_defined(self):
        """Ensure pipeline stages are defined."""
        assert len(PIPELINE_STAGES) > 0
        assert all("id" in stage for stage in PIPELINE_STAGES)
        assert all("title" in stage for stage in PIPELINE_STAGES)

    def test_stage_ids_order(self):
        """Verify expected stages are present in order."""
        stage_ids = get_stage_ids()
        assert "anonymize" in stage_ids
        assert "extract" in stage_ids
        assert "sort" in stage_ids
        # Verify order
        assert stage_ids.index("anonymize") < stage_ids.index("extract")
        assert stage_ids.index("extract") < stage_ids.index("sort")

    def test_sort_is_multi_step_stage(self):
        """Verify sort stage has multiple steps."""
        assert is_multi_step_stage("sort") is True
        assert is_multi_step_stage("anonymize") is False
        assert is_multi_step_stage("extract") is False

    def test_get_sort_step_ids(self):
        """Verify sort stage has expected steps."""
        step_ids = get_step_ids_for_stage("sort")
        assert "checkup" in step_ids
        assert "stack_fingerprint" in step_ids


class TestPipelineItems:
    """Tests for get_pipeline_items function."""

    def test_with_anonymization_enabled(self):
        """Test pipeline items with anonymization."""
        items = get_pipeline_items(anonymization_enabled=True)
        stage_ids = [item["stage_id"] for item in items]
        assert "anonymize" in stage_ids
        assert items[0]["stage_id"] == "anonymize"

    def test_without_anonymization(self):
        """Test pipeline items without anonymization."""
        items = get_pipeline_items(anonymization_enabled=False)
        stage_ids = [item["stage_id"] for item in items]
        assert "anonymize" not in stage_ids
        assert items[0]["stage_id"] == "extract"

    def test_sort_order_is_sequential(self):
        """Verify sort_order is sequential starting from 0."""
        items = get_pipeline_items(anonymization_enabled=True)
        for i, item in enumerate(items):
            assert item["sort_order"] == i

    def test_multi_step_stages_expanded(self):
        """Verify multi-step stages are expanded to individual items."""
        items = get_pipeline_items(anonymization_enabled=False)
        # Sort stage should have multiple items
        sort_items = [item for item in items if item["stage_id"] == "sort"]
        assert len(sort_items) >= 2
        # Each should have a step_id
        assert all(item["step_id"] is not None for item in sort_items)

    def test_simple_stages_have_null_step_id(self):
        """Verify simple stages have step_id=None."""
        items = get_pipeline_items(anonymization_enabled=True)
        extract_items = [item for item in items if item["stage_id"] == "extract"]
        assert len(extract_items) == 1
        assert extract_items[0]["step_id"] is None


class TestStepNavigation:
    """Tests for step navigation helpers."""

    def test_get_next_step_in_stage(self):
        """Test getting next step within a stage."""
        step_ids = get_step_ids_for_stage("sort")
        if len(step_ids) >= 2:
            first_step = step_ids[0]
            second_step = step_ids[1]
            assert get_next_step_in_stage("sort", first_step) == second_step

    def test_get_next_step_at_end_returns_none(self):
        """Test getting next step at end of stage returns None."""
        step_ids = get_step_ids_for_stage("sort")
        last_step = step_ids[-1]
        assert get_next_step_in_stage("sort", last_step) is None

    def test_get_previous_step_in_stage(self):
        """Test getting previous step within a stage."""
        step_ids = get_step_ids_for_stage("sort")
        if len(step_ids) >= 2:
            first_step = step_ids[0]
            second_step = step_ids[1]
            assert get_previous_step_in_stage("sort", second_step) == first_step

    def test_get_previous_step_at_start_returns_none(self):
        """Test getting previous step at start of stage returns None."""
        step_ids = get_step_ids_for_stage("sort")
        first_step = step_ids[0]
        assert get_previous_step_in_stage("sort", first_step) is None


class TestDefaultConfigs:
    """Tests for default configuration generation."""

    def test_anonymize_config_has_required_keys(self):
        """Verify anonymize config has required keys."""
        config = get_default_stage_config("anonymize", "test_cohort", "/test")
        assert "patient_id" in config
        assert "study_dates" in config
        assert "audit_export" in config

    def test_extract_config_has_required_keys(self):
        """Verify extract config has required keys."""
        config = get_default_stage_config("extract", "test_cohort", "/test")
        assert "process_pool_workers" in config

    def test_sort_config_has_required_keys(self):
        """Verify sort config has required keys."""
        config = get_default_stage_config("sort", "test_cohort", "/test")
        assert "profile" in config
        assert "selectedModalities" in config

    def test_unknown_stage_returns_empty_config(self):
        """Verify unknown stage returns empty config."""
        config = get_default_stage_config("unknown_stage", "test", "/test")
        assert config == {}
