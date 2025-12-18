"""NILS Dataset Pipeline - Unified pipeline state management.

This module provides a single source of truth for tracking the state of
cohort processing pipelines, replacing the previous JSON-based stages
column and separate sorting tables.

Key components:
- NilsDatasetPipelineStep: ORM model for pipeline step tracking
- NilsDatasetPipelineService: Service for managing pipeline state
- ordering.py: Single source of truth for pipeline structure
"""

from .models import NilsDatasetPipelineStep
from .service import nils_pipeline_service, NilsDatasetPipelineService
from .ordering import PIPELINE_STAGES, get_pipeline_items, get_stage_ids, get_step_ids_for_stage

__all__ = [
    "NilsDatasetPipelineStep",
    "NilsDatasetPipelineService",
    "nils_pipeline_service",
    "PIPELINE_STAGES",
    "get_pipeline_items",
    "get_stage_ids",
    "get_step_ids_for_stage",
]
