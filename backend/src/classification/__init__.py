"""
MRI/CT/PET Series Stack Classification System

Version: 3.1.0

This module implements the provenance-first classification system
for neuroimaging series stacks.

Main Components:
- core/: Core data structures (context, evidence, output)
- detectors/: Individual axis detectors
- branches/: Classification branches (RawRecon, SyMRI, SWI, DTI)
- detection_yaml/: YAML-driven detection configuration
- pipeline.py: Main classification orchestrator

Usage:
    from classification.core.context import ClassificationContext
    from classification.core.output import ClassificationResult
    from classification.pipeline import ClassificationPipeline
    
    # Create context from fingerprint
    ctx = ClassificationContext.from_fingerprint(fingerprint_dict)
    
    # Run classification
    pipeline = ClassificationPipeline()
    result = pipeline.classify(ctx)
    
    # Get database-ready dict
    db_row = result.to_dict()
"""

from .core.context import ClassificationContext
from .core.evidence import Evidence, EvidenceSource, AxisResult
from .core.output import ClassificationResult

__version__ = "3.1.0"

__all__ = [
    "ClassificationContext",
    "ClassificationResult",
    "Evidence",
    "EvidenceSource",
    "AxisResult",
]
