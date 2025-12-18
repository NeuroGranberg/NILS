"""
Classification Detectors

Individual axis detectors for the classification pipeline.

Each detector:
- Loads configuration from YAML
- Analyzes ClassificationContext
- Returns AxisResult with evidence and confidence

Available Detectors:
- ProvenanceDetector: Detects processing pipeline/provenance (10 types)
- TechniqueDetector: Detects MRI acquisition technique (41 techniques, 4 families)
- ModifierDetector: Detects sequence modifiers (14 modifiers, 2 exclusion groups)
- BaseContrastDetector: Detects base tissue contrast (10 base types)
- AccelerationDetector: Detects k-space acceleration (5 methods, additive)
- ConstructDetector: Detects computed constructs (35 constructs, 9 categories)
- ContrastDetector: Detects contrast agent status (pre/post contrast)
- BodyPartDetector: Detects spinal cord imaging (triggers review)
"""

from .base_detector import BaseDetector
from .provenance import ProvenanceDetector, ProvenanceResult
from .technique import TechniqueDetector, TechniqueResult
from .modifier import ModifierDetector, ModifierResult, ModifierMatch
from .base_contrast import BaseContrastDetector, BaseContrastResult
from .acceleration import AccelerationDetector, AccelerationResult, AccelerationDetectorOutput
from .construct import ConstructDetector, ConstructMatch, ConstructDetectorOutput
from .contrast import ContrastDetector, ContrastResult
from .body_part import BodyPartDetector, BodyPartResult

__all__ = [
    "BaseDetector",
    "ProvenanceDetector",
    "ProvenanceResult",
    "TechniqueDetector",
    "TechniqueResult",
    "ModifierDetector",
    "ModifierResult",
    "ModifierMatch",
    "BaseContrastDetector",
    "BaseContrastResult",
    "AccelerationDetector",
    "AccelerationResult",
    "AccelerationDetectorOutput",
    "ConstructDetector",
    "ConstructMatch",
    "ConstructDetectorOutput",
    "ContrastDetector",
    "ContrastResult",
    "BodyPartDetector",
    "BodyPartResult",
]
