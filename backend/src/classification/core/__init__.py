"""
Core data structures for classification.

- context.py: ClassificationContext with all DICOM tag parsers
- evidence.py: Evidence tracking and confidence calculation
- output.py: ClassificationResult matching database schema
"""

from .context import (
    ClassificationContext,
    parse_image_type,
    parse_scanning_sequence,
    parse_sequence_variant,
    parse_scan_options,
    parse_sequence_name,
)
from .evidence import (
    Evidence,
    EvidenceSource,
    AxisResult,
    EVIDENCE_WEIGHTS,
    calculate_confidence,
    select_best_candidate,
    get_implied_base,
    TECHNIQUE_IMPLIES_BASE,
)

# Note: EvidenceSource simplified in v3.1.1
# - TEXT_EXACT removed (use HIGH_VALUE_TOKEN for parsed tag flags)
# - TEXT_FUZZY renamed to TEXT_SEARCH (for text_search_blob patterns)
from .output import (
    ClassificationResult,
    create_excluded_result,
    create_localizer_result,
    REVIEW_REASON_CODES,
)

__all__ = [
    # Context
    "ClassificationContext",
    "parse_image_type",
    "parse_scanning_sequence",
    "parse_sequence_variant",
    "parse_scan_options",
    "parse_sequence_name",
    # Evidence
    "Evidence",
    "EvidenceSource",
    "AxisResult",
    "EVIDENCE_WEIGHTS",
    "calculate_confidence",
    "select_best_candidate",
    "get_implied_base",
    "TECHNIQUE_IMPLIES_BASE",
    # Output
    "ClassificationResult",
    "create_excluded_result",
    "create_localizer_result",
    "REVIEW_REASON_CODES",
]
