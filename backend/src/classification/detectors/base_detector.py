"""
Base Detector Abstract Class

Provides common functionality for all classification detectors:
- YAML configuration loading
- Evidence creation helpers
- Abstract detect() method

Version: 1.0.0
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from ..core.context import ClassificationContext
from ..core.evidence import (
    AxisResult,
    Evidence,
    EvidenceSource,
    EVIDENCE_WEIGHTS,
)


class BaseDetector(ABC):
    """
    Abstract base class for all classification detectors.
    
    Provides:
    - YAML configuration loading
    - Helper methods for evidence creation
    - Abstract detect() method for subclasses
    
    Subclasses must implement:
    - detect(ctx) -> AxisResult
    - axis_name property
    """
    
    def __init__(self, yaml_path: Optional[Path] = None):
        """
        Initialize detector with optional YAML configuration.
        
        Args:
            yaml_path: Path to detection YAML file. If None, config will be empty.
        """
        self.config: Dict[str, Any] = {}
        self.yaml_path = yaml_path
        
        if yaml_path and yaml_path.exists():
            self.config = self._load_yaml(yaml_path)
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """
        Load YAML configuration file.
        
        Args:
            path: Path to YAML file
        
        Returns:
            Parsed YAML as dictionary
        """
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    
    @abstractmethod
    def detect(self, ctx: ClassificationContext) -> AxisResult:
        """
        Perform detection on the given context.
        
        Args:
            ctx: Classification context with all fingerprint data
        
        Returns:
            AxisResult with detected value, confidence, and evidence
        """
        pass
    
    @property
    @abstractmethod
    def axis_name(self) -> str:
        """
        Return the name of the axis this detector handles.
        
        Examples: "provenance", "technique", "base", "modifier"
        """
        pass
    
    # =========================================================================
    # Evidence Creation Helpers
    # =========================================================================
    
    def create_token_evidence(
        self,
        field: str,
        flag: str,
        target: str,
        description: Optional[str] = None
    ) -> Evidence:
        """
        Create evidence from a parsed DICOM token/flag.
        
        Uses HIGH_VALUE_TOKEN source with 0.95 weight.
        
        Args:
            field: Parser name (e.g., "parsed_image_type")
            flag: Flag name (e.g., "has_swi")
            target: Target classification value
            description: Optional custom description
        
        Returns:
            Evidence instance
        """
        return Evidence(
            source=EvidenceSource.HIGH_VALUE_TOKEN,
            field=field,
            value=flag,
            target=target,
            weight=EVIDENCE_WEIGHTS[EvidenceSource.HIGH_VALUE_TOKEN],
            description=description or f"{flag}=True in {field}",
        )
    
    def create_text_evidence(
        self,
        keyword: str,
        target: str,
        description: Optional[str] = None
    ) -> Evidence:
        """
        Create evidence from text_search_blob keyword match.
        
        Uses TEXT_SEARCH source with 0.75 weight.
        
        Args:
            keyword: Matched keyword
            target: Target classification value
            description: Optional custom description
        
        Returns:
            Evidence instance
        """
        return Evidence(
            source=EvidenceSource.TEXT_SEARCH,
            field="text_search_blob",
            value=keyword,
            target=target,
            weight=EVIDENCE_WEIGHTS[EvidenceSource.TEXT_SEARCH],
            description=description or f"Keyword '{keyword}' found in text_search_blob",
        )
    
    def check_parsed_flags(
        self,
        ctx: ClassificationContext,
        parser_name: str,
        flags: List[str],
        target: str
    ) -> List[Evidence]:
        """
        Check multiple flags in a parsed dictionary and create evidence for matches.
        
        Args:
            ctx: Classification context
            parser_name: Name of parser (e.g., "parsed_image_type")
            flags: List of flag names to check
            target: Target classification value
        
        Returns:
            List of Evidence for each matching flag
        """
        evidence: List[Evidence] = []
        
        # Map parser names to context properties
        parser_map = {
            "parsed_image_type": ctx.parsed_image_type,
            "parsed_scanning_sequence": ctx.parsed_scanning_sequence,
            "parsed_sequence_variant": ctx.parsed_sequence_variant,
            "parsed_scan_options": ctx.parsed_scan_options,
            "parsed_sequence_name": ctx.parsed_sequence_name,
        }
        
        parsed = parser_map.get(parser_name)
        if not parsed:
            return evidence
        
        for flag in flags:
            if parsed.get(flag, False):
                evidence.append(self.create_token_evidence(
                    field=parser_name,
                    flag=flag,
                    target=target,
                ))
        
        return evidence
    
    def calculate_confidence_from_evidence(
        self,
        evidence: List[Evidence],
        base_confidence: float = 0.80
    ) -> float:
        """
        Calculate confidence score from evidence list.
        
        Uses max evidence weight with boost for multiple sources.
        
        Args:
            evidence: List of evidence
            base_confidence: Default confidence if no evidence
        
        Returns:
            Confidence score (0.0 - 1.0)
        """
        if not evidence:
            return base_confidence
        
        # Use max evidence weight
        max_weight = max(e.weight for e in evidence)
        
        # Boost if multiple independent source types agree
        source_types = set(e.source for e in evidence)
        if len(source_types) >= 2:
            boost = 0.05 * (len(source_types) - 1)
            max_weight = min(max_weight + boost, 0.99)
        
        return max_weight
