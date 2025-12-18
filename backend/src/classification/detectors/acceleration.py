"""
Acceleration Detector for MRI sequences.

Detects k-space acceleration methods:
- ParallelImaging (GRAPPA/SENSE/ARC)
- SimultaneousMultiSlice (SMS/Multiband)
- PartialFourier (PFP/PFF)
- CompressedSensing
- ViewSharing (TWIST/TRICKS/Keyhole)

These are ADDITIVE - multiple accelerations can be detected simultaneously.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
import re
import yaml
from pathlib import Path

from classification.core.context import ClassificationContext
from classification.core.evidence import Evidence, EvidenceSource


@dataclass
class AccelerationResult:
    """Result for a single detected acceleration method."""
    
    name: str
    confidence: float
    detection_method: str
    evidence: List[Evidence] = field(default_factory=list)
    subtype: Optional[str] = None  # e.g., "phase" or "frequency" for PartialFourier
    
    @property
    def value(self) -> str:
        """Alias for name for consistency with other detectors."""
        return self.name


@dataclass
class AccelerationDetectorOutput:
    """Complete output from AccelerationDetector."""
    
    accelerations: List[AccelerationResult] = field(default_factory=list)
    
    @property
    def values(self) -> List[str]:
        """Get list of acceleration names."""
        return [a.name for a in self.accelerations]
    
    @property
    def has_acceleration(self) -> bool:
        """Whether any acceleration was detected."""
        return len(self.accelerations) > 0
    
    def has(self, name: str) -> bool:
        """Check if a specific acceleration was detected."""
        return name in self.values
    
    def get(self, name: str) -> Optional[AccelerationResult]:
        """Get a specific acceleration result by name."""
        for a in self.accelerations:
            if a.name == name:
                return a
        return None


class AccelerationDetector:
    """
    Detects k-space acceleration methods from DICOM metadata.
    
    Acceleration methods reduce acquisition time through various strategies:
    - ParallelImaging: Coil sensitivity encoding (GRAPPA/SENSE/ARC)
    - SimultaneousMultiSlice: Multiple slices excited at once (MB/SMS)
    - PartialFourier: Partial k-space acquisition (PFP/PFF)
    - CompressedSensing: Sparse reconstruction from undersampled data
    - ViewSharing: Temporal k-space sharing (TWIST/TRICKS)
    
    Output is a LIST of detected accelerations (can be empty or multiple).
    """
    
    # Confidence thresholds for different detection methods
    CONFIDENCE_THRESHOLDS = {
        "unified_flag": 0.95,
        "scan_options": 0.90,
        "dicom_tag": 0.90,
        "keywords": 0.80,
        "sequence_pattern": 0.75,
    }
    
    # =========================================================================
    # PARALLEL IMAGING
    # =========================================================================
    PI_KEYWORDS = [
        "grappa",
        "sense",
        "asset",
        "ipat",
        "msense",
        "speedup",
        "accelerat",  # matches acceleration, accelerated
    ]
    
    # Keywords that require word boundaries
    PI_BOUNDED_KEYWORDS = [
        r"\barc\b",      # avoid matching "search", "march", etc.
        r"\barc\[",      # ARC[2x1]
        r"_arc_",
        r"_arc\b",
    ]
    
    PI_EXCLUDE = {"hypersense"}  # Separate category
    
    # =========================================================================
    # SIMULTANEOUS MULTI-SLICE (SMS/Multiband)
    # =========================================================================
    SMS_KEYWORDS = [
        "multiband",
        "multib",
        "hyperband",
    ]
    
    SMS_BOUNDED_KEYWORDS = [
        r"\bmb\d",       # mb2, mb3, mb4, etc.
        r"\bmb\[",       # MB[3]
        r"\bmb=",
        r"_mb\d",
        r"_mb_",
        r"\bsms\b",
        r"_sms_",
        r"_sms\b",
    ]
    
    SMS_EXCLUDE = {
        "combat",      # contains "mb"
        "ambig",       # contains "mb" 
        "membrane",
        "chamber",
        "number",
        "symbol",
        "assembly",
        "climbing",
        "plumb",
    }
    
    SMS_SEQ_PATTERNS = [
        r"cmrr",         # CMRR multiband sequences
        r"hyperband",
    ]
    
    # =========================================================================
    # PARTIAL FOURIER
    # =========================================================================
    PF_KEYWORDS = [
        "partial fourier",
        "partial_fourier",
        "half fourier",
        "half-fourier",
        "halffourier",
    ]
    
    PF_BOUNDED_KEYWORDS = [
        r"\bpf\b",       # Standalone PF
        r"5/8",
        r"6/8",
        r"7/8",
    ]
    
    # =========================================================================
    # COMPRESSED SENSING
    # =========================================================================
    CS_KEYWORDS = [
        "compressed sensing",
        "compressed_sensing",
        "compressedsense",
        "sparse",
        "wave-caipi",
        "wave caipi",
        "caipi",
    ]
    
    CS_BOUNDED_KEYWORDS = [
        r"\bcs\[",       # CS[factor]
        r"_cs_",
        r"_cs\b",
    ]
    
    CS_EXCLUDE = {"csf", "csa"}  # Cerebrospinal fluid, CSA data
    
    # =========================================================================
    # VIEW SHARING
    # =========================================================================
    VS_KEYWORDS = [
        "twist",
        "tricks",
        "keyhole",
        "view sharing",
        "viewsharing",
        "disco",
        "4d-trak",
        "4dtrak",
        "time-resolved",
        "differential subsampling",
    ]
    
    VS_SEQ_PATTERNS = [
        r"fldyn",        # Dynamic with view sharing
    ]
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize AccelerationDetector.
        
        Args:
            config_path: Optional path to YAML config (for future use)
        """
        self._config_path = config_path
        self._config = None
        
        # Pre-compile regex patterns for performance
        self._pi_patterns = [re.compile(p, re.IGNORECASE) for p in self.PI_BOUNDED_KEYWORDS]
        self._sms_patterns = [re.compile(p, re.IGNORECASE) for p in self.SMS_BOUNDED_KEYWORDS]
        self._sms_seq_patterns = [re.compile(p, re.IGNORECASE) for p in self.SMS_SEQ_PATTERNS]
        self._pf_patterns = [re.compile(p, re.IGNORECASE) for p in self.PF_BOUNDED_KEYWORDS]
        self._cs_patterns = [re.compile(p, re.IGNORECASE) for p in self.CS_BOUNDED_KEYWORDS]
        self._vs_seq_patterns = [re.compile(p, re.IGNORECASE) for p in self.VS_SEQ_PATTERNS]
    
    def detect(self, ctx: ClassificationContext) -> AccelerationDetectorOutput:
        """
        Detect all acceleration methods from context.
        
        Args:
            ctx: Classification context with DICOM metadata
            
        Returns:
            AccelerationDetectorOutput with list of detected accelerations
        """
        results: List[AccelerationResult] = []
        
        uf = ctx.unified_flags
        pso = ctx.parsed_scan_options
        text_blob = (ctx.text_search_blob or "").lower()
        seq_name = (ctx.stack_sequence_name or "").lower()
        
        # Detect each acceleration type
        pi_result = self._detect_parallel_imaging(uf, pso, text_blob, ctx)
        if pi_result:
            results.append(pi_result)
        
        sms_result = self._detect_sms(uf, text_blob, seq_name)
        if sms_result:
            results.append(sms_result)
        
        pf_results = self._detect_partial_fourier(uf, pso, text_blob)
        results.extend(pf_results)
        
        cs_result = self._detect_compressed_sensing(pso, text_blob, seq_name)
        if cs_result:
            results.append(cs_result)
        
        vs_result = self._detect_view_sharing(text_blob, seq_name)
        if vs_result:
            results.append(vs_result)
        
        return AccelerationDetectorOutput(accelerations=results)
    
    def _detect_parallel_imaging(
        self,
        uf: Dict[str, bool],
        pso: Dict[str, bool],
        text_blob: str,
        ctx: ClassificationContext,
    ) -> Optional[AccelerationResult]:
        """
        Detect Parallel Imaging (GRAPPA/SENSE/ARC/ASSET).
        """
        evidence_list: List[Evidence] = []
        
        # Priority 1: unified_flags
        if uf.get("has_parallel_imaging"):
            # Check which specific type
            sources = []
            if pso.get("has_parallel_gems"):
                sources.append("ACC_GEMS")
            if pso.get("has_hypersense"):
                sources.append("HYPERSENSE")
            if pso.get("has_cs_gems"):
                sources.append("CS_GEMS")
            
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=f"has_parallel_imaging ({', '.join(sources) if sources else 'true'})",
                target="ParallelImaging",
                weight=self.CONFIDENCE_THRESHOLDS["unified_flag"],
                description="Parallel imaging detected from scan options",
            )
            return AccelerationResult(
                name="ParallelImaging",
                confidence=self.CONFIDENCE_THRESHOLDS["unified_flag"],
                detection_method="unified_flag",
                evidence=[evidence],
            )
        
        # Priority 2: mr_parallel_acquisition_technique (if available)
        # This would come from ctx if we parse that field
        
        # Priority 3: Keywords
        # Check for exclusions first
        if any(ex in text_blob for ex in self.PI_EXCLUDE):
            # Still check for other PI keywords
            pass
        
        # Check simple keywords
        for kw in self.PI_KEYWORDS:
            if kw in text_blob:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=kw,
                    target="ParallelImaging",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"Parallel imaging keyword: {kw}",
                )
                return AccelerationResult(
                    name="ParallelImaging",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword",
                    evidence=[evidence],
                )
        
        # Check bounded patterns
        for pattern in self._pi_patterns:
            match = pattern.search(text_blob)
            if match:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=match.group(),
                    target="ParallelImaging",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"Parallel imaging pattern: {match.group()}",
                )
                return AccelerationResult(
                    name="ParallelImaging",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword_pattern",
                    evidence=[evidence],
                )
        
        return None
    
    def _detect_sms(
        self,
        uf: Dict[str, bool],
        text_blob: str,
        seq_name: str,
    ) -> Optional[AccelerationResult]:
        """
        Detect Simultaneous Multi-Slice (SMS/Multiband).
        """
        # Check for exclusions first
        for ex in self.SMS_EXCLUDE:
            if ex in text_blob:
                # Remove the exclusion word and continue checking
                text_blob = text_blob.replace(ex, " ")
        
        # Check simple keywords
        for kw in self.SMS_KEYWORDS:
            if kw in text_blob:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=kw,
                    target="SimultaneousMultiSlice",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"SMS/Multiband keyword: {kw}",
                )
                return AccelerationResult(
                    name="SimultaneousMultiSlice",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword",
                    evidence=[evidence],
                )
        
        # Check bounded patterns
        for pattern in self._sms_patterns:
            match = pattern.search(text_blob)
            if match:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=match.group(),
                    target="SimultaneousMultiSlice",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"SMS/Multiband pattern: {match.group()}",
                )
                return AccelerationResult(
                    name="SimultaneousMultiSlice",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword_pattern",
                    evidence=[evidence],
                )
        
        # Check sequence name patterns
        for pattern in self._sms_seq_patterns:
            match = pattern.search(seq_name)
            if match:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="stack_sequence_name",
                    value=match.group(),
                    target="SimultaneousMultiSlice",
                    weight=self.CONFIDENCE_THRESHOLDS["sequence_pattern"],
                    description=f"SMS sequence pattern: {match.group()}",
                )
                return AccelerationResult(
                    name="SimultaneousMultiSlice",
                    confidence=self.CONFIDENCE_THRESHOLDS["sequence_pattern"],
                    detection_method="sequence_pattern",
                    evidence=[evidence],
                )
        
        return None
    
    def _detect_partial_fourier(
        self,
        uf: Dict[str, bool],
        pso: Dict[str, bool],
        text_blob: str,
    ) -> List[AccelerationResult]:
        """
        Detect Partial Fourier (PFP/PFF).
        
        Returns up to 2 results (phase and frequency can be separate).
        """
        results: List[AccelerationResult] = []
        
        # Priority 1: unified_flags
        if uf.get("has_partial_fourier"):
            # Determine subtype(s)
            has_phase = pso.get("has_partial_fourier_phase", False)
            has_freq = pso.get("has_partial_fourier_freq", False)
            
            subtypes = []
            if has_phase:
                subtypes.append("phase")
            if has_freq:
                subtypes.append("frequency")
            
            # Create single result with subtype info
            subtype_str = "+".join(subtypes) if subtypes else None
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=f"has_partial_fourier (PFP={has_phase}, PFF={has_freq})",
                target="PartialFourier",
                weight=self.CONFIDENCE_THRESHOLDS["unified_flag"],
                description=f"Partial Fourier: {subtype_str or 'detected'}",
            )
            results.append(AccelerationResult(
                name="PartialFourier",
                confidence=self.CONFIDENCE_THRESHOLDS["unified_flag"],
                detection_method="unified_flag",
                evidence=[evidence],
                subtype=subtype_str,
            ))
            return results
        
        # Priority 2: Keywords
        for kw in self.PF_KEYWORDS:
            if kw in text_blob:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=kw,
                    target="PartialFourier",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"Partial Fourier keyword: {kw}",
                )
                results.append(AccelerationResult(
                    name="PartialFourier",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword",
                    evidence=[evidence],
                ))
                return results
        
        # Check bounded patterns
        for pattern in self._pf_patterns:
            match = pattern.search(text_blob)
            if match:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=match.group(),
                    target="PartialFourier",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"Partial Fourier pattern: {match.group()}",
                )
                results.append(AccelerationResult(
                    name="PartialFourier",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword_pattern",
                    evidence=[evidence],
                ))
                return results
        
        return results
    
    def _detect_compressed_sensing(
        self,
        pso: Dict[str, bool],
        text_blob: str,
        seq_name: str,
    ) -> Optional[AccelerationResult]:
        """
        Detect Compressed Sensing.
        """
        # Priority 1: scan_options flags
        if pso.get("has_cs_gems"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="scan_options",
                value="CS_GEMS",
                target="CompressedSensing",
                weight=self.CONFIDENCE_THRESHOLDS["scan_options"],
                description="GE Compressed Sensing flag",
            )
            return AccelerationResult(
                name="CompressedSensing",
                confidence=self.CONFIDENCE_THRESHOLDS["scan_options"],
                detection_method="scan_options",
                evidence=[evidence],
            )
        
        # Check for exclusions
        for ex in self.CS_EXCLUDE:
            if ex in text_blob:
                text_blob = text_blob.replace(ex, " ")
        
        # Priority 2: Keywords
        for kw in self.CS_KEYWORDS:
            if kw in text_blob:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=kw,
                    target="CompressedSensing",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"Compressed Sensing keyword: {kw}",
                )
                return AccelerationResult(
                    name="CompressedSensing",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword",
                    evidence=[evidence],
                )
        
        # Check bounded patterns
        for pattern in self._cs_patterns:
            match = pattern.search(text_blob)
            if match:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=match.group(),
                    target="CompressedSensing",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"Compressed Sensing pattern: {match.group()}",
                )
                return AccelerationResult(
                    name="CompressedSensing",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword_pattern",
                    evidence=[evidence],
                )
        
        return None
    
    def _detect_view_sharing(
        self,
        text_blob: str,
        seq_name: str,
    ) -> Optional[AccelerationResult]:
        """
        Detect View Sharing (TWIST/TRICKS/Keyhole).
        """
        # Check keywords
        for kw in self.VS_KEYWORDS:
            if kw in text_blob:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=kw,
                    target="ViewSharing",
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"View Sharing keyword: {kw}",
                )
                return AccelerationResult(
                    name="ViewSharing",
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keyword",
                    evidence=[evidence],
                )
        
        # Check sequence name patterns
        for pattern in self._vs_seq_patterns:
            match = pattern.search(seq_name)
            if match:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="stack_sequence_name",
                    value=match.group(),
                    target="ViewSharing",
                    weight=self.CONFIDENCE_THRESHOLDS["sequence_pattern"],
                    description=f"View Sharing sequence pattern: {match.group()}",
                )
                return AccelerationResult(
                    name="ViewSharing",
                    confidence=self.CONFIDENCE_THRESHOLDS["sequence_pattern"],
                    detection_method="sequence_pattern",
                    evidence=[evidence],
                )
        
        return None
    
    def explain_detection(self, ctx: ClassificationContext) -> str:
        """
        Generate human-readable explanation of detection results.
        
        Args:
            ctx: Classification context
            
        Returns:
            Formatted string explaining detection
        """
        output = self.detect(ctx)
        
        if not output.has_acceleration:
            return "No acceleration methods detected."
        
        lines = [f"Detected {len(output.accelerations)} acceleration method(s):"]
        
        for accel in output.accelerations:
            lines.append(f"\n  {accel.name}:")
            lines.append(f"    Confidence: {accel.confidence:.0%}")
            lines.append(f"    Method: {accel.detection_method}")
            if accel.subtype:
                lines.append(f"    Subtype: {accel.subtype}")
            for ev in accel.evidence:
                lines.append(f"    Evidence: {ev.description}")
        
        return "\n".join(lines)
    
    @staticmethod
    def get_all_accelerations() -> List[str]:
        """Get list of all possible acceleration types."""
        return [
            "ParallelImaging",
            "SimultaneousMultiSlice",
            "PartialFourier",
            "CompressedSensing",
            "ViewSharing",
        ]
