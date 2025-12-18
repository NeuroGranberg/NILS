"""
Classification Branches

Each branch handles classification for a specific provenance type.
The pipeline routes to the appropriate branch based on detected provenance.

Branches are THIN ORCHESTRATORS that override only base contrast and
construct detection. All other detectors (Technique, Modifier, Acceleration)
run normally for all branches.

Branches:
- common.py: BranchResult dataclass and shared utilities
- swi.py: SWI taxonomy classification (Magnitude, Phase, SWI, MinIP, QSM)
- symri.py: SyMRI/Synthetic MRI classification (maps, synthetics, myelin)
- epimix.py: EPIMix multicontrast EPI classification (T1/T2-FLAIR, T2w, DWI, ADC, T2*w)

The rawrecon branch uses standard detectors directly (no overrides).

Version: 1.1.0
"""

from .common import (
    BranchResult,
    SWI_OUTPUT_TYPES,
    SYMRI_OUTPUT_TYPES,
    EPIMIX_OUTPUT_TYPES,
    DIXON_OUTPUT_TYPES,
)
from .swi import (
    apply_swi_logic,
    detect_swi_output_type,
    get_swi_output_info,
)
from .symri import (
    apply_symri_logic,
    detect_symri_output_type,
    get_symri_output_info,
    classify_symri_complex_data,
)
from .epimix import (
    apply_epimix_logic,
    detect_epimix_output_type,
    get_epimix_output_info,
)

__all__ = [
    # Common
    "BranchResult",
    "SWI_OUTPUT_TYPES",
    "SYMRI_OUTPUT_TYPES",
    "EPIMIX_OUTPUT_TYPES",
    "DIXON_OUTPUT_TYPES",
    # SWI branch
    "apply_swi_logic",
    "detect_swi_output_type",
    "get_swi_output_info",
    # SyMRI branch
    "apply_symri_logic",
    "detect_symri_output_type",
    "get_symri_output_info",
    "classify_symri_complex_data",
    # EPIMix branch
    "apply_epimix_logic",
    "detect_epimix_output_type",
    "get_epimix_output_info",
]
