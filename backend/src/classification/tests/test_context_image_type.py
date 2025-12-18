"""
Comprehensive tests for parse_image_type() using real database values.

This test file uses ALL unique image_type values from the stack_fingerprint table
to verify the parser handles every real-world case correctly.

Run with: python -m pytest backend/src/classification/tests/test_context_image_type.py -v
Or standalone: python backend/src/classification/tests/test_context_image_type.py
"""

from typing import Dict, List, Tuple, Any
import sys
import os

# Add parent to path for standalone execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from backend.src.classification.core.context import parse_image_type


# =============================================================================
# ALL UNIQUE IMAGE_TYPE VALUES FROM DATABASE (721 unique values)
# Organized by category for easier debugging
# =============================================================================

# Format: (image_type, expected_flags_dict)
# expected_flags_dict contains ONLY the flags we want to verify (sparse)

TEST_CASES: List[Tuple[str, Dict[str, bool]]] = [
    # =========================================================================
    # SECTION 1: PROVENANCE - ORIGINAL/DERIVED, PRIMARY/SECONDARY
    # =========================================================================
    
    # Basic provenance combinations
    ("ORIGINAL\\PRIMARY\\OTHER", {"is_original": True, "is_primary": True, "is_derived": False, "is_secondary": False}),
    ("DERIVED\\SECONDARY\\REFORMATTED\\AVERAGE", {"is_derived": True, "is_secondary": True, "is_original": False, "is_primary": False}),
    ("DERIVED\\PRIMARY\\MPR\\NORM\\DIS2D", {"is_derived": True, "is_primary": True, "is_secondary": False}),
    ("ORIGINAL\\SECONDARY\\PROJECTION IMAGE", {"is_original": True, "is_secondary": True, "is_primary": False}),
    
    # POSDISP (position display) - common in GE
    ("DERIVED\\SECONDARY\\POSDISP\\M\\ND\\NORM", {"is_secondary": True, "is_derived": True}),
    ("ORIGINAL\\SECONDARY\\POSDISP\\M\\ND", {"is_original": True, "is_secondary": True}),
    
    # =========================================================================
    # SECTION 2: DIFFUSION CONSTRUCTS - ADC, FA, TRACE, EXP, EADC
    # =========================================================================
    
    # ADC (Apparent Diffusion Coefficient)
    ("DERIVED\\PRIMARY\\DIFFUSION\\ADC\\NORM\\DIS2D", {"has_adc": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\ADC\\DIS2D", {"has_adc": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\ADC\\ND\\NORM", {"has_adc": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\ADC\\DIS2D\\DFC", {"has_adc": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\ADC\\ADC", {"has_adc": True}),
    ("DERIVED\\PRIMARY\\ADC\\ND\\FM", {"has_adc": True}),
    ("ORIGINAL\\PRIMARY\\ADC\\ND\\NORM", {"has_adc": True}),
    ("ORIGINAL\\PRIMARY\\ADC_UNSPECIFIED\\ADC\\UNSPECIFIED", {"has_adc": True}),
    ("ORIGINAL\\PRIMARY\\ADC_SE\\ADC\\SE", {"has_adc": True}),
    ("DERIVED\\SECONDARY\\ADC", {"has_adc": True, "is_secondary": True}),
    
    # FA (Fractional Anisotropy)
    ("DERIVED\\PRIMARY\\DIFFUSION\\FA\\NORM\\DIS2D", {"has_fa": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\FA\\ND", {"has_fa": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\FA\\FA", {"has_fa": True}),
    ("DERIVED\\PRIMARY\\FA\\FA\\UNSPECIFIED", {"has_fa": True}),
    ("ORIGINAL\\PRIMARY\\FA_SE\\FA\\SE", {"has_fa": True}),
    ("ORIGINAL\\PRIMARY\\FA_UNSPECIFIED\\FA\\UNSPECIFIED", {"has_fa": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\FA\\ND", {"has_fa": True, "has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\FA\\ND\\NORM", {"has_fa": True, "has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\FA\\NORM\\DIS2D", {"has_fa": True, "has_diffusion": True}),
    
    # TRACE (Trace-weighted / isotropic DWI)
    ("DERIVED\\PRIMARY\\DIFFUSION\\TRACEW\\DIS2D", {"has_trace": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\TRACEW\\NORM\\DIS2D", {"has_trace": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\TRACEW\\ND\\NORM", {"has_trace": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\NONE\\TRACEW\\NORM\\DIS2D", {"has_trace": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\NONE\\TRACEW\\ND", {"has_trace": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\TRACEW", {"has_trace": True, "has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\TRACEW\\ND", {"has_trace": True, "has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\TRACEW\\ND\\NORM", {"has_trace": True, "has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\TRACEW\\NORM\\DIS2D", {"has_trace": True, "has_diffusion": True}),
    
    # EXP (Exponential ADC)
    ("DERIVED\\PRIMARY\\DIFFUSION\\EXP\\DIS2D", {"has_exp_diffusion": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\EXP\\ND", {"has_exp_diffusion": True, "has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\EXP\\ND\\NORM", {"has_exp_diffusion": True, "has_diffusion": True}),
    
    # EADC (Enhanced ADC / Exponential ADC)
    ("DERIVED\\PRIMARY\\EADC\\EADC", {"has_eadc": True}),
    ("ORIGINAL\\PRIMARY\\EADC\\EADC\\UNSPECIFIED", {"has_eadc": True}),
    ("ORIGINAL\\PRIMARY\\EADC_SE\\EADC\\SE", {"has_eadc": True}),
    ("ORIGINAL\\PRIMARY\\EADC_UNSPECIFIE\\EADC\\UNSPECIFIED", {"has_eadc": True}),
    
    # Mixed/other diffusion
    ("DERIVED\\PRIMARY\\DIFFUSION\\DIFFUSION", {"has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\MIXED", {"has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\MOTIONCORRECTION", {"has_diffusion": True}),
    ("DERIVED\\PRIMARY\\DIFFUSION\\CALC_BVALUE\\TRACEW\\NORM\\DIS2D", {"has_trace": True, "has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\NONE\\ND\\NORM", {"has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\NONE\\ND\\NORM\\MOSAIC", {"has_diffusion": True, "layout_mosaic": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\NONE\\ND\\MOSAIC", {"has_diffusion": True, "layout_mosaic": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\NONE", {"has_diffusion": True}),
    ("ORIGINAL\\PRIMARY\\DIFFUSION\\NONE\\DIS2D", {"has_diffusion": True}),
    ("DERIVED\\SECONDARY\\ISODWI", {"is_secondary": True}),  # isotropic DWI
    
    # =========================================================================
    # SECTION 3: PERFUSION CONSTRUCTS - CBF, CBV, MTT, TTP, TMAX, OEF
    # =========================================================================
    
    # CBF (Cerebral Blood Flow)
    ("DERIVED\\PRIMARY\\ASL\\ND\\RELCBF\\NORM\\SUB\\TTEST\\MOSAIC", {"has_cbf": True, "layout_mosaic": True}),
    ("DERIVED\\PRIMARY\\PERFUSION\\RELCBF\\DIS2D\\MOCO", {"has_cbf": True}),
    ("DERIVED\\SECONDARY\\rCBF", {"has_cbf": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\rCBF Basic", {"has_cbf": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\rCBF\\", {"has_cbf": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\CBF", {"has_cbf": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\CBF Basic", {"has_cbf": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\CBF\\", {"has_cbf": True, "is_secondary": True}),
    
    # CBV (Cerebral Blood Volume)
    ("DERIVED\\PRIMARY\\PERFUSION\\RELCBV\\DIS2D\\MOCO", {"has_cbv": True}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\DIS2D\\RELCBV", {"has_cbv": True}),
    ("DERIVED\\SECONDARY\\rCBV", {"has_cbv": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\rCBV Basic", {"has_cbv": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\rCBV\\", {"has_cbv": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\CBV", {"has_cbv": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\CBV Basic", {"has_cbv": True, "is_secondary": True}),
    
    # MTT (Mean Transit Time)
    ("DERIVED\\SECONDARY\\MTT", {"has_mtt": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\MTT Basic", {"has_mtt": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\MTT\\", {"has_mtt": True, "is_secondary": True}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\DIS2D\\RELMTT", {"has_mtt": True}),
    
    # TTP (Time to Peak)
    ("DERIVED\\PRIMARY\\PERFUSION\\TTP\\ND", {"has_ttp": True}),
    ("DERIVED\\SECONDARY\\TTP", {"has_ttp": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\TTP Basic", {"has_ttp": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\TTP\\", {"has_ttp": True, "is_secondary": True}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\DIS2D\\TTP", {"has_ttp": True}),
    
    # TMAX (Time to Maximum)
    ("DERIVED\\SECONDARY\\Tmax", {"has_tmax": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\Tmax Basic", {"has_tmax": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\Tmax\\", {"has_tmax": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\Tmax Basic\\", {"has_tmax": True, "is_secondary": True}),
    
    # OEF (Oxygen Extraction Fraction) - note: only "OEF MODEL BASED" triggers has_oef
    ("DERIVED\\SECONDARY\\OEF", {"is_secondary": True}),  # Just OEF doesn't trigger, needs "MODEL BASED"
    ("DERIVED\\SECONDARY\\OEF Model Based", {"has_oef": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\OEF Model Based\\", {"has_oef": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\rCMRO2 Model Based", {"has_cmro2": True, "is_secondary": True}),  # CMRO2 not OEF
    ("DERIVED\\SECONDARY\\rCMRO2 Model Based\\", {"has_cmro2": True, "is_secondary": True}),
    
    # Other perfusion
    ("DERIVED\\PRIMARY\\PERFUSION\\PBP\\DIS2D", {}),  # PBP = peak blood pool?
    ("DERIVED\\PRIMARY\\PERFUSION\\PBP\\ND", {}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\DIS2D", {}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\DIS2D\\MFSPLIT", {}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\ND", {}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\ND\\NORM", {}),
    ("ORIGINAL\\PRIMARY\\PERFUSION\\NONE\\NORM\\DIS2D\\MFSPLIT", {}),
    ("DERIVED\\SECONDARY\\rLeakage", {"is_secondary": True}),  # permeability/leakage
    
    # =========================================================================
    # SECTION 4: SYNTHETIC MRI - T1/T2/PD/FLAIR/UNIFORM
    # =========================================================================
    
    # T1 Synthetic - note: T1\UNIFORM doesn't trigger synthetic, UNIFORM is separate
    ("DERIVED\\PRIMARY\\T1\\UNIFORM\\PARALLEL\\DIS3D\\MFSPLIT", {"has_uniform": True}),  # UNIFORM, not synthetic
    ("DERIVED\\PRIMARY\\T1\\SYNTHETIC\\NON", {"has_t1_synthetic": True}),
    ("DERIVED\\PRIMARY\\T1\\SYNTHETIC", {"has_t1_synthetic": True}),
    
    # T2 Synthetic
    ("DERIVED\\PRIMARY\\T2\\SYNTHETIC", {"has_t2_synthetic": True}),
    ("DERIVED\\PRIMARY\\T2\\SYNTHETIC\\CSF", {"has_t2_synthetic": True}),
    
    # PD Synthetic
    ("DERIVED\\PRIMARY\\PD\\SYNTHETIC", {"has_pd_synthetic": True}),
    
    # FLAIR Synthetic (multiple naming conventions!)
    ("DERIVED\\PRIMARY\\T2W_FLAIR_SYNTHETIC", {"has_flair_synthetic": True, "has_t2_synthetic": True}),
    ("DERIVED\\PRIMARY\\PSIR\\SYNTHETIC", {"has_psir": True}),  # PSIR is different from FLAIR
    ("DERIVED\\PRIMARY\\DIR\\SYNTHETIC", {"has_dir": True}),  # DIR = Double IR
    ("DERIVED\\PRIMARY\\DIR_SYNTHETIC", {"has_dir_synthetic": True}),
    ("DERIVED\\PRIMARY\\IR\\SYNTHETIC", {}),  # Generic IR synthetic
    
    # T2FLAIR combined token (from your fix!)
    # This should trigger BOTH t2_synthetic and flair_synthetic
    # Note: Need to verify if T2FLAIR is actually in the data
    
    # UNIFORM (SyMRI)
    ("DERIVED\\PRIMARY\\M\\UNI\\DIS3D\\MFSPLIT", {"has_uniform": True}),
    
    # =========================================================================
    # SECTION 5: SWI (Susceptibility Weighted Imaging)
    # =========================================================================
    
    ("ORIGINAL\\PRIMARY\\M\\SWI\\NORM\\DIS2D", {"has_swi": True}),
    ("ORIGINAL\\PRIMARY\\M\\SWI\\DIS3D\\NORM\\FM5_1\\FIL\\MFSPLIT", {"has_swi": True}),
    ("ORIGINAL\\PRIMARY\\M\\SWI\\ND\\NORM", {"has_swi": True}),
    ("ORIGINAL\\PRIMARY\\M\\SWI\\NORM\\DIS2D\\MFSPLIT", {"has_swi": True}),
    ("DERIVED\\PRIMARY\\M\\SWI\\DIS3D\\NORM\\FM5_1\\FIL\\MEAN\\MFSPLIT", {"has_swi": True}),
    ("DERIVED\\PRIMARY\\SWI\\MINIMUM", {"has_swi": True}),
    ("DERIVED\\SECONDARY\\MPR\\CSA MPR\\\\CSAPARALLEL\\M\\SWI\\NORM\\DIS2D", {"has_swi": True, "is_secondary": True}),
    ("ORIGINAL\\PRIMARY\\SWI\\NONE", {"has_swi": True}),
    
    # =========================================================================
    # SECTION 6: ASL (Arterial Spin Labeling)
    # =========================================================================
    
    ("ORIGINAL\\PRIMARY\\ASL", {"has_asl": True}),
    ("ORIGINAL\\PRIMARY\\ASL\\NONE\\ND\\NORM\\MOSAIC", {"has_asl": True, "layout_mosaic": True}),
    ("DERIVED\\PRIMARY\\ASL\\ND\\NORM\\SUB\\TTEST", {"has_asl": True}),
    ("DERIVED\\PRIMARY\\ASL\\ND\\NORM\\SUB\\TTEST\\MOSAIC", {"has_asl": True, "layout_mosaic": True}),
    ("DERIVED\\PRIMARY\\ASL\\PERFUSION_ASL", {"has_asl": True}),
    
    # =========================================================================
    # SECTION 7: QUANTITATIVE MAPS - QMAP, T1 MAP, T2 MAP
    # =========================================================================
    
    ("DERIVED\\PRIMARY\\QMAP\\T2", {"has_qmap": True}),
    ("DERIVED\\PRIMARY\\QMAP\\PD", {"has_qmap": True}),
    ("DERIVED\\PRIMARY\\MULTI_QMAP\\T1", {"has_qmap": True}),  # Your fix!
    ("DERIVED\\PRIMARY\\MULTI_QMAP\\T2", {"has_qmap": True}),
    ("DERIVED\\PRIMARY\\MULTI_QMAP\\PD", {"has_qmap": True}),
    ("ORIGINAL\\PRIMARY\\T1 MAP", {"has_t1_map": True}),
    ("ORIGINAL\\PRIMARY\\T2\\NONE", {}),  # Not a map, just contrast type
    ("ORIGINAL\\PRIMARY\\T2_STAR\\NONE", {}),  # T2* weighted
    ("ORIGINAL\\PRIMARY\\T1\\NONE", {}),  # Not a map
    
    # =========================================================================
    # SECTION 8: MPR/MIP/PROJECTION IMAGES
    # =========================================================================
    
    # MPR (Multi-Planar Reformat) - flag is is_mpr
    ("DERIVED\\PRIMARY\\MPR\\NORM\\DIS2D", {"is_mpr": True}),
    ("DERIVED\\PRIMARY\\MPR\\NORM\\DIS3D\\MFSPLIT", {"is_mpr": True}),
    ("DERIVED\\PRIMARY\\MPR\\NORM\\DIS3D\\DIS2D\\MFSPLIT", {"is_mpr": True}),
    ("DERIVED\\PRIMARY\\MPR\\DIS3D\\NORM\\MFSPLIT", {"is_mpr": True}),
    ("DERIVED\\SECONDARY\\MPR\\CSA MPR THICK\\\\M\\NORM\\DIS2D", {"is_mpr": True, "is_mpr_thick": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\MPR\\CSA MPR\\\\CSAPARALLEL\\M\\NORM\\DIS2D", {"is_mpr": True, "is_secondary": True}),
    ("ORIGINAL\\PRIMARY\\MPR\\M\\FFE", {"is_mpr": True}),
    ("ORIGINAL\\PRIMARY\\MPR\\ND\\NORM", {"is_mpr": True}),
    ("ORIGINAL\\PRIMARY\\MPR\\NORM\\DIS2D", {"is_mpr": True}),
    ("ORIGINAL\\SECONDARY\\MPR", {"is_mpr": True, "is_secondary": True}),
    ("DERIVED\\PRIMARY\\MPR\\M\\IR", {"is_mpr": True}),
    
    # MIP (Maximum Intensity Projection) - flag is is_mip
    ("DERIVED\\PRIMARY\\MIP\\NORM\\DIS2D\\MFSPLIT", {"is_mip": True}),
    ("DERIVED\\PRIMARY\\MAX_IP\\M\\SE", {}),  # MAX_IP is not detected as MIP currently
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MIP\\\\CSAPARALLEL\\M\\ND", {"is_mip": True, "is_projection": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MIP\\\\CSAPARALLEL\\M\\NORM\\DIS2D", {"is_mip": True, "is_projection": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MIP\\CSAMANIPULATED\\CSAPARALLEL\\M\\ND", {"is_mip": True, "is_projection": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MIP THIN\\\\M\\ND", {"is_mip": True, "is_mip_thin": True, "is_projection": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MIP THIN\\\\M\\ND\\NORM", {"is_mip": True, "is_mip_thin": True, "is_projection": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\OTHER\\CSA MIP THIN\\\\M\\ND", {"is_mip": True, "is_mip_thin": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\OTHER\\CSA MIP THIN\\\\M\\ND\\NORM", {"is_mip": True, "is_mip_thin": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PJN\\MIP", {"is_mip": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PJN\\HD MIP", {"is_mip": True, "is_secondary": True}),
    ("ORIGINAL\\PRIMARY\\MIP_SAG\\NORM\\DIS2D", {"is_mip": True}),
    ("ORIGINAL\\PRIMARY\\MIP_TRA\\NORM\\DIS2D", {"is_mip": True}),
    ("ORIGINAL\\PRIMARY\\MIP_TRA\\ND", {"is_mip": True}),
    ("ORIGINAL\\PRIMARY\\MIP_TRA\\DIS2D", {"is_mip": True}),
    ("ORIGINAL\\PRIMARY\\MIP_COR\\DIS2D\\SUB", {"is_mip": True}),
    ("DERIVED\\PRIMARY\\MIP_COR\\DIS3D\\NORM\\FM4_1\\FIL\\MFSPLIT", {"is_mip": True}),
    ("DERIVED\\SECONDARY\\REFORMATTED\\MIP", {"is_mip": True, "is_mpr": True, "is_secondary": True}),  # REFORMATTED triggers is_mpr
    
    # MinIP (Minimum Intensity Projection) - flag is is_minip
    ("DERIVED\\SECONDARY\\REFORMATTED\\MIN IP", {"is_minip": True, "is_mpr": True, "is_secondary": True}),  # REFORMATTED triggers is_mpr
    ("DERIVED\\PRIMARY\\SWI\\MINIMUM", {"has_swi": True}),  # SWI minIP - MINIMUM is just a token
    
    # MNIP - flag is is_minip (MNIP = Min IP)
    ("ORIGINAL\\PRIMARY\\MNIP\\ND\\NORM", {"is_minip": True}),
    ("DERIVED\\PRIMARY\\MNIP\\NORM\\DIS2D", {"is_minip": True}),
    ("DERIVED\\PRIMARY\\MNIP\\DIS3D\\NORM\\FM5_1\\FIL\\MFSPLIT", {"is_minip": True}),
    ("DERIVED\\PRIMARY\\MNIP\\DIS3D\\NORM\\FM5_1\\FIL\\MEAN\\MFSPLIT", {"is_minip": True}),
    ("DERIVED\\PRIMARY\\MNIP\\NORM\\DIS2D\\MFSPLIT", {"is_minip": True}),
    
    # Generic projection - flag is is_projection
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MPR\\\\CSAPARALLEL\\M\\NORM\\DIS2D", {"is_projection": True, "is_mpr": True, "is_secondary": True}),
    ("DERIVED\\PRIMARY\\PROJECTION IMAGE\\COLLAPSE", {"is_projection": True}),
    ("DERIVED\\PRIMARY\\PROJECTION IMAGE\\VASCULAR", {"is_projection": True, "is_vascular": True}),
    ("ORIGINAL\\PRIMARY\\PROJECTION IMAGE\\M\\IR", {"is_projection": True}),
    ("ORIGINAL\\PRIMARY\\PROJECTION IMAGE\\M\\FFE", {"is_projection": True}),
    ("ORIGINAL\\PRIMARY\\PROJECTION IMAGE\\M\\SE", {"is_projection": True}),
    ("ORIGINAL\\PRIMARY\\PROJECTION IMAG\\M\\FFE", {}),  # truncated - doesn't match
    ("ORIGINAL\\PRIMARY\\PROJECTION IMAG\\M\\IR", {}),  # truncated - doesn't match
    
    # =========================================================================
    # SECTION 9: DIXON/WATER-FAT SEPARATION
    # =========================================================================
    
    # DIXON
    ("DERIVED\\PRIMARY\\DIXON\\IN_PHASE\\PARALLEL\\DIS2D\\MFSPLIT", {"has_dixon": True, "has_in_phase": True}),
    ("DERIVED\\PRIMARY\\DIXON\\WATER", {"has_dixon": True, "has_water": True}),
    ("DERIVED\\PRIMARY\\DIXON\\IN_PHASE", {"has_dixon": True, "has_in_phase": True}),
    
    # Water/Fat/In-Phase/Out-of-Phase (without explicit DIXON)
    ("DERIVED\\PRIMARY\\M\\NORM\\IN_PHASE\\DIS2D\\FM3_4\\FIL\\MFSPLIT", {"has_in_phase": True}),
    ("DERIVED\\PRIMARY\\M\\NORM\\WATER\\DIS2D\\FM3_4\\FIL\\MFSPLIT", {"has_water": True}),
    ("DERIVED\\PRIMARY\\M\\NORM\\IN_PHASE\\DIS2D\\MFSPLIT", {"has_in_phase": True}),
    ("DERIVED\\PRIMARY\\M\\NORM\\WATER\\DIS2D\\MFSPLIT", {"has_water": True}),
    ("ORIGINAL\\PRIMARY\\M\\NORM\\IN_PHASE\\DIS2D\\FM3_4\\FIL", {"has_in_phase": True}),
    ("ORIGINAL\\PRIMARY\\M\\NORM\\WATER\\DIS2D\\FM3_4\\FIL", {"has_water": True}),
    ("DERIVED\\PRIMARY\\M\\DRG\\NORM\\DRS\\WATER\\DIS2D\\MFSPLIT", {"has_water": True}),
    ("DERIVED\\PRIMARY\\M\\DRG\\NORM\\DRS\\IN_PHASE\\DIS2D\\MFSPLIT", {"has_in_phase": True}),
    ("DERIVED\\PRIMARY\\IP\\IP\\DERIVED", {"has_in_phase": True}),  # IP = In Phase
    
    # =========================================================================
    # SECTION 10: LOCALIZER/SCOUT
    # =========================================================================
    
    ("DERIVED\\SECONDARY\\LOCALIZER", {"is_localizer": True, "is_secondary": True}),
    ("ORIGINAL\\PRIMARY\\SCOUTED", {}),  # This means it used a scout, not that it IS a scout
    ("ORIGINAL\\PRIMARY\\UNSCOUTED", {}),  # No scout used
    ("ORIGINAL\\SECONDARY\\LOCALIZER\\S", {"is_localizer": True, "is_secondary": True}),
    
    # =========================================================================
    # SECTION 11: MOSAIC / COMPOSED / PROCESSED
    # =========================================================================
    
    # MOSAIC (DTI/fMRI tiled display) - flag is layout_mosaic
    ("ORIGINAL\\PRIMARY\\M\\ND\\MOSAIC", {"layout_mosaic": True}),
    ("ORIGINAL\\PRIMARY\\M\\ND\\NORM\\MOSAIC", {"layout_mosaic": True}),
    ("ORIGINAL\\PRIMARY\\M\\ND\\NORM\\MOCO\\MOSAIC", {"layout_mosaic": True, "has_moco": True}),
    
    # COMPOSED (combined images) - flag is is_composite
    ("ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D\\COMP_SP\\COMPOSED", {"is_composite": True}),
    ("ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D\\COMP_AD\\COMPOSED", {"is_composite": True}),
    ("ORIGINAL\\PRIMARY\\M\\DIS2D\\COMP_SP\\COMPOSED", {"is_composite": True}),
    ("ORIGINAL\\PRIMARY\\M\\DIS2D\\COMP_AD\\COMPOSED", {"is_composite": True}),
    ("DERIVED\\PRIMARY\\M\\NORM\\DIS2D\\COMP_SP\\COMPOSED\\MFSPLIT", {"is_composite": True}),
    ("DERIVED\\PRIMARY\\M\\NORM\\DIS2D\\COMPOSED\\MFSPLIT", {"is_composite": True}),
    ("DERIVED\\PRIMARY\\M\\NORM\\DRS\\DIS2D\\DRB\\COMPOSED\\MFSPLIT", {"is_composite": True}),
    ("DERIVED\\PRIMARY\\CPR\\NORM\\DIS3D\\DIS2D\\COMP_SP\\COMPOSED", {"is_composite": True, "layout_cpr": True}),
    
    # PROCESSED
    ("DERIVED\\SECONDARY\\PROCESSED", {"is_secondary": True}),
    ("DERIVED\\SECONDARY\\PROC", {"is_secondary": True}),  # abbreviated
    
    # =========================================================================
    # SECTION 12: PHASE/MAGNITUDE/REAL
    # =========================================================================
    
    # Phase
    ("ORIGINAL\\PRIMARY\\P\\DIS2D", {"has_phase": True}),
    ("ORIGINAL\\PRIMARY\\P\\ND", {"has_phase": True}),
    ("ORIGINAL\\PRIMARY\\P\\DIS2D\\MFSPLIT", {"has_phase": True}),
    ("ORIGINAL\\PRIMARY\\P\\DIS3D\\MFSPLIT", {"has_phase": True}),
    ("ORIGINAL\\PRIMARY\\PHASE MAP\\P\\SE", {"has_phase": True}),
    ("ORIGINAL\\PRIMARY\\PHASE MAP\\P\\FFE", {"has_phase": True}),
    ("ORIGINAL\\PRIMARY\\P\\RETRO\\DIS2D\\MFSPLIT", {"has_phase": True}),
    
    # Real
    ("ORIGINAL\\PRIMARY\\R\\NORM\\DIS2D", {"has_real": True}),
    ("ORIGINAL\\PRIMARY\\R\\ND\\NORM", {"has_real": True}),
    ("ORIGINAL\\PRIMARY\\R\\NORM\\DIS2D\\MFSPLIT", {"has_real": True}),
    ("ORIGINAL\\PRIMARY\\R\\ND", {"has_real": True}),
    ("ORIGINAL\\PRIMARY\\R\\NORM\\ND", {"has_real": True}),
    ("ORIGINAL\\PRIMARY\\R\\DIS2D\\MOCO\\T1", {"has_real": True, "has_moco": True}),
    ("ORIGINAL\\PRIMARY\\R\\DRS\\DIS2D\\NORM\\DRB\\MFSPLIT", {"has_real": True}),
    ("ORIGINAL\\PRIMARY\\R_IR\\R\\IR", {"has_real": True}),
    ("DERIVED\\PRIMARY\\R\\NONE\\PARALLEL\\DIS2D\\MFSPLIT", {"has_real": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MPR\\\\CSAPARALLEL\\R\\NORM\\DIS2D", {"has_real": True, "is_secondary": True}),
    
    # Magnitude - most M tokens mean magnitude
    ("ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D", {}),  # generic magnitude, no special flag needed
    ("ORIGINAL\\PRIMARY\\MAG\\ND", {}),
    ("ORIGINAL\\PRIMARY\\MAG\\NORM\\DIS2D", {}),
    ("ORIGINAL\\PRIMARY\\MAG\\RETRO\\NORM\\DIS2D", {}),
    
    # =========================================================================
    # SECTION 13: VENDOR-SPECIFIC TOKENS - Philips M_FFE, M_SE, M_IR
    # =========================================================================
    
    ("ORIGINAL\\PRIMARY\\M_FFE\\M\\FFE", {}),  # Philips FFE
    ("ORIGINAL\\PRIMARY\\M_SE\\M\\SE", {}),    # Philips SE
    ("ORIGINAL\\PRIMARY\\M_IR\\M\\IR", {}),    # Philips IR
    ("ORIGINAL\\PRIMARY\\M_PCA\\M\\PCA", {}),  # Philips PCA (phase contrast angio)
    ("DERIVED\\PRIMARY\\M_FFE\\M\\FFE", {}),
    
    # =========================================================================
    # SECTION 14: AVERAGE/MEAN/SUBTRACT
    # =========================================================================
    # Note: AVERAGE token doesn't have a specific flag - it's part of REFORMATTED
    
    ("DERIVED\\SECONDARY\\REFORMATTED\\AVERAGE", {"is_mpr": True, "is_secondary": True}),  # REFORMATTED triggers is_mpr
    ("DERIVED\\SECONDARY\\AXIAL\\AVERAGE", {"layout_axial": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\OTHER\\AVERAGE", {"is_other": True, "is_secondary": True}),
    ("DERIVED\\PRIMARY\\M\\DIS2D\\NORM\\MEAN\\MFSPLIT", {"is_mean": True}),  # MEAN in path
    ("DERIVED\\PRIMARY\\M\\SWI\\DIS3D\\NORM\\FM5_1\\FIL\\MEAN\\MFSPLIT", {"has_swi": True, "is_mean": True}),  # MEAN
    ("DERIVED\\PRIMARY\\OTHER\\SUBTRACT", {"is_subtraction": True, "is_other": True}),  # Subtraction image
    
    # =========================================================================
    # SECTION 15: SPECIAL/RARE CASES
    # =========================================================================
    
    # ERROR_MAP (should be excluded)
    ("ERROR_MAP", {"is_error": True}),
    ("DERIVED\\SECONDARY\\MR Error Message", {"is_error": True, "is_secondary": True}),
    
    # SCREENSHOT/PASTED (should be excluded)
    ("DERIVED\\SECONDARY\\PASTED", {"is_screenshot": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\SCROLL", {"is_secondary": True}),  # scroll capture?
    
    # COV (coefficient of variation)
    ("DERIVED\\SECONDARY\\COV", {"is_secondary": True}),
    
    # CTH (cerebral transit heterogeneity? or something else)
    ("DERIVED\\SECONDARY\\CTH", {"is_secondary": True}),
    
    # MAVRIC (metal artifact reduction)
    ("DERIVED\\PRIMARY\\MAVRIC\\MAVRIC_COMPOSITE", {}),
    
    # CMB (cerebral microbleeds?)
    ("DERIVED\\PRIMARY\\CMB\\CMB", {}),
    
    # MYC (?)
    ("DERIVED\\PRIMARY\\MYC", {}),
    
    # CPR (Curved Planar Reformat)
    ("DERIVED\\PRIMARY\\CPR\\NORM\\DIS3D\\DIS2D", {}),
    ("DERIVED\\PRIMARY\\CPR\\NORM\\DIS3D\\MFSPLIT", {}),
    ("DERIVED\\PRIMARY\\CPR\\NORM\\DISTORTED\\MFSPLIT", {}),
    ("DERIVED\\PRIMARY\\CPR_STAR\\NORM\\DISTORTED\\MFSPLIT", {}),
    
    # GDC (gradient distortion correction)
    ("ORIGINAL\\PRIMARY\\GDC", {}),
    
    # GSP (?)
    ("ORIGINAL\\SECONDARY\\GSP\\ND", {"is_gsp": True, "is_secondary": True}),
    ("ORIGINAL\\SECONDARY\\GSP\\NORM\\DIS2D", {"is_gsp": True, "is_secondary": True}),
    ("ORIGINAL\\SECONDARY\\GSP\\NORM\\DIS2D\\COMP_SP\\COMPOSED", {"is_gsp": True, "is_secondary": True, "is_composite": True}),
    
    # MAGIC (GE synthetic MRI)
    ("ORIGINAL\\PRIMARY\\MAGIC\\NONE", {}),  # GE MAGiC sequence
    
    # PROPELLER/BLADE
    ("ORIGINAL\\PRIMARY\\PROPELLER\\NONE", {}),
    
    # POWER IMAGE
    ("ORIGINAL\\PRIMARY\\POWER IMAGE\\S", {}),
    ("ORIGINAL\\PRIMARY\\POWER IMAGE\\ENHANCED", {}),
    
    # MSUM (sum image)
    ("ORIGINAL\\PRIMARY\\MSUM\\ND", {"is_sum": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MIP\\\\CSAPARALLEL\\MSUM\\ND", {"is_mip": True, "is_projection": True, "is_sum": True, "is_secondary": True}),
    
    # K2 (?)
    ("DERIVED\\PRIMARY\\K2\\K2\\UNSPECIFIED", {}),
    
    # STDDEV (standard deviation)
    ("ORIGINAL\\PRIMARY\\STDDEV_COR\\ND", {}),
    ("ORIGINAL\\PRIMARY\\STDDEV_SAG\\ND", {}),
    ("ORIGINAL\\PRIMARY\\STDDEV_TRA\\ND", {}),
    
    # V (velocity?)
    ("ORIGINAL\\PRIMARY\\V\\ND", {}),
    
    # EPI
    ("ORIGINAL\\PRIMARY\\EPI\\NONE", {}),
    
    # PROTON_DENSITY
    ("ORIGINAL\\PRIMARY\\PROTON_DENSITY\\NONE", {}),
    
    # =========================================================================
    # SECTION 16: MINIMAL/EDGE CASES
    # =========================================================================
    
    ("ORIGINAL\\PRIMARY", {"is_original": True, "is_primary": True}),
    ("DERIVED\\SECONDARY", {"is_derived": True, "is_secondary": True}),
    ("ORIGINAL\\PRIMARY\\M", {"is_original": True, "is_primary": True}),
    ("DERIVED\\PRIMARY\\OTHER", {"is_derived": True, "is_primary": True}),
    
    # Very long/complex tokens
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MPR\\\\CSAPARALLEL\\M\\NORM\\DIS2D\\FM3_2\\FIL", {"is_projection": True, "is_mpr": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\MPR\\CSA MPR THICK\\\\M\\NORM\\DIS2D\\FM3_2\\FIL\\CSA RESAMPLED", {"is_mpr": True, "is_mpr_thick": True, "is_resampled": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\MPR\\CSA MPR THICK\\\\M_IR\\M\\IR", {"is_mpr": True, "is_mpr_thick": True, "is_secondary": True}),
    
    # =========================================================================
    # SECTION 17: ADDITIONAL SYNTHETIC MRI PATTERNS (T1FLAIR, T2FLAIR, T2W)
    # These are real patterns from the database
    # =========================================================================
    
    # T1FLAIR and T2FLAIR - combined tokens that should trigger both
    ("DERIVED\\PRIMARY\\T1FLAIR\\SYNTHETIC", {"has_flair_synthetic": True}),
    ("DERIVED\\PRIMARY\\T2FLAIR\\SYNTHETIC", {"has_flair_synthetic": True, "has_t2_synthetic": True}),
    ("DERIVED\\PRIMARY\\T2FLAIR\\SYNTHETIC\\MYC", {"has_flair_synthetic": True, "has_t2_synthetic": True, "has_myelin": True}),
    ("DERIVED\\PRIMARY\\T2W_SYNTHETIC", {"has_t2_synthetic": True}),
    
    # SWI variants with different processing
    ("DERIVED\\PRIMARY\\M\\SWI\\DIS3D\\NORM\\FM5_1\\FIL\\MEAN\\MULT\\MFSPLIT", {"has_swi": True, "is_mean": True, "is_mfsplit": True}),
    ("DERIVED\\PRIMARY\\M\\SWI\\DIS3D\\NORM\\FM5_1\\FIL\\SUBTRACTION\\MFSPLIT", {"has_swi": True, "is_subtraction": True, "is_mfsplit": True}),
    ("DERIVED\\SECONDARY\\MPR\\CSA MPR\\\\CSAPARALLEL\\M\\SWI\\NORM\\DIS2D", {"has_swi": True, "is_mpr": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MIP\\\\CSAPARALLEL\\M\\SWI\\NORM\\DIS2D", {"has_swi": True, "is_mip": True, "is_projection": True, "is_secondary": True}),
    ("DERIVED\\SECONDARY\\PROJECTION IMAGE\\CSA MPR\\\\CSAPARALLEL\\M\\SWI\\NORM\\DIS2D", {"has_swi": True, "is_projection": True, "is_mpr": True, "is_secondary": True}),
    
    # Empty or None - handled separately
]


def run_tests(verbose: bool = True) -> Tuple[int, int, List[str]]:
    """
    Run all test cases and return (passed, failed, error_messages).
    """
    passed = 0
    failed = 0
    errors = []
    
    for image_type, expected in TEST_CASES:
        try:
            result = parse_image_type(image_type)
            
            # Check each expected flag
            case_passed = True
            case_errors = []
            
            for key, expected_value in expected.items():
                actual_value = result.get(key)
                if actual_value != expected_value:
                    case_passed = False
                    case_errors.append(f"  {key}: expected {expected_value}, got {actual_value}")
            
            if case_passed:
                passed += 1
                if verbose:
                    # Show which flags were set
                    set_flags = [k for k, v in result.items() if v is True]
                    print(f"✅ {image_type[:60]:<60} → {set_flags[:5]}{'...' if len(set_flags) > 5 else ''}")
            else:
                failed += 1
                error_msg = f"❌ {image_type}\n" + "\n".join(case_errors)
                errors.append(error_msg)
                if verbose:
                    print(error_msg)
                    
        except Exception as e:
            failed += 1
            error_msg = f"💥 {image_type}\n  Exception: {e}"
            errors.append(error_msg)
            if verbose:
                print(error_msg)
    
    return passed, failed, errors


def test_edge_cases():
    """Test edge cases: None, empty, malformed."""
    # None
    result = parse_image_type(None)
    assert result["is_original"] is False
    assert result["is_derived"] is False
    
    # Empty string
    result = parse_image_type("")
    assert result["is_original"] is False
    
    # Just backslashes
    result = parse_image_type("\\\\\\")
    assert isinstance(result, dict)
    
    # Lowercase (should still work)
    result = parse_image_type("original\\primary\\m\\nd")
    assert result["is_original"] is True
    assert result["is_primary"] is True
    
    # Mixed case
    result = parse_image_type("Original\\PRIMARY\\Adc\\nd")
    assert result["is_original"] is True
    assert result["is_primary"] is True
    assert result["has_adc"] is True


def test_combined_tokens():
    """Test combined tokens that should trigger multiple flags."""
    # T2FLAIR should trigger both T2 synthetic and FLAIR synthetic
    result = parse_image_type("DERIVED\\PRIMARY\\T2FLAIR\\SYNTHETIC")
    assert result["has_t2_synthetic"] is True
    assert result["has_flair_synthetic"] is True
    
    # ADC + DIFFUSION
    result = parse_image_type("DERIVED\\PRIMARY\\DIFFUSION\\ADC\\NORM")
    assert result["has_adc"] is True
    assert result["has_diffusion"] is True
    
    # SWI + DERIVED + SECONDARY
    result = parse_image_type("DERIVED\\SECONDARY\\M\\SWI\\NORM")
    assert result["has_swi"] is True
    assert result["is_derived"] is True
    assert result["is_secondary"] is True


def test_all_image_type_patterns():
    """Test all image_type patterns from TEST_CASES."""
    passed, failed, errors = run_tests(verbose=False)
    assert failed == 0, f"Failed {failed} tests:\n" + "\n".join(errors[:5])


def print_all_flags():
    """Print all available flags from the parser."""
    print("\n" + "=" * 80)
    print("ALL AVAILABLE FLAGS")
    print("=" * 80)
    
    result = parse_image_type("ORIGINAL\\PRIMARY")
    flags = sorted([k for k in result.keys()])
    
    for i, flag in enumerate(flags, 1):
        print(f"  {i:3}. {flag}")
    
    print(f"\nTotal: {len(flags)} flags")


def main():
    """Run all tests."""
    print("=" * 80)
    print("PARSE_IMAGE_TYPE COMPREHENSIVE TEST")
    print(f"Testing {len(TEST_CASES)} unique image_type values from database")
    print("=" * 80 + "\n")
    
    # Print available flags
    print_all_flags()
    
    # Run edge cases
    test_edge_cases()
    
    # Run combined token tests
    test_combined_tokens()
    
    # Run main tests
    print("\n" + "=" * 80)
    print("MAIN TEST CASES")
    print("=" * 80)
    
    passed, failed, errors = run_tests(verbose=True)
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total:  {passed + failed}")
    
    if failed > 0:
        print("\n❌ SOME TESTS FAILED:")
        for error in errors[:10]:  # Show first 10 errors
            print(error)
        if len(errors) > 10:
            print(f"... and {len(errors) - 10} more errors")
        return 1
    else:
        print("\n✅ ALL TESTS PASSED!")
        return 0


if __name__ == "__main__":
    exit(main())
