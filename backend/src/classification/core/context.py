"""
Classification Context Module

Encapsulates all information needed to classify a single series stack.
Provides cached parsing of DICOM tags into structured boolean flags.

Version: 3.3.0

Changelog:
- 3.3.0: Added branch-specific unified_flags for SWI, SyMRI, and Dixon
         New flags: is_swi_magnitude, is_swi_processed, has_qsm, is_epi_swi,
                   is_symri_source, has_pd_map, has_b1_map, has_fat_fraction,
                   is_dixon_water, is_dixon_fat
- 3.2.0: Added unified_flags property with 55 aggregated flags across 6 categories
         Added mr_acquisition_type and stack_key fields to from_fingerprint()
- 3.1.0: Initial release with 5 parsers for DICOM tag parsing
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set


# =============================================================================
# Token Parsing Functions
# =============================================================================


def parse_image_type(image_type: str) -> Dict[str, Any]:
    """
    Parse ImageType (0008,0008) into boolean flags using token search.
    
    Tokens can appear in ANY position after Value 2.
    Uses set membership for position-independent matching.
    
    Args:
        image_type: Raw ImageType string, e.g., "ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D"
    
    Returns:
        Dict with boolean flags for each token category
    """
    # Normalize and tokenize
    tokens: Set[str] = set()
    raw_upper = (image_type or "").upper()
    for part in re.split(r'[\\\/\s]+', raw_upper):
        if part:
            tokens.add(part)
    
    return {
        # === 1. Core Flags ===
        "is_original": "ORIGINAL" in tokens,
        "is_derived": "DERIVED" in tokens,
        "is_primary": "PRIMARY" in tokens,
        "is_secondary": "SECONDARY" in tokens,
        "is_localizer": "LOCALIZER" in tokens,
        "is_error": any(t in tokens for t in ["ERROR_MAP"]) or "MR ERROR MESSAGE" in raw_upper,
        
        # === 2. Diffusion (Broad) ===
        "has_diffusion": any(t in tokens for t in [
            "DIFFUSION", "ADC", "FA", "TRACEW", "ISODWI", "EXP",
            "EADC", "CALC_BVALUE", "MIXED", "MIX", "MOTIONCORRECTION"
        ]),
        "has_trace": any(t in tokens for t in ["TRACEW", "ISODWI"]),
        "has_adc": "ADC" in tokens,
        "has_eadc": "EADC" in tokens,
        "has_fa": "FA" in tokens,
        "has_exp_diffusion": "EXP" in tokens,
        "has_mixed": any(t in tokens for t in ["MIXED", "MIX"]),
        "has_calc_bval": "CALC_BVALUE" in tokens,
        "has_moco": any(t in tokens for t in ["MOTIONCORRECTION", "MOCO"]),
        
        # === 3. Perfusion/ASL ===
        "has_perfusion": any(t in tokens for t in ["PERFUSION", "PWI", "PERFUSION_ASL"]) or "MR PWI DSC" in raw_upper,
        "has_asl": any(t in tokens for t in ["ASL", "PERFUSION_ASL"]),
        "has_cbf": any(t in tokens for t in ["CBF", "RCBF", "RELCBF"]),
        "has_cbv": any(t in tokens for t in ["CBV", "RCBV", "RELCBV", "RCBVCORR", "RCBVUNCORR"]),
        "has_mtt": any(t in tokens for t in ["MTT", "RELMTT"]),
        "has_ttp": "TTP" in tokens,
        "has_tmax": any(t in tokens for t in ["TMAX"]) or "TMAX BASIC" in raw_upper,
        "has_oef": "OEF MODEL BASED" in raw_upper,
        "has_cmro2": "RCMRO2 MODEL BASED" in raw_upper,
        "has_leakage": "RLEAKAGE" in tokens,
        "has_peak_bolus": "PBP" in tokens,
        "has_brain_mask": "BRAIN MASK MR PWI DSC" in raw_upper,
        
        # === 4. Quantitative/Maps ===
        # Note: MULTI_QMAP is also a QMAP variant
        "has_qmap": "QMAP" in tokens or "MULTI_QMAP" in tokens,
        # MULTI_QMAP - GE MAGiC bundled T1/T2/PD maps in single series
        "has_multi_qmap": "MULTI_QMAP" in tokens,
        "has_t1_map": any(t in tokens for t in ["T1MAP"]) or "T1 MAP" in raw_upper or ("T1MAP" in tokens and "DERIVED" in tokens and "SYNTHETIC" not in tokens),
        "has_t2_map": any(t in tokens for t in ["T2MAP"]) or "T2 MAP" in raw_upper or ("T2MAP" in tokens and "DERIVED" in tokens and "SYNTHETIC" not in tokens),
        "has_pd_map": any(t in tokens for t in ["PDMAP", "PD_MAP"]) or "PD MAP" in raw_upper or ("PDMAP" in tokens and "MAP" in tokens),
        "has_r1": "R1" in tokens,
        "has_r2": "R2" in tokens,
        "has_t0": "T0" in tokens,
        "has_k2": "K2" in tokens,
        "has_b1_map": any(t in tokens for t in ["B1MAP", "B1_MAP", "B1+"]) or "B1 MAP" in raw_upper,
        "has_b0_map": any(t in tokens for t in ["B0MAP", "B0_MAP", "FIELDMAP"]) or "B0 MAP" in raw_upper or "FIELD MAP" in raw_upper,
        "has_fat_fraction": any(t in tokens for t in ["FF", "FATFRACTION", "FAT_FRACTION", "PDFF"]) or "FAT FRACTION" in raw_upper,
        # Synthetic MRI outputs - handle combined tokens like T2FLAIR
        "has_pd_synthetic": any(t in tokens for t in ["PDW_SYNTHETIC"]) or ("PD" in tokens and "SYNTHETIC" in raw_upper),
        "has_t1_synthetic": "T1W_SYNTHETIC" in tokens or "T1\\SYNTHETIC" in raw_upper or ("T1" in tokens and "SYNTHETIC" in tokens),
        "has_t2_synthetic": "T2W_SYNTHETIC" in tokens or "T2\\SYNTHETIC" in raw_upper or ("T2" in tokens and "SYNTHETIC" in tokens) or "T2FLAIR" in tokens or "T2W_FLAIR_SYNTHETIC" in tokens,
        # FLAIR synthetic - T2FLAIR is a combined token, also check individual tokens
        "has_flair_synthetic": any(t in tokens for t in ["T2FLAIR", "T1FLAIR", "T2W_FLAIR_SYNTHETIC"]) or "T1FLAIR SYNTHETIC" in raw_upper or "T2FLAIR SYNTHETIC" in raw_upper,
        "has_uniform": "UNIFORM" in tokens or "T1\\UNIFORM" in raw_upper or "M\\UNI" in raw_upper,
        "has_maximum": "MAXIMUM" in tokens or "T1\\MAXIMUM" in raw_upper or "T2\\MAXIMUM" in raw_upper,
        "has_myelin": "MYC" in tokens,
        "has_navail": "NAVAIL" in tokens,
        
        # === 5. Dixon/Fat-Water ===
        "has_dixon": "DIXON" in tokens,
        "has_water": any(t in tokens for t in ["WATER"]) or (len(tokens) > 0 and "W" in tokens and "WATER" not in raw_upper),
        "has_fat": any(t in tokens for t in ["FAT"]) or (len(tokens) > 0 and "F" in tokens and "FAT" not in raw_upper),
        "has_in_phase": any(t in tokens for t in ["IN_PHASE", "IP"]),
        "has_out_phase": any(t in tokens for t in ["OUT_PHASE", "OP", "OPP_PHASE"]),
        
        # === 6. PSIR/DIR/MAVRIC ===
        "has_psir": any(t in tokens for t in ["PSIR"]) or "PSIR SYNTHETIC" in raw_upper,
        "has_dir": "DIR" in tokens and "DIRECTORY" not in raw_upper,
        "has_dir_synthetic": "DIR SYNTHETIC" in raw_upper or "DIR_SYNTHETIC" in tokens,
        "has_mavric": any(t in tokens for t in ["MAVRIC", "MAVRIC_COMPOSITE"]),
        "has_cmb": "CMB" in tokens,
        
        # === 7. Orientation/Layout ===
        "layout_axial": "AXIAL" in tokens,
        "layout_sagittal": "SAG" in tokens,
        "layout_coronal": "COR" in tokens,
        "layout_transverse": "TRA" in tokens,
        "layout_mosaic": "MOSAIC" in tokens,
        "layout_parallel": "PARALLEL" in tokens,
        "layout_radial": "RADIAL" in tokens,
        "layout_cpr": "CPR" in tokens,
        "is_distorted": "DISTORTED" in tokens,
        "is_2d_view": "DIS2D" in tokens,
        "is_3d_view": "DIS3D" in tokens,
        "is_mfsplit": "MFSPLIT" in tokens,
        
        # === 8. Processing ===
        "is_normalized": "NORM" in tokens,
        "is_subtraction": any(t in tokens for t in ["SUB", "SUBTRACT", "SUBTRACTION"]),
        "is_dfc": "DFC" in tokens,
        "is_processed": any(t in tokens for t in ["PROC", "PROCESSED"]),
        "is_resampled": "RESAMPLED" in tokens or "CSA RESAMPLED" in raw_upper,
        "is_mean": "MEAN" in tokens,
        "is_sum": "MSUM" in tokens,
        "is_composite": any(t in tokens for t in [
            "MULTIECHOCOMBINED", "COMP_AD", "COMP_SP", "COMP_AN",
            "COMP_MIP", "COMPOSED", "COMPOSITE"
        ]),
        "is_mpr": any(t in tokens for t in ["MPR", "REFORMATTED"]),
        "is_mpr_thick": "MPR THICK" in raw_upper or "CSA MPR THICK" in raw_upper,
        
        # === 9. Projection ===
        "is_mip": any(t in tokens for t in ["MIP", "MIP_COR", "MIP_SAG", "MIP_TRA"]) or "CSA MIP" in raw_upper,
        "is_minip": any(t in tokens for t in ["MINIP", "MNIP"]) or "MIN IP" in raw_upper,
        "is_projection": "PROJECTION IMAGE" in raw_upper,
        "is_vascular": "VASCULAR" in tokens,
        "is_scroll": "SCROLL" in tokens,
        "is_mip_thin": "CSA MIP THIN" in raw_upper,
        
        # === 10. Components ===
        "has_magnitude": any(t in tokens for t in ["MAGNITUDE", "M_FFE", "M_SE", "M_IR", "M_PCA", "SW_M_FFE"]) or ("M" in tokens and len(tokens) > 2),
        "has_phase": any(t in tokens for t in ["PHASE", "SW_P_FFE"]) or "PHASE MAP" in raw_upper or ("P" in tokens and len(tokens) > 2),
        "has_real": any(t in tokens for t in ["REAL"]) or ("R" in tokens and "REFORMATTED" not in tokens),
        "has_imaginary": any(t in tokens for t in ["IMAGINARY"]) or ("I" in tokens and "IR" not in tokens and "IN_PHASE" not in tokens),
        "has_swi": any(t in tokens for t in ["SWI", "SW_M_FFE", "SW_P_FFE", "SW_M", "SW_P"]),
        # QSM - Quantitative Susceptibility Mapping (derived from SWI phase data)
        "has_qsm": any(t in tokens for t in ["QSM", "SUSCEPTIBILITY"]) or "QUANTITATIVE SUSCEPTIBILITY" in raw_upper,
        
        # === 11. Technique Hints (from ImageType) ===
        "hint_gre": any(t in tokens for t in ["M_FFE", "SW_M_FFE", "SW_P_FFE"]),
        "hint_se": "M_SE" in tokens,
        "hint_ir": "M_IR" in tokens,
        "hint_pca": "M_PCA" in tokens,
        
        # === 12. Synthetic MRI markers ===
        "is_synthetic": any(t in tokens for t in ["SYNTHETIC"]) or "SYNTHETIC" in raw_upper,
        "has_stir_synthetic": "T2STIR" in tokens and "SYNTHETIC" in raw_upper,
        "has_psir_synthetic": "PSIR" in tokens and "SYNTHETIC" in raw_upper,

        # === 12b. SyMRI-specific ImageType tokens ===
        # QMAP with specific map type (e.g., DERIVED\PRIMARY\QMAP\T1)
        "has_qmap_t1": "QMAP" in tokens and "T1" in tokens and "T2" not in tokens,
        "has_qmap_t2": "QMAP" in tokens and "T2" in tokens,
        "has_qmap_pd": "QMAP" in tokens and "PD" in tokens,
        # DIR synthetic variants (DIR\SYNTHETIC or DIR_SYNTHETIC)
        "has_dir_synthetic_token": ("DIR" in tokens and "SYNTHETIC" in tokens) or "DIR_SYNTHETIC" in tokens,
        # PSIR synthetic variants (PSIR\SYNTHETIC or PSIR_SYNTHETIC)
        "has_psir_synthetic_token": ("PSIR" in tokens and "SYNTHETIC" in tokens) or "PSIR_SYNTHETIC" in tokens,
        # GE MAGiC raw data
        "is_magic_raw": "MAGIC" in tokens and "ORIGINAL" in tokens,

        # === 13. BOLD/fMRI ===
        "has_fmri": "FMRI" in tokens or "EPI" in tokens,
        
        # === 14. QA/QC/Other ===
        "is_qa": any(t in tokens for t in ["CTH", "COV", "STDDEV_COR", "STDDEV_SAG", "STDDEV_TRA"]),
        "is_screenshot": any(t in tokens for t in ["SCREENSHOT", "PASTED"]),
        "is_posdisp": "POSDISP" in tokens,
        "is_gsp": "GSP" in tokens,
        "is_other": any(t in tokens for t in ["OTHER", "UNKNOWN"]),
        "is_none": "NONE" in tokens,
        
        # === 13. Siemens Filter Tokens ===
        "has_filter_mask": any(t in tokens for t in ["FM3_2", "FM4_1", "FM5_1"]),
        "has_filter_sharpening": any(t in tokens for t in ["FS2_4", "SH1_1", "SH3_1", "SH4_1", "SH5_1", "SH5_4"]),
        "is_csa_manipulated": "CSAMANIPULATED" in tokens,
        "is_csa_parallel": "CSAPARALLEL" in tokens,
        
        # === All Tokens (for custom matching) ===
        "all_tokens": tokens,
    }


def parse_scanning_sequence(seq: str) -> Dict[str, Any]:
    """
    Parse ScanningSequence (0018,0020) into boolean flags.
    
    Handles both single values ("SE") and list-like strings ("['SE', 'IR']").
    
    Args:
        seq: Raw ScanningSequence string
    
    Returns:
        Dict with boolean flags for sequence types
    """
    tokens: Set[str] = set()
    if seq:
        # Remove brackets and quotes, split by comma/space/backslash
        cleaned = re.sub(r"[\[\]'\"()]", "", seq.upper())
        for part in re.split(r'[,\s\\]+', cleaned):
            if part:
                tokens.add(part)
    
    return {
        "has_se": "SE" in tokens,
        "has_gre": any(t in tokens for t in ["GR", "GE", "FE", "FE3D"]),
        "has_fse": "FSE" in tokens,
        "has_ir": "IR" in tokens,
        "has_irfse": "IRFSE" in tokens,
        "has_epi": "EP" in tokens,
        "has_research": "RM" in tokens,
        "has_qmap": "QMAP" in tokens,
        "has_synthetic": any(t in tokens for t in ["SYNTHETIC"]) or "SE (SYNTHETIC)" in seq.upper() if seq else False or "IR (SYNTHETIC)" in seq.upper() if seq else False,
        "is_generated": "GENERATED" in tokens,
        "has_saturation": "S" in tokens and "SE" not in tokens and "SS" not in tokens,
        "all_tokens": tokens,
    }


def parse_sequence_variant(variant: str) -> Dict[str, Any]:
    """
    Parse SequenceVariant (0018,0021) into boolean flags.
    
    Args:
        variant: Raw SequenceVariant string, e.g., "SK" or "['SK', 'SP', 'MP']"
    
    Returns:
        Dict with boolean flags for variant types
    """
    tokens: Set[str] = set()
    if variant:
        cleaned = re.sub(r"[\[\]'\"()]", "", variant.upper())
        # Split on comma/space/backslash (DICOM multi-value delimiter)
        for part in re.split(r'[,\s\\]+', cleaned):
            if part:
                tokens.add(part)
    
    return {
        "has_mag_prepared": "MP" in tokens,
        "has_mtc": "MTC" in tokens,
        "has_segmented_kspace": "SK" in tokens,
        "has_spoiled": "SP" in tokens,
        "has_steady_state": "SS" in tokens,
        "has_oversampling": any(t in tokens for t in ["OSP", "OS"]),
        "is_none": "NONE" in tokens,
        "is_other": "OTHER" in tokens,
        "is_synthetic": "SYNTHETIC" in tokens,
        "is_tof": "TOF" in tokens,
        "all_tokens": tokens,
    }


def parse_scan_options(options: str) -> Dict[str, Any]:
    """
    Parse ScanOptions (0018,0022) into boolean flags.
    
    Handles vendor-specific options (GE GEMS, Siemens, Philips).
    
    Args:
        options: Raw ScanOptions string
    
    Returns:
        Dict with boolean flags for scan options
    """
    tokens: Set[str] = set()
    raw_upper = (options or "").upper()
    if options:
        cleaned = re.sub(r"[\[\]'\"()]", "", raw_upper)
        # Split on comma, space, and backslash (DICOM multi-value delimiter)
        for part in re.split(r'[,\s\\]+', cleaned):
            if part:
                tokens.add(part)
    
    return {
        # === Gating ===
        "has_gating": any(t in tokens for t in ["CG", "RG", "PPG", "IPG"]),
        
        # === GE GEMS Options ===
        "has_parallel_gems": "ACC_GEMS" in tokens,
        "has_hypersense": "HYPERSENSE_GEMS" in tokens,
        "has_cs_gems": "CS_GEMS" in tokens,
        "has_sat_gems": any(t in tokens for t in ["SAT_GEMS", "SAT1", "SAT2", "SAT3", "SAT4"]),
        "has_inflow_gems": "IFLOW_GEMS" in tokens,
        "has_tof_gems": "VASCTOF_GEMS" in tokens,
        "has_cine_gems": "CINE_GEMS" in tokens,
        "has_seq_gems": "SEQ_GEMS" in tokens,
        "has_trf_gems": "TRF_GEMS" in tokens,
        "has_spiral_gems": "SPIRAL_GEMS" in tokens,
        "has_propeller_gems": any(t in tokens for t in ["PROP_GEMS", "PARTL_BLADE_GEMS"]),
        "has_ideal_gems": "IDEAL_GEMS" in tokens,
        "has_flex_gems": "FLEX_GEMS" in tokens,
        "has_mrf_gems": "MRF_GEMS" in tokens,
        "has_hyperband_gems": "HYPERBAND_GEMS" in tokens,
        "has_promo_gems": "PROMO_GEMS" in tokens,
        "has_dwduo_gems": "DWDUO_GEMS" in tokens,
        "has_t1flair_gems": "T1FLAIR_GEMS" in tokens,
        "has_t2flair_gems": "T2FLAIR_GEMS" in tokens,
        "has_mp_gems": "MP_GEMS" in tokens,
        "has_ir_gems": "IR_GEMS" in tokens,
        "has_fast_gems": "FAST_GEMS" in tokens,
        "has_edr_gems": "EDR_GEMS" in tokens,
        "has_filtered_gems": "FILTERED_GEMS" in tokens,
        "has_fsa_gems": "FSA_GEMS" in tokens,
        "has_fsl_gems": "FSL_GEMS" in tokens,
        "has_fss_gems": "FSS_GEMS" in tokens,
        "has_fsi_gems": "FSI_GEMS" in tokens,
        "has_fsp_gems": "FSP_GEMS" in tokens,
        "has_fsr_gems": "FSR_GEMS" in tokens,
        
        # === Standard Options ===
        "has_ir": "IR" in tokens,
        "has_fat_sat": any(t in tokens for t in ["FS", "SFS"]) and "FSE" not in tokens,
        "has_mt": "MT" in tokens,
        "has_flow_comp": "FC" in tokens,
        "has_partial_fourier_phase": "PFP" in tokens,
        "has_partial_fourier_freq": "PFF" in tokens,
        "has_phase_reorder": "PER" in tokens,
        "has_spatial_sat": "SP" in tokens and "SPGR" not in raw_upper,
        "has_water_exc": "WE" in tokens,
        "has_no_phase_wrap": "NPW" in tokens,
        "has_scout_mode": "SCOUT MODE" in raw_upper,
        
        # === Dixon Options ===
        "has_dix_fat": "DIXF" in tokens,
        "has_dix_water": "DIXW" in tokens,
        
        # === Other ===
        "has_cycinv": "CYCINV" in tokens,
        "has_aveext": "AVEEXT" in tokens,
        "has_enhanced": "ENHANCED" in tokens,
        "has_axial_mode": "AXIAL MODE" in raw_upper,
        
        "all_tokens": tokens,
    }


def parse_sequence_name(name: str) -> Dict[str, Any]:
    """
    Parse SequenceName (0018,0024) using pattern matching.
    
    Primarily for Siemens sequence name patterns, but includes
    vendor-agnostic patterns where possible.
    
    Args:
        name: Raw SequenceName string, e.g., "*tfl3d1_16"
    
    Returns:
        Dict with boolean flags for sequence patterns
    """
    if not name:
        return {
            "raw_name": "",
            "is_epi_diff_b0": False,
            "is_epi_diff": False,
            "is_epi_diff_resolve": False,
            "is_epi_gre": False,
            "is_epi_se": False,
            "is_epi_ir": False,
            "is_se": False,
            "is_tse": False,
            "is_tse_body": False,
            "is_ir_tse": False,
            "is_haste": False,
            "is_flash": False,
            "is_tfl": False,
            "is_gre": False,
            "is_fgre": False,
            "is_space": False,
            "is_space_ir": False,
            "is_swi": False,
            "is_me_se": False,
            "is_mdme": False,
            "is_qalas": False,
            "is_quant_map": False,
            "is_pc": False,
            "is_tof": False,
            "is_fse": False,
            "is_ir_fse": False,
            "is_mprage": False,
            "is_flair_seq": False,
            "is_ciss": False,
            "is_localizer": False,
            "is_wip": False,
            "is_tune": False,
            "is_trufisp": False,
        }
    
    name_lower = name.lower()
    
    return {
        "raw_name": name,
        
        # === 1. Diffusion/EPI ===
        # Patterns: ep_b (standard), blade_b (motion-corrected), ep_calc (calculated), ep_d3ta (DTI), ep_hds
        "is_epi_diff_b0": ("ep_b0" in name_lower or "blade_b0" in name_lower) and "ep_b1" not in name_lower,
        "is_epi_diff": any(p in name_lower for p in ["ep_b", "blade_b", "ep_calc", "ep_d3ta", "ep_hds"]),
        "is_epi_diff_resolve": "re_b" in name_lower,
        "is_epi_gre": "epfid" in name_lower,
        "is_epi_se": "epse" in name_lower,
        "is_epi_ir": "epir" in name_lower,
        
        # === 2. Spin Echo/TSE ===
        "is_se": any(p in name_lower for p in ["*se1", "*se2d", "se2d"]) and "tse" not in name_lower,
        # TSE patterns: tse1, tse2, tse3, tse_ (variant), tse- (dash), tseR (restore), tseB (blade)
        "is_tse": any(p in name_lower for p in ["tse1", "tse2", "tse3", "tse_", "tse-", "tser", "tseb", "ts1", "ts2", "ts3"]),
        "is_tse_body": "tser" in name_lower,  # tseR = TSE restore (body-optimized)
        "is_ir_tse": "tir" in name_lower and "stir" not in name_lower,
        
        # === 3. HASTE/SS-TSE ===
        "is_haste": any(p in name_lower for p in ["h2d", "h3d", "hr2d", "haste"]),
        
        # === 4. Gradient Echo (FLASH/GRE) ===
        # Note: fl1scout contains fl1, but we exclude flair patterns
        "is_flash": any(p in name_lower for p in ["fl1", "fl2d", "fl3d"]) and "flair" not in name_lower,
        "is_tfl": "tfl" in name_lower,
        "is_gre": any(p in name_lower for p in ["*gre", "ffe"]) and "mprage" not in name_lower,
        "is_fgre": "fgre" in name_lower,
        
        # === 5. SPACE/3D-TSE ===
        "is_space": "spc" in name_lower and "spcir" not in name_lower,
        "is_space_ir": "spcir" in name_lower,
        
        # === 6. SWI ===
        "is_swi": any(p in name_lower for p in ["swi", "qswi"]),
        
        # === 7. Multi-Echo ===
        "is_me_se": any(p in name_lower for p in ["me2d", "me3d", "*me1", "memp"]),
        
        # === 8. Quantitative/MDME ===
        "is_mdme": "mdme" in name_lower,
        "is_qalas": "qalas" in name_lower,
        "is_quant_map": any(p in name_lower for p in ["qtir", "qtse", "qfl"]),
        
        # === 9. Flow/Angio ===
        # Note: "*pc2d", "*pc3d" - avoid matching "spc3d" (SPACE)
        "is_pc": any(p in name_lower for p in ["*pc2d", "*pc3d", "_pc2d", "_pc3d"]) or name_lower.startswith("pc2d") or name_lower.startswith("pc3d"),
        "is_tof": any(p in name_lower for p in ["tof"]) and "stir" not in name_lower,
        
        # === 10. FSE/IRFSE (Non-Siemens) ===
        "is_fse": "fse" in name_lower and "irfse" not in name_lower,
        "is_ir_fse": "irfse" in name_lower,
        
        # === 11. MPRAGE/Structural ===
        # Note: These are pattern hints only. Actual technique classification
        # requires combining with scanning_sequence/sequence_variant downstream.
        # Siemens MPRAGE: *tfl3d1*, *mpr*, GE: bravo*, ir-fspgr
        "is_mprage": any(p in name_lower for p in ["mprage", "bravo", "tfl3d1", "irfspgr", "ir-spgr", "mpr"]),
        "is_flair_seq": any(p in name_lower for p in ["flair", "mrcflair"]) and "t1flair" not in name_lower and "t2flair" not in name_lower,
        
        # === 12. CISS/bSSFP ===
        "is_ciss": any(p in name_lower for p in ["ciss", "ci3d"]),
        
        # === 13. Localizer ===
        "is_localizer": any(p in name_lower for p in ["scout", "localizer", "loc_"]),
        
        # === 14. Misc ===
        "is_wip": "wip" in name_lower,
        "is_tune": any(p in name_lower for p in ["tun_", "tun-", "tune", "fi3d1tun"]),

        # === 15. Trufisp ===
        "is_trufisp": "tfi2d" in name_lower,
    }


# =============================================================================
# Classification Context
# =============================================================================


@dataclass
class ClassificationContext:
    """
    All inputs needed to classify a single series stack.
    
    Provides cached parsing of DICOM tags into structured boolean flags.
    Use the parsed_* properties to access pre-computed flags.
    
    Example:
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D",
            scanning_sequence="['GR', 'IR']",
            ...
        )
        
        if ctx.parsed_image_type["has_magnitude"]:
            ...
        
        if ctx.parsed_scanning_sequence["has_ir"]:
            ...
    """
    
    # === Raw Fingerprint Fields ===
    image_type: Optional[str] = None
    scanning_sequence: Optional[str] = None
    sequence_variant: Optional[str] = None
    scan_options: Optional[str] = None
    stack_sequence_name: Optional[str] = None
    text_search_blob: Optional[str] = None
    contrast_search_blob: Optional[str] = None
    manufacturer: Optional[str] = None
    
    # === Physics Parameters ===
    mr_tr: Optional[float] = None
    mr_te: Optional[float] = None
    mr_ti: Optional[float] = None
    mr_flip_angle: Optional[float] = None
    mr_echo_train_length: Optional[int] = None
    mr_diffusion_b_value: Optional[str] = None
    
    # === Geometry ===
    stack_orientation: Optional[str] = None
    fov_x: Optional[float] = None
    fov_y: Optional[float] = None
    aspect_ratio: Optional[float] = None
    stack_n_instances: int = 0
    
    # === Acquisition Properties (from fingerprint) ===
    mr_acquisition_type: Optional[str] = None  # "2D" or "3D"
    stack_key: Optional[str] = None  # "multi_echo", "multi_ti", "multi_flip_angle", etc.
    
    # === Cached Parsed Results (internal) ===
    _parsed_image_type: Optional[Dict[str, Any]] = field(default=None, repr=False)
    _parsed_scanning_sequence: Optional[Dict[str, Any]] = field(default=None, repr=False)
    _parsed_sequence_variant: Optional[Dict[str, Any]] = field(default=None, repr=False)
    _parsed_scan_options: Optional[Dict[str, Any]] = field(default=None, repr=False)
    _parsed_sequence_name: Optional[Dict[str, Any]] = field(default=None, repr=False)
    
    # =========================================================================
    # Cached Properties
    # =========================================================================
    
    @property
    def parsed_image_type(self) -> Dict[str, Any]:
        """Parse and cache ImageType flags."""
        if self._parsed_image_type is None:
            self._parsed_image_type = parse_image_type(self.image_type or "")
        return self._parsed_image_type
    
    @property
    def parsed_scanning_sequence(self) -> Dict[str, Any]:
        """Parse and cache ScanningSequence flags."""
        if self._parsed_scanning_sequence is None:
            self._parsed_scanning_sequence = parse_scanning_sequence(self.scanning_sequence or "")
        return self._parsed_scanning_sequence
    
    @property
    def parsed_sequence_variant(self) -> Dict[str, Any]:
        """Parse and cache SequenceVariant flags."""
        if self._parsed_sequence_variant is None:
            self._parsed_sequence_variant = parse_sequence_variant(self.sequence_variant or "")
        return self._parsed_sequence_variant
    
    @property
    def parsed_scan_options(self) -> Dict[str, Any]:
        """Parse and cache ScanOptions flags."""
        if self._parsed_scan_options is None:
            self._parsed_scan_options = parse_scan_options(self.scan_options or "")
        return self._parsed_scan_options
    
    @property
    def parsed_sequence_name(self) -> Dict[str, Any]:
        """Parse and cache SequenceName patterns."""
        if self._parsed_sequence_name is None:
            self._parsed_sequence_name = parse_sequence_name(self.stack_sequence_name or "")
        return self._parsed_sequence_name
    
    @property
    def vendor(self) -> str:
        """
        Normalized vendor identifier.
        
        Returns:
            One of: "SIEMENS", "GE", "PHILIPS", "OTHER"
        """
        if self.manufacturer:
            m = self.manufacturer.upper()
            if "SIEMENS" in m:
                return "SIEMENS"
            if "GE" in m or "GENERAL ELECTRIC" in m:
                return "GE"
            if "PHILIPS" in m:
                return "PHILIPS"
            if "CANON" in m or "TOSHIBA" in m:
                return "CANON"
            if "HITACHI" in m:
                return "HITACHI"
        return "OTHER"
    
    # =========================================================================
    # Unified Flags - Aggregated from all parsers
    # =========================================================================
    
    @property
    def unified_flags(self) -> Dict[str, bool]:
        """
        Unified high-value flags aggregated from all parsers.
        
        Each flag combines evidence from multiple sources (vendor-agnostic).
        This is the PRIMARY interface for technique detection.
        
        Categories:
            1. Pulse Sequence Physics - SE, GRE, EPI, IR (what RF physics?)
            2. Technique Indicators - TSE, SPACE, HASTE, MPRAGE, etc.
            3. Sequence Variants/Modifiers - spoiled, steady-state, fat-sat, etc.
            4. Acquisition Properties - 2D/3D, multi-echo, multi-TI, multi-FA
            5. Derived/Synthetic/Quantitative - ADC, FA, synthetic outputs, qmaps
            6. Image Type/Component - magnitude, phase, Dixon, PSIR, etc.
            7. Post-Processing/Reconstruction - MIP, MPR, subtraction, etc.
            8. Core Image Flags - original, derived, primary, secondary, etc.
        
        Returns:
            Dict of unified boolean flags
        """
        pit = self.parsed_image_type
        pss = self.parsed_scanning_sequence
        psv = self.parsed_sequence_variant
        pso = self.parsed_scan_options
        psn = self.parsed_sequence_name
        
        return {
            # =================================================================
            # 1. PULSE SEQUENCE PHYSICS (What RF/gradient physics is used?)
            # =================================================================
            
            # Spin Echo - uses 180° refocusing pulse
            "has_se": any([
                pss["has_se"],
                pss["has_fse"],
                pss["has_irfse"],
                pit["hint_se"],
                psn["is_se"],
                psn["is_tse"],
                psn["is_haste"],
                psn["is_space"],
                psn["is_space_ir"],
                psn["is_ir_tse"],
                psn["is_fse"],
                psn["is_ir_fse"],
                psn["is_me_se"],
                psn["is_mdme"],
                psn["is_epi_se"],
            ]),
            
            # Gradient Echo - uses gradient refocusing only
            "has_gre": any([
                pss["has_gre"],
                pit["hint_gre"],
                psn["is_gre"],
                psn["is_fgre"],
                psn["is_flash"],
                psn["is_tfl"],
                psn["is_mprage"],
                psn["is_swi"],
                psn["is_ciss"],
                psn["is_tof"],
                psn["is_pc"],
                psn["is_epi_gre"],
            ]),
            
            # Echo Planar Imaging - EPI readout
            "has_epi": any([
                pss["has_epi"],
                psn["is_epi_diff"],
                psn["is_epi_gre"],
                psn["is_epi_se"],
                psn["is_epi_ir"],
                psn["is_epi_diff_resolve"],
                psn["is_epi_diff_b0"],
            ]),
            
            # Inversion Recovery - IR preparation pulse (includes DIR = Double IR)
            # NOTE: This includes BOTH SE-based IR (FLAIR, STIR) AND GRE-based IR (MPRAGE)
            "has_ir": any([
                pss["has_ir"],
                pss["has_irfse"],
                pit["hint_ir"],
                pit["has_dir"],  # DIR = Double IR
                pit["has_dir_synthetic"],
                pso["has_ir"],
                pso["has_ir_gems"],
                pso["has_t1flair_gems"],
                pso["has_t2flair_gems"],
                psn["is_ir_tse"],
                psn["is_ir_fse"],
                psn["is_space_ir"],
                psn["is_epi_ir"],
                psn["is_mprage"],  # MPRAGE is IR-prepared GRE
                psn["is_flair_seq"],
            ]),
            
            # SE-based Inversion Recovery (for modifier detection)
            # This is specifically SE+IR combinations (FLAIR, STIR, DIR, TIRM)
            # NOT GRE+IR combinations (MPRAGE, MP2RAGE)
            # Used by ModifierDetector for IR contrast modifiers
            "has_ir_se": any([
                pss["has_irfse"],  # IRFSE in ScanningSequence
                psn["is_ir_tse"],  # IR-TSE sequence name
                psn["is_ir_fse"],  # IR-FSE sequence name
                psn["is_space_ir"],  # 3D IR-TSE (SPACE with IR)
                psn["is_flair_seq"],  # FLAIR sequence name
                # SE + IR combination (but not EPI which can be SE-EPI)
                (pss["has_se"] or pss["has_fse"]) and pss["has_ir"] and not pss["has_epi"],
            ]),
            
            # Saturation pulse
            "has_saturation": any([
                pss["has_saturation"],
                pso["has_sat_gems"],
                pso["has_spatial_sat"],
            ]),
            
            # =================================================================
            # 2. TECHNIQUE INDICATORS (Specific technique patterns)
            # =================================================================
            
            # --- SE Family ---
            # TSE/FSE - Turbo/Fast Spin Echo
            "is_tse": any([
                psn["is_tse"],
                psn["is_tse_body"],
                psn["is_fse"],
                psv["has_segmented_kspace"] and pss["has_se"],
            ]),
            
            # SPACE/CUBE/VISTA - 3D TSE
            "is_space": any([
                psn["is_space"],
                psn["is_space_ir"],
            ]),
            
            # HASTE/SSFSE - Single-shot TSE
            "is_haste": psn["is_haste"],
            
            # Multi-echo SE
            "is_me_se": psn["is_me_se"],
            
            # MDME - Multi-dynamic multi-echo (SyMRI)
            "is_mdme": psn["is_mdme"],
            
            # QALAS - Quantitative mapping
            "is_qalas": psn["is_qalas"],
            
            # --- GRE Family ---
            # FLASH/SPGR - Spoiled GRE
            "is_flash": any([
                psn["is_flash"],
                psn["is_fgre"],
            ]),
            
            # TurboFLASH/TFE - Fast spoiled GRE
            "is_tfl": psn["is_tfl"],
            
            # MPRAGE/BRAVO - IR-prepared 3D GRE
            "is_mprage": psn["is_mprage"],
            
            # SSFP/bSSFP/FIESTA - Steady-state
            "is_ssfp": psn["is_trufisp"],
            
            # CISS/FIESTA-C - Constructive interference
            "is_ciss": psn["is_ciss"],
            
            # SWI - Susceptibility-weighted
            "is_swi": any([
                psn["is_swi"],
                pit["has_swi"],
            ]),
            
            # SWI flag from ImageType (for construct detection)
            "has_swi": pit["has_swi"],
            
            # --- Angio/Flow ---
            # TOF - Time-of-flight MRA
            "is_tof": any([
                psn["is_tof"],
                psv["is_tof"],
                pso["has_tof_gems"],
                pso["has_inflow_gems"],
            ]),
            
            # PC - Phase contrast MRA/flow
            "is_pc": any([
                psn["is_pc"],
                pit["hint_pca"],
            ]),
            
            # FLAIR - Fluid-attenuated IR (T1 or T2)
            "is_flair": any([
                psn["is_flair_seq"],
                pso["has_t1flair_gems"],
                pso["has_t2flair_gems"],
                pit["has_flair_synthetic"],
            ]),
            
            # DIR - Double Inversion Recovery
            "is_dir": any([
                pit["has_dir"],
                pit["has_dir_synthetic"],
            ]),
            
            # --- EPI/Functional/Diffusion ---
            # DWI/DTI - Diffusion-weighted
            # RESOLVE/MUSE - Multi-shot diffusion EPI (specific technique)
            # Detected from sequence name pattern (*re_b*) - must be checked before generic is_dwi
            "is_epi_diff_resolve": psn["is_epi_diff_resolve"],

            # Note: b_value=0 alone is NOT diffusion-weighted (it's a reference image)
            # We require EITHER:
            #   1. Definitive DWI indicators from sequence name/ImageType, OR
            #   2. b_value > 0 (actual diffusion weighting)
            # b_value=0 only triggers DWI if combined with EPI physics (likely b0 reference in DWI series)
            "is_dwi": any([
                psn["is_epi_diff"],
                psn["is_epi_diff_resolve"],
                psn["is_epi_diff_b0"],
                pit["has_diffusion"],
                # b_value > 0 is definitive diffusion weighting
                self._has_positive_b_value(),
            ]),
            
            # BOLD/fMRI
            # Note: EPI-GRE alone is not sufficient - could be SWI, T2*, etc.
            # Phase images are NEVER BOLD (BOLD produces magnitude timeseries)
            "is_bold": any([
                pit["has_fmri"],
            ]) and not pit["has_phase"],
            
            # ASL - Arterial Spin Labeling
            "is_asl": pit["has_asl"],
            
            # Perfusion (DSC/DCE)
            "is_perfusion": pit["has_perfusion"],
            
            # --- Special Techniques ---
            # MRF - MR Fingerprinting
            "is_mrf": pso["has_mrf_gems"],
            
            # Radial/PROPELLER/BLADE trajectory
            "is_radial": any([
                pso["has_propeller_gems"],
                pit["layout_radial"],
            ]),
            
            # Spiral trajectory
            "is_spiral": pso["has_spiral_gems"],
            
            # MAVRIC/metal artifact reduction
            "is_mavric": pit["has_mavric"],
            
            # IDEAL - fat/water separation
            "is_ideal": pso["has_ideal_gems"],
            
            # Cine
            "is_cine": pso["has_cine_gems"],
            
            # Quantitative map sequences
            "is_quant_map": psn["is_quant_map"],
            
            # =================================================================
            # 3. SEQUENCE VARIANTS / MODIFIERS
            # =================================================================
            
            # Spoiled (RF/gradient spoiling)
            "has_spoiled": psv["has_spoiled"],
            
            # Steady-state
            "has_steady_state": psv["has_steady_state"],
            
            # Segmented k-space (echo train)
            "has_segmented_kspace": psv["has_segmented_kspace"],
            
            # Magnetization prepared
            "has_mag_prepared": any([
                psv["has_mag_prepared"],
                pso["has_mp_gems"],
            ]),
            
            # Magnetization transfer contrast
            "has_mtc": any([
                psv["has_mtc"],
                pso["has_mt"],
            ]),
            
            # Oversampling
            "has_oversampling": psv["has_oversampling"],
            
            # Fat saturation
            "has_fat_sat": pso["has_fat_sat"],
            
            # Water excitation
            "has_water_excitation": pso["has_water_exc"],
            
            # Flow compensation
            "has_flow_comp": pso["has_flow_comp"],
            
            # Partial Fourier
            "has_partial_fourier": any([
                pso["has_partial_fourier_phase"],
                pso["has_partial_fourier_freq"],
            ]),
            
            # Parallel imaging
            "has_parallel_imaging": any([
                pso["has_parallel_gems"],
                pso["has_hypersense"],
                pso["has_cs_gems"],
            ]),
            
            # Gating (cardiac, respiratory, etc.)
            "has_gating": pso["has_gating"],
            
            # =================================================================
            # 4. ACQUISITION PROPERTIES (from fingerprint + stack_key)
            # =================================================================
            
            # 3D acquisition
            "is_3d": any([
                self.mr_acquisition_type == "3D",
                pit["is_3d_view"],
            ]),
            
            # 2D acquisition
            "is_2d": any([
                self.mr_acquisition_type == "2D",
                pit["is_2d_view"],
            ]),
            
            # Multi-echo (series split by echo time)
            "is_multi_echo": self.stack_key == "multi_echo",
            
            # Multi-TI (series split by inversion time)
            "is_multi_ti": self.stack_key == "multi_ti",
            
            # Multi-FA (series split by flip angle)
            "is_multi_fa": self.stack_key == "multi_flip_angle",
            
            # =================================================================
            # 5. DERIVED / SYNTHETIC / QUANTITATIVE
            # =================================================================
            
            # --- Core Derived/Synthetic ---
            # Derived image (not original acquisition)
            "is_derived": pit["is_derived"],
            
            # Synthetic MRI output (any)
            "is_synthetic": any([
                pit["is_synthetic"],
                pss["has_synthetic"],
                psv["is_synthetic"],
                pit["has_t1_synthetic"],
                pit["has_t2_synthetic"],
                pit["has_pd_synthetic"],
                pit["has_flair_synthetic"],
                pit["has_stir_synthetic"],
                pit["has_psir_synthetic"],
                pit["has_dir_synthetic"],
            ]),
            
            # Specific synthetic outputs
            "has_t1_synthetic": pit["has_t1_synthetic"],
            "has_t2_synthetic": pit["has_t2_synthetic"],
            "has_pd_synthetic": pit["has_pd_synthetic"],
            "has_flair_synthetic": pit["has_flair_synthetic"],
            "has_myelin": pit["has_myelin"],
            
            # --- Quantitative Maps ---
            # Any quantitative map
            "is_qmap": any([
                pit["has_qmap"],
                pss["has_qmap"],
                pit["has_t1_map"],
                pit["has_t2_map"],
                pit["has_r1"],
                pit["has_r2"],
            ]),
            
            # T1 map
            "has_t1_map": pit["has_t1_map"],
            
            # T2 map
            "has_t2_map": pit["has_t2_map"],
            
            # R1 map (1/T1)
            "has_r1": pit["has_r1"],
            
            # R2 map (1/T2)
            "has_r2": pit["has_r2"],

            # --- MP2RAGE Outputs ---
            # MP2RAGE produces multiple outputs: INV1, INV2, UNI, UNI-DEN, T1map
            # INV1/INV2 are ORIGINAL, others are DERIVED
            # Detection uses TI-based threshold (1800ms) with text fallback

            # Uniform image (bias-corrected T1w from MP2RAGE)
            # Detected via ImageType tokens: M\UNI, UNIFORM
            "has_uniform": pit["has_uniform"],

            # Uniform denoised (UNI-DEN) - uses β regularization for clean background
            # Primary input for segmentation pipelines
            "is_uniform_denoised": self._is_uniform_denoised(),

            # MP2RAGE INV1 - first inversion (short TI ~700-1000ms, T1-weighted)
            # TI-based detection with text fallback
            "is_mp2rage_inv1": self._is_mp2rage_inv1_by_ti() or self._has_inv1_in_text(),

            # MP2RAGE INV2 - second inversion (long TI ~2500-3200ms, PD-weighted)
            # TI-based detection with text fallback
            "is_mp2rage_inv2": self._is_mp2rage_inv2_by_ti() or self._has_inv2_in_text(),

            # --- Diffusion-Derived ---
            # Any diffusion-derived (ADC, FA, etc.)
            "is_diffusion_derived": any([
                pit["has_adc"],
                pit["has_eadc"],
                pit["has_fa"],
                pit["has_trace"],
                pit["has_exp_diffusion"],
                pit["has_calc_bval"],
            ]),
            
            # Specific diffusion maps
            "has_adc": pit["has_adc"],
            "has_eadc": pit["has_eadc"],
            "has_fa": pit["has_fa"],
            "has_trace": pit["has_trace"],
            
            # --- Perfusion-Derived ---
            # Any perfusion-derived (CBF, CBV, MTT, etc.)
            "is_perfusion_derived": any([
                pit["has_cbf"],
                pit["has_cbv"],
                pit["has_mtt"],
                pit["has_ttp"],
                pit["has_tmax"],
                pit["has_oef"],
                pit["has_cmro2"],
                pit["has_leakage"],
            ]),
            
            # Specific perfusion maps
            "has_cbf": pit["has_cbf"],
            "has_cbv": pit["has_cbv"],
            "has_mtt": pit["has_mtt"],
            "has_tmax": pit["has_tmax"],
            "has_ttp": pit["has_ttp"],
            
            # =================================================================
            # 6. IMAGE TYPE / COMPONENT FLAGS
            # =================================================================
            
            # --- Core Image Type ---
            # Original acquisition
            "is_original": pit["is_original"],
            
            # Primary image
            "is_primary": pit["is_primary"],
            
            # Secondary image
            "is_secondary": pit["is_secondary"],
            
            # Localizer/Scout
            "is_localizer": any([
                pit["is_localizer"],
                psn["is_localizer"],
                pso["has_scout_mode"],
            ]),
            
            # --- Complex Components ---
            # Magnitude image
            "has_magnitude": pit["has_magnitude"],
            
            # Phase image
            "has_phase": pit["has_phase"],
            
            # Real component
            "has_real": pit["has_real"],
            
            # Imaginary component
            "has_imaginary": pit["has_imaginary"],
            
            # --- Dixon/Fat-Water ---
            # Dixon technique
            "has_dixon": any([
                pit["has_dixon"],
                pso["has_dix_fat"],
                pso["has_dix_water"],
            ]),
            
            # Water image
            "has_water": pit["has_water"],
            
            # Fat image
            "has_fat": pit["has_fat"],
            
            # In-phase
            "has_in_phase": pit["has_in_phase"],
            
            # Out-of-phase (opposed phase)
            "has_out_phase": pit["has_out_phase"],
            
            # --- Special IR Reconstructions ---
            # PSIR (phase-sensitive IR)
            "has_psir": any([
                pit["has_psir"],
                pit["has_psir_synthetic"],
            ]),
            
            # STIR synthetic
            "has_stir": pit["has_stir_synthetic"],
            
            # =================================================================
            # 7. POST-PROCESSING / RECONSTRUCTION
            # =================================================================
            
            # MIP - Maximum Intensity Projection
            "is_mip": any([
                pit["is_mip"],
                pit["is_mip_thin"],
            ]),
            
            # MinIP - Minimum Intensity Projection
            "is_minip": pit["is_minip"],
            
            # Any projection image
            "is_projection": pit["is_projection"],
            
            # MPR - Multiplanar Reconstruction
            # Detected from: ImageType tokens (MPR, REFORMATTED) OR text_search_blob
            # Text-based detection only applies to DERIVED images to avoid
            # false positives from MPRAGE sequence names in ORIGINAL images
            "is_mpr": any([
                pit["is_mpr"],
                pit["is_mpr_thick"],
                self._has_mpr_in_text(),  # DERIVED + "mpr" word in text
            ]),
            
            # Subtraction image
            "is_subtraction": pit["is_subtraction"],
            
            # Mean/Average image
            "is_mean": pit["is_mean"],
            
            # Composite/Combined image
            "is_composite": pit["is_composite"],
            
            # Vascular processing
            "is_vascular": pit["is_vascular"],
            
            # Motion correction
            "has_moco": pit["has_moco"],
            
            # Distortion correction
            "has_distortion_correction": any([
                pit["is_2d_view"],  # DIS2D
                pit["is_3d_view"],  # DIS3D
            ]),
            "has_dis2d": pit["is_2d_view"],
            "has_dis3d": pit["is_3d_view"],
            
            # Normalized
            "is_normalized": pit["is_normalized"],
            
            # =================================================================
            # 8. EXCLUSION / QA FLAGS
            # =================================================================
            
            # Error image
            "is_error": pit["is_error"],
            
            # Screenshot/pasted
            "is_screenshot": pit["is_screenshot"],
            
            # QA/QC images
            "is_qa": pit["is_qa"],
            
            # Tuning/calibration
            "is_tune": psn["is_tune"],
            
            # WIP (work in progress)
            "is_wip": psn["is_wip"],
            
            # =================================================================
            # 9. BRANCH-SPECIFIC FLAGS (SWI, SyMRI, Dixon)
            # =================================================================
            
            # --- SWI Branch Flags ---
            # SWI Magnitude (source image, T2*-weighted)
            "is_swi_magnitude": any([
                pit["has_magnitude"] and (psn["is_swi"] or pit["has_swi"]),
                pit["has_swi"] and not pit["is_minip"] and not pit["has_phase"],
            ]),
            
            # SWI Processed (combined/filtered SWI)
            "is_swi_processed": any([
                pit["has_swi"] and pit["is_derived"],
                "SW_M_FFE" in (pit.get("all_tokens") or set()) and pit["is_derived"],
            ]),
            
            # QSM - Quantitative Susceptibility Mapping
            "has_qsm": pit.get("has_qsm", False),
            
            # EPI-based SWI (fast acquisition variant)
            "is_epi_swi": any([
                psn["is_swi"] and pss["has_epi"],
                psn["is_epi_gre"] and pit["has_swi"],
            ]),
            
            # --- SyMRI Branch Flags ---
            # SyMRI raw source data (MDME/QALAS acquisition, not derived outputs)
            "is_symri_source": any([
                psn["is_mdme"] and pit["is_original"],
                psn["is_qalas"] and pit["is_original"],
            ]),

            # Quantitative maps from QMAP tokens (e.g., DERIVED\PRIMARY\QMAP\T1)
            "has_qmap_t1": pit.get("has_qmap_t1", False),
            "has_qmap_t2": pit.get("has_qmap_t2", False),
            "has_qmap_pd": pit.get("has_qmap_pd", False),
            # MULTI_QMAP - GE MAGiC bundled T1/T2/PD maps in single series
            "has_multi_qmap": pit.get("has_multi_qmap", False),

            # Specific SyMRI map types
            "has_pd_map": pit.get("has_pd_map", False),
            "has_b1_map": pit.get("has_b1_map", False),
            "has_b0_map": pit.get("has_b0_map", False),

            # DIR Synthetic (Double Inversion Recovery) - handles both patterns
            "has_dir_synthetic": any([
                pit.get("has_dir_synthetic", False),
                pit.get("has_dir_synthetic_token", False),
            ]),

            # PSIR Synthetic (Phase-Sensitive IR) - handles both patterns
            "has_psir_synthetic": any([
                pit.get("has_psir_synthetic", False),
                pit.get("has_psir_synthetic_token", False),
            ]),

            # STIR Synthetic
            "has_stir_synthetic": pit.get("has_stir_synthetic", False),

            # GE MAGiC raw data
            "is_magic_raw": pit.get("is_magic_raw", False),
            
            # --- Dixon Branch Flags ---
            # Dixon water image (anatomy visible)
            "is_dixon_water": any([
                pit["has_water"] and pit["has_dixon"],
                pit["has_water"] and pso["has_dix_water"],
                pit["has_water"] and (pit["has_in_phase"] or pit["has_out_phase"]),
            ]),
            
            # Dixon fat image (fat visible)
            "is_dixon_fat": any([
                pit["has_fat"] and pit["has_dixon"],
                pit["has_fat"] and pso["has_dix_fat"],
                pit["has_fat"] and (pit["has_in_phase"] or pit["has_out_phase"]),
            ]),
            
            # Dixon in-phase (water + fat constructive)
            "is_dixon_in_phase": pit["has_in_phase"],
            
            # Dixon out-of-phase / opposed phase (water + fat destructive)
            "is_dixon_out_phase": pit["has_out_phase"],
            
            # Fat fraction map (derived from Dixon)
            "has_fat_fraction": pit.get("has_fat_fraction", False),
        }
    
    # =========================================================================
    # Convenience Methods
    # =========================================================================
    
    # Maximum realistic diffusion b-value in s/mm²
    # Clinical DWI: 0-3000, Research HARDI/DSI: up to 10000-15000
    # Values beyond 20000 are unrealistic and indicate garbage data
    MAX_REALISTIC_B_VALUE = 20000
    
    def _has_positive_b_value(self) -> bool:
        """
        Check if diffusion b-value is present, greater than zero, and realistic.

        b_value=0 is a reference image, not diffusion-weighted.
        Only b_value > 0 indicates actual diffusion weighting.

        Validation:
            - Must be > 0 (not a b0 reference)
            - Must be <= MAX_REALISTIC_B_VALUE (20000 s/mm²) to filter garbage data
            - Values like 7.23e+75 are clearly invalid DICOM data

        Returns:
            True if b_value exists, is > 0, and is realistic
        """
        if self.mr_diffusion_b_value is None:
            return False
        try:
            # Handle string or numeric b_value
            b_val = float(self.mr_diffusion_b_value)
            # Must be positive AND within realistic range
            return 0 < b_val <= self.MAX_REALISTIC_B_VALUE
        except (ValueError, TypeError):
            # If we can't parse it, assume it's not valid
            return False

    def _has_mpr_in_text(self) -> bool:
        """
        Check if text_search_blob contains 'mpr' as a word, for DERIVED images only.

        This detects MPR (Multi-Planar Reconstruction) reformats that don't have
        the MPR token in ImageType but do have 'mpr' in SeriesDescription/ProtocolName.

        Key insight: The word 'mpr' in text_search_blob can mean:
        - For ORIGINAL images: Part of MPRAGE sequence name (NOT an MPR reformat)
        - For DERIVED images: Indicates an MPR reformat (IS an MPR reformat)

        The DERIVED flag in ImageType is the reliable discriminator because:
        - ORIGINAL + "mpr" in text = MPRAGE acquisition (correctly excluded)
        - DERIVED + "mpr" in text = MPR reformat (correctly detected)

        Note: We don't need to exclude 'mprage' because DERIVED images with
        'mprage mpr' in text ARE legitimate MPR reformats of MPRAGE acquisitions.

        Returns:
            True if DERIVED image with 'mpr' word boundary in text_search_blob
        """
        pit = self.parsed_image_type

        # Only applies to DERIVED images
        if not pit["is_derived"]:
            return False

        # Check for 'mpr' as a word (not substring) in text_search_blob
        text = (self.text_search_blob or "").lower()
        if not text:
            return False

        # Use word boundary matching: \bmpr\b
        # This matches "mpr", "mpr cor", "mprage mpr sag" but NOT "compress", "empty"
        return bool(re.search(r'\bmpr\b', text))

    # =========================================================================
    # MP2RAGE Output Detection Helpers
    # =========================================================================
    # MP2RAGE produces multiple outputs with distinct characteristics:
    # - INV1: First inversion image (TI ~700-1000ms), strong T1 weighting
    # - INV2: Second inversion image (TI ~2500-3200ms), PD-like weighting
    # - UNI: Uniform/bias-corrected T1w (has salt-and-pepper noise)
    # - UNI-DEN: Denoised uniform (clean background, primary for segmentation)
    # - T1map: Quantitative T1 relaxation map
    #
    # Detection strategy:
    # - INV1/INV2: TI-based threshold (1800ms) with text fallback
    # - UNI/UNI-DEN: ImageType tokens (M\UNI, UNIFORM) + text patterns
    # =========================================================================

    # TI threshold to distinguish INV1 from INV2
    # INV1: 700-1000ms (max observed: 1000ms)
    # INV2: 2500-3200ms (min observed: 2500ms)
    # Safe midpoint with margin: 1800ms
    MP2RAGE_TI_THRESHOLD = 1800.0

    def _has_mp2rage_context(self) -> bool:
        """
        Check if this stack is in an MP2RAGE context.

        MP2RAGE detection requires 'mp2rage' keyword in text_search_blob.
        This prevents false positives on other IR sequences (FLAIR, STIR, etc.)
        that may have similar TI values.

        Returns:
            True if MP2RAGE context is detected
        """
        text = (self.text_search_blob or "").lower()
        return "mp2rage" in text

    def _is_mp2rage_inv1_by_ti(self) -> bool:
        """
        Check if this is an MP2RAGE INV1 based on TI value.

        INV1 is acquired at short TI (~700-1000ms) for strong T1 weighting.
        Only applies to ORIGINAL images in MP2RAGE context.

        Returns:
            True if TI suggests this is INV1
        """
        pit = self.parsed_image_type

        # Only applies to ORIGINAL images
        if not pit["is_original"]:
            return False

        # Require MP2RAGE context to avoid false positives on other IR sequences
        if not self._has_mp2rage_context():
            return False

        # Need valid TI value
        if not self.mr_ti or self.mr_ti <= 0:
            return False

        return self.mr_ti < self.MP2RAGE_TI_THRESHOLD

    def _is_mp2rage_inv2_by_ti(self) -> bool:
        """
        Check if this is an MP2RAGE INV2 based on TI value.

        INV2 is acquired at long TI (~2500-3200ms) for PD-like weighting.
        Only applies to ORIGINAL images in MP2RAGE context.

        Returns:
            True if TI suggests this is INV2
        """
        pit = self.parsed_image_type

        # Only applies to ORIGINAL images
        if not pit["is_original"]:
            return False

        # Require MP2RAGE context to avoid false positives on other IR sequences
        if not self._has_mp2rage_context():
            return False

        # Need valid TI value
        if not self.mr_ti or self.mr_ti <= 0:
            return False

        return self.mr_ti >= self.MP2RAGE_TI_THRESHOLD

    def _has_inv1_in_text(self) -> bool:
        """
        Check if text_search_blob contains 'inv1' as a word.
        Fallback detection for MP2RAGE INV1 when TI is unavailable.

        Returns:
            True if 'inv1' found as word boundary match
        """
        text = (self.text_search_blob or "").lower()
        if not text:
            return False
        return bool(re.search(r'\binv1\b', text))

    def _has_inv2_in_text(self) -> bool:
        """
        Check if text_search_blob contains 'inv2' as a word.
        Fallback detection for MP2RAGE INV2 when TI is unavailable.

        Returns:
            True if 'inv2' found as word boundary match
        """
        text = (self.text_search_blob or "").lower()
        if not text:
            return False
        return bool(re.search(r'\binv2\b', text))

    def _is_uniform_denoised(self) -> bool:
        r"""
        Check if this is an MP2RAGE UNI-DEN (denoised uniform) image.

        UNI-DEN uses a regularization factor (β) to suppress salt-and-pepper
        noise in the background. It's the primary input for segmentation.

        Detection: has_uniform flag + text pattern for denoised variant.
        Both UNI and UNI-DEN have M\UNI or UNIFORM in ImageType.

        Returns:
            True if this is a denoised uniform image
        """
        pit = self.parsed_image_type

        # Must be DERIVED (UNI/UNI-DEN are computed from INV1+INV2)
        if not pit["is_derived"]:
            return False

        # Must have uniform indicator in ImageType
        if not pit["has_uniform"]:
            return False

        # Check for denoised pattern in text
        # Patterns: "uni - den", "uni-den", "uniden", "uniform denoised"
        text = (self.text_search_blob or "").lower()
        if not text:
            return False

        # Match various denoised patterns
        denoised_patterns = [
            r'uni\s*-\s*den',      # "uni - den", "uni-den"
            r'uniden',             # "uniden"
            r'uniform\s+denoised', # "uniform denoised"
            r'denoised\s+uniform', # "denoised uniform"
        ]

        for pattern in denoised_patterns:
            if re.search(pattern, text):
                return True

        return False

    def should_exclude(self) -> bool:
        """
        Check if this stack should be excluded from classification.
        
        Excludes:
        - SECONDARY without PRIMARY (workstation reformats)
        - Screenshots/pasted images
        - Error maps
        
        Returns:
            True if should be excluded
        """
        pit = self.parsed_image_type
        
        # SECONDARY without PRIMARY
        if pit["is_secondary"] and not pit["is_primary"]:
            return True
        
        # Screenshots
        if pit["is_screenshot"]:
            return True
        
        # Error maps
        if pit["is_error"]:
            return True
        
        return False
    
    def has_any_diffusion_construct(self) -> bool:
        """Check if any diffusion-derived construct is present."""
        pit = self.parsed_image_type
        return any([
            pit["has_adc"],
            pit["has_eadc"],
            pit["has_fa"],
            pit["has_trace"],
            pit["has_exp_diffusion"],
        ])
    
    def has_any_perfusion_construct(self) -> bool:
        """Check if any perfusion-derived construct is present."""
        pit = self.parsed_image_type
        return any([
            pit["has_cbf"],
            pit["has_cbv"],
            pit["has_mtt"],
            pit["has_ttp"],
            pit["has_tmax"],
            pit["has_oef"],
        ])
    
    def has_any_synthetic(self) -> bool:
        """Check if any synthetic MRI indicator is present."""
        pit = self.parsed_image_type
        psn = self.parsed_sequence_name
        pss = self.parsed_scanning_sequence
        
        return any([
            pit["has_t1_synthetic"],
            pit["has_t2_synthetic"],
            pit["has_pd_synthetic"],
            pit["has_flair_synthetic"],
            pit["has_uniform"],
            psn["is_mdme"],
            psn["is_qalas"],
            pss["has_synthetic"],
        ])
    
    @classmethod
    def from_fingerprint(cls, fp: Dict[str, Any]) -> "ClassificationContext":
        """
        Create context from a StackFingerprint dictionary.
        
        Args:
            fp: Dictionary with fingerprint fields
        
        Returns:
            ClassificationContext instance
        """
        return cls(
            image_type=fp.get("image_type"),
            scanning_sequence=fp.get("scanning_sequence"),
            sequence_variant=fp.get("sequence_variant"),
            scan_options=fp.get("scan_options"),
            stack_sequence_name=fp.get("stack_sequence_name"),
            text_search_blob=fp.get("text_search_blob"),
            contrast_search_blob=fp.get("contrast_search_blob"),
            manufacturer=fp.get("manufacturer"),
            mr_tr=fp.get("mr_tr"),
            mr_te=fp.get("mr_te"),
            mr_ti=fp.get("mr_ti"),
            mr_flip_angle=fp.get("mr_flip_angle"),
            mr_echo_train_length=fp.get("mr_echo_train_length"),
            mr_diffusion_b_value=fp.get("mr_diffusion_b_value"),
            stack_orientation=fp.get("stack_orientation"),
            fov_x=fp.get("fov_x"),
            fov_y=fp.get("fov_y"),
            aspect_ratio=fp.get("aspect_ratio"),
            stack_n_instances=fp.get("stack_n_instances", 0),
            # New fields for unified flags
            mr_acquisition_type=fp.get("mr_acquisition_type"),
            stack_key=fp.get("stack_key"),
        )
