"""
Comprehensive tests for parse_sequence_name() using real database values.

This test file uses unique stack_sequence_name values from the stack_fingerprint table
to verify the parser handles every real-world case correctly.

The sequence name parser extracts technique hints from Siemens/GE/Philips sequence names.
These flags are used downstream by detectors to determine the actual technique.

Run with: python -m pytest backend/src/classification/tests/test_context_sequence_name.py -v
Or standalone: python backend/src/classification/tests/test_context_sequence_name.py
"""

from typing import Dict, List, Tuple, Any
import sys
import os

# Add parent to path for standalone execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from backend.src.classification.core.context import parse_sequence_name


# =============================================================================
# TEST CASES FROM DATABASE (830 unique sequence names)
# Organized by technique category
# =============================================================================

# Format: (sequence_name, expected_flags_dict)
# expected_flags_dict contains ONLY the flags we want to verify (sparse)

TEST_CASES: List[Tuple[str, Dict[str, bool]]] = [
    # =========================================================================
    # SECTION 1: LOCALIZER/SCOUT
    # These should be filtered out early in the pipeline
    # =========================================================================
    
    ("scout", {"is_localizer": True}),
    ("fl1Scout", {"is_localizer": True, "is_flash": True}),
    ("localizer", {"is_localizer": True}),
    
    # =========================================================================
    # SECTION 2: FLASH/2D GRE - Siemens FLASH sequences
    # Pattern: *fl{2d,3d}{1,2,...} or fl{2d,3d}
    # Technique: SP-GRE (Spoiled GRE)
    # =========================================================================
    
    # Most common localizer sequence (41K cases)
    ("*fl2d1", {"is_flash": True}),
    ("fl2d1", {"is_flash": True}),
    ("*fl2d1r", {"is_flash": True}),
    ("*fl2d1_7", {"is_flash": True}),
    ("*fl2d2", {"is_flash": True}),
    ("*fl2d8", {"is_flash": True}),
    ("fl2d", {"is_flash": True}),
    ("fl1", {"is_flash": True}),  # fl1 matches is_flash pattern
    
    # 3D FLASH
    ("*fl3d1", {"is_flash": True}),
    ("*fl3d1_ns", {"is_flash": True}),
    ("*fl3d1r", {"is_flash": True}),
    ("*fl3d1r_t50", {"is_flash": True}),
    ("*fl3d1r_t70", {"is_flash": True}),
    ("*fl3d1r_tm", {"is_flash": True}),
    ("*fl3d1r_ts", {"is_flash": True}),
    ("*fl3d2", {"is_flash": True}),
    ("*fl3d2_ns", {"is_flash": True}),
    ("fl3d1", {"is_flash": True}),
    ("fl3d2r", {"is_flash": True}),
    ("fl3d1r_t70", {"is_flash": True}),
    
    # Dynamic FLASH
    ("*fldyn3d1", {"is_flash": False}),  # fldyn is different pattern
    ("*fldyn3d1_ns", {"is_flash": False}),
    ("*fldyn3d2", {"is_flash": False}),
    
    # =========================================================================
    # SECTION 3: TurboFLASH (TFL) - Fast T1-weighted GRE
    # Pattern: tfl{2d,3d}
    # Technique: Can be MPRAGE (with IR) or FSP-GRE (without IR)
    # =========================================================================
    
    # Most common MPRAGE sequence (21K cases)
    # Note: TFL contains fl3d, so is_flash is also True - this is expected
    # The technique detector will use is_tfl + IR to distinguish MPRAGE from FSP-GRE
    ("*tfl3d1_16", {"is_tfl": True, "is_flash": True}),
    ("*tfl3d1", {"is_tfl": True, "is_flash": True}),
    ("*tfl3d1_ns", {"is_tfl": True, "is_flash": True}),
    ("*tfl3d1_16ns", {"is_tfl": True, "is_flash": True}),
    ("*tfl3d1_192ns", {"is_tfl": True, "is_flash": True}),
    ("*tfl3d1_164ns", {"is_tfl": True, "is_flash": True}),
    ("*tfl3d1_240", {"is_tfl": True, "is_flash": True}),
    ("*tfl3d1_230", {"is_tfl": True, "is_flash": True}),
    ("tfl3d1", {"is_tfl": True, "is_flash": True}),
    ("tfl3d1_ns", {"is_tfl": True, "is_flash": True}),
    
    # =========================================================================
    # SECTION 4: TSE/FSE - Turbo/Fast Spin Echo
    # Pattern: tse{2d,3d} or fse
    # Technique: TSE
    # =========================================================================
    
    # 2D TSE variants (very common)
    ("*tse2d1_3", {"is_tse": True}),
    ("*tse2d1_4", {"is_tse": True}),
    ("*tse2d1_5", {"is_tse": True}),
    ("*tse2d1_6", {"is_tse": True}),
    ("*tse2d1_7", {"is_tse": True}),
    ("*tse2d1_11", {"is_tse": True}),
    ("*tse2d1_13", {"is_tse": True}),
    ("*tse2d1_15", {"is_tse": True}),
    ("*tse2d1_16", {"is_tse": True}),
    ("*tse2d1_17", {"is_tse": True}),
    ("*tse2d1_18", {"is_tse": True}),
    ("*tse2d1_19", {"is_tse": True}),
    ("*tse2d1_22", {"is_tse": True}),
    ("*tse2d2_5", {"is_tse": True}),
    ("*tse2d2_6", {"is_tse": True}),
    ("*tse2d2_7", {"is_tse": True}),
    ("*tse2d2rs5", {"is_tse": True}),
    ("tse2-5", {"is_tse": True}),
    ("tse1-7", {"is_tse": True}),
    ("tse1-15", {"is_tse": True}),
    ("tse1-3", {"is_tse": True}),
    ("tse1-11", {"is_tse": True}),
    ("tse2d1_3", {"is_tse": True}),
    ("tse2d1_15", {"is_tse": True}),
    ("tse2d1_16", {"is_tse": True}),
    ("tse2d1_18", {"is_tse": True}),
    ("tse2d2rs5", {"is_tse": True}),
    ("tse2d2_6", {"is_tse": True}),
    ("tse2d2_7", {"is_tse": True}),
    
    # TSE with restore/body variants
    ("*tseR2d1rs12", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rr19", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rs16", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rr18", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1_17", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rs17", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rr11", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rr17", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1_15", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rr13", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rs13", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rr15", {"is_tse": True, "is_tse_body": True}),
    ("*tseR2d1rr16", {"is_tse": True, "is_tse_body": True}),
    ("tseR2d1rs17", {"is_tse": True, "is_tse_body": True}),
    ("tseR2d1rr13", {"is_tse": True, "is_tse_body": True}),
    ("tseR2d1rr18", {"is_tse": True, "is_tse_body": True}),
    ("tseR2d1rr37", {"is_tse": True, "is_tse_body": True}),
    
    # TSE with rr/rs suffixes (likely specific variants)
    ("*tse2d1rr22", {"is_tse": True}),
    ("*tse2d1rs15", {"is_tse": True}),
    ("*tse2d1rr12", {"is_tse": True}),
    ("*tse2d1rr2", {"is_tse": True}),
    ("*tse2d1rs3", {"is_tse": True}),
    ("*tse2d1rs4", {"is_tse": True}),
    ("*tse2d1rs13", {"is_tse": True}),
    ("*tse2d1rr3", {"is_tse": True}),
    ("*tse2d1rr15", {"is_tse": True}),
    ("*tse2d1rr18", {"is_tse": True}),
    ("tse2d1rr22", {"is_tse": True}),
    ("tse2d1rs15", {"is_tse": True}),
    ("tse2d1rs4", {"is_tse": True}),
    ("tse2d1rr2", {"is_tse": True}),
    
    # TSE with blade (motion correction)
    ("*tseB2d1_35", {"is_tse": True}),
    ("*tseB2d1_17", {"is_tse": True}),
    ("tseB2d1_35", {"is_tse": True}),
    
    # 3D TSE
    ("tse3d1_107ns", {"is_tse": True}),
    
    # FSE (GE naming)
    ("fse", {"is_fse": True, "is_tse": False}),
    ("FSE", {"is_fse": True}),
    ("FSE+15_nBW_slt", {"is_fse": True}),
    ("IRFSE", {"is_ir_fse": True, "is_fse": False}),
    
    # Short notation
    ("ts2_5", {"is_tse": False}),  # ts2 doesn't match tse pattern
    ("ts1_7", {"is_tse": False}),
    ("ts1_15", {"is_tse": False}),
    ("ts1_3", {"is_tse": False}),
    ("ts1_23", {"is_tse": False}),
    
    # =========================================================================
    # SECTION 5: IR-TSE/TIR - Inversion Recovery TSE (FLAIR, STIR, etc.)
    # Pattern: tir{2d,3d} or tirm
    # Technique: IR-TSE (with various TI for FLAIR, STIR, DIR)
    # =========================================================================
    
    # TIR sequences (11K+ cases)
    ("tir2d2_5", {"is_ir_tse": True, "is_tse": False}),
    ("*tir2d1_21", {"is_ir_tse": True}),
    ("*tir2d1_12", {"is_ir_tse": True}),
    ("*tir2d1rr18", {"is_ir_tse": True}),
    ("*tir2d1_16", {"is_ir_tse": True}),
    ("*tir2d1_17", {"is_ir_tse": True}),
    ("*tir2d1_9", {"is_ir_tse": True}),
    ("*tir2d1_13", {"is_ir_tse": True}),
    ("*tir2d1_8", {"is_ir_tse": True}),
    ("*tir2d1_27", {"is_ir_tse": True}),
    ("*tir2d1_11", {"is_ir_tse": True}),
    ("*tir2d1_7", {"is_ir_tse": True}),
    ("*tir2d1_14", {"is_ir_tse": True}),
    ("*tir2d1_18", {"is_ir_tse": True}),
    ("*tir2d1_10", {"is_ir_tse": True}),
    ("*tir2d1rr14", {"is_ir_tse": True}),
    ("tir2d1_21", {"is_ir_tse": True}),
    ("tir2d1_12", {"is_ir_tse": True}),
    ("tir2d1_16", {"is_ir_tse": True}),
    ("tir2d1rr18", {"is_ir_tse": True}),
    ("tir2d1_13", {"is_ir_tse": True}),
    ("tir2d1_10", {"is_ir_tse": True}),
    
    # TIRM (Turbo IR magnitude)
    ("tirm1", {"is_ir_tse": True}),
    ("tir1m11", {"is_ir_tse": True}),
    ("tir1i7m", {"is_ir_tse": True}),
    ("tir1_11m", {"is_ir_tse": True}),
    ("tir1m7", {"is_ir_tse": True}),
    
    # 3D TIR
    ("tir3d1_107ns", {"is_ir_tse": True}),
    
    # Quantitative TIR
    ("qtir2d2_5", {"is_ir_tse": True, "is_quant_map": True}),
    
    # =========================================================================
    # SECTION 6: SPACE/3D-TSE - SPACE, CUBE, VISTA sequences
    # Pattern: spc{3d,ir,R}
    # Technique: 3D-TSE
    # =========================================================================
    
    # SPACE with IR (FLAIR 3D) - 20K+ cases
    ("*spcir_242ns", {"is_space_ir": True, "is_space": False}),
    ("*spcir_278ns", {"is_space_ir": True}),
    ("*spcir_257ns", {"is_space_ir": True}),
    ("*spcir_220ns", {"is_space_ir": True}),
    ("*spcir_260ns", {"is_space_ir": True}),
    ("*spcir_256ns", {"is_space_ir": True}),
    ("*spcir_125", {"is_space_ir": True}),
    ("*spcir3d1_242ns", {"is_space_ir": True}),
    ("*spcir3d1_282ns", {"is_space_ir": True}),
    ("*spcir3d1_246ns", {"is_space_ir": True}),
    ("*spcir3d1_214ns", {"is_space_ir": True}),
    ("spcir3d1_242ns", {"is_space_ir": True}),
    
    # SPACE (T2 or PD, no IR)
    ("*spcR_282ns", {"is_space": True, "is_space_ir": False}),
    ("*spcR_36ns", {"is_space": True}),
    ("*spcR_42ns", {"is_space": True}),
    ("*spcR_44ns", {"is_space": True}),
    ("*spcR_88", {"is_space": True}),
    ("*spcR_270", {"is_space": True}),
    ("*spcR3d1_95", {"is_space": True}),
    ("*spc3d1_214ns", {"is_space": True}),
    ("spcR3d1_87", {"is_space": True}),
    ("spcR3d1_95", {"is_space": True}),
    
    # WIP SPACE with DANTE
    ("wip-spc-t2p+ir-dante-260ns", {"is_space": True, "is_wip": True}),
    
    # =========================================================================
    # SECTION 7: HASTE - Single-Shot TSE
    # Pattern: h{2d,3d} or haste
    # Technique: SS-TSE
    # =========================================================================
    
    ("*h2d1_205", {"is_haste": True}),
    ("*h2d1_256", {"is_haste": True}),
    ("*h2d1_115", {"is_haste": True}),
    ("*h2d1_141", {"is_haste": True}),
    ("*h2d1_102", {"is_haste": True}),
    ("*h2d1_73", {"is_haste": True}),
    ("*h2d1_109", {"is_haste": True}),
    ("*h2d1_90", {"is_haste": True}),
    ("*h2d1_96", {"is_haste": True}),
    ("*h2d1_179", {"is_haste": True}),
    ("*h2d1_232", {"is_haste": True}),
    ("*h2d1_101", {"is_haste": True}),
    ("*h2d1_122", {"is_haste": True}),
    ("*h2d1_134", {"is_haste": True}),
    ("*h2d1_144", {"is_haste": True}),
    ("*h2d1_176", {"is_haste": True}),
    ("*h2d1_192", {"is_haste": True}),
    ("*h2d1_224", {"is_haste": True}),
    ("*h2d1_240", {"is_haste": True}),
    ("*h2d1_269", {"is_haste": True}),
    ("*h2d1_307", {"is_haste": True}),
    ("*h2d1_320", {"is_haste": True}),
    ("*h2d1_369", {"is_haste": True}),
    ("*h3d1_256", {"is_haste": True}),
    ("*hR2d1_192", {"is_haste": True}),
    ("h2d1_256", {"is_haste": True}),
    
    # =========================================================================
    # SECTION 8: EPI DIFFUSION - DWI/DTI sequences
    # Pattern: ep_b{value} or ep_d3ta (DTI)
    # Technique: DWI-EPI
    # =========================================================================
    
    # b0 (non-diffusion weighted reference)
    ("*ep_b0", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("*ep_b0_1000", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("*ep_b0_2000", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("*ep_b0_3000", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("*ep_b0_800", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("*ep_b0_1200", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("ep_b0", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("ep_b0_1000", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    
    # b50 (low b-value)
    ("*ep_b50_1000", {"is_epi_diff": True, "is_epi_diff_b0": False}),
    ("*ep_b50t", {"is_epi_diff": True}),
    ("ep_b50_1000", {"is_epi_diff": True}),
    ("ep_b50t", {"is_epi_diff": True}),
    
    # b500
    ("*ep_b500t", {"is_epi_diff": True}),
    ("*ep_b500s#2", {"is_epi_diff": True}),
    ("ep_b500t", {"is_epi_diff": True}),
    
    # b800
    ("*ep_b800t", {"is_epi_diff": True}),
    ("*ep_b800#1", {"is_epi_diff": True}),
    
    # b1000 (standard clinical DWI)
    ("*ep_b1000t", {"is_epi_diff": True}),
    ("*ep_b1000", {"is_epi_diff": True}),
    ("*ep_b1000#1", {"is_epi_diff": True}),
    ("*ep_b1000#2", {"is_epi_diff": True}),
    ("*ep_b1000#3", {"is_epi_diff": True}),
    ("*ep_b1000#4", {"is_epi_diff": True}),
    ("*ep_b1000#5", {"is_epi_diff": True}),
    ("*ep_b1000#6", {"is_epi_diff": True}),
    ("*ep_b1000#7", {"is_epi_diff": True}),
    ("*ep_b1000#8", {"is_epi_diff": True}),
    ("*ep_b1000#9", {"is_epi_diff": True}),
    ("*ep_b1000#10", {"is_epi_diff": True}),
    ("*ep_b1000#11", {"is_epi_diff": True}),
    ("*ep_b1000#12", {"is_epi_diff": True}),
    ("ep_b1000t", {"is_epi_diff": True}),
    ("ep_b1000", {"is_epi_diff": True}),
    
    # b1200
    ("*ep_b1200t", {"is_epi_diff": True}),
    
    # b2000 (high b-value)
    ("*ep_b2000t", {"is_epi_diff": True}),
    
    # b3000 (very high b-value)
    ("*ep_b3000t", {"is_epi_diff": True}),
    ("*ep_b3000#1", {"is_epi_diff": True}),
    ("*ep_b3000#11", {"is_epi_diff": True}),
    
    # DTI (d3ta = DTI)
    ("ep_d3ta", {"is_epi_diff": True}),
    
    # HDS (high-density sampling?)
    ("ep_hds", {"is_epi_diff": True}),
    
    # Calculated b-value
    ("*ep_calc_b1500", {"is_epi_diff": True}),
    
    # BLADE diffusion (motion correction)
    ("*blade_b0", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    ("*blade_b0_1000", {"is_epi_diff": True, "is_epi_diff_b0": True}),
    
    # =========================================================================
    # SECTION 9: EPI RESOLVE - Multi-shot EPI diffusion
    # Pattern: re_b{value}
    # Technique: MS-EPI (RESOLVE)
    # =========================================================================
    
    ("*re_b0_1000", {"is_epi_diff_resolve": True}),
    ("*re_b1000t", {"is_epi_diff_resolve": True}),
    ("*re_b0", {"is_epi_diff_resolve": True}),
    
    # =========================================================================
    # SECTION 10: EPI GRE - BOLD/fMRI/Perfusion
    # Pattern: epfid or epfid{2d,3d}
    # Technique: GRE-EPI
    # =========================================================================
    
    ("*epfid2d1_94", {"is_epi_gre": True}),
    ("*epfid2d1_128", {"is_epi_gre": True}),
    ("*epfid2d1_102", {"is_epi_gre": True}),
    ("*epfid2d1_120", {"is_epi_gre": True}),
    ("*epfid2d1_64", {"is_epi_gre": True}),
    ("*epfid2d1_80", {"is_epi_gre": True}),
    ("*epfid2d1_29", {"is_epi_gre": True}),
    ("*epfid2d1_96", {"is_epi_gre": True}),
    ("epfid3d1_15", {"is_epi_gre": True}),
    ("epfid2d1_96", {"is_epi_gre": True}),
    
    # =========================================================================
    # SECTION 11: EPI SE/IR - Spin Echo / IR EPI
    # Pattern: epse or epir
    # Technique: SE-EPI
    # =========================================================================
    
    ("*epse2d2_4", {"is_epi_se": True}),
    ("*epir2d1_154", {"is_epi_ir": True}),
    ("*epir2d1_2", {"is_epi_ir": True}),
    
    # =========================================================================
    # SECTION 12: SE - Basic Spin Echo
    # Pattern: *se{2d,1} or se{2d,1}
    # Technique: SE
    # =========================================================================
    
    ("*se2d1", {"is_se": True}),
    ("*se2d1r", {"is_se": True}),
    ("se2d1", {"is_se": True}),
    ("se2d1r", {"is_se": True}),
    ("se1a", {"is_se": False}),  # se1a doesn't match pattern
    ("se1", {"is_se": False}),   # se1 doesn't match pattern
    ("SE", {"is_se": False}),    # uppercase SE doesn't match lowercase pattern
    
    # =========================================================================
    # SECTION 13: ME (Multi-Echo) SE/GRE
    # Pattern: me{2d,3d} or memp
    # Technique: ME-SE or ME-GRE
    # =========================================================================
    
    ("*me2d1r5", {"is_me_se": True}),
    ("*me2d1r4", {"is_me_se": True}),
    ("*me2d1r6", {"is_me_se": True}),
    ("me2d1r4", {"is_me_se": True}),
    ("memp", {"is_me_se": True}),
    
    # =========================================================================
    # SECTION 14: SWI - Susceptibility Weighted Imaging
    # Pattern: swi or qswi
    # Technique: comb-ME-GRE
    # =========================================================================
    
    ("*swi3d1r", {"is_swi": True}),
    ("*swi3d4r", {"is_swi": True}),
    ("*swiW3d1r", {"is_swi": True}),
    ("swi3d1r", {"is_swi": True}),
    
    # =========================================================================
    # SECTION 15: TOF/MRA - Time of Flight Angiography
    # Pattern: tfi or tof or mra
    # Technique: TOF-MRA
    # =========================================================================
    
    ("*tfi2d1", {"is_tof": True}),
    ("tfi2d1", {"is_tof": True}),
    ("tfi2d1_62", {"is_tof": True}),
    
    # =========================================================================
    # SECTION 16: PC (Phase Contrast)
    # Pattern: pc{2d,3d}
    # Technique: PC-MRA
    # =========================================================================
    
    ("PC2Da", {"is_pc": True}),
    
    # =========================================================================
    # SECTION 17: MDME/QALAS - Quantitative MRI
    # Pattern: mdme or qalas
    # Technique: MDME or QALAS
    # =========================================================================
    
    ("*MDME2d2_5", {"is_mdme": True}),
    ("qalas3d3d1_125ns", {"is_qalas": True}),
    ("qalas3d3d1_126ns", {"is_qalas": True}),
    
    # =========================================================================
    # SECTION 18: CISS - Constructive Interference Steady State
    # Pattern: ciss or ci{2d,3d}
    # Technique: pbSS-GRE
    # =========================================================================
    
    ("*ci3d1", {"is_ciss": True}),
    
    # =========================================================================
    # SECTION 19: FLAIR - Fluid Attenuated IR (sequence name)
    # Pattern: flair or mrcflair
    # Modifier: FLAIR
    # =========================================================================
    
    ("flair", {"is_flair_seq": True}),
    ("mrcflair", {"is_flair_seq": True}),
    
    # =========================================================================
    # SECTION 20: MPRAGE - Explicit MPRAGE or BRAVO in name
    # Pattern: mprage or bravo
    # Technique: MPRAGE
    # =========================================================================
    
    ("mpr", {"is_mprage": False}),  # mpr alone doesn't match mprage
    ("mpr1T1ns", {"is_mprage": False}),  # contains mpr but not mprage
    # Note: actual MPRAGE detection requires combining tfl + IR
    
    # =========================================================================
    # SECTION 21: TUNE/CALIBRATION
    # Pattern: tune or fi3d1tun
    # Should be excluded from analysis
    # =========================================================================
    
    ("tun_s", {"is_tune": True}),
    ("fi3d1tun", {"is_tune": True}),
    
    # =========================================================================
    # SECTION 22: WIP - Work in Progress sequences
    # Pattern: wip
    # =========================================================================
    
    ("wip-spc-t2p+ir-dante-260ns", {"is_wip": True, "is_space": True}),
    
    # =========================================================================
    # SECTION 23: GRE/FFE - Generic GRE patterns
    # Pattern: gre or ffe
    # Technique: GRE
    # =========================================================================
    
    ("FE_slt", {"is_gre": False}),  # FE doesn't match ffe pattern exactly
    ("FE-80/11", {"is_gre": False}),
    
    # =========================================================================
    # SECTION 24: CSI - Chemical Shift Imaging (Spectroscopy)
    # =========================================================================
    
    ("*csi_se", {"is_se": False}),  # csi_se shouldn't match regular SE
    
    # =========================================================================
    # SECTION 25: FM (Field Map)
    # =========================================================================
    
    ("*fm2d2r", {"is_flash": False}),  # fm sequences are field maps
    
    # =========================================================================
    # SECTION 26: NULL/EDGE CASES
    # =========================================================================
    
    # Empty and None handled separately in test_edge_cases()
]


def run_tests(verbose: bool = True) -> Tuple[int, int, List[str]]:
    """
    Run all test cases and return (passed, failed, error_messages).
    """
    passed = 0
    failed = 0
    errors = []
    
    for seq_name, expected in TEST_CASES:
        try:
            result = parse_sequence_name(seq_name)
            
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
                    set_flags = [k for k, v in result.items() if v is True and k != "raw_name"]
                    print(f"✅ {seq_name:<35} → {set_flags}")
            else:
                failed += 1
                error_msg = f"❌ {seq_name}\n" + "\n".join(case_errors)
                errors.append(error_msg)
                if verbose:
                    print(error_msg)
                    
        except Exception as e:
            failed += 1
            error_msg = f"💥 {seq_name}\n  Exception: {e}"
            errors.append(error_msg)
            if verbose:
                print(error_msg)
    
    return passed, failed, errors


def test_edge_cases():
    """Test edge cases: None, empty, malformed."""
    # None
    result = parse_sequence_name(None)
    assert result["raw_name"] == ""
    assert result["is_tfl"] is False
    assert result["is_tse"] is False
    
    # Empty string
    result = parse_sequence_name("")
    assert result["raw_name"] == ""
    
    # Whitespace
    result = parse_sequence_name("   ")
    assert result["is_flash"] is False
    
    # Mixed case (should be case-insensitive)
    result = parse_sequence_name("*TFL3D1_16")
    assert result["is_tfl"] is True
    
    result = parse_sequence_name("*TSE2D1_15")
    assert result["is_tse"] is True
    
    result = parse_sequence_name("HASTE")
    assert result["is_haste"] is True


def test_technique_mapping():
    """Test that sequence patterns map to correct techniques per technique.json."""
    # Based on technique.json, verify key mappings
    technique_tests = [
        # TSE family
        ("*tse2d1_15", "is_tse", "TSE"),
        ("tse2-5", "is_tse", "TSE"),
        ("fse", "is_fse", "TSE (FSE variant)"),
        
        # 3D-TSE / SPACE
        ("*spcir_242ns", "is_space_ir", "3D-TSE with IR"),
        ("*spcR_282ns", "is_space", "3D-TSE"),
        
        # SS-TSE / HASTE
        ("*h2d1_256", "is_haste", "SS-TSE"),
        
        # IR-TSE
        ("tir2d2_5", "is_ir_tse", "IR-TSE"),
        ("tirm1", "is_ir_tse", "IR-TSE (TIRM)"),
        
        # GRE / FLASH
        ("*fl2d1", "is_flash", "SP-GRE (FLASH)"),
        ("*fl3d1_ns", "is_flash", "SP-GRE (FLASH 3D)"),
        
        # TurboFLASH
        ("*tfl3d1_16", "is_tfl", "FSP-GRE or MPRAGE (needs IR check)"),
        
        # DWI-EPI
        ("*ep_b1000t", "is_epi_diff", "DWI-EPI"),
        ("*re_b1000t", "is_epi_diff_resolve", "MS-EPI (RESOLVE)"),
        
        # GRE-EPI
        ("*epfid2d1_94", "is_epi_gre", "GRE-EPI"),
        
        # SWI
        ("*swi3d1r", "is_swi", "comb-ME-GRE (SWI)"),
        
        # TOF-MRA
        ("*tfi2d1", "is_tof", "TOF-MRA"),
        
        # CISS
        ("*ci3d1", "is_ciss", "pbSS-GRE (CISS)"),
        
        # Quantitative
        ("*MDME2d2_5", "is_mdme", "MDME"),
        ("qalas3d3d1_125ns", "is_qalas", "QALAS"),
    ]
    
    for seq_name, expected_flag, technique_desc in technique_tests:
        result = parse_sequence_name(seq_name)
        assert result.get(expected_flag), f"{seq_name} should have {expected_flag} for {technique_desc}"


def test_all_sequence_name_patterns():
    """Test all sequence_name patterns from TEST_CASES."""
    passed, failed, errors = run_tests(verbose=False)
    assert failed == 0, f"Failed {failed} tests:\n" + "\n".join(errors[:5])


def print_all_flags():
    """Print all available flags from the parser."""
    print("\n" + "=" * 80)
    print("ALL AVAILABLE FLAGS")
    print("=" * 80)
    
    result = parse_sequence_name("test")
    flags = sorted([k for k in result.keys() if k != "raw_name"])
    
    for i, flag in enumerate(flags, 1):
        print(f"  {i:3}. {flag}")
    
    print(f"\nTotal: {len(flags)} flags")


def main():
    """Run all tests."""
    print("=" * 80)
    print("PARSE_SEQUENCE_NAME COMPREHENSIVE TEST")
    print(f"Testing {len(TEST_CASES)} unique sequence_name patterns from database")
    print("=" * 80 + "\n")
    
    # Print available flags
    print_all_flags()
    
    # Run edge cases
    test_edge_cases()
    
    # Run technique mapping tests
    technique_ok = test_technique_mapping()
    
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
    
    if failed > 0 or not technique_ok:
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
