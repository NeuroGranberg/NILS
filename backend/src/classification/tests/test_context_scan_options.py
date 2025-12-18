"""
Comprehensive tests for parse_scan_options() using real database values.

This test file uses unique scan_options values from the stack_fingerprint table
to verify the parser handles every real-world case correctly.

The scan_options parser extracts acquisition modifiers from the DICOM ScanOptions tag (0018,0022).
This includes vendor-specific options like GE GEMS flags and standard options.

Run with: python -m pytest backend/src/classification/tests/test_context_scan_options.py -v
Or standalone: python backend/src/classification/tests/test_context_scan_options.py
"""

from typing import Dict, List, Tuple, Any
import sys
import os

# Add parent to path for standalone execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from backend.src.classification.core.context import parse_scan_options


# =============================================================================
# REPRESENTATIVE SCAN_OPTIONS VALUES FROM DATABASE
# Selected to cover all major patterns (60+ unique values, many combinations)
# =============================================================================

TEST_CASES: List[Tuple[str, Dict[str, bool]]] = [
    # =========================================================================
    # SECTION 1: SINGLE VALUES - Standard options
    # =========================================================================
    
    # IR - Inversion Recovery (46K cases)
    ("IR", {"has_ir": True, "has_fat_sat": False}),
    
    # FS - Fat Saturation (14K cases)
    ("FS", {"has_fat_sat": True, "has_ir": False}),
    
    # SP - Spatial Saturation (12K cases)
    ("SP", {"has_spatial_sat": True}),
    
    # PFP - Partial Fourier Phase (11K cases)
    ("PFP", {"has_partial_fourier_phase": True}),
    
    # PER - Phase Encoding Reorder (12K cases)
    ("PER", {"has_phase_reorder": True}),
    
    # PFF - Partial Fourier Frequency (4.5K cases)
    ("PFF", {"has_partial_fourier_freq": True}),
    
    # FC - Flow Compensation (1.5K cases)
    ("FC", {"has_flow_comp": True}),
    
    # OTHER (8K cases)
    ("OTHER", {"has_ir": False, "has_fat_sat": False}),
    
    # SAT1 (14K cases) - Saturation band
    ("SAT1", {"has_sat_gems": True}),
    
    # SAT3 (970 cases) - Saturation band
    ("SAT3", {"has_sat_gems": True}),
    
    # =========================================================================
    # SECTION 2: STANDARD COMBINATIONS
    # =========================================================================
    
    # IR + PFP + FS (28K cases)
    ("['IR', 'PFP', 'FS']", {"has_ir": True, "has_partial_fourier_phase": True, "has_fat_sat": True}),
    
    # PFP + FS (12K cases)
    ("['PFP', 'FS']", {"has_partial_fourier_phase": True, "has_fat_sat": True}),
    
    # PFP + PER (5K cases)
    ("['PFP', 'PER']", {"has_partial_fourier_phase": True, "has_phase_reorder": True}),
    
    # PFP + PFF + PER (7K cases)
    ("['PFP', 'PFF', 'PER']", {"has_partial_fourier_phase": True, "has_partial_fourier_freq": True, "has_phase_reorder": True}),
    
    # CG + RG + PER (5K cases) - Cardiac/Respiratory gating
    ("['CG', 'RG', 'PER']", {"has_gating": True, "has_phase_reorder": True}),
    
    # IR + PFP (4.5K cases)
    ("['IR', 'PFP']", {"has_ir": True, "has_partial_fourier_phase": True}),
    
    # SP + PER (4.4K cases)
    ("['SP', 'PER']", {"has_spatial_sat": True, "has_phase_reorder": True}),
    
    # IR + FS (2.7K cases)
    ("['IR', 'FS']", {"has_ir": True, "has_fat_sat": True}),
    
    # IR + SAT1 (2.3K cases)
    ("['IR', 'SAT1']", {"has_ir": True, "has_sat_gems": True}),
    
    # PFP + WE (2K cases) - Water Excitation
    ("['PFP', 'WE']", {"has_partial_fourier_phase": True, "has_water_exc": True}),
    
    # PFP + FS + PER (3K cases)
    ("['PFP', 'FS', 'PER']", {"has_partial_fourier_phase": True, "has_fat_sat": True, "has_phase_reorder": True}),
    
    # FS + PER (1.6K cases)
    ("['FS', 'PER']", {"has_fat_sat": True, "has_phase_reorder": True}),
    
    # PFP + PFF + CG + RG + PER (2K cases)
    ("['PFP', 'PFF', 'CG', 'RG', 'PER']", {"has_partial_fourier_phase": True, "has_partial_fourier_freq": True, "has_gating": True, "has_phase_reorder": True}),
    
    # PFP + PFF + CG + RG + FS + PER (2K cases)
    ("['PFP', 'PFF', 'CG', 'RG', 'FS', 'PER']", {"has_partial_fourier_phase": True, "has_partial_fourier_freq": True, "has_gating": True, "has_fat_sat": True, "has_phase_reorder": True}),
    
    # SP + CG + RG + PER (1.5K cases)
    ("['SP', 'CG', 'RG', 'PER']", {"has_spatial_sat": True, "has_gating": True, "has_phase_reorder": True}),
    
    # PFP + PFF (777 cases)
    ("['PFP', 'PFF']", {"has_partial_fourier_phase": True, "has_partial_fourier_freq": True}),
    
    # =========================================================================
    # SECTION 3: GE GEMS OPTIONS
    # =========================================================================
    
    # FAST_GEMS + EDR_GEMS + FILTERED_GEMS + ACC_GEMS + IR_GEMS (10K cases)
    ("['FAST_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'IR_GEMS']", {
        "has_fast_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_ir_gems": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + HYPERSENSE_GEMS + FILTERED_GEMS + ACC_GEMS + FS + FSS_GEMS + IR_GEMS (8.5K cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'HYPERSENSE_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FS', 'FSS_GEMS', 'IR_GEMS']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_hypersense": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_fat_sat": True,
        "has_fss_gems": True,
        "has_ir_gems": True,
    }),
    
    # FAST_GEMS + SEQ_GEMS + PFF (7.5K cases)
    ("['FAST_GEMS', 'SEQ_GEMS', 'PFF']", {
        "has_fast_gems": True,
        "has_seq_gems": True,
        "has_partial_fourier_freq": True,
    }),
    
    # FAST_GEMS + EDR_GEMS + SEQ_GEMS + TRF_GEMS + SS_GEMS + ACC_GEMS + PFP (6.4K cases)
    ("['FAST_GEMS', 'EDR_GEMS', 'SEQ_GEMS', 'TRF_GEMS', 'SS_GEMS', 'ACC_GEMS', 'PFP']", {
        "has_fast_gems": True,
        "has_edr_gems": True,
        "has_seq_gems": True,
        "has_trf_gems": True,
        "has_parallel_gems": True,
        "has_partial_fourier_phase": True,
    }),
    
    # FAST_GEMS + EDR_GEMS + FILTERED_GEMS + ACC_GEMS + FSP_GEMS + IR_GEMS (5.8K cases)
    ("['FAST_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FSP_GEMS', 'IR_GEMS']", {
        "has_fast_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_fsp_gems": True,
        "has_ir_gems": True,
    }),
    
    # FAST_GEMS + EDR_GEMS + FILTERED_GEMS + ACC_GEMS (4.9K cases)
    ("['FAST_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'ACC_GEMS']", {
        "has_fast_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + FILTERED_GEMS + ACC_GEMS + FS + FILTERED_GEMS (4.4K cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FS', 'FILTERED_GEMS']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_fat_sat": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + FILTERED_GEMS + ACC_GEMS + FS (4.2K cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FS']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_fat_sat": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + HYPERSENSE_GEMS + FILTERED_GEMS + ACC_GEMS + FS + IR_GEMS (4K cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'HYPERSENSE_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FS', 'IR_GEMS']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_hypersense": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_fat_sat": True,
        "has_ir_gems": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + FILTERED_GEMS + ACC_GEMS + FS + IR_GEMS (3.6K cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FS', 'IR_GEMS']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_fat_sat": True,
        "has_ir_gems": True,
    }),
    
    # EDR_GEMS + TRF_GEMS + FILTERED_GEMS + ACC_GEMS + FSA_GEMS (3K cases)
    ("['EDR_GEMS', 'TRF_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FSA_GEMS']", {
        "has_edr_gems": True,
        "has_trf_gems": True,
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_fsa_gems": True,
    }),
    
    # FAST_GEMS + EDR_GEMS + SEQ_GEMS + TRF_GEMS + SS_GEMS + ART_GEMS + ACC_GEMS + PFP (2.7K cases)
    ("['FAST_GEMS', 'EDR_GEMS', 'SEQ_GEMS', 'TRF_GEMS', 'SS_GEMS', 'ART_GEMS', 'ACC_GEMS', 'PFP']", {
        "has_fast_gems": True,
        "has_edr_gems": True,
        "has_seq_gems": True,
        "has_trf_gems": True,
        "has_parallel_gems": True,
        "has_partial_fourier_phase": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + ACC_GEMS + FS + FILTERED_GEMS (2.6K cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'ACC_GEMS', 'FS', 'FILTERED_GEMS']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_parallel_gems": True,
        "has_fat_sat": True,
        "has_filtered_gems": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + FILTERED_GEMS + SP (2.4K cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'SP']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_spatial_sat": True,
    }),
    
    # FAST_GEMS + VB_GEMS (1.2K cases)
    ("['FAST_GEMS', 'VB_GEMS']", {
        "has_fast_gems": True,
    }),
    
    # FAST_GEMS + PROP_GEMS + TRF_GEMS + FILTERED_GEMS (1.1K cases) - PROPELLER
    ("['FAST_GEMS', 'PROP_GEMS', 'TRF_GEMS', 'FILTERED_GEMS']", {
        "has_fast_gems": True,
        "has_propeller_gems": True,
        "has_trf_gems": True,
        "has_filtered_gems": True,
    }),
    
    # FAST_GEMS + PROP_GEMS + ACC_GEMS + TRF_GEMS + FILTERED_GEMS + FSA_GEMS (994 cases)
    ("['FAST_GEMS', 'PROP_GEMS', 'ACC_GEMS', 'TRF_GEMS', 'FILTERED_GEMS', 'FSA_GEMS']", {
        "has_fast_gems": True,
        "has_propeller_gems": True,
        "has_parallel_gems": True,
        "has_trf_gems": True,
        "has_filtered_gems": True,
        "has_fsa_gems": True,
    }),
    
    # EPI_GEMS + FILTERED_GEMS + ACC_GEMS + PFF (942 cases)
    ("['EPI_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'PFF']", {
        "has_filtered_gems": True,
        "has_parallel_gems": True,
        "has_partial_fourier_freq": True,
    }),
    
    # FILTERED_GEMS alone (1.1K cases)
    ("FILTERED_GEMS", {"has_filtered_gems": True}),
    
    # SAT_GEMS + VB_GEMS + PFF + SP (972 cases)
    ("['SAT_GEMS', 'VB_GEMS', 'PFF', 'SP']", {
        "has_sat_gems": True,
        "has_partial_fourier_freq": True,
        "has_spatial_sat": True,
    }),
    
    # FAST_GEMS + PROP_GEMS + ACC_GEMS + TRF_GEMS + FILTERED_GEMS (957 cases)
    ("['FAST_GEMS', 'PROP_GEMS', 'ACC_GEMS', 'TRF_GEMS', 'FILTERED_GEMS']", {
        "has_fast_gems": True,
        "has_propeller_gems": True,
        "has_parallel_gems": True,
        "has_trf_gems": True,
        "has_filtered_gems": True,
    }),
    
    # SAT_GEMS + EDR_GEMS + FILTERED_GEMS + PFF + SP (927 cases)
    ("['SAT_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'PFF', 'SP']", {
        "has_sat_gems": True,
        "has_edr_gems": True,
        "has_filtered_gems": True,
        "has_partial_fourier_freq": True,
        "has_spatial_sat": True,
    }),
    
    # VB_GEMS + EDR_GEMS + FILTERED_GEMS (881 cases)
    ("['VB_GEMS', 'EDR_GEMS', 'FILTERED_GEMS']", {
        "has_edr_gems": True,
        "has_filtered_gems": True,
    }),
    
    # SAT_GEMS + FILTERED_GEMS + PFF + SP (818 cases)
    ("['SAT_GEMS', 'FILTERED_GEMS', 'PFF', 'SP']", {
        "has_sat_gems": True,
        "has_filtered_gems": True,
        "has_partial_fourier_freq": True,
        "has_spatial_sat": True,
    }),
    
    # FAST_GEMS + EDR_GEMS + ACC_GEMS + IR_GEMS (798 cases)
    ("['FAST_GEMS', 'EDR_GEMS', 'ACC_GEMS', 'IR_GEMS']", {
        "has_fast_gems": True,
        "has_edr_gems": True,
        "has_parallel_gems": True,
        "has_ir_gems": True,
    }),
    
    # =========================================================================
    # SECTION 4: EDGE CASES
    # =========================================================================
    
    # Numeric value (likely error) - 989 cases
    ("1140850688", {"has_ir": False, "has_fat_sat": False}),
    
    # EDR_GEMS + FILTERED_GEMS (1K cases)
    ("['EDR_GEMS', 'FILTERED_GEMS']", {"has_edr_gems": True, "has_filtered_gems": True}),
]


def run_tests(verbose: bool = True) -> Tuple[int, int, List[str]]:
    """
    Run all test cases and return (passed, failed, error_messages).
    """
    passed = 0
    failed = 0
    errors = []
    
    for scan_opt, expected in TEST_CASES:
        try:
            result = parse_scan_options(scan_opt)
            
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
                    set_flags = [k for k, v in result.items() if v is True and k != "all_tokens"]
                    print(f"✅ {scan_opt[:50]:<50} → {set_flags[:4]}{'...' if len(set_flags) > 4 else ''}")
            else:
                failed += 1
                error_msg = f"❌ {scan_opt}\n" + "\n".join(case_errors)
                errors.append(error_msg)
                if verbose:
                    print(error_msg)
                    
        except Exception as e:
            failed += 1
            error_msg = f"💥 {scan_opt}\n  Exception: {e}"
            errors.append(error_msg)
            if verbose:
                print(error_msg)
    
    return passed, failed, errors


def test_edge_cases():
    """Test edge cases: None, empty, malformed."""
    # None
    result = parse_scan_options(None)
    assert result["has_ir"] is False
    assert result["has_fat_sat"] is False
    assert result["all_tokens"] == set()
    
    # Empty string
    result = parse_scan_options("")
    assert result["has_ir"] is False
    
    # Lowercase
    result = parse_scan_options("ir")
    assert result["has_ir"] is True
    
    result = parse_scan_options("['ir', 'fs']")
    assert result["has_ir"] is True
    assert result["has_fat_sat"] is True


def test_fat_sat_variants():
    """Test that different fat saturation patterns are detected."""
    # FS - standard fat sat
    result = parse_scan_options("FS")
    assert result["has_fat_sat"] is True
    
    # SFS - spectral fat sat
    result = parse_scan_options("SFS")
    assert result["has_fat_sat"] is True
    
    # FSE should NOT trigger fat_sat (it's Fast Spin Echo)
    result = parse_scan_options("FSE")
    assert result["has_fat_sat"] is False


def test_gating_detection():
    """Test cardiac and respiratory gating detection."""
    result = parse_scan_options("CG")
    assert result["has_gating"] is True
    
    result = parse_scan_options("RG")
    assert result["has_gating"] is True
    
    result = parse_scan_options("['CG', 'RG']")
    assert result["has_gating"] is True


def test_all_scan_options_patterns():
    """Test all scan_options patterns from TEST_CASES."""
    passed, failed, errors = run_tests(verbose=False)
    assert failed == 0, f"Failed {failed} tests:\n" + "\n".join(errors[:5])


def print_all_flags():
    """Print all available flags from the parser."""
    print("\n" + "=" * 80)
    print("ALL AVAILABLE FLAGS")
    print("=" * 80)
    
    result = parse_scan_options("IR")
    flags = sorted([k for k in result.keys() if k != "all_tokens"])
    
    for i, flag in enumerate(flags, 1):
        print(f"  {i:3}. {flag}")
    
    print(f"\nTotal: {len(flags)} flags")


def main():
    """Run all tests."""
    print("=" * 80)
    print("PARSE_SCAN_OPTIONS COMPREHENSIVE TEST")
    print(f"Testing {len(TEST_CASES)} unique scan_options patterns from database")
    print("=" * 80 + "\n")
    
    # Print available flags
    print_all_flags()
    
    # Run edge cases
    print("\n" + "=" * 80)
    print("EDGE CASES")
    print("=" * 80)
    test_edge_cases()
    print("✅ All edge cases passed")
    
    # Run fat sat tests
    print("\n" + "=" * 80)
    print("FAT SATURATION TESTS")
    print("=" * 80)
    test_fat_sat_variants()
    print("✅ All fat sat tests passed")
    
    # Run gating tests
    print("\n" + "=" * 80)
    print("GATING TESTS")
    print("=" * 80)
    test_gating_detection()
    print("✅ All gating tests passed")
    
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
        for error in errors[:10]:
            print(error)
        return 1
    else:
        print("\n✅ ALL TESTS PASSED!")
        return 0


if __name__ == "__main__":
    exit(main())
