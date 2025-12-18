"""
Comprehensive tests for parse_sequence_variant() using real database values.

This test file uses ALL unique sequence_variant values from the stack_fingerprint table
to verify the parser handles every real-world case correctly.

The sequence_variant parser extracts technique modifiers from the DICOM SequenceVariant tag (0018,0021).
Standard values defined in DICOM:
- SK = Segmented k-space
- MTC = Magnetization Transfer Contrast
- SS = Steady State
- SP = Spoiled
- MP = Magnetization Prepared (MPRAGE, etc.)
- OSP = Oversampling Phase
- NONE = No variant
- TOF = Time of Flight

Run with: python -m pytest backend/src/classification/tests/test_context_sequence_variant.py -v
Or standalone: python backend/src/classification/tests/test_context_sequence_variant.py
"""

from typing import Dict, List, Tuple, Any
import sys
import os

# Add parent to path for standalone execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from backend.src.classification.core.context import parse_sequence_variant


# =============================================================================
# ALL UNIQUE SEQUENCE_VARIANT VALUES FROM DATABASE (38 unique values)
# =============================================================================

TEST_CASES: List[Tuple[str, Dict[str, bool]]] = [
    # =========================================================================
    # SECTION 1: SINGLE VALUES - Most common
    # =========================================================================
    
    # SK - Segmented k-space (93K cases) - TSE/FSE
    ("SK", {"has_segmented_kspace": True, "has_spoiled": False, "has_mag_prepared": False}),
    
    # NONE - No variant (39K cases) - basic sequences
    ("NONE", {"is_none": True, "has_segmented_kspace": False}),
    
    # SP - Spoiled (39K cases) - SPGR/FLASH
    ("SP", {"has_spoiled": True, "has_segmented_kspace": False}),
    
    # OSP - Oversampling Phase (18K cases)
    ("OSP", {"has_oversampling": True}),
    
    # SS - Steady State (17K cases) - SSFP/FISP
    ("SS", {"has_steady_state": True}),
    
    # MP - Magnetization Prepared (5K cases) - MPRAGE
    ("MP", {"has_mag_prepared": True, "has_spoiled": False}),
    
    # OS - Oversampling (9 cases) - alternate notation
    ("OS", {"has_oversampling": True}),
    
    # MTC - Magnetization Transfer Contrast (1 case)
    ("MTC", {"has_mtc": True}),
    
    # OTHER (221 cases)
    ("OTHER", {"is_other": True}),
    
    # SYNTHETIC (939 cases) - SyMRI
    ("SYNTHETIC", {"is_synthetic": True}),
    
    # =========================================================================
    # SECTION 2: COMMON COMBINATIONS - GRE variants
    # =========================================================================
    
    # SK + SP + OSP - Standard TSE with spoiling and oversampling (43K cases)
    ("['SK', 'SP', 'OSP']", {"has_segmented_kspace": True, "has_spoiled": True, "has_oversampling": True}),
    
    # SK + SP + MP - MPRAGE (40K cases)
    ("['SK', 'SP', 'MP']", {"has_segmented_kspace": True, "has_spoiled": True, "has_mag_prepared": True}),
    
    # SK + SP + MP + OSP - MPRAGE with oversampling (37K cases)
    ("['SK', 'SP', 'MP', 'OSP']", {"has_segmented_kspace": True, "has_spoiled": True, "has_mag_prepared": True, "has_oversampling": True}),
    
    # SK + SP - TSE with spoiling (30K cases)
    ("['SK', 'SP']", {"has_segmented_kspace": True, "has_spoiled": True}),
    
    # SP + OSP - Spoiled with oversampling (24K cases)
    ("['SP', 'OSP']", {"has_spoiled": True, "has_oversampling": True}),
    
    # SP + SK - Same as SK+SP (22K cases)
    ("['SP', 'SK']", {"has_spoiled": True, "has_segmented_kspace": True}),
    
    # SK + OSP (5.5K cases)
    ("['SK', 'OSP']", {"has_segmented_kspace": True, "has_oversampling": True}),
    
    # SP + MP (3.5K cases)
    ("['SP', 'MP']", {"has_spoiled": True, "has_mag_prepared": True}),
    
    # SP + MP + OSP (2.9K cases)
    ("['SP', 'MP', 'OSP']", {"has_spoiled": True, "has_mag_prepared": True, "has_oversampling": True}),
    
    # =========================================================================
    # SECTION 3: STEADY STATE COMBINATIONS
    # =========================================================================
    
    # SS + SK - SSFP with segmented (29K cases)
    ("['SS', 'SK']", {"has_steady_state": True, "has_segmented_kspace": True}),
    
    # SS + SP (1.4K cases)
    ("['SS', 'SP']", {"has_steady_state": True, "has_spoiled": True}),
    
    # SS + SP + SK (1.1K cases)
    ("['SS', 'SP', 'SK']", {"has_steady_state": True, "has_spoiled": True, "has_segmented_kspace": True}),
    
    # SS + OSP (557 cases)
    ("['SS', 'OSP']", {"has_steady_state": True, "has_oversampling": True}),
    
    # SS + SK + OSP (361 cases)
    ("['SS', 'SK', 'OSP']", {"has_steady_state": True, "has_segmented_kspace": True, "has_oversampling": True}),
    
    # SK + SS + MP + OSP (34 cases)
    ("['SK', 'SS', 'MP', 'OSP']", {"has_segmented_kspace": True, "has_steady_state": True, "has_mag_prepared": True, "has_oversampling": True}),
    
    # SK + SS + OSP (1 case)
    ("['SK', 'SS', 'OSP']", {"has_segmented_kspace": True, "has_steady_state": True, "has_oversampling": True}),
    
    # SS + SP + SK + OSP (1 case)
    ("['SS', 'SP', 'SK', 'OSP']", {"has_steady_state": True, "has_spoiled": True, "has_segmented_kspace": True, "has_oversampling": True}),
    
    # =========================================================================
    # SECTION 4: TOF/MRA COMBINATIONS
    # =========================================================================
    
    # TOF + MTC + SP (340 cases) - TOF MRA with MTC
    ("['TOF', 'MTC', 'SP']", {"is_tof": True, "has_mtc": True, "has_spoiled": True}),
    
    # MTC + SP (302 cases)
    ("['MTC', 'SP']", {"has_mtc": True, "has_spoiled": True}),
    
    # TOF + SP + OSP (277 cases)
    ("['TOF', 'SP', 'OSP']", {"is_tof": True, "has_spoiled": True, "has_oversampling": True}),
    
    # TOF + MTC + SP + OSP (201 cases)
    ("['TOF', 'MTC', 'SP', 'OSP']", {"is_tof": True, "has_mtc": True, "has_spoiled": True, "has_oversampling": True}),
    
    # TOF + SP (112 cases)
    ("['TOF', 'SP']", {"is_tof": True, "has_spoiled": True}),
    
    # MTC + SK (22 cases)
    ("['MTC', 'SK']", {"has_mtc": True, "has_segmented_kspace": True}),
    
    # MTC + SP + OSP (3 cases)
    ("['MTC', 'SP', 'OSP']", {"has_mtc": True, "has_spoiled": True, "has_oversampling": True}),
    
    # SP + MTC + SS + OSP (39 cases)
    ("['SP', 'MTC', 'SS', 'OSP']", {"has_spoiled": True, "has_mtc": True, "has_steady_state": True, "has_oversampling": True}),
    
    # SK + MTC + SP + OSP (26 cases)
    ("['SK', 'MTC', 'SP', 'OSP']", {"has_segmented_kspace": True, "has_mtc": True, "has_spoiled": True, "has_oversampling": True}),
    
    # SK + MTC + SP + MP (2 cases)
    ("['SK', 'MTC', 'SP', 'MP']", {"has_segmented_kspace": True, "has_mtc": True, "has_spoiled": True, "has_mag_prepared": True}),
    
    # SP + MTC + SK (1 case)
    ("['SP', 'MTC', 'SK']", {"has_spoiled": True, "has_mtc": True, "has_segmented_kspace": True}),
]


def run_tests(verbose: bool = True) -> Tuple[int, int, List[str]]:
    """
    Run all test cases and return (passed, failed, error_messages).
    """
    passed = 0
    failed = 0
    errors = []
    
    for seq_variant, expected in TEST_CASES:
        try:
            result = parse_sequence_variant(seq_variant)
            
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
                    tokens = result.get("all_tokens", set())
                    print(f"✅ {seq_variant:<35} → tokens={tokens} flags={set_flags}")
            else:
                failed += 1
                error_msg = f"❌ {seq_variant}\n" + "\n".join(case_errors)
                errors.append(error_msg)
                if verbose:
                    print(error_msg)
                    
        except Exception as e:
            failed += 1
            error_msg = f"💥 {seq_variant}\n  Exception: {e}"
            errors.append(error_msg)
            if verbose:
                print(error_msg)
    
    return passed, failed, errors


def test_edge_cases():
    """Test edge cases: None, empty, malformed."""
    # None
    result = parse_sequence_variant(None)
    assert result["has_mag_prepared"] is False
    assert result["has_spoiled"] is False
    assert result["all_tokens"] == set()
    
    # Empty string
    result = parse_sequence_variant("")
    assert result["has_segmented_kspace"] is False
    
    # Lowercase
    result = parse_sequence_variant("sk")
    assert result["has_segmented_kspace"] is True
    
    result = parse_sequence_variant("['sk', 'sp', 'mp']")
    assert result["has_segmented_kspace"] is True
    assert result["has_spoiled"] is True
    assert result["has_mag_prepared"] is True


def test_mprage_detection():
    """Test that MP (Magnetization Prepared) is correctly detected for MPRAGE."""
    # MPRAGE always has MP
    result = parse_sequence_variant("['SK', 'SP', 'MP']")
    assert result["has_mag_prepared"] is True
    
    # Standard MPRAGE with oversampling
    result = parse_sequence_variant("['SK', 'SP', 'MP', 'OSP']")
    assert result["has_mag_prepared"] is True
    assert result["has_oversampling"] is True
    
    # Non-MPRAGE shouldn't have MP
    result = parse_sequence_variant("['SK', 'SP']")
    assert result["has_mag_prepared"] is False


def test_all_sequence_variant_patterns():
    """Test all sequence_variant patterns from TEST_CASES."""
    passed, failed, errors = run_tests(verbose=False)
    assert failed == 0, f"Failed {failed} tests:\n" + "\n".join(errors[:5])


def print_all_flags():
    """Print all available flags from the parser."""
    print("\n" + "=" * 80)
    print("ALL AVAILABLE FLAGS")
    print("=" * 80)
    
    result = parse_sequence_variant("SK")
    flags = sorted([k for k in result.keys() if k != "all_tokens"])
    
    for i, flag in enumerate(flags, 1):
        print(f"  {i:3}. {flag}")
    
    print(f"\nTotal: {len(flags)} flags")


def main():
    """Run all tests."""
    print("=" * 80)
    print("PARSE_SEQUENCE_VARIANT COMPREHENSIVE TEST")
    print(f"Testing {len(TEST_CASES)} unique sequence_variant values from database")
    print("=" * 80 + "\n")
    
    # Print available flags
    print_all_flags()
    
    # Run edge cases
    print("\n" + "=" * 80)
    print("EDGE CASES")
    print("=" * 80)
    test_edge_cases()
    print("✅ All edge cases passed")
    
    # Run MPRAGE detection tests
    print("\n" + "=" * 80)
    print("MPRAGE DETECTION TESTS")
    print("=" * 80)
    test_mprage_detection()
    print("✅ All MPRAGE detection tests passed")
    
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
