"""
Comprehensive tests for parse_scanning_sequence() using real database values.

This test file uses ALL unique scanning_sequence values from the stack_fingerprint table
to verify the parser handles every real-world case correctly.

The scanning_sequence parser extracts technique hints from the DICOM ScanningSequence tag (0018,0020).
Standard values defined in DICOM:
- SE = Spin Echo
- IR = Inversion Recovery  
- GR = Gradient Recalled
- EP = Echo Planar
- RM = Research Mode

Run with: python -m pytest backend/src/classification/tests/test_context_scanning_sequence.py -v
Or standalone: python backend/src/classification/tests/test_context_scanning_sequence.py
"""

from typing import Dict, List, Tuple, Any
import sys
import os

# Add parent to path for standalone execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from backend.src.classification.core.context import parse_scanning_sequence


# =============================================================================
# ALL UNIQUE SCANNING_SEQUENCE VALUES FROM DATABASE (32 unique values)
# =============================================================================

# Format: (scanning_sequence, expected_flags_dict)
# expected_flags_dict contains ONLY the flags we want to verify (sparse)

TEST_CASES: List[Tuple[str, Dict[str, bool]]] = [
    # =========================================================================
    # SECTION 1: SINGLE VALUES - Most common
    # =========================================================================
    
    # SE - Spin Echo (131K cases)
    ("SE", {"has_se": True, "has_gre": False, "has_ir": False, "has_epi": False}),
    
    # GR - Gradient Recalled (120K cases)
    ("GR", {"has_gre": True, "has_se": False, "has_ir": False, "has_epi": False}),
    
    # RM - Research Mode (36K cases)
    ("RM", {"has_research": True, "has_se": False, "has_gre": False}),
    
    # EP - Echo Planar (18K cases)
    ("EP", {"has_epi": True, "has_se": False, "has_gre": False}),
    
    # IR - Inversion Recovery alone (10K cases)
    ("IR", {"has_ir": True, "has_se": False, "has_gre": False}),
    
    # =========================================================================
    # SECTION 2: LIST VALUES - Common combinations
    # =========================================================================
    
    # SE + IR - FLAIR, STIR, etc. (86K cases)
    ("['SE', 'IR']", {"has_se": True, "has_ir": True, "has_gre": False, "has_epi": False}),
    
    # GR + IR - MPRAGE, IR-GRE (41K cases)
    ("['GR', 'IR']", {"has_gre": True, "has_ir": True, "has_se": False, "has_epi": False}),
    
    # EP + SE - SE-EPI (4K cases)
    ("['EP', 'SE']", {"has_epi": True, "has_se": True, "has_gre": False}),
    
    # IR + SE - Same as SE+IR, different order (3K cases)
    ("['IR', 'SE']", {"has_ir": True, "has_se": True, "has_gre": False}),
    
    # SE + EP - Same as EP+SE, different order (2K cases)
    ("['SE', 'EP']", {"has_se": True, "has_epi": True}),
    
    # IR + GR - Same as GR+IR, different order (1.5K cases)
    ("['IR', 'GR']", {"has_ir": True, "has_gre": True, "has_se": False}),
    
    # EP + RM - EPI Research Mode (1.3K cases)
    ("['EP', 'RM']", {"has_epi": True, "has_research": True}),
    
    # GR + EP - GRE-EPI (1K cases)
    ("['GR', 'EP']", {"has_gre": True, "has_epi": True, "has_se": False}),
    
    # RM + IR - Research Mode with IR (400 cases)
    ("['RM', 'IR']", {"has_research": True, "has_ir": True}),
    
    # EP + SE + EP - Triple combination (185 cases)
    ("['EP', 'SE', 'EP']", {"has_epi": True, "has_se": True}),
    
    # EP + GR - Same as GR+EP (135 cases)
    ("['EP', 'GR']", {"has_epi": True, "has_gre": True}),
    
    # EP + IR - EPI with IR prep (1 case)
    ("['EP', 'IR']", {"has_epi": True, "has_ir": True}),
    
    # =========================================================================
    # SECTION 3: SYNTHETIC/GENERATED - SyMRI and derived
    # =========================================================================
    
    # GENERATED - Computer generated (65 cases)
    ("GENERATED", {"is_generated": True, "has_se": False}),
    
    # SE + SYNTHETIC - Synthetic SE (61 cases)
    ("['SE', 'SYNTHETIC']", {"has_se": True, "has_synthetic": True}),
    
    # QMAP - Quantitative mapping (54 cases)
    ("QMAP", {"has_qmap": True}),
    
    # IR + SE + SYNTHETIC - Synthetic IR-SE (48 cases)
    ("['IR', 'SE', 'SYNTHETIC']", {"has_ir": True, "has_se": True, "has_synthetic": True}),
    
    # GENERATED + SCREENSHOT (9 cases)
    ("['GENERATED', 'SCREENSHOT']", {"is_generated": True}),
    
    # SE (Synthetic) - parenthetical notation (7 cases)
    ("SE (Synthetic)", {"has_se": True, "has_synthetic": True}),
    
    # IR (Synthetic) - parenthetical notation (2 cases)
    ("IR (Synthetic)", {"has_ir": True, "has_synthetic": True}),
    
    # Synthetic alone (2 cases)
    ("Synthetic", {"has_synthetic": True}),
    
    # =========================================================================
    # SECTION 4: VENDOR-SPECIFIC / RARE VALUES
    # =========================================================================
    
    # GE - GE Healthcare specific (37 cases)
    ("GE", {"has_gre": True}),  # GE is a GRE variant
    
    # FE - Field Echo / Philips (16 cases)
    ("FE", {"has_gre": True}),  # FE is a GRE variant
    
    # FSE - Fast Spin Echo (15 cases)
    ("FSE", {"has_fse": True, "has_se": False}),  # FSE is separate from SE
    
    # FE3D - 3D Field Echo (9 cases)
    ("FE3D", {"has_gre": True}),  # FE3D is a GRE variant
    
    # IRFSE - IR Fast Spin Echo (7 cases)
    ("IRFSE", {"has_irfse": True, "has_ir": False, "has_fse": False}),  # IRFSE is its own flag
    
    # EP + S - EPI with saturation (4 cases)
    ("['EP', 'S']", {"has_epi": True, "has_saturation": True}),
    
    # Numeric value (likely error/artifact) (89 cases)
    ("2", {"has_se": False, "has_gre": False, "has_ir": False}),
    
    # =========================================================================
    # SECTION 5: EDGE CASES
    # =========================================================================
    
    # Empty/None handled separately in test_edge_cases()
]


def run_tests(verbose: bool = True) -> Tuple[int, int, List[str]]:
    """
    Run all test cases and return (passed, failed, error_messages).
    """
    passed = 0
    failed = 0
    errors = []
    
    for scanning_seq, expected in TEST_CASES:
        try:
            result = parse_scanning_sequence(scanning_seq)
            
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
                    set_flags = [k for k, v in result.items() if v is True and k != "all_tokens"]
                    tokens = result.get("all_tokens", set())
                    print(f"✅ {scanning_seq:<35} → tokens={tokens} flags={set_flags}")
            else:
                failed += 1
                error_msg = f"❌ {scanning_seq}\n" + "\n".join(case_errors)
                errors.append(error_msg)
                if verbose:
                    print(error_msg)
                    
        except Exception as e:
            failed += 1
            error_msg = f"💥 {scanning_seq}\n  Exception: {e}"
            errors.append(error_msg)
            if verbose:
                print(error_msg)
    
    return passed, failed, errors


def test_edge_cases():
    """Test edge cases: None, empty, malformed."""
    # None
    result = parse_scanning_sequence(None)
    assert result["has_se"] is False
    assert result["has_gre"] is False
    assert result["has_ir"] is False
    assert result["all_tokens"] == set()
    
    # Empty string
    result = parse_scanning_sequence("")
    assert result["has_se"] is False
    assert result["all_tokens"] == set()
    
    # Whitespace only
    result = parse_scanning_sequence("   ")
    assert result["has_se"] is False
    
    # Lowercase (should be case-insensitive)
    result = parse_scanning_sequence("se")
    assert result["has_se"] is True
    
    result = parse_scanning_sequence("['se', 'ir']")
    assert result["has_se"] is True
    assert result["has_ir"] is True
    
    # Mixed case
    result = parse_scanning_sequence("Se")
    assert result["has_se"] is True
    
    result = parse_scanning_sequence("['Gr', 'Ir']")
    assert result["has_gre"] is True
    assert result["has_ir"] is True


def test_technique_inference():
    """Test that scanning_sequence values map to expected techniques."""
    # SE alone → Spin Echo techniques (SE, TSE)
    result = parse_scanning_sequence("SE")
    assert result["has_se"] is True
    assert result["has_ir"] is False
    
    # SE + IR → FLAIR, STIR, or other IR-SE techniques
    result = parse_scanning_sequence("['SE', 'IR']")
    assert result["has_se"] is True
    assert result["has_ir"] is True
    
    # GR alone → GRE techniques (FLASH, SPGR)
    result = parse_scanning_sequence("GR")
    assert result["has_gre"] is True
    assert result["has_ir"] is False
    
    # GR + IR → MPRAGE, IR-GRE
    result = parse_scanning_sequence("['GR', 'IR']")
    assert result["has_gre"] is True
    assert result["has_ir"] is True
    
    # EP alone → EPI techniques
    result = parse_scanning_sequence("EP")
    assert result["has_epi"] is True
    
    # EP + SE → SE-EPI (DWI often uses this)
    result = parse_scanning_sequence("['EP', 'SE']")
    assert result["has_epi"] is True
    assert result["has_se"] is True
    
    # EP + GR → GRE-EPI (BOLD fMRI)
    result = parse_scanning_sequence("['GR', 'EP']")
    assert result["has_epi"] is True
    assert result["has_gre"] is True


def test_all_scanning_sequence_patterns():
    """Test all scanning_sequence patterns from TEST_CASES."""
    passed, failed, errors = run_tests(verbose=False)
    assert failed == 0, f"Failed {failed} tests:\n" + "\n".join(errors[:5])


def print_all_flags():
    """Print all available flags from the parser."""
    print("\n" + "=" * 80)
    print("ALL AVAILABLE FLAGS")
    print("=" * 80)
    
    result = parse_scanning_sequence("SE")
    flags = sorted([k for k in result.keys() if k != "all_tokens"])
    
    for i, flag in enumerate(flags, 1):
        print(f"  {i:3}. {flag}")
    
    print(f"\nTotal: {len(flags)} flags")


def main():
    """Run all tests."""
    print("=" * 80)
    print("PARSE_SCANNING_SEQUENCE COMPREHENSIVE TEST")
    print(f"Testing {len(TEST_CASES)} unique scanning_sequence values from database")
    print("=" * 80 + "\n")
    
    # Print available flags
    print_all_flags()
    
    # Run edge cases
    print("\n" + "=" * 80)
    print("EDGE CASES")
    print("=" * 80)
    test_edge_cases()
    print("✅ All edge cases passed")
    
    # Run technique inference tests
    print("\n" + "=" * 80)
    print("TECHNIQUE INFERENCE TESTS")
    print("=" * 80)
    test_technique_inference()
    print("✅ All technique inference tests passed")
    
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
