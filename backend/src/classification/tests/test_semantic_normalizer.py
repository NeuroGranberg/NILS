"""
Test Semantic Normalizer

Tests for text normalization and tokenization, especially the fix for + and - character handling.
"""

import pytest

from sort.semantic_normalizer import normalize_text_blob
from classification.utils import match_semantic_keyword


class TestSemanticNormalizer:
    """Test semantic text normalization functionality."""
    
    def test_plus_minus_tokenization(self):
        """Test that + and - characters are properly tokenized as separate tokens."""
        
        # Test cases that were failing before the fix
        # Note: semantic normalization transforms t1→t1w, t2→t2w
        test_cases = [
            ("mp2rage+ k images", ["mp2rage", "+", "k", "images"]),
            ("dwi- brain", ["dwi", "-", "brain"]),  
            ("t1+ post contrast", ["t1w", "+", "post", "contrast"]),  # t1 → t1w
            ("t2-weighted", ["t2w", "-", "weighted"]),  # t2 → t2w
            ("mp2rage+gd", ["mp2rage", "+", "gd"]),
        ]
        
        for input_text, expected_tokens in test_cases:
            normalized = normalize_text_blob(input_text)
            actual_tokens = normalized.split() if normalized else []
            
            # Check that expected tokens are present
            for token in expected_tokens:
                assert token in actual_tokens, f"Token '{token}' not found in {actual_tokens} for input '{input_text}'"
    
    def test_keyword_matching_with_plus_minus(self):
        """Test that keyword matching works correctly after + and - tokenization."""
        
        test_cases = [
            # The original failing case
            ("sag 3d t1w mp2rage+ k images", "mp2rage", True),
            ("sag 3d t1w mp2rage k images", "mp2rage", True),  # Without +
            
            # Other contrast cases
            ("ax dwi- brain", "dwi", True),
            ("ax dwi+ brain", "dwi", True),
            ("t1+ post contrast", "t1w", True),  # t1 becomes t1w
            ("t2-weighted flair", "t2w", True),  # t2 becomes t2w
            
            # Ensure we preserve contrast information
            ("mp2rage+ gd", "+", True),  # Can match the + token
            ("dwi- pre contrast", "-", True),  # Can match the - token
            
            # Negative cases
            ("space t1 images", "mp2rage", False),  # mp2rage not present
            ("mp2rage images", "+", False),  # + not present
        ]
        
        for input_text, keyword, should_match in test_cases:
            normalized = normalize_text_blob(input_text)
            matches = match_semantic_keyword(normalized, keyword) if normalized else False
            
            if should_match:
                assert matches, f"Keyword '{keyword}' should match in '{input_text}' (normalized: '{normalized}')"
            else:
                assert not matches, f"Keyword '{keyword}' should NOT match in '{input_text}' (normalized: '{normalized}')"
    
    def test_mp2rage_classification_fix(self):
        """Test the specific MP2RAGE classification issue that was fixed."""
        
        # The exact text from stack 336587 that was failing
        problematic_text = "sag 3d t1w mp2rage+ k images startfl3d1 164ns brain head not for diagnostic use 06 26 min 0 35x0 8 mm map"
        
        # Should work after the fix  
        normalized = normalize_text_blob(problematic_text)
        assert normalized is not None
        
        # Should be able to match "mp2rage" keyword using semantic substring matching
        assert match_semantic_keyword(normalized, "mp2rage"), f"Failed to match 'mp2rage' in: {normalized}"
        
        # Should also be able to match contrast information
        assert match_semantic_keyword(normalized, "+"), f"Failed to match '+' in: {normalized}"
    
    def test_preserve_existing_functionality(self):
        """Ensure existing normalization still works correctly."""
        
        test_cases = [
            # Basic normalization (accounting for semantic transformations)
            ("T1_MPRAGE_SAG", "t1w mprage sag"),  # t1 → t1w
            ("T2*weighted", "t2starweighted"),  # t2*weighted → t2starweighted (single token)
            ("3D-FLAIR", "3d flair"),
            
            # Multi-word preservation
            ("double inversion recovery", "double inversion recovery"),
            ("time of flight", "time of flight"),
        ]
        
        for input_text, expected_content in test_cases:
            normalized = normalize_text_blob(input_text)
            assert normalized is not None
            
            # Check that expected content is preserved
            expected_tokens = expected_content.split()
            actual_tokens = normalized.split()
            
            for token in expected_tokens:
                assert token in actual_tokens, f"Expected token '{token}' not found in '{normalized}' for input '{input_text}'"
    
    def test_edge_cases(self):
        """Test edge cases for + and - handling."""
        
        test_cases = [
            # Multiple + or - characters
            ("mp2rage++ enhanced", ["mp2rage", "+", "+", "enhanced"]),
            ("dwi-- pre contrast", ["dwi", "-", "-", "pre", "contrast"]),
            
            # + and - at start/end
            ("+gd enhanced", ["+", "gd", "enhanced"]),
            ("enhanced +", ["enhanced", "+"]),
            
            # Mixed with other punctuation
            ("t1+/post", ["t1w", "+", "post"]),  # t1 → t1w, / should be removed/spaced
            
            # Empty/None input
            ("", None),
            (None, None),
        ]
        
        for input_text, expected in test_cases:
            normalized = normalize_text_blob(input_text)
            
            if expected is None:
                assert normalized is None
            else:
                actual_tokens = normalized.split() if normalized else []
                for token in expected:
                    assert token in actual_tokens, f"Token '{token}' not found in {actual_tokens} for input '{input_text}'"

    def test_token_removal_rules(self):
        """Ensure configured removal tokens are dropped from normalized output."""

        normalized = normalize_text_blob("Routine protocol sequence exam brain")
        assert normalized is not None

        tokens = normalized.split()
        for unwanted in ["protocol", "sequence", "exam"]:
            assert unwanted not in tokens, f"Token '{unwanted}' should be removed from '{normalized}'"

    def test_raw_removals_happen_before_normalization(self):
        """Raw substrings should be stripped before token replacements occur."""

        input_text = 'Localizer " RÖR PÅ DXSIN SE PÄRM" sag SE brain'
        normalized = normalize_text_blob(input_text)
        assert normalized is not None

        # Swedish snippet should be removed entirely (case-sensitive match)
        assert "rör" not in normalized
        assert "dxsin" not in normalized

        # The remaining legitimate SE should still expand to spin-echo once
        assert normalized.split().count("spin-echo") == 1


class TestKeywordMatching:
    """Test exact token matching functionality with the normalization fix."""
    
    def test_mp2rage_vs_mprage_detection(self):
        """Test that MP2RAGE vs MPRAGE detection works correctly after fix."""
        
        # Cases that should match MP2RAGE
        mp2rage_cases = [
            "sag 3d mp2rage images",
            "sag 3d mp2rage+ images",  # The fixed case
            "mp2rage t1 mapping",
            "3d mp2rage protocol",
        ]
        
        for text in mp2rage_cases:
            normalized = normalize_text_blob(text)
            assert match_semantic_keyword(normalized, "mp2rage"), f"Should match 'mp2rage' in '{text}'"
            
            # Should also be able to distinguish from MPRAGE  
            # (Note: "mp2rage" contains "mprage" as substring, but token matching should be exact)
            # This tests that we're doing proper token-based matching, not substring matching
        
        # Cases that should match MPRAGE but not MP2RAGE
        mprage_cases = [
            "sag 3d mprage images",
            "3d mprage t1",
            "bravo mprage protocol",
        ]
        
        for text in mprage_cases:
            normalized = normalize_text_blob(text)
            assert match_semantic_keyword(normalized, "mprage"), f"Should match 'mprage' in '{text}'"
            assert not match_semantic_keyword(normalized, "mp2rage"), f"Should NOT match 'mp2rage' in '{text}'"
    
    def test_localizer_exact_phrase_matching(self):
        """Test that keywords are matched as exact phrases, not individual words.
        
        This prevents false positives where "3 plan" would match text containing
        "3" and "plan" separately but not the actual phrase "3 plan".
        
        Regression test for Stack 7142 misclassification issue.
        """
        
        # Stack 7142 case - should NOT match "3 plan"  
        # The phrase "3 plan" does not exist in this text
        mprage_with_plan = "t1w mpr ns sag 1mm iso rek 2mm tre plan startfl3d1 16ns brain head"
        
        # Should NOT match "3 plan" phrase (the fix)
        assert match_semantic_keyword(mprage_with_plan, "3 plan") is False
        
        # But should match individual words that exist
        assert match_semantic_keyword(mprage_with_plan, "plan") is True
        assert match_semantic_keyword(mprage_with_plan, "mpr") is True
        
        # Real localizer cases should still work - contain the actual phrase "3 plan"
        real_localizer_cases = [
            "brain scout 3 plan localizer",
            "survey 3 plan autoalign", 
            "calibration 3 plan sequence"
        ]
        
        for text in real_localizer_cases:
            assert match_semantic_keyword(text, "3 plan") is True, f"Should match '3 plan' phrase in '{text}'"
