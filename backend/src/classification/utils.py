"""
Classification Utilities

Shared helper functions for semantic text matching, normalization, etc.
"""

import re
from typing import List, Optional


def match_semantic_keyword(text: str, keyword: str) -> bool:
    """
    Simple exact phrase substring matching for semantically normalized text.
    
    Since text_search_blob is semantically normalized with problematic short
    tokens expanded (ir → inversion-recovery, pd → proton-density, etc.),
    simple substring matching is safe and more intuitive than complex token matching.
    
    Treats all keywords as exact phrases to match, regardless of length.
    
    Examples:
        match_semantic_keyword("3d inversion-recovery mpr", "dir") → False
        match_semantic_keyword("3d inversion-recovery mpr", "inversion-recovery") → True  
        match_semantic_keyword("mp2rage+ k images", "mp2rage") → True
        match_semantic_keyword("t2 flair brain", "t2 flair") → True (exact phrase)
        match_semantic_keyword("plan startfl3d1", "3 plan") → False (phrase not found)
        match_semantic_keyword("brain 3 plan scout", "3 plan") → True (exact phrase found)
    
    Args:
        text: Pre-normalized text_search_blob with semantic tokenization
        keyword: Keyword phrase to match exactly
    
    Returns:
        True if keyword phrase is found as substring
    """
    if not text or not keyword:
        return False
    
    # Simple exact phrase matching - treat all keywords the same way
    return keyword.lower() in text.lower()


def match_any_keyword(
    text: str,
    keywords: List[str],
) -> Optional[str]:
    """
    Match any keyword in text using semantic substring matching.
    
    Uses match_semantic_keyword for reliable matching with semantically normalized
    text_search_blob. Since semantic normalization expands problematic short tokens,
    substring matching is safe and more intuitive.
    
    Args:
        text: Pre-normalized text_search_blob with semantic normalization
        keywords: List of keywords to match
    
    Returns:
        The matched keyword or None
    """
    if not text:
        return None
    
    for kw in keywords:
        if match_semantic_keyword(text, kw):
            return kw
    
    return None


def normalize_text(text: str) -> str:
    """
    Normalize text for matching.
    
    - Lowercase
    - Remove special characters except space and underscore
    - Collapse multiple spaces
    
    Args:
        text: Raw text
    
    Returns:
        Normalized text
    """
    if not text:
        return ""
    
    normalized = re.sub(r'[^a-z0-9\s_]', ' ', text.lower())
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.strip()


# Maximum realistic diffusion b-value in s/mm²
# Clinical DWI: 0-3000, Research HARDI/DSI: up to 10000-15000
# Values beyond 20000 are unrealistic and indicate garbage data (e.g., 7.23e+75)
MAX_REALISTIC_B_VALUE = 20000


def parse_b_values(b_value_str: str) -> List[int]:
    """
    Parse diffusion b-values from string, filtering unrealistic values.
    
    Handles formats like: "0", "1000", "0\\1000", "0,500,1000"
    
    Validation:
        - Values must be >= 0 and <= MAX_REALISTIC_B_VALUE (20000 s/mm²)
        - Garbage values like 7.23e+75 are filtered out
    
    Args:
        b_value_str: Raw b-value string
    
    Returns:
        List of valid integer b-values (sorted, unique)
    """
    if not b_value_str:
        return []
    
    # Split by common delimiters
    parts = re.split(r'[\\\/,\s]+', b_value_str)
    
    b_values = []
    for part in parts:
        try:
            b_val = int(float(part))
            # Filter out garbage values - must be within realistic range
            if 0 <= b_val <= MAX_REALISTIC_B_VALUE:
                b_values.append(b_val)
        except (ValueError, TypeError):
            continue
    
    return sorted(set(b_values))


def has_high_b_value(b_value_str: str, threshold: int = 100) -> bool:
    """
    Check if any b-value exceeds threshold.
    
    Args:
        b_value_str: Raw b-value string
        threshold: Minimum b-value to consider "high"
    
    Returns:
        True if any b-value > threshold
    """
    b_values = parse_b_values(b_value_str)
    return any(b > threshold for b in b_values)


def csv_to_list(csv_str: str) -> List[str]:
    """Convert CSV string to list, filtering empty values."""
    return [v.strip() for v in csv_str.split(",") if v.strip()]


def list_to_csv(values: List[str], sort: bool = True) -> str:
    """
    Convert list to CSV string.
    
    Args:
        values: List of string values
        sort: If True, sort alphabetically
    
    Returns:
        Comma-separated string
    """
    unique = list(set(values))
    if sort:
        unique.sort()
    return ",".join(unique)
