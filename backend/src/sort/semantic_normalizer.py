"""
Semantic Text Normalizer

Normalizes text_search_blob by:
1. Replacing meaningful characters with text (e.g., * → star)
2. Replacing separator characters with spaces
3. Removing noise characters
4. Tokenizing and deduplicating (preserving order)
5. Replacing ambiguous tokens with canonical forms
6. Applying conditional replacements based on context

This ensures EXACT keyword matching works reliably in classification.

Version: 1.1.0
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


def _deduplicate_preserve_order(tokens: list[str]) -> list[str]:
    """
    Remove duplicate tokens while preserving original order.
    
    Args:
        tokens: List of tokens (may contain duplicates)
    
    Returns:
        List of unique tokens in original order (first occurrence kept)
    """
    seen: set[str] = set()
    result: list[str] = []
    for token in tokens:
        if token not in seen:
            seen.add(token)
            result.append(token)
    return result


class SemanticNormalizer:
    """
    Normalize text into semantic tokens for reliable keyword matching.
    
    Loads configuration from semantic-token-map.yaml and applies:
    - Character replacements (before tokenization)
    - Token replacements (after tokenization)
    - Conditional replacements (context-aware)
    
    Order is preserved to maintain multi-word phrases like "+gd", "15 ml", "t2 flair".
    """
    
    YAML_FILENAME = "semantic-token-map.yaml"
    
    def __init__(self, yaml_path: Path | None = None):
        """
        Initialize normalizer with configuration.
        
        Args:
            yaml_path: Path to semantic-token-map.yaml.
                      If None, uses default location (same directory as this file).
        """
        if yaml_path is None:
            # More robust path calculation for Docker/different environments
            yaml_path = Path(__file__).resolve().parent / self.YAML_FILENAME
        
        self.config = self._load_yaml(yaml_path)
        
        # Debug logging if config fails to load
        if not self.config:
            print(f"WARNING: Failed to load semantic token map from {yaml_path}")
            print(f"File exists: {yaml_path.exists()}")
            self.config = {}
        
        # Parse character replacements
        char_config = self.config.get("character_replacements", {})
        self._meaningful_chars: dict[str, str] = char_config.get("meaningful", {})
        self._to_space_chars: list[str] = char_config.get("to_space", [])
        self._remove_chars: list[str] = char_config.get("remove", [])

        # Raw removals (run before any normalization)
        raw_removals = self.config.get("raw_removals", []) or []
        self._raw_removals: list[str] = [
            r for r in raw_removals
            if isinstance(r, str) and r.strip()
        ]
        
        # Parse token replacements (build reverse lookup: token → canonical)
        self._token_map: dict[str, str] = {}
        for canonical, tokens in self.config.get("token_replacements", {}).items():
            for token in tokens:
                self._token_map[token.lower()] = canonical.lower()

        # Parse token removals (tokens to drop entirely after dedup)
        self._tokens_to_remove: set[str] = set(
            token.lower()
            for token in self.config.get("token_removals", []) or []
            if token
        )
        
        # Parse conditional replacements
        self._conditional: list[dict[str, Any]] = []
        for canonical, rule in self.config.get("conditional_replacements", {}).items():
            self._conditional.append({
                "canonical": canonical.lower(),
                "replace": rule.get("replace", "").lower(),
                "when_has_any": [t.lower() for t in rule.get("when_has_any", [])],
                "when_has_all": [t.lower() for t in rule.get("when_has_all", [])],
            })
    
    def _load_yaml(self, path: Path) -> dict[str, Any]:
        """Load YAML configuration file."""
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    
    def normalize(self, text: str | None) -> str | None:
        """
        Normalize text into semantic tokens.
        
        Preserves original token order for multi-word phrase matching
        (e.g., "+gd", "15 ml", "t2 flair").
        
        Args:
            text: Raw concatenated text from DICOM fields
        
        Returns:
            Space-separated, deduplicated (order-preserved), normalized tokens.
            Returns None if input is None or empty after normalization.
        """
        if not text:
            return None
        
        # Step 0: Remove literal substrings before any normalization
        for removal in self._raw_removals:
            if removal in text:
                text = text.replace(removal, " ")

        if not text.strip():
            return None
        
        # Step 1: Character replacements (meaningful → text)
        for char, replacement in self._meaningful_chars.items():
            text = text.replace(char, replacement)
        
        # Step 2: Character replacements (separators → space)
        for char in self._to_space_chars:
            text = text.replace(char, " ")
        
        # Step 3: Character replacements (noise → remove)
        for char in self._remove_chars:
            text = text.replace(char, "")
        
        # Step 4: Lowercase
        text = text.lower()
        
        # Step 4.5: Add spaces around + and - for proper tokenization
        # This ensures "mp2rage+" becomes "mp2rage + " → ["mp2rage", "+"]
        # while preserving contrast meaning (+ = with contrast, - = without contrast)
        text = re.sub(r'([+\-])', r' \1 ', text)
        
        # Step 5: Remove remaining special chars (keep alphanumeric, space, underscore, +, -)
        text = re.sub(r'[^a-z0-9\s_+\-]', ' ', text)
        
        # Step 6: Tokenize (split by space and underscore)
        tokens = re.split(r'[\s_]+', text)
        tokens = [t.strip() for t in tokens if t.strip()]
        
        # Step 7: Deduplicate while preserving order
        tokens = _deduplicate_preserve_order(tokens)
        
        # Step 7.5: Remove boilerplate tokens
        if self._tokens_to_remove:
            tokens = [t for t in tokens if t not in self._tokens_to_remove]

        if not tokens:
            return None

        # Build a set for fast lookups in conditional replacements
        token_set = set(tokens)
        
        # Step 8: Token replacements (unconditional) - in place, preserving order
        tokens = [
            self._token_map.get(token, token)
            for token in tokens
        ]
        # Update the set after replacements
        token_set = set(tokens)
        
        # Step 9: Conditional replacements - in place, preserving order
        for rule in self._conditional:
            replace_token = rule["replace"]
            if replace_token not in token_set:
                continue
            
            # Check conditions
            should_replace = False
            
            if rule["when_has_any"]:
                # Replace if ANY context token is present
                if any(ctx in token_set for ctx in rule["when_has_any"]):
                    should_replace = True
            
            if rule["when_has_all"]:
                # Replace if ALL context tokens are present
                if all(ctx in token_set for ctx in rule["when_has_all"]):
                    should_replace = True
            
            if should_replace:
                # Replace in the list, preserving position
                tokens = [
                    rule["canonical"] if t == replace_token else t
                    for t in tokens
                ]
                token_set.discard(replace_token)
                token_set.add(rule["canonical"])
        
        # Step 10: Join with spaces (order preserved)
        if not tokens:
            return None
        
        result = " ".join(tokens)
        return result if result else None


# Module-level singleton for reuse
_normalizer: SemanticNormalizer | None = None


def get_normalizer(force_reload: bool = False) -> SemanticNormalizer:
    """Get or create the module-level normalizer singleton."""
    global _normalizer
    if _normalizer is None or force_reload:
        _normalizer = SemanticNormalizer()
    return _normalizer


def normalize_text_blob(text: str | None) -> str | None:
    """
    Convenience function to normalize text using the singleton normalizer.

    Args:
        text: Raw concatenated text from DICOM fields

    Returns:
        Normalized text blob with semantic tokens
    """
    return get_normalizer().normalize(text)


def normalize_sequence_name(sequence_name: str | None) -> str | None:
    """
    Normalize sequence name with special handling for vendor markers.

    Removes asterisks (*) which are vendor-specific markers used by manufacturers
    like Siemens to denote user-modified sequences (e.g., "*fl3d2_ns").

    Args:
        sequence_name: Raw sequence name from DICOM (0018,0024)

    Returns:
        Normalized sequence name without asterisks but with semantic tokens

    Example:
        "*fl3d2_ns" -> "flash 3d 2 ns" (if flash token mapping exists)
    """
    if not sequence_name:
        return None

    # Remove asterisks (vendor markers)
    cleaned = sequence_name.replace('*', '')

    # Apply standard semantic normalization
    return get_normalizer().normalize(cleaned)
