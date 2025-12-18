"""
Modifier Validation Test

This test queries real fingerprints from the database for each modifier keyword,
runs them through our parsers, and reports which high-value flags are actually TRUE.

This helps validate and refine the modifier-detection.yaml configuration.

Run with: python backend/src/classification/tests/test_modifier_validation.py
"""

import sys
import os
from collections import defaultdict

# Add parent to path for standalone execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from backend.src.classification.core.context import (
    parse_image_type,
    parse_scanning_sequence,
    parse_sequence_variant,
    parse_scan_options,
    parse_sequence_name,
)

# Try to import database connection
try:
    import psycopg2
    HAS_DB = True
except ImportError:
    HAS_DB = False
    print("WARNING: psycopg2 not installed, using mock data")


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5532,  # Docker mapped port
        database="neurotoolkit_metadata",
        user="postgres",
        password="postgres"
    )


def get_fingerprints_for_modifier(keyword: str, limit: int = 500):
    """Get fingerprints that have the keyword in text_search_blob."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
    SELECT 
        image_type,
        scanning_sequence,
        sequence_variant,
        scan_options,
        stack_sequence_name,
        text_search_blob
    FROM stack_fingerprint 
    WHERE LOWER(text_search_blob) LIKE %s
    LIMIT %s
    """
    
    cur.execute(query, (f'%{keyword.lower()}%', limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows


def analyze_modifier(keyword: str, limit: int = 500):
    """
    Analyze fingerprints for a modifier keyword and report flag frequencies.
    """
    print(f"\n{'='*70}")
    print(f"MODIFIER: {keyword.upper()}")
    print(f"{'='*70}")
    
    rows = get_fingerprints_for_modifier(keyword, limit)
    total = len(rows)
    
    if total == 0:
        print(f"  No fingerprints found for '{keyword}'")
        return {}
    
    print(f"  Analyzed {total} fingerprints")
    
    # Track flag frequencies
    image_type_flags = defaultdict(int)
    scanning_seq_flags = defaultdict(int)
    seq_variant_flags = defaultdict(int)
    scan_options_flags = defaultdict(int)
    seq_name_flags = defaultdict(int)
    
    for row in rows:
        image_type, scanning_seq, seq_variant, scan_opts, seq_name, text_blob = row
        
        # Parse each field
        pit = parse_image_type(image_type or "")
        pss = parse_scanning_sequence(scanning_seq or "")
        psv = parse_sequence_variant(seq_variant or "")
        pso = parse_scan_options(scan_opts or "")
        psn = parse_sequence_name(seq_name or "")
        
        # Count flags that are True
        for k, v in pit.items():
            if v is True:
                image_type_flags[k] += 1
        
        for k, v in pss.items():
            if v is True:
                scanning_seq_flags[k] += 1
        
        for k, v in psv.items():
            if v is True:
                seq_variant_flags[k] += 1
        
        for k, v in pso.items():
            if v is True:
                scan_options_flags[k] += 1
        
        for k, v in psn.items():
            if v is True:
                seq_name_flags[k] += 1
    
    # Report significant flags (>10% frequency)
    threshold = total * 0.10
    
    def report_flags(name, flags_dict):
        significant = [(k, v) for k, v in flags_dict.items() if v >= threshold]
        significant.sort(key=lambda x: -x[1])
        if significant:
            print(f"\n  {name}:")
            for flag, count in significant[:10]:
                pct = count / total * 100
                print(f"    {flag:<30} {count:>5} ({pct:>5.1f}%)")
    
    report_flags("parsed_image_type", image_type_flags)
    report_flags("parsed_scanning_sequence", scanning_seq_flags)
    report_flags("parsed_sequence_variant", seq_variant_flags)
    report_flags("parsed_scan_options", scan_options_flags)
    report_flags("parsed_sequence_name", seq_name_flags)
    
    return {
        "total": total,
        "image_type": dict(image_type_flags),
        "scanning_sequence": dict(scanning_seq_flags),
        "sequence_variant": dict(seq_variant_flags),
        "scan_options": dict(scan_options_flags),
        "sequence_name": dict(seq_name_flags),
    }


def main():
    """Run validation for all modifiers."""
    
    if not HAS_DB:
        print("Database not available. Please run with database connection.")
        return
    
    # List of modifiers to validate
    modifiers = [
        # IR family
        ("flair", "FLAIR"),
        ("stir", "STIR"),
        ("dir", "DIR"),  # Note: may need specific pattern to avoid false matches
        ("psir", "PSIR"),
        
        # Fat suppression
        ("fat sat", "FatSat"),
        ("fatsat", "FatSat"),
        ("spair", "FatSat-SPAIR"),
        ("spir", "FatSat-SPIR"),
        
        # Dixon
        ("dixon", "Dixon"),
        ("in phase", "InPhase"),
        ("opposed phase", "OpposedPhase"),
        
        # Other
        ("mt ", "MT"),  # Space to avoid false matches
        ("flow comp", "FlowComp"),
        ("asl", "ASL"),
        ("dsc", "DSC"),
        ("dce", "DCE"),
        
        # Water excitation
        ("water excitation", "WaterExcitation"),
    ]
    
    print("=" * 70)
    print("MODIFIER VALIDATION TEST")
    print("Analyzing real database fingerprints to validate high-value tag flags")
    print("=" * 70)
    
    results = {}
    for keyword, name in modifiers:
        try:
            results[name] = analyze_modifier(keyword, limit=500)
        except Exception as e:
            print(f"\n  ERROR for {name}: {e}")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY - Recommended High-Value Tags per Modifier")
    print("=" * 70)
    
    for name, data in results.items():
        if not data or data.get("total", 0) == 0:
            continue
        
        total = data["total"]
        print(f"\n{name} (n={total}):")
        
        # Find flags with >50% frequency - these are reliable
        for parser_name in ["image_type", "scanning_sequence", "sequence_variant", "scan_options", "sequence_name"]:
            flags = data.get(parser_name, {})
            reliable = [(k, v) for k, v in flags.items() if v >= total * 0.5]
            reliable.sort(key=lambda x: -x[1])
            if reliable:
                flag_str = ", ".join([f"{k}({v*100//total}%)" for k, v in reliable[:5]])
                print(f"  {parser_name}: {flag_str}")


if __name__ == "__main__":
    main()
