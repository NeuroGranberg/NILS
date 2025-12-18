"""
Provenance Validation Test

This test queries real fingerprints from the database for each provenance type,
analyzes ALL fields to find distinguishing patterns for robust detection.

Run with: python backend/src/classification/tests/test_provenance_validation.py
"""

import sys
import os
from collections import defaultdict
import json
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from backend.src.classification.core.context import (
    parse_image_type,
    parse_scanning_sequence,
    parse_sequence_variant,
    parse_scan_options,
    parse_sequence_name,
)

try:
    import psycopg2
    HAS_DB = True
except ImportError:
    HAS_DB = False


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5532,
        database="neurotoolkit_metadata",
        user="postgres",
        password="postgres"
    )


def search_fingerprints(pattern: str, field: str = "text_search_blob", limit: int = 500):
    """Search fingerprints by pattern in specified field."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = f"""
    SELECT 
        image_type,
        scanning_sequence,
        sequence_variant,
        scan_options,
        stack_sequence_name,
        text_search_blob,
        mr_tr,
        mr_te,
        mr_ti,
        mr_flip_angle,
        mr_diffusion_b_value,
        mr_acquisition_type
    FROM stack_fingerprint 
    WHERE LOWER({field}) LIKE %s
    LIMIT %s
    """
    
    cur.execute(query, (f'%{pattern.lower()}%', limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows


def search_by_image_type_flag(flag_pattern: str, limit: int = 500):
    """Search fingerprints where image_type contains a specific pattern."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
    SELECT 
        image_type,
        scanning_sequence,
        sequence_variant,
        scan_options,
        stack_sequence_name,
        text_search_blob,
        mr_tr,
        mr_te,
        mr_ti,
        mr_flip_angle,
        mr_diffusion_b_value,
        mr_acquisition_type
    FROM stack_fingerprint 
    WHERE LOWER(image_type) LIKE %s
    LIMIT %s
    """
    
    cur.execute(query, (f'%{flag_pattern.lower()}%', limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows


def analyze_rows(rows, name: str):
    """Analyze fingerprint rows and report patterns."""
    total = len(rows)
    if total == 0:
        print(f"  No fingerprints found")
        return None
    
    print(f"  Analyzed {total} fingerprints")
    
    # Track all patterns
    image_type_flags = defaultdict(int)
    scanning_seq_flags = defaultdict(int)
    seq_variant_flags = defaultdict(int)
    scan_options_flags = defaultdict(int)
    seq_name_flags = defaultdict(int)
    
    # Raw field values for pattern discovery
    raw_image_types = defaultdict(int)
    raw_seq_names = defaultdict(int)
    
    for row in rows:
        (image_type, scanning_seq, seq_variant, scan_opts, seq_name, text_blob,
         mr_tr, mr_te, mr_ti, mr_flip, mr_diff_b, mr_acq) = row
        
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
        
        # Collect raw values
        if image_type:
            raw_image_types[image_type] += 1
        if seq_name:
            raw_seq_names[seq_name] += 1
    
    # Report significant flags (>20% for provenance)
    threshold = total * 0.20
    
    def report_flags(parser_name, flags_dict, min_pct=20):
        significant = [(k, v) for k, v in flags_dict.items() if v >= total * min_pct / 100]
        significant.sort(key=lambda x: -x[1])
        if significant:
            print(f"\n  {parser_name}:")
            for flag, count in significant[:12]:
                pct = count / total * 100
                print(f"    {flag:<35} {count:>4} ({pct:>5.1f}%)")
    
    report_flags("parsed_image_type", image_type_flags, 30)
    report_flags("parsed_scanning_sequence", scanning_seq_flags, 30)
    report_flags("parsed_sequence_variant", seq_variant_flags, 30)
    report_flags("parsed_scan_options", scan_options_flags, 30)
    report_flags("parsed_sequence_name", seq_name_flags, 30)
    
    # Report top raw values
    def report_raw(name, values_dict, top_n=8):
        top_vals = sorted(values_dict.items(), key=lambda x: -x[1])[:top_n]
        if top_vals:
            print(f"\n  {name} (top {top_n}):")
            for val, count in top_vals:
                pct = count / total * 100
                print(f"    {val[:60]:<60} {count:>4} ({pct:>5.1f}%)")
    
    report_raw("Raw image_type", raw_image_types)
    report_raw("Raw sequence_name", raw_seq_names)
    
    return {
        "total": total,
        "image_type_flags": dict(image_type_flags),
        "scanning_seq_flags": dict(scanning_seq_flags),
        "seq_variant_flags": dict(seq_variant_flags),
        "scan_options_flags": dict(scan_options_flags),
        "seq_name_flags": dict(seq_name_flags),
    }


def main():
    """Run comprehensive provenance validation."""
    
    if not HAS_DB:
        print("Database not available.")
        return
    
    print("=" * 80)
    print("PROVENANCE VALIDATION TEST")
    print("Comprehensive analysis of database fingerprints for provenance detection")
    print("=" * 80)
    
    # =========================================================================
    # 1. SyMRI / Synthetic MRI
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: SyMRI (Synthetic MRI)")
    print("=" * 80)
    
    # Search by text
    for keyword in ["symri", "synthetic", "magic", "mdme", "qalas", "syntac"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=200)
        if rows:
            analyze_rows(rows, keyword)
    
    # Search by image_type flags
    for flag in ["t1map", "t2map", "pdmap", "synthetic", "myelin", "uniform", "quant"]:
        print(f"\n--- Image type contains: '{flag}' ---")
        rows = search_by_image_type_flag(flag, limit=200)
        if rows:
            analyze_rows(rows, flag)
    
    # =========================================================================
    # 2. SWI Reconstruction
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: SWI Reconstruction")
    print("=" * 80)
    
    for keyword in ["swi", "swan", "venobold"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=300)
        if rows:
            analyze_rows(rows, keyword)
    
    # Search SWI subtypes in image_type
    for flag in ["\\\\swi\\\\", "phase", "minip"]:
        print(f"\n--- Image type contains: '{flag}' ---")
        rows = search_by_image_type_flag(flag, limit=200)
        if rows:
            analyze_rows(rows, flag)
    
    # =========================================================================
    # 3. DTI / Diffusion Reconstruction
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: DTI / Diffusion Reconstruction")
    print("=" * 80)
    
    for keyword in ["adc", "dti", "fa ", "trace", "tensor", "diffusion"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=300)
        if rows:
            analyze_rows(rows, keyword)
    
    # Search DTI-specific image_type flags
    for flag in ["adc", "\\\\fa\\\\", "trace", "eadc", "tensor"]:
        print(f"\n--- Image type contains: '{flag}' ---")
        rows = search_by_image_type_flag(flag, limit=200)
        if rows:
            analyze_rows(rows, flag)
    
    # =========================================================================
    # 4. Perfusion Reconstruction
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: Perfusion Reconstruction")
    print("=" * 80)
    
    for keyword in ["cbf", "cbv", "mtt", "ttp", "tmax", "perfusion", "asl", "dsc"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=200)
        if rows:
            analyze_rows(rows, keyword)
    
    # =========================================================================
    # 5. BOLD/fMRI
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: BOLD/fMRI")
    print("=" * 80)
    
    for keyword in ["bold", "fmri", "resting state", "task"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=200)
        if rows:
            analyze_rows(rows, keyword)
    
    # =========================================================================
    # 6. Deep Learning / AI Reconstruction
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: Deep Learning Reconstruction")
    print("=" * 80)
    
    for keyword in ["dlr", "deep", "air recon", "resolve"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=200)
        if rows:
            analyze_rows(rows, keyword)
    
    # =========================================================================
    # 7. Motion/Distortion Correction
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: Motion/Distortion Correction")
    print("=" * 80)
    
    for keyword in ["moco", "motion", "distortion", "unwarped", "topup"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=100)
        if rows:
            analyze_rows(rows, keyword)
    
    # =========================================================================
    # 8. MIP/MPR Derived
    # =========================================================================
    print("\n" + "=" * 80)
    print("PROVENANCE: MIP/MPR Derived")
    print("=" * 80)
    
    for keyword in ["mip", "mpr", "minip", "projection"]:
        print(f"\n--- Keyword: '{keyword}' ---")
        rows = search_fingerprints(keyword, limit=200)
        if rows:
            analyze_rows(rows, keyword)
    
    for flag in ["mip", "mpr", "projection", "reformat"]:
        print(f"\n--- Image type contains: '{flag}' ---")
        rows = search_by_image_type_flag(flag, limit=200)
        if rows:
            analyze_rows(rows, flag)


if __name__ == "__main__":
    main()
