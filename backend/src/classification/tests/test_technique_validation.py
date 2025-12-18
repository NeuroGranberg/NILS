"""
Technique Validation Test

This test queries real fingerprints from the database for each technique keyword,
runs them through our parsers, and reports which high-value flags are actually TRUE.
Also reports MR physics parameter statistics.

Run with: python backend/src/classification/tests/test_technique_validation.py
"""

import sys
import os
from collections import defaultdict
import json

# Add parent to path for standalone execution
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


def get_fingerprints_for_technique(keyword: str, limit: int = 300):
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
        text_search_blob,
        mr_tr,
        mr_te,
        mr_ti,
        mr_flip_angle,
        mr_echo_train_length,
        mr_acquisition_type,
        mr_angio_flag,
        mr_diffusion_b_value,
        mr_phase_contrast
    FROM stack_fingerprint 
    WHERE LOWER(text_search_blob) LIKE %s
    LIMIT %s
    """
    
    cur.execute(query, (f'%{keyword.lower()}%', limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows


def percentile(data, p):
    """Calculate percentile of non-None values."""
    clean = [x for x in data if x is not None]
    if not clean:
        return None
    clean.sort()
    k = (len(clean) - 1) * p / 100
    f = int(k)
    c = f + 1 if f + 1 < len(clean) else f
    return clean[f] + (clean[c] - clean[f]) * (k - f) if c != f else clean[f]


def analyze_technique(keyword: str, name: str, limit: int = 300):
    """
    Analyze fingerprints for a technique keyword and report flag frequencies + physics.
    """
    print(f"\n{'='*70}")
    print(f"TECHNIQUE: {name} (keyword: '{keyword}')")
    print(f"{'='*70}")
    
    rows = get_fingerprints_for_technique(keyword, limit)
    total = len(rows)
    
    if total == 0:
        print(f"  No fingerprints found")
        return None
    
    print(f"  Analyzed {total} fingerprints")
    
    # Track flag frequencies
    image_type_flags = defaultdict(int)
    scanning_seq_flags = defaultdict(int)
    seq_variant_flags = defaultdict(int)
    scan_options_flags = defaultdict(int)
    seq_name_flags = defaultdict(int)
    
    # Track physics parameters
    tr_values = []
    te_values = []
    ti_values = []
    flip_values = []
    etl_values = []
    acq_types = defaultdict(int)
    angio_flags = defaultdict(int)
    phase_contrast = defaultdict(int)
    has_diffusion = 0
    
    for row in rows:
        (image_type, scanning_seq, seq_variant, scan_opts, seq_name, text_blob,
         mr_tr, mr_te, mr_ti, mr_flip, mr_etl, mr_acq_type, mr_angio, mr_diff_b, mr_pc) = row
        
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
        
        # Collect physics
        if mr_tr and mr_tr > 0:
            tr_values.append(mr_tr)
        if mr_te and mr_te > 0:
            te_values.append(mr_te)
        if mr_ti and mr_ti > 0:
            ti_values.append(mr_ti)
        if mr_flip and mr_flip > 0:
            flip_values.append(mr_flip)
        if mr_etl and mr_etl > 0:
            etl_values.append(mr_etl)
        if mr_acq_type:
            acq_types[mr_acq_type] += 1
        if mr_angio:
            angio_flags[mr_angio] += 1
        if mr_pc:
            phase_contrast[mr_pc] += 1
        if mr_diff_b and mr_diff_b.strip():
            has_diffusion += 1
    
    # Report physics
    print(f"\n  PHYSICS PARAMETERS:")
    if tr_values:
        print(f"    TR:   p05={percentile(tr_values, 5):.0f}, p50={percentile(tr_values, 50):.0f}, p95={percentile(tr_values, 95):.0f} ms (n={len(tr_values)})")
    if te_values:
        print(f"    TE:   p05={percentile(te_values, 5):.1f}, p50={percentile(te_values, 50):.1f}, p95={percentile(te_values, 95):.1f} ms (n={len(te_values)})")
    if ti_values:
        print(f"    TI:   p05={percentile(ti_values, 5):.0f}, p50={percentile(ti_values, 50):.0f}, p95={percentile(ti_values, 95):.0f} ms (n={len(ti_values)})")
    if flip_values:
        print(f"    Flip: p05={percentile(flip_values, 5):.0f}, p50={percentile(flip_values, 50):.0f}, p95={percentile(flip_values, 95):.0f}° (n={len(flip_values)})")
    if etl_values:
        print(f"    ETL:  p05={percentile(etl_values, 5):.0f}, p50={percentile(etl_values, 50):.0f}, p95={percentile(etl_values, 95):.0f} (n={len(etl_values)})")
    if acq_types:
        acq_str = ", ".join([f"{k}:{v}" for k, v in sorted(acq_types.items(), key=lambda x: -x[1])])
        print(f"    AcqType: {acq_str}")
    if angio_flags:
        angio_str = ", ".join([f"{k}:{v}" for k, v in sorted(angio_flags.items(), key=lambda x: -x[1])])
        print(f"    Angio: {angio_str}")
    if phase_contrast:
        pc_str = ", ".join([f"{k}:{v}" for k, v in sorted(phase_contrast.items(), key=lambda x: -x[1])])
        print(f"    PhaseContrast: {pc_str}")
    if has_diffusion > 0:
        print(f"    Diffusion b-values present: {has_diffusion}/{total} ({has_diffusion*100//total}%)")
    
    # Report significant flags (>30% frequency for techniques)
    threshold = total * 0.30
    
    def report_flags(parser_name, flags_dict):
        significant = [(k, v) for k, v in flags_dict.items() if v >= threshold]
        significant.sort(key=lambda x: -x[1])
        if significant:
            print(f"\n  {parser_name}:")
            for flag, count in significant[:8]:
                pct = count / total * 100
                print(f"    {flag:<30} {count:>4} ({pct:>5.1f}%)")
    
    report_flags("parsed_scanning_sequence", scanning_seq_flags)
    report_flags("parsed_sequence_variant", seq_variant_flags)
    report_flags("parsed_scan_options", scan_options_flags)
    report_flags("parsed_sequence_name", seq_name_flags)
    report_flags("parsed_image_type", image_type_flags)
    
    return {
        "total": total,
        "physics": {
            "tr": {"p05": percentile(tr_values, 5), "p50": percentile(tr_values, 50), "p95": percentile(tr_values, 95)} if tr_values else None,
            "te": {"p05": percentile(te_values, 5), "p50": percentile(te_values, 50), "p95": percentile(te_values, 95)} if te_values else None,
            "ti": {"p05": percentile(ti_values, 5), "p50": percentile(ti_values, 50), "p95": percentile(ti_values, 95)} if ti_values else None,
            "flip": {"p05": percentile(flip_values, 5), "p50": percentile(flip_values, 50), "p95": percentile(flip_values, 95)} if flip_values else None,
            "etl": {"p05": percentile(etl_values, 5), "p50": percentile(etl_values, 50), "p95": percentile(etl_values, 95)} if etl_values else None,
        },
        "scanning_sequence": {k: v for k, v in scanning_seq_flags.items() if v >= threshold},
        "sequence_variant": {k: v for k, v in seq_variant_flags.items() if v >= threshold},
        "scan_options": {k: v for k, v in scan_options_flags.items() if v >= threshold},
        "sequence_name": {k: v for k, v in seq_name_flags.items() if v >= threshold},
    }


def main():
    """Run validation for key techniques."""
    
    if not HAS_DB:
        print("Database not available.")
        return
    
    # Key techniques to validate - selected from technique.json
    # Format: (keyword, name)
    techniques = [
        # Spin Echo family
        ("tse", "TSE"),
        ("fse", "FSE"),  
        ("space", "3D-TSE/SPACE"),
        ("haste", "SS-TSE/HASTE"),
        
        # GRE family
        ("flash", "SP-GRE/FLASH"),
        ("spgr", "SPGR"),
        ("vibe", "VI-GRE/VIBE"),
        ("fiesta", "bSSFP/FIESTA"),
        ("ciss", "pbSS-GRE/CISS"),
        ("medic", "comb-ME-GRE/MEDIC"),
        
        # MPRAGE family
        ("mprage", "MPRAGE"),
        ("bravo", "MPRAGE/BRAVO"),
        ("mp2rage", "MP2RAGE"),
        
        # EPI family
        ("epi", "EPI"),
        ("bold", "GRE-EPI/BOLD"),
        ("resolve", "MS-EPI/RESOLVE"),
        
        # Diffusion
        ("dwi", "DWI-EPI"),
        ("dti", "DTI"),
        
        # MRA
        ("tof", "TOF-MRA"),
        ("pc ", "PC-MRA"),  # space to avoid false matches
        
        # Quantitative
        ("mdme", "MDME"),
        ("qalas", "QALAS"),
        
        # SWI
        ("swi", "SWI"),
    ]
    
    print("=" * 70)
    print("TECHNIQUE VALIDATION TEST")
    print("Analyzing real database fingerprints")
    print("=" * 70)
    
    results = {}
    for keyword, name in techniques:
        try:
            result = analyze_technique(keyword, name, limit=300)
            if result:
                results[name] = result
        except Exception as e:
            print(f"\n  ERROR for {name}: {e}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY TABLE")
    print("=" * 70)
    print(f"{'Technique':<20} {'n':>5} {'TR p50':>8} {'TE p50':>8} {'TI p50':>8} {'Flip p50':>8} Key Flags")
    print("-" * 100)
    
    for name, data in results.items():
        n = data['total']
        phys = data.get('physics', {})
        tr = phys.get('tr', {}).get('p50', '-') if phys.get('tr') else '-'
        te = phys.get('te', {}).get('p50', '-') if phys.get('te') else '-'
        ti = phys.get('ti', {}).get('p50', '-') if phys.get('ti') else '-'
        flip = phys.get('flip', {}).get('p50', '-') if phys.get('flip') else '-'
        
        # Get top flags
        top_flags = []
        for parser in ['scanning_sequence', 'sequence_variant', 'sequence_name']:
            flags = data.get(parser, {})
            for flag, count in sorted(flags.items(), key=lambda x: -x[1])[:2]:
                if count >= n * 0.5:
                    top_flags.append(flag)
        
        tr_str = f"{tr:.0f}" if isinstance(tr, (int, float)) else tr
        te_str = f"{te:.1f}" if isinstance(te, (int, float)) else te
        ti_str = f"{ti:.0f}" if isinstance(ti, (int, float)) else ti
        flip_str = f"{flip:.0f}" if isinstance(flip, (int, float)) else flip
        
        print(f"{name:<20} {n:>5} {tr_str:>8} {te_str:>8} {ti_str:>8} {flip_str:>8} {', '.join(top_flags[:3])}")


if __name__ == "__main__":
    main()
