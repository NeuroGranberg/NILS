# DWI Metadata Enrichment: Investigation & Design Reference

## Overview

DWI (Diffusion-Weighted Imaging) stacks carry three properties that are clinically
meaningful and required for accurate BIDS naming, QC review, and export:

| Property | Example in name | Purpose |
|---|---|---|
| b-value | `b1000`, `b3000` | Identifies diffusion weighting shell |
| Phase encoding direction | `AP`, `PA`, `RL`, `LR` | Identifies reversed-PE pair for susceptibility correction |
| Number of gradient directions | `dir32`, `dir73` | Identifies acquisition density |

These are **not derivable from standard DICOM tags alone** — they require
manufacturer-specific private tags, CSA header parsing (Siemens), or text
fallback. This document records the full investigation and the agreed design.

---

## Database Baseline (29,394 DWI stacks)

| Manufacturer | N stacks | b-val from tag | b-val in text | AP/PA in text | N-dirs in text |
|---|---|---|---|---|---|
| Siemens | 6,609 | 0%* | 71% | 34% | ~2% |
| GE | 2,548 | 24%† | ~0% | 0% | ~1% |
| Philips | 1,016 | 98% | ~6% | 0% | 0% |

\* `mri_series_details.diffusion_b_value` maps to standard DICOM keyword
  `DiffusionBValue` (tag `0018,9087`) — a mandatory tag only in Enhanced MR SOP
  class. Classic Siemens MR stores b-value exclusively in private tag `(0019,100C)`.

† GE classic stores b-value in `(0043,1039)`.

The standard `phase_encoding_direction` column in `mri_series_details` is NULL
for **all 355,582 rows** — tag `(0018,1312)` is mapped but encodes only `ROW`/`COL`
(frequency vs phase axis), never the anatomical AP/PA direction.

---

## Private Tag Reference by Manufacturer

### Siemens (classic MR Image Storage, NUMARIS/4)

#### b-value
- **Tag**: `(0019,100C)` VR=IS, keyword `B_value`
- **Value**: integer string, e.g. `'1000'`
- **Verified on**: Prisma_fit, Skyra, Verio scanners across ALS and iAID cohorts
- **Notes**: Also duplicated in CSA header `(0029,1010)` field `B_value`
- **b0 marker**: `(0019,100D)` CS `DiffusionDirectionality` = `'NONE'` (b0 volume)
  vs `'DIRECTIONAL'` (diffusion-weighted volume)

#### Phase Encoding Direction (AP/PA/RL/LR)
Must be **computed** — no single tag gives anatomical direction directly.

**Required inputs:**

| Tag | Keyword | Example | Role |
|---|---|---|---|
| `(0018,1312)` | `InPlanePhaseEncodingDirection` | `'COL'` | Phase is along columns or rows |
| `(0029,1010)` | CSA header → `PhaseEncodingDirectionPositive` | `'1'` | Sign of phase direction |
| `(0020,0037)` | `ImageOrientationPatient` | `1\0\0\0\1\0` | Image plane cosines |

**Algorithm:**

```
1. Parse IOP into row_cosine (IOP[0:3]) and col_cosine (IOP[3:6])
2. Select phase_cosine:
     if InPlanePhaseEncodingDirection == 'COL': phase_cosine = col_cosine
     else:                                       phase_cosine = row_cosine
3. Find dominant anatomical axis of phase_cosine:
     axes = [R, A, S]
     dominant_idx = argmax(abs(phase_cosine))
     dominant_sign = sign(phase_cosine[dominant_idx])
4. Apply PhaseEncodingDirectionPositive:
     if pe_positive == 1: effective_sign = dominant_sign
     if pe_positive == 0: effective_sign = -dominant_sign
5. Map to label:
     (R, +1) → 'RL'  (R, -1) → 'LR'
     (A, +1) → 'AP'  (A, -1) → 'PA'
     (S, +1) → 'IS'  (S, -1) → 'SI'
```

**Verified example:**
- Series: `ep2d_diff_2mm_hcp_32_b1000_AP`
- IOP = `[1,0,0,0,1,0]`, InPlanePhaseDir = `COL`, PhaseEncDirPositive = `1`
- col_cosine = `[0,1,0]` → dominant axis = A (index 1), sign = +1
- pe_positive = 1 → effective_sign = +1 → label = **AP** ✓

**CSA header parsing:**
The CSA header at `(0029,1010)` uses Siemens' proprietary SV10 binary format.
Field `PhaseEncodingDirectionPositive` value `'1'` = positive, `'0'` = negative.
The SV10 parser must handle: magic bytes `b'SV10'`, 4-byte tag count, then for
each tag: 64-byte null-padded name, 4-byte vm, 4-byte vr, 4-byte syngo_dt,
4-byte n_items, 4-byte unused, then n_items × (4-byte length + 12-byte unused +
length bytes, DWORD-aligned). See `parse_siemens_csa()` in implementation.

#### Number of gradient directions
- Count unique non-zero gradient vectors from `(0019,100E)` VR=FD (3-element)
  across all instances in the stack where `DiffusionDirectionality != 'NONE'`
- Gradient direction per instance is already stored as `diffusion_gradient_orientation`
  in `mri_series_details` (and mirrored in `stack_fingerprint.mr_diffusion_b_value`)

---

### GE (classic MR Image Storage, GENESIS_SIGNA)

#### b-value
- **Tag**: `(0043,1039)` VR=IS, 4-element array
- **Value**: first element = b-value. E.g. `[1000, 8, 0, 7]` → b-value = 1000
- **Coverage**: ~24% of GE DWI stacks already have standard `DiffusionBValue`
  (tag `0018,9087`); prefer `(0043,1039)` as primary for classic GE

#### Phase Encoding Direction
- **Not available as anatomical AP/PA** from any GE private tag
- GE only provides `(0018,1312)` = `ROW`/`COL` — cannot resolve to AP/PA without
  knowing patient orientation relative to bore
- Fallback: text regex on series description (rare — GE protocols almost never
  encode PE direction in series names)

#### Number of gradient directions
- **Tag**: `(0043,1030)` VR=SS — direct integer count, very reliable
- E.g. `14` = 14 diffusion gradient directions
- Also available: `(0043,102F)` VR=SS = direction index for the current instance
  (0-based), useful for validation

#### Direction index and gradient vectors
- `(0043,102F)` SS — index of this instance's gradient direction
- `(0043,1038)` FL array — all gradient vectors for the acquisition (N×3 flattened)
- `(0043,1032)` SS — some form of direction flag (meaning unclear, value `2` observed)

---

### Philips (classic and Enhanced MR)

#### b-value
Two reliable sources — prefer `(2001,1003)` as it is always per-instance:

| Tag | VR | Notes |
|---|---|---|
| `(2001,1003)` | FL | Philips private `DiffusionBValueNumber`; direct float per instance |
| `(0018,9087)` | FD | Standard Enhanced MR `DiffusionBValue`; available on newer Ingenia |

**b0 sentinel:** Philips writes `1.6999999760721821e+38` (≈ `1.7e+38`) as a
sentinel value meaning "not a directional DWI". Filter instances where
`(0018,9075) DiffusionDirectionality == 'ISOTROPIC'` or where
`(2001,1004) DiffusionDirection == 'I'`.

#### Phase Encoding Direction
- **Tag**: `(2001,1004)` CS — values: `'I'` (isotropic), `'X'`, `'Y'`, `'Z'`
- This is in gradient/scanner frame, not anatomical. Mapping to AP/PA requires
  IOP, which works only for standard orientations (transversal, sagittal, coronal)
- **Practical decision**: treat Philips PE direction as **not available** from
  private tags; use text fallback only (Philips rarely encodes AP/PA in series names)

#### Number of gradient directions
- No direct count tag found
- Compute by counting instances per stack where `DiffusionDirectionality != 'ISOTROPIC'`
  (i.e., where `(2001,1004) != 'I'` or `(0018,9075) != 'ISOTROPIC'`)
- Alternatively count unique non-zero `(0018,9089) DiffusionGradientDirection` vectors

---

## Text Fallback Patterns

Applied when private tags are absent or yield no value. Search in
`series_description`, `series_comments`, `image_comments`, `protocol_name`
(concatenated, lowercased).

### b-value text regex
```python
re.search(r'\bb(\d{3,4})\b', text, re.IGNORECASE)
# Matches: b500, b800, b1000, b1200, b1990, b3000
# Returns: int(match.group(1))
```

### Phase encoding direction text regex
```python
re.search(r'\b(AP|PA|RL|LR)\b', text)
# Matches: AP, PA, RL, LR (case-sensitive, word-boundary)
# Returns: match.group(1)
```

### Number of directions text regex (low coverage, mostly Siemens HCP)
```python
re.search(r'\b(\d{2,3})(?:dir|_dir)\b', text, re.IGNORECASE)
# Matches: 32dir, 73dir, 64dir
# Also: r'\bdir(\d{2,3})\b'
```

---

## Multi-b-value Stacks

A single series stack **can contain multiple b-values** when the scanner stores
all b-shells in one series (common in Philips). Examples: b=0 + b=1000 in same
series UID.

**Storage strategy for `series_classification_cache`:**

| Column | Value | Example |
|---|---|---|
| `dwi_b_value` | Max non-zero b-value in the stack (the "shell" for naming) | `1000` |
| `dwi_b_values_csv` | Comma-separated sorted unique b-values in the stack | `"0,1000"` |

**Rationale:** `dwi_b_value` drives the stack name (`b1000`). Including b=0 in the
name would be confusing and inconsistent with conventions. `dwi_b_values_csv`
provides a complete audit trail and enables future queries filtering by b=0
presence (fieldmap-style acquisitions) or multi-shell.

**Aggregation logic (per stack, from instances):**

```python
all_b_values = {int(b) for instance in stack_instances
                if (b := get_b_value(instance)) is not None and b < 1e+37}
dwi_b_values_csv = ",".join(str(b) for b in sorted(all_b_values))
dwi_b_value = max((b for b in all_b_values if b > 0), default=0)
```

A stack containing only b=0 (pure fieldmap/reference) gets `dwi_b_value = 0`
and `dwi_b_values_csv = "0"`.

---

## New Database Columns

Four columns added to `series_classification_cache`, populated during Sort Step 4:

| Column | Type | Description |
|---|---|---|
| `dwi_b_value` | `REAL` | Max non-zero b-value in stack (naming shell) |
| `dwi_b_values_csv` | `TEXT` | All unique b-values in stack, comma-separated |
| `dwi_pe_direction` | `TEXT` | Anatomical PE direction: `AP`/`PA`/`RL`/`LR`/`IS`/`SI` |
| `dwi_n_directions` | `INTEGER` | Count of unique gradient directions (excluding b0) |

These columns are `NULL` for non-DWI stacks and remain `NULL` if derivation
fails for all sources.

---

## Source Priority by Field and Manufacturer

### dwi_b_value / dwi_b_values_csv

| Priority | Siemens | GE | Philips |
|---|---|---|---|
| 1 | `(0019,100C)` private tag | `(0043,1039)[0]` private tag | `(2001,1003)` private tag |
| 2 | CSA `B_value` field | `(0018,9087)` standard tag | `(0018,9087)` standard tag |
| 3 | Text regex `b(\d{3,4})` | Text regex (very rare) | Text regex (rare) |

### dwi_pe_direction

| Priority | Siemens | GE | Philips |
|---|---|---|---|
| 1 | Compute from CSA + IOP | — | — |
| 2 | Text regex `(AP\|PA\|RL\|LR)` | Text regex | Text regex |

### dwi_n_directions

| Priority | Siemens | GE | Philips |
|---|---|---|---|
| 1 | Count unique non-zero `(0019,100E)` across stack | `(0043,1030)` direct tag | Count non-isotropic instances |
| 2 | Text regex `(\d{2,3})dir` | — | — |

---

## Impact on Stack Naming

### Export (`bids/exporter.py` → `_build_stack_name()`)

Current format:
```
[BodyPart]_{Orient}_{base}_{acq_type}_{modifiers}_{technique}_{accel}_{construct}
```

Proposed DWI additions (appended when non-null):
```
..._b{dwi_b_value}_dir{dwi_n_directions}_{dwi_pe_direction}
```

Example:
```
Brain_Ax_DWI_ep2d_b1000_dir32_AP
```

Multi-b-value stack (Philips, b=0+b=1000 in one series):
```
Brain_Ax_DWI_DwiSE_b1000_dir32
```
`dwi_b_values_csv = "0,1000"` is stored but not part of the name — the name
uses the max non-zero shell only.

### UI QC Viewer (`QCViewerPage.tsx` → `buildStackName()`)
Same axes applied; `dwi_b_value`, `dwi_n_directions`, `dwi_pe_direction` fields
returned in the classification cache API response and incorporated in the
client-side name builder.

---

## Implementation Phases

### Phase A — Extract: new per-instance private tag columns in `mri_series_details`

Add columns to capture raw private tag values at extraction time:

| Column | Tag | VR | Manufacturer |
|---|---|---|---|
| `dwi_siemens_b_value` | `(0019,100C)` | IS | Siemens |
| `dwi_siemens_directionality` | `(0019,100D)` | CS | Siemens |
| `dwi_siemens_pe_dir_positive` | CSA `(0029,1010)` → `PhaseEncodingDirectionPositive` | — | Siemens |
| `dwi_ge_b_value_array` | `(0043,1039)` | IS (multi) | GE |
| `dwi_ge_n_directions` | `(0043,1030)` | SS | GE |
| `dwi_philips_b_value` | `(2001,1003)` | FL | Philips |

The existing `diffusion_b_value` (standard `0018,9087`) and
`diffusion_gradient_orientation` (standard `0018,9089`) columns are already
captured in `mri_series_details` and remain as secondary sources.

Private tags are read in `extract/worker.py` using `pydicom.dcmread(... specific_tags=...)`
extended with the additional private tag addresses.

### Phase B — Sort Step 4: aggregate per-stack and compute enrichment columns

A new DWI Enrichment Phase in `step4_completion.py`:

1. For each DWI stack: collect all instances' private b-value columns
2. Aggregate to `dwi_b_value` (max non-zero) and `dwi_b_values_csv` (all unique)
3. Derive `dwi_pe_direction` via Siemens CSA algorithm or text fallback
4. Compute `dwi_n_directions` from gradient direction counts or GE direct tag
5. Write to `series_classification_cache`

---

## Known Limitations

| Limitation | Impact |
|---|---|
| GE anatomical PE direction not available from tags | `dwi_pe_direction` will be NULL for GE unless text encodes it |
| Siemens CSA not present on all Siemens scanners (edge cases: Aera XA firmware) | Falls back to text |
| Philips PE direction in scanner frame, not anatomical | `dwi_pe_direction` from Philips tags not implemented; text-only fallback |
| `specific_tags` in pydicom read cannot target arbitrary private tags by address without group-length tricks | Private tags must be read without `specific_tags` restriction for the DWI group, or the tags added to the allowed-tags set |
| b=0 sentinel in Philips (`1.7e+38`) must be filtered | Already handled in aggregation logic above |
