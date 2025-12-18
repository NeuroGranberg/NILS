# Acceleration Axis

The **Acceleration** axis identifies k-space acceleration methods used to reduce scan time. It answers the question: *"How was the acquisition sped up?"*

---

## Overview

Accelerations are **additive** - multiple methods can be used simultaneously in a single acquisition.

| Acceleration | Abbreviation | Mechanism |
|--------------|--------------|-----------|
| **ParallelImaging** | PI | Coil sensitivity encoding |
| **SimultaneousMultiSlice** | SMS | Multiband excitation |
| **PartialFourier** | PF | Incomplete k-space with reconstruction |
| **CompressedSensing** | CS | Sparse reconstruction |
| **ViewSharing** | VS | Temporal k-space reuse |

---

## Key Concepts

### Additive Logic

Multiple accelerations can coexist:

```
fMRI acquisition → acceleration_csv = "ParallelImaging,PartialFourier,SimultaneousMultiSlice"
Standard T1w    → acceleration_csv = "ParallelImaging"
Dynamic MRA     → acceleration_csv = "ParallelImaging,ViewSharing"
```

### Database Coverage

From 457K+ validated fingerprints:

| Acceleration | Stack Count | Common Use |
|--------------|-------------|------------|
| Partial Fourier (Phase) | 98,118 | TSE, EPI |
| Parallel Imaging | 80,746 | Nearly universal |
| Multiband/SMS | 37,982 | fMRI, DWI |
| Partial Fourier (Freq) | 35,041 | Less common |
| HyperSense | 12,969 | GE specific |
| Compressed Sensing | ~200 | Newer sequences |

---

## Detection Strategy

Acceleration uses a **five-tier detection priority**:

1. **Unified flags** (95%) - Pre-computed from multiple sources
2. **Scan options flags** (90%) - Vendor-specific DICOM tags
3. **DICOM tag** (90%) - MR Parallel Acquisition Technique
4. **Keywords** (80%) - Text matching
5. **Sequence name patterns** (75%) - Regex matching

---

## ParallelImaging (GRAPPA/SENSE/ARC)

**Physics:** Uses spatial sensitivity profiles of multiple receiver coils to reduce phase-encoding steps. Missing k-space data is reconstructed using coil sensitivity information.

### Vendor Names

| Vendor | Names |
|--------|-------|
| Siemens | GRAPPA, mSENSE, iPAT |
| GE | ARC, ASSET |
| Philips | SENSE, SPEEDER |
| Canon | SPEEDER |

### Detection

**Unified flags:**
- `has_parallel_imaging`

**Scan options:**
- `has_parallel_gems` (GE ACC_GEMS)
- `has_hypersense` (GE HyperSense)

**DICOM tag:** MR Parallel Acquisition Technique (0018,9078)
- Values: SENSE, GRAPPA, SPEEDER, CSENSE

**Keywords:**
- `grappa`, `sense`, `asset`, `ipat`, `msense`, `accelerat`
- Bounded patterns: `\barc\b`, `arc[`, `_arc_`

**Exclusions:** `hypersense` (separate category)

### Characteristics

- **Typical R factor:** 2-4x
- **Requirement:** Multi-channel coil arrays
- **Trade-off:** SNR reduction (√R penalty)
- Nearly universal in modern clinical MRI

---

## SimultaneousMultiSlice (SMS/Multiband)

**Physics:** Uses composite RF pulses to excite multiple slices at once. Slice separation uses coil sensitivity encoding (similar to parallel imaging).

### Aliases

- Multiband (MB)
- SMS
- SMS-EPI
- HyperBand

### Detection

**Keywords:**
- `multiband`, `multib`, `hyperband`
- Bounded patterns: `\bmb\d`, `mb[`, `\bsms\b`, `_sms_`

**Sequence patterns:**
- `cmrr` (CMRR multiband sequences)
- `hyperband`

**Exclusions:** `combat`, `ambig`, `membrane`, `chamber`, `number`

### Characteristics

- **Typical MB factor:** 2-8x
- **Primary use:** EPI sequences (fMRI, DWI)
- **Trade-off:** SNR reduction, slice cross-talk
- Requires post-processing to separate slices

---

## PartialFourier (Half-Fourier)

**Physics:** Exploits Hermitian symmetry of k-space to acquire only slightly more than half of k-space. Missing data is estimated using homodyne/POCS reconstruction.

### Subtypes

| Subtype | Direction | Scan Option |
|---------|-----------|-------------|
| PFP | Phase-encoding | More common |
| PFF | Frequency-encoding | Less common |

### Aliases

- Half-Fourier
- 5/8, 6/8, 7/8 (fraction of k-space)

### Detection

**Unified flags:**
- `has_partial_fourier`

**Scan options:**
- `has_partial_fourier_phase` (PFP)
- `has_partial_fourier_freq` (PFF)

**Keywords:**
- `partial fourier`, `half fourier`, `half-fourier`
- Bounded patterns: `\bpf\b`, `5/8`, `6/8`, `7/8`

### Characteristics

- **Time reduction:** ~20-40%
- **SNR penalty:** Some due to reconstruction
- **Essential for:** HASTE/SS-FSE sequences
- Can specify both phase and frequency directions

---

## CompressedSensing (CS)

**Physics:** Exploits sparsity of MR images in transform domains (wavelets, etc.) to reconstruct from highly undersampled, incoherently sampled k-space. Uses iterative nonlinear reconstruction.

### Aliases

- CS-SENSE
- Sparse MRI
- Wave-CAIPI

### Detection

**Scan options:**
- `has_cs_gems` (GE CS_GEMS)

**Keywords:**
- `compressed sensing`, `compressedsense`, `sparse`
- `wave-caipi`, `caipi`
- Bounded patterns: `\bcs\[`, `_cs_`

**Exclusions:** `csf` (cerebrospinal fluid), `csa`

### Characteristics

- **Clinically available:** ~2015+
- **Typical R factor:** 4-10x+ possible
- **Reconstruction:** Computationally intensive
- Often combined with parallel imaging

---

## ViewSharing (TWIST/TRICKS/Keyhole)

**Physics:** Updates only central (low-frequency) k-space frequently while reusing peripheral (high-frequency) data from adjacent time frames. Enables high temporal resolution for dynamic imaging.

### Aliases

| Vendor | Name |
|--------|------|
| Siemens | TWIST |
| GE | TRICKS, DISCO, 4D-TRAK |
| Generic | Keyhole |

### Detection

**Keywords:**
- `twist`, `tricks`, `keyhole`
- `view sharing`, `disco`, `4d-trak`
- `time-resolved`, `differential subsampling`

**Sequence patterns:**
- `fldyn` (dynamic with view sharing)

### Characteristics

- **Primary use:** Dynamic/DCE imaging, time-resolved MRA
- **Trade-off:** Spatial resolution for temporal resolution
- Often combined with parallel imaging

---

## Output Format

Acceleration output is a **list** (can be empty or contain multiple):

```python
{
    "acceleration_csv": "ParallelImaging,PartialFourier,SimultaneousMultiSlice"
}
```

Or as list:
```python
["ParallelImaging", "PartialFourier", "SimultaneousMultiSlice"]
```

---

## Common Combinations

| Sequence Type | Typical Accelerations |
|---------------|----------------------|
| Standard T1 MPRAGE | ParallelImaging |
| T2 TSE | ParallelImaging, PartialFourier |
| DWI-EPI | ParallelImaging, PartialFourier |
| fMRI (multiband) | ParallelImaging, PartialFourier, SimultaneousMultiSlice |
| Time-resolved MRA | ParallelImaging, ViewSharing |
| 3D with CS | ParallelImaging, CompressedSensing |
| HASTE | PartialFourier |

---

## Confidence Levels

| Detection Method | Confidence |
|-----------------|------------|
| Unified flag | 95% |
| Scan options | 90% |
| DICOM tag | 90% |
| Keywords | 80% |
| Sequence pattern | 75% |

---

## Examples

| Series Description | Detected Accelerations |
|-------------------|----------------------|
| `t1_mprage_sag_p2_iso` | ParallelImaging |
| `ep2d_diff_mddw_30_mb3_p2` | ParallelImaging, SimultaneousMultiSlice |
| `tse_tra_fs_pf` | PartialFourier |
| `t1_space_sag_grappa2` | ParallelImaging |
| `ep_bold_mb4` | SimultaneousMultiSlice |
| `twist_mra_tra` | ViewSharing |
| `t1_mprage_cs` | CompressedSensing |

---

## Detection Notes

### False Positive Prevention

Several keywords have built-in exclusion patterns to prevent false positives:

| Acceleration | Exclusions |
|--------------|------------|
| ParallelImaging | `hypersense` |
| SMS | `combat`, `ambig`, `membrane`, `chamber`, `number` |
| CompressedSensing | `csf`, `csa` |

### Word Boundary Patterns

Some keywords use word boundaries to avoid false matches:
- `\barc\b` - Matches "ARC" but not "search" or "march"
- `\bmb\d` - Matches "mb2", "mb3" but not "symbol"
- `\bsms\b` - Matches standalone "SMS"

---

## YAML Configuration

Acceleration is configured in `backend/src/classification/detection_yaml/acceleration-detection.yaml`:

```yaml
accelerations:
  ParallelImaging:
    name: "ParallelImaging"
    abbreviation: "PI"
    description: "Coil-sensitivity-based acceleration"

    detection:
      unified_flags:
        - has_parallel_imaging
      scan_options_flags:
        - has_parallel_gems
        - has_hypersense
      dicom_tag:
        tag: "(0018,9078)"
        values: ["SENSE", "GRAPPA"]
      keywords:
        - "grappa"
        - "sense"
      exclude_keywords:
        - "hypersense"

  SimultaneousMultiSlice:
    name: "SimultaneousMultiSlice"
    abbreviation: "SMS"
    detection:
      keywords:
        - "multiband"
        - "hyperband"
      sequence_name_patterns:
        - "cmrr"
      exclude_keywords:
        - "combat"

detection_priority:
  - unified_flags
  - scan_options_flags
  - dicom_tag
  - keywords
  - sequence_name_patterns

confidence_thresholds:
  unified_flag: 0.95
  scan_options: 0.90
  keywords: 0.80
```

---

## Convenience Methods

```python
# Check if any acceleration detected
output.has_acceleration  # True/False

# Check specific acceleration
output.has("ParallelImaging")  # True/False

# Get all detected acceleration names
output.values  # ["ParallelImaging", "PartialFourier"]

# Get specific acceleration result with details
result = output.get("PartialFourier")
result.subtype  # "phase" or "frequency" or "phase+frequency"
result.confidence  # 0.95
result.detection_method  # "unified_flag"
```
