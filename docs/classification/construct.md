# Construct Axis

The **Construct** axis identifies computed/derived outputs from MRI acquisitions. It answers the question: *"What type of calculated map or derived image is this?"*

---

## Overview

Constructs are **computed outputs** - not directly acquired, but calculated from raw acquisition data.

| Category | Constructs | Source |
|----------|------------|--------|
| **Diffusion** | ADC, eADC, FA, Trace, MD | DWI/DTI |
| **Perfusion** | CBF, CBV, MTT, Tmax, TTP | DSC/ASL |
| **Dixon** | Water, Fat, InPhase, OutPhase | Dixon acquisition |
| **Quantitative** | T1map, T2map, R1map, R2map, PDmap | Relaxometry |
| **MP2RAGE** | INV1, INV2, Uniform, Denoised | MP2RAGE sequence |
| **Synthetic** | SyntheticT1w, SyntheticT2w, SyntheticFLAIR | SyMRI/MAGiC |
| **SWI** | SWI, Phase, QSM | SWI processing |
| **Projection** | MIP, MinIP, MPR | Post-processing |
| **Field Maps** | B0map, B1map | Calibration |
| **Components** | Magnitude, Real, Imaginary | Complex data |

---

## Key Concepts

### Construct vs DICOM DERIVED

These are **not the same**:

- **DICOM DERIVED**: Generic flag meaning "pixels computed from other images"
- **Construct**: The specific TYPE of computed output (ADC, FA, T1map, etc.)

A DERIVED image could be an ADC map, an MPR reformat, or a subtraction image - the construct tells you which.

### Additive Logic

Like Modifiers, constructs are additive - multiple can apply:

```
DTI acquisition → construct_csv = "ADC,FA"
MDME/SyMRI     → construct_csv = "SyntheticT1w,T1map,T2map"
```

### Output Format

- Comma-separated
- Alphabetically sorted
- Empty string for original acquisitions (no constructs)

---

## Detection Strategy

Same three-tier approach:

### Tier 1: Exclusive Flag (95% confidence)

```yaml
ADC:
  detection:
    exclusive: has_adc
```

### Tier 2: Keywords Match (85% confidence)

```yaml
ADC:
  keywords:
    - "adc"
    - "apparent diffusion coefficient"
```

### Tier 3: Combination (75% confidence)

```yaml
SWI:
  detection:
    combination:
      - has_swi
      - is_derived
```

---

## Diffusion Constructs

Derived from diffusion-weighted imaging (DWI/DTI/HARDI).

### ADC (Apparent Diffusion Coefficient)

**Units:** mm²/s

**Detection:**
- Exclusive flag: `has_adc`
- Keywords: `adc`, `apparent diffusion coefficient`

**Computation:** Fitted from multi-b-value DWI using: S(b) = S₀ × exp(-b × ADC)

**Clinical interpretation:**
- Low ADC = restricted diffusion (acute stroke, tumor cellularity)
- High ADC = free water diffusion

---

### eADC (Exponential ADC)

**Formula:** eADC = exp(-b × ADC)

**Detection:**
- Exclusive flag: `has_eadc`
- Keywords: `eadc`, `exponential adc`

**Use:** Some clinical applications prefer exponential form.

---

### FA (Fractional Anisotropy)

**Range:** 0 (isotropic) to 1 (perfectly anisotropic)

**Detection:**
- Exclusive flag: `has_fa`
- Keywords: `fractional anisotropy`, `fa map`

**Clinical interpretation:**
- High FA in white matter tracts (organized fibers)
- Low FA in gray matter, CSF
- FA reduction indicates white matter damage

---

### Trace

**Description:** Direction-independent diffusion-weighted image.

**Detection:**
- Exclusive flag: `has_trace`
- Keywords: `trace`, `tracew`, `isodwi`

**Computation:** Combined DWI from multiple directions (geometric mean).

---

### MD (Mean Diffusivity)

**Description:** Average diffusion in all directions.

**Detection:** Keywords only: `mean diffusivity`, `md map`

**Relationship:** MD ≈ ADC for isotropic diffusion.

---

## Perfusion Constructs

Derived from DSC-MRI or ASL perfusion imaging.

### CBF (Cerebral Blood Flow)

**Units:** ml/100g/min

**Detection:**
- Exclusive flag: `has_cbf`
- Keywords: `cbf`, `cerebral blood flow`

**Clinical use:** Tissue perfusion assessment.

---

### CBV (Cerebral Blood Volume)

**Units:** ml/100g

**Detection:**
- Exclusive flag: `has_cbv`
- Keywords: `cbv`, `cerebral blood volume`

**Clinical use:** Vascular density, tumor grading.

---

### MTT (Mean Transit Time)

**Units:** seconds

**Detection:**
- Exclusive flag: `has_mtt`
- Keywords: `mtt`, `mean transit time`

**Relationship:** MTT = CBV / CBF (central volume theorem).

---

### Tmax (Time to Maximum)

**Description:** Time to maximum of residue function.

**Detection:**
- Exclusive flag: `has_tmax`
- Keywords: `tmax`, `time to max`

**Clinical use:** Stroke - penumbra identification (Tmax > 6s threshold).

---

### TTP (Time to Peak)

**Description:** Time to peak of contrast bolus.

**Detection:**
- Exclusive flag: `has_ttp`
- Keywords: `ttp`, `time to peak`

---

## Dixon Constructs

Fat-water separation outputs from Dixon acquisition.

### Water

**Description:** Water-only image (fat suppressed).

**Detection:**
- Exclusive flag: `has_water`
- Keywords: `water only`, `dixon water`

**Advantage:** Better fat suppression near metal than spectral methods.

---

### Fat

**Description:** Fat-only image (water suppressed).

**Detection:**
- Exclusive flag: `has_fat`
- Keywords: `fat only`, `dixon fat`

**Use:** Fat quantification, lipid assessment.

---

### InPhase

**Description:** Echo where water and fat signals are in phase.

**Detection:**
- Exclusive flag: `has_in_phase`
- Keywords: `in phase`, `inphase`

**Note:** Can be original acquisition, not necessarily derived.

---

### OutPhase

**Description:** Echo where water and fat signals are opposed.

**Detection:**
- Exclusive flag: `has_out_phase`
- Keywords: `opposed phase`, `out of phase`

**Use:** Signal cancellation at fat-water interfaces reveals fat content.

---

## Quantitative Map Constructs

Relaxometry and parameter maps.

### T1map

**Units:** milliseconds

**Detection:**
- Exclusive flag: `has_t1_map`
- Keywords: `t1 map`, `t1 relaxation`
- Combination: `is_qmap`

**Acquisition methods:** VFA-GRE, IR sequences, MP2RAGE.

---

### T2map

**Units:** milliseconds

**Detection:**
- Exclusive flag: `has_t2_map`
- Keywords: `t2 map`, `t2 relaxation`

**Acquisition method:** Multi-echo SE.

---

### R1map / R2map

**Description:** Relaxation rate maps (1/T1 and 1/T2).

**Detection:**
- Exclusive flags: `has_r1`, `has_r2`
- Keywords: `r1 map`, `r2 map`

---

### PDmap

**Description:** Quantitative proton density map.

**Detection:** Keywords: `pd map`, `proton density map`

---

## MP2RAGE Constructs

Outputs from MP2RAGE acquisition. All are fundamentally T1-weighted.

### INV1 (First Inversion)

**TI:** ~700-1000ms (short)

**Detection:**
- Exclusive flag: `is_mp2rage_inv1`
- Keywords: `inv1`, `inv 1`

**Characteristics:**
- Strong T1 weighting
- Lower SNR than INV2
- Not derived (original acquisition)

---

### INV2 (Second Inversion)

**TI:** ~2500-3200ms (long)

**Detection:**
- Exclusive flag: `is_mp2rage_inv2`
- Keywords: `inv2`, `inv 2`

**Characteristics:**
- Higher SNR
- Weaker T1 contrast
- Not derived (original acquisition)

---

### Uniform

**Description:** Bias-corrected T1w image.

**Detection:**
- Exclusive flag: `has_uniform`
- Keywords: `uni image`, `uniform image`

**Computation:**
```
S_UNI = Re(INV1* · INV2) / (|INV1|² + |INV2|²)
```

**Caveat:** Has "salt-and-pepper" noise in background (division of small numbers in air).

---

### Denoised (UniformDenoised)

**Description:** Bias-corrected with clean background.

**Detection:**
- Exclusive flag: `is_uniform_denoised`
- Keywords: `uni-den`, `uniform denoised`

**Computation:**
```
S_UNI-DEN = Re(INV1* · INV2) / (|INV1|² + |INV2|² + β)
```

The regularization term β suppresses background noise.

**Use:** **Primary input for segmentation** (FreeSurfer, FSL, SPM) - has clean dark background.

---

## Synthetic MRI Constructs

Generated from quantitative maps (SyMRI/MAGiC).

### SyntheticT1w

**Detection:**
- Exclusive flag: `has_t1_synthetic`
- Keywords: `synthetic t1`

**Source:** Computed from T1, T2, PD maps using Bloch equations.

---

### SyntheticT2w

**Detection:**
- Exclusive flag: `has_t2_synthetic`
- Keywords: `synthetic t2`

---

### SyntheticFLAIR

**Detection:**
- Exclusive flag: `has_flair_synthetic`
- Keywords: `synthetic flair`

**Advantage:** FLAIR contrast without additional scan time.

---

### SyntheticPDw

**Detection:**
- Exclusive flag: `has_pd_synthetic`
- Keywords: `synthetic pd`

---

### MyelinMap

**Description:** Myelin content / myelin water fraction.

**Detection:**
- Exclusive flag: `has_myelin`
- Keywords: `myelin map`, `mwf`

**Use:** White matter integrity assessment, MS research.

---

## SWI Constructs

Susceptibility-weighted imaging outputs.

### SWI (Processed)

**Description:** Magnitude × phase mask^n for maximum susceptibility sensitivity.

**Detection:**
- Keywords: `swi processed`, `swi combined`
- Combination: `has_swi` + `is_derived`

**Note:** SWI branch (`branches/swi.py`) handles this directly for SWI provenance.

---

### Phase

**Description:** Phase image (wrapped or unwrapped).

**Detection:**
- Exclusive flag: `has_phase`
- Keywords: `phase`, `phase map`

**Use:**
- QSM derivation
- Iron vs calcium differentiation
- Phase-contrast flow

**Note:** Can be original acquisition, not necessarily derived.

---

### QSM (Quantitative Susceptibility Mapping)

**Units:** ppm (parts per million)

**Detection:** Keywords: `qsm`, `susceptibility map`, `chi map`

**Computation:** Derived from phase via dipole inversion.

**Clinical use:** Iron quantification, calcification detection.

---

## Projection Constructs

Post-processing reformats.

### MIP (Maximum Intensity Projection)

**Detection:**
- Exclusive flag: `is_mip`
- Keywords: `mip`, `maximum intensity projection`

**Use:** Angiography visualization, vessel overview.

---

### MinIP (Minimum Intensity Projection)

**Detection:**
- Exclusive flag: `is_minip`
- Keywords: `minip`, `minimum intensity projection`

**Use:** Airway visualization, SWI vein display.

---

### MPR (Multiplanar Reformation)

**Detection:**
- Exclusive flag: `is_mpr`
- Keywords: `multiplanar reformat`, `reformatted`

**Note:** Keyword "mpr" excluded to avoid false matches with "mprage".

---

## Field Map Constructs

Calibration maps.

### B0map

**Description:** Static field off-resonance map.

**Detection:** Keywords: `b0 map`, `field map`, `fieldmap`

**Use:** Distortion correction, shimming.

---

### B1map

**Description:** RF transmit/receive field map.

**Detection:** Keywords: `b1 map`, `b1+`, `b1-`

**Use:** RF inhomogeneity correction.

---

## Component Constructs

Complex data components.

### Magnitude

**Description:** Magnitude from complex data.

**Detection:** Keywords only: `magnitude image`, `magnitude only`

**Note:** `has_magnitude` flag not used for detection - most images are magnitude.

---

### Real

**Detection:**
- Exclusive flag: `has_real`
- Keywords: `real image`, `real component`

---

### Imaginary

**Detection:**
- Exclusive flag: `has_imaginary`
- Keywords: `imaginary image`, `imaginary component`

---

## Original vs Derived

Most constructs require `is_derived=True` in DICOM ImageType:

| Requires Derived | Construct Examples |
|------------------|-------------------|
| **Yes** | ADC, FA, CBF, T1map, SyntheticT1w, MIP |
| **No** | INV1, INV2, InPhase, OutPhase, Phase, Magnitude |

Some outputs (like MP2RAGE inversions, Dixon echoes, phase images) are **original** acquisitions, not derived.

---

## Common Output Examples

| Acquisition | construct_csv |
|-------------|---------------|
| Standard T1w MPRAGE | `""` (empty - no constructs) |
| DTI with maps | `"ADC,FA"` |
| DSC perfusion | `"CBF,CBV,MTT"` |
| Dixon | `"Fat,Water"` |
| SyMRI/MAGiC | `"SyntheticFLAIR,SyntheticT1w,T1map,T2map"` |
| MP2RAGE | `"Denoised,T1map"` or `"INV1"` |
| TOF-MRA MIP | `"MIP"` |
| SWI | `"Phase,SWI"` |

---

## Confidence Levels

| Detection Method | Confidence |
|-----------------|------------|
| Exclusive flag | 95% |
| Keywords match | 85% |
| Combination (AND) | 75% |

---

## YAML Configuration

Constructs are configured in `backend/src/classification/detection_yaml/construct-detection.yaml`:

```yaml
constructs:
  ADC:
    name: "ADC"
    category: "diffusion"
    description: "Apparent Diffusion Coefficient map"
    keywords:
      - "adc"
      - "apparent diffusion coefficient"
    detection:
      exclusive: has_adc
    requires_derived: true

  INV1:
    name: "INV1"
    category: "mp2rage"
    detection:
      exclusive: is_mp2rage_inv1
    requires_derived: false  # Original acquisition

rules:
  allow_multiple: true
  priority_order:
    - "ADC"
    - "FA"
    # ... more specific first
```

---

## Convenience Methods

The detector provides helper methods:

```python
# Get all constructs by category
detector.get_constructs_by_category("diffusion")  # ["ADC", "FA", "MD", "Trace", "eADC"]

# Get all categories
detector.get_categories()  # ["component", "diffusion", "dixon", "mp2rage", ...]

# Check if specific construct detected
output.has("ADC")  # True/False

# Get constructs by category from output
output.by_category("perfusion")  # List of ConstructMatch objects
```
