# Modifier Axis

The **Modifier** axis identifies acquisition enhancements applied to the base technique. It answers the question: *"How was the acquisition modified from the standard technique?"*

---

## Overview

Modifiers are **additive** - multiple modifiers can apply to a single series. Output is a comma-separated list (alphabetically sorted).

| Category | Modifiers | Purpose |
|----------|-----------|---------|
| **IR Contrast** | FLAIR, STIR, DIR, PSIR, IR | Tissue nulling via inversion recovery |
| **Fat Suppression** | FatSat, WaterExc, Dixon | Remove/separate fat signal |
| **Signal Modifiers** | MT, FlowComp, MoCo | Modify tissue/flow signals |
| **Trajectory** | Radial, Spiral | Non-Cartesian k-space |
| **Blood Signal** | BlackBlood, BrightBlood | Vessel visualization |

---

## Key Concepts

### Additive Logic

Unlike Technique (single value), multiple modifiers can coexist:

```
T2-FLAIR + FatSat → modifier_csv = "FatSat,FLAIR"
Dixon + Radial    → modifier_csv = "Dixon,Radial"
```

### Mutual Exclusion Groups

Some modifiers are mutually exclusive within their group:

| Group | Members | Rule |
|-------|---------|------|
| **IR_CONTRAST** | FLAIR, STIR, DIR, PSIR, IR | Highest priority wins |
| **TRAJECTORY** | Radial, Spiral | Highest priority wins |

Within IR_CONTRAST, you can't have both FLAIR and STIR - they use different TI values for different tissue nulling.

### Output Format

Modifiers are output as `modifier_csv`:
- Comma-separated
- Alphabetically sorted
- Empty string if no modifiers

Examples:
- `"FLAIR"` - Single modifier
- `"Dixon,FatSat"` - Multiple modifiers
- `""` - No modifiers detected

---

## Detection Strategy

Same three-tier approach as other axes (first match wins per modifier):

### Tier 1: Exclusive Flag (95% confidence)

```yaml
FLAIR:
  detection:
    exclusive: is_flair
```

### Tier 2: Keywords Match (85% confidence)

```yaml
FLAIR:
  keywords:
    - "flair"
    - "dark fluid"
```

### Tier 3: Combination (75% confidence)

```yaml
Dixon:
  detection:
    combination:
      - has_in_phase
      - has_out_phase
```

---

## IR Contrast Modifiers

Inversion Recovery modifiers use a 180° inversion pulse with specific TI to null different tissues.

### FLAIR (Fluid-Attenuated IR)

**TI:** ~2000-2500ms at 3T (nulls CSF)

**Detection:**
- Exclusive flag: `is_flair`
- Keywords: `flair`, `dark fluid`, `da fl`

**Physics:** Long TI nulls CSF signal, making periventricular lesions more visible.

**Clinical use:**
- MS lesion detection
- Stroke imaging
- Tumor visualization near ventricles

!!! note "T1-FLAIR vs T2-FLAIR"
    FLAIR is a **modifier**, not a base contrast. The base (T1w vs T2w) is determined by TE. Most clinical FLAIR is T2-FLAIR.

---

### STIR (Short-TI IR)

**TI:** ~150-200ms at 3T (nulls fat)

**Detection:**
- Exclusive flag: `has_stir`
- Keywords: `stir`, `short tau`, `tirm`

**Physics:** Short TI nulls fat signal regardless of B0 homogeneity.

**Clinical use:**
- Robust fat suppression
- Bone marrow edema
- Orbital imaging

**Caveat:** Also suppresses gadolinium-enhanced tissue (short T1).

---

### DIR (Double IR)

**Physics:** Two inversion pulses to null two tissue types.

**Detection:**
- Exclusive flag: `is_dir`
- Keywords: `dir`, `double ir`, `dual ir`

**Common configuration:** CSF + white matter nulling → gray matter only visible.

**Clinical use:**
- Cortical lesion detection in MS
- Gray matter imaging

---

### PSIR (Phase-Sensitive IR)

**Physics:** Preserves magnetization sign (not just magnitude).

**Detection:**
- Exclusive flag: `has_psir`
- Keywords: `psir`, `phase sensitive`

**Benefit:** Improved gray/white contrast and dynamic range.

**Clinical use:** Often combined with MPRAGE for better tissue delineation.

---

### IR (Generic)

**Detection:**
- Exclusive flag: `has_ir_se` (SE-based IR only)
- Keywords: `inversion recovery`

**Note:** Fallback when no specific IR variant matches. Uses `has_ir_se` to exclude GRE-based IR (MPRAGE, MP2RAGE) where IR is part of the technique, not a modifier.

---

## Fat Suppression Modifiers

### FatSat (Frequency-Selective)

**Physics:** Spectral saturation pulse at fat frequency before excitation.

**Detection:**
- Exclusive flag: `has_fat_sat`
- Keywords: `fatsat`, `fat sat`, `chemsat`, `spair`

**Vendor names:**
- Generic: FatSat
- Philips: SPIR, SPAIR

**Requirement:** Good B0 homogeneity for effective suppression.

---

### WaterExc (Water-Only Excitation)

**Physics:** Binomial or spectral-spatial pulse that excites only water protons.

**Detection:**
- Exclusive flag: `has_water_excitation`
- Keywords: `water excitation`, `proset`, `wex`

**Characteristics:**
- Faster than fat saturation
- Less robust at air-tissue interfaces
- Alternative to FatSat

---

### Dixon (Fat-Water Separation)

**Physics:** Acquires in-phase and out-of-phase echoes to separate fat and water.

**Detection:**
- Exclusive flag: `has_dixon`
- Keywords: `dixon`, `ideal`, `mdixon`, `lava-flex`
- Combination: `has_in_phase` + `has_out_phase`

**Outputs:**
- Water-only image
- Fat-only image
- In-phase image
- Out-of-phase image

**Vendor names:**
- GE: IDEAL
- Philips: mDixon
- Siemens: Dixon

---

## Signal Modifiers

### MT (Magnetization Transfer)

**Physics:** Off-resonance RF saturates bound (macromolecular) protons. Saturation transfers to free water.

**Detection:**
- Exclusive flag: `has_mtc`
- Keywords: `magnetization transfer`, `mtc`

**Effect:** Tissues with bound protons (myelin, collagen) show signal loss.

**Clinical use:**
- Myelin integrity assessment
- MS lesion characterization

---

### FlowComp (Flow Compensation)

**Physics:** Gradient waveforms designed to null velocity-induced phase.

**Detection:**
- Exclusive flag: `has_flow_comp`
- Keywords: `flow comp`, `flowcomp`, `gmn`, `fc`

**Purpose:** Reduces flow artifacts in vessels.

**Variants:**
- First-order (velocity)
- Higher-order (acceleration)

---

### MoCo (Motion Correction)

**Physics:** Retrospective or prospective motion correction.

**Detection:**
- Exclusive flag: `has_moco`
- Keywords: `moco`, `mocoseries`, `motion correct`

**Applications:**
- fMRI
- Diffusion imaging
- Pediatric/uncooperative patients

**Note:** Often indicated in DICOM ImageType as "MOCO" or "MOTIONCORRECTION".

---

## Trajectory Modifiers

### Radial (PROPELLER/BLADE)

**Physics:** Radial k-space spokes through center.

**Detection:**
- Exclusive flag: `is_radial`
- Keywords: `propeller`, `blade`, `multivane`, `radial`

**Vendor names:**
- GE: PROPELLER
- Siemens: BLADE
- Philips: MultiVane

**Benefit:** Motion-robust due to oversampled k-space center.

---

### Spiral

**Physics:** Spiral readout from k-space center.

**Detection:**
- Exclusive flag: `is_spiral`
- Keywords: `spiral`

**Characteristics:**
- Very efficient k-space coverage
- Sensitive to off-resonance artifacts
- Used in ASL, fMRI research

---

## Blood Signal Modifiers

### BlackBlood

**Physics:** Double IR or motion-sensitizing prep to null flowing blood.

**Detection:** Keywords only: `black blood`, `blackblood`, `dark blood`

**Clinical use:**
- Vessel wall imaging
- Cardiac imaging
- Plaque characterization

---

### BrightBlood

**Physics:** Inflow enhancement (TOF-like) for bright arterial blood.

**Detection:** Keywords only: `bright blood`, `brightblood`

**Clinical use:**
- Cardiac imaging
- Angiography variants

---

## Exclusion Group Logic

When multiple modifiers from the same exclusion group match, **highest priority wins**:

### IR_CONTRAST Group

| Modifier | Priority | TI Range |
|----------|----------|----------|
| FLAIR | 1 | ~2000-2500ms (CSF null) |
| STIR | 2 | ~150-200ms (fat null) |
| DIR | 3 | Two TIs |
| PSIR | 4 | Phase-sensitive |
| IR | 99 | Generic fallback |

If both FLAIR and STIR keywords appear, FLAIR wins (lower priority number = higher priority).

### TRAJECTORY Group

| Modifier | Priority |
|----------|----------|
| Radial | 1 |
| Spiral | 2 |

---

## Confidence Levels

| Detection Method | Confidence |
|-----------------|------------|
| Exclusive flag | 95% |
| Keywords match | 85% |
| Combination (AND) | 75% |
| No modifiers (valid) | 80% |

**Note:** "No modifiers detected" is a valid, confident result - many series have no modifiers.

---

## Common Combinations

| Series Type | Typical Modifiers |
|-------------|-------------------|
| T2-FLAIR | `FLAIR` |
| STIR | `STIR` |
| T1 post-contrast FatSat | `FatSat` |
| Dixon VIBE | `Dixon` |
| PROPELLER T2 | `Radial` |
| FLAIR with fat suppression | `FatSat,FLAIR` |
| Black blood TSE | `BlackBlood` |

---

## Modifier vs Technique

Some acquisitions could be viewed as either:

| Acquisition | Classification |
|-------------|----------------|
| STIR | Technique: TSE/IR-TSE, Modifier: STIR |
| FLAIR | Technique: TSE, Modifier: FLAIR |
| MPRAGE | Technique: MPRAGE (IR is part of technique) |
| PROPELLER T2 | Technique: TSE, Modifier: Radial |

**Rule:** IR is a modifier for SE sequences, but part of the technique for GRE (MPRAGE, MP2RAGE).

---

## YAML Configuration

Modifiers are configured in `backend/src/classification/detection_yaml/modifier-detection.yaml`:

```yaml
exclusion_groups:
  IR_CONTRAST:
    description: "IR-based contrast modifiers"
    members: ["FLAIR", "STIR", "DIR", "PSIR"]
    fallback: "IR"

  TRAJECTORY:
    members: ["Radial", "Spiral"]

modifiers:
  FLAIR:
    name: "FLAIR"
    group: "IR_CONTRAST"
    priority: 1
    keywords:
      - "flair"
      - "dark fluid"
    detection:
      exclusive: is_flair

  FatSat:
    name: "FatSat"
    group: null  # Independent - can combine with any
    keywords:
      - "fatsat"
      - "spair"
    detection:
      exclusive: has_fat_sat

rules:
  priority_order:
    - "FLAIR"
    - "STIR"
    # ... IR group first
    - "FatSat"
    - "Dixon"
    # ... independent modifiers
```

---

## Convenience Checks

The detector provides helper methods:

```python
# Check if any IR modifier present
detector.has_ir_modifier(result)  # True if FLAIR, STIR, DIR, PSIR, or IR

# Check if any fat suppression
detector.has_fat_suppression(result)  # True if FatSat, WaterExc, STIR, or Dixon

# Check if trajectory modifier
detector.has_trajectory_modifier(result)  # True if Radial or Spiral
```
