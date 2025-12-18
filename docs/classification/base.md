# Base Contrast Axis

The **Base** axis represents the fundamental tissue contrast weighting of an MRI series. It answers the question: *"What physical contrast mechanism is this image based on?"*

---

## Overview

Base contrast is determined by the MRI physics parameters (TR, TE, TI) and acquisition type. Each base contrast emphasizes different tissue properties:

| Base | Full Name | Primary Signal | Clinical Use |
|------|-----------|----------------|--------------|
| **T1w** | T1-weighted | Short TR/TE, WM bright | Anatomy, Gd enhancement |
| **T2w** | T2-weighted | Long TR/TE, fluid bright | Pathology detection |
| **PDw** | Proton density | Long TR, short TE | Tissue composition |
| **T2\*w** | T2-star weighted | GRE, susceptibility | Iron, blood products |
| **DWI** | Diffusion-weighted | Water motion | Stroke, tumors |
| **PWI** | Perfusion-weighted | Blood flow | Hemodynamics |
| **SWI** | Susceptibility-weighted | Phase + magnitude | Veins, microbleeds |
| **MTw** | Magnetization transfer | Bound protons | Myelin integrity |
| **T1rho** | T1-rho weighted | Spin-lock | Cartilage, research |

---

## Detection Strategy

NILS uses a **four-tier detection approach** (in priority order):

### Tier 1: Technique Inference

Some pulse sequences physics-lock to a specific base contrast:

| Technique | Implied Base | Confidence |
|-----------|--------------|------------|
| MPRAGE, MEMPRAGE, MP2RAGE | T1w | 95% |
| TOF-MRA | T1w | 90% |
| ME-GRE, comb-ME-GRE | T2*w | 85-90% |
| SWI | SWI | 95% |
| DWI-EPI | DWI | 95% |
| ASL-EPI, DSC-EPI | PWI | 90-95% |

**Why?** These techniques have physics constraints that determine contrast. MPRAGE always has T1-weighting due to its IR-prepared 3D GRE design.

### Tier 2: Exclusive Flags

Definitive signals from `unified_flags` that immediately identify base:

**DWI indicators:**
- `is_dwi` - Diffusion sequence flag
- `has_adc` - ADC map present
- `has_fa` - FA map present
- `has_trace` - Trace diffusion

**PWI indicators:**
- `is_perfusion` - Perfusion flag
- `is_asl` - Arterial spin labeling
- `has_cbf`, `has_cbv`, `has_mtt`, `has_tmax`, `has_ttp` - Perfusion maps

**SWI indicators:**
- `is_swi` - SWI sequence
- `has_swi` - SWI processing

**MTw indicator:**
- `has_mtc` - Magnetization transfer contrast

**Synthetic MRI outputs:**
- `has_t1_synthetic` → T1w
- `has_t2_synthetic` → T2w
- `has_flair_synthetic` → T2w (FLAIR base)
- `has_pd_synthetic` → PDw

### Tier 3: Keyword Matching

Text search in series description for explicit contrast labels:

| Keywords | Base |
|----------|------|
| `t1w`, `t1 weighted`, `t1-w` | T1w |
| `t2w`, `t2 weighted`, `t2-w` | T2w |
| `pdw`, `proton density` | PDw |
| `t2*`, `t2star` | T2*w |
| `dwi`, `diffusion`, `dti` | DWI |
| `pwi`, `perfusion`, `dsc`, `dce` | PWI |
| `swi`, `swan`, `venobold` | SWI |
| `mtw`, `magnetization transfer` | MTw |

### Tier 4: Physics-Based Inference

When other methods fail, use TR/TE/TI thresholds (validated against 400K+ fingerprints):

#### Spin Echo (SE) Family
| Contrast | TR | TE | Notes |
|----------|----|----|-------|
| T1w | < 1000ms | < 30ms | Short TR/TE |
| T2w | > 2000ms | > 50ms | Long TR/TE |
| PDw | > 2000ms | < 30ms | Long TR, short TE |

#### Gradient Echo (GRE) Family
| Contrast | TE | Notes |
|----------|----|----|
| T1w | < 10ms | Short TE |
| T2*w | > 15ms | Long TE for susceptibility |

#### Inversion Recovery (IR) Family
| Contrast | TI | Notes |
|----------|----|----|
| T1w (standard IR) | 300-1500ms | Medium TI |
| T2w (STIR-like) | < 300ms | Short TI nulls fat |

---

## Special Cases

### FLAIR Differentiation

FLAIR can be **T1-FLAIR** or **T2-FLAIR**, differentiated by TE:

| Type | TE Threshold | Base | Typical Use |
|------|-------------|------|-------------|
| T1-FLAIR | < 40ms | T1w | Research |
| T2-FLAIR | ≥ 40ms | T2w | Clinical (most common) |

Database validation shows:
- 97% of series with "t1" keyword + FLAIR have TE < 40ms
- 99.9% of series with "t2" keyword + FLAIR have TE ≥ 80ms

!!! note "FLAIR Modifier"
    The **FLAIR** modifier is detected on the Modifier axis separately. Here we only determine the underlying base contrast (T1w vs T2w).

### Dual-Echo PD+T2

Dual-echo sequences acquire both PD and T2 in a single acquisition. NILS splits these into separate stacks and uses TE to determine which echo:

| Echo Type | TE | Base |
|-----------|----|----|
| PD echo | < 40ms | PDw |
| T2 echo | ≥ 40ms | T2w |

Detection triggers when BOTH "pd" and "t2" keywords appear in the series description.

### MP2RAGE Outputs

MP2RAGE is fundamentally a T1-weighted technique. All outputs receive T1w base:

| Output | Flags | Base |
|--------|-------|------|
| INV1 | `is_mp2rage_inv1` | T1w |
| INV2 | `is_mp2rage_inv2` | T1w |
| UNI | `has_uniform` | T1w |
| UNI-DEN | `is_uniform_denoised` | T1w |
| T1 Map | `has_t1_map` | T1w |

### Quantitative Maps

Relaxometry maps have an implied base from their measurement:

| Map | Base | Rationale |
|-----|------|-----------|
| T1 Map, R1 | T1w | Measures T1 relaxation |
| T2 Map, R2 | T2w | Measures T2 relaxation |

---

## Primary Tissue Contrasts

### T1w (T1-Weighted)

**Physics:** Short TR (~500-800ms) allows incomplete T1 recovery, emphasizing T1 differences. Short TE (~10-20ms) minimizes T2 effects.

**Signal characteristics:**
- White matter: **bright** (short T1)
- Gray matter: intermediate
- CSF: **dark** (long T1)
- Fat: bright

**Clinical applications:**
- Anatomy reference
- Gadolinium enhancement
- Post-contrast lesion detection

### T2w (T2-Weighted)

**Physics:** Long TR (>2000ms) allows full T1 recovery, eliminating T1 contrast. Long TE (>50ms) emphasizes T2 differences.

**Signal characteristics:**
- White matter: dark
- Gray matter: intermediate
- CSF: **very bright** (long T2)
- Edema: bright

**Clinical applications:**
- Pathology detection
- MS lesions
- Tumor visualization
- Infection/inflammation

### PDw (Proton Density)

**Physics:** Long TR (>2000ms) removes T1 contrast. Short TE (<30ms) minimizes T2 contrast. Signal primarily reflects hydrogen density.

**Signal characteristics:**
- Intermediate contrast
- CSF: bright
- Tissues differentiated by proton content

**Clinical applications:**
- Often paired with T2w (dual-echo)
- Cartilage imaging
- Research applications

### T2*w (T2-Star Weighted)

**Physics:** GRE sequence without 180° refocusing pulse. Sensitive to microscopic field inhomogeneities (susceptibility effects).

**Signal characteristics:**
- Blood products: **dark** (paramagnetic)
- Iron deposits: dark
- Calcium: dark
- Air-tissue interfaces: signal dropout

**Clinical applications:**
- Hemorrhage detection
- Microbleed identification
- Iron quantification
- Basis for SWI and BOLD

---

## Specialized Contrasts

### DWI (Diffusion-Weighted)

**Physics:** Measures random motion of water molecules using diffusion gradients. Contrast depends on b-value.

**Signal characteristics:**
- Restricted diffusion: **bright** (acute stroke)
- Free water: dark
- High b-value → more diffusion weighting

**Derived maps:**
- ADC (Apparent Diffusion Coefficient)
- FA (Fractional Anisotropy)
- MD (Mean Diffusivity)
- Trace

### PWI (Perfusion-Weighted)

**Physics:** Measures blood flow dynamics. Multiple methods:
- **DSC**: Dynamic susceptibility contrast (T2*-based)
- **DCE**: Dynamic contrast-enhanced (T1-based)
- **ASL**: Arterial spin labeling (non-contrast)

**Outputs:**
- CBF (Cerebral Blood Flow)
- CBV (Cerebral Blood Volume)
- MTT (Mean Transit Time)
- Tmax, TTP (time parameters)

### SWI (Susceptibility-Weighted)

**Physics:** Combines GRE magnitude and filtered phase images to enhance susceptibility contrast.

**Signal characteristics:**
- Paramagnetic substances: very dark
- Veins: dark (deoxyhemoglobin)
- Superior to T2* for small structures

**Applications:**
- Microbleed detection
- Venous imaging
- Iron quantification
- QSM derivation

### MTw (Magnetization Transfer)

**Physics:** Off-resonance RF pulse saturates bound (macromolecular) protons. Saturation transfers to free water pool, reducing signal in tissues with bound protons.

**Signal characteristics:**
- Myelin-rich tissue: signal loss
- Free water: minimal effect

**Applications:**
- MS lesion characterization
- Myelin integrity assessment

---

## Confidence Levels

Detection confidence varies by method:

| Method | Confidence | Rationale |
|--------|------------|-----------|
| Technique inference | 95% | Physics-locked |
| Exclusive flags | 90% | Definitive markers |
| Keywords | 85% | Text-based |
| FLAIR/Dual-echo TE | 85% | Physics threshold |
| Physics ranges | 70% | Edge case fallback |
| Unknown fallback | 50% | Could not determine |

---

## Conflict Detection

NILS checks for conflicts between detection method and text evidence:

- If physics predicts T1w but text contains "t2w" → **conflict flagged**
- Conflicts trigger manual review
- Authoritative methods (technique inference, exclusive flags) skip conflict check

---

## YAML Configuration

Base detection is configured in `backend/src/classification/detection_yaml/base-detection.yaml`:

```yaml
bases:
  T1w:
    name: "T1w"
    description: "T1-weighted; short TR/TE, white matter bright"
    keywords:
      - "t1w"
      - "t1 weighted"
    physics:
      se:
        tr_max: 1000
        te_max: 30
      gre:
        te_max: 10

technique_inference:
  MPRAGE: ["T1w", 0.95]
  MP2RAGE: ["T1w", 0.95]
  # ...

rules:
  allow_multiple: false  # Only one base per series
  priority_order:
    - "DWI"
    - "PWI"
    - "SWI"
    # ... (specialized first, then primary)
```
