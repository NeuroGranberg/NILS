# Technique Axis

The **Technique** axis identifies the MRI pulse sequence family used for acquisition. It answers the question: *"What pulse sequence was used to acquire this image?"*

---

## Overview

MRI techniques are grouped into four physics-based families:

| Family | Physics | Key Characteristic | Examples |
|--------|---------|-------------------|----------|
| **SE** | Spin Echo | 180° refocusing pulse | TSE, HASTE, SPACE |
| **GRE** | Gradient Echo | Gradient refocusing only | FLASH, MPRAGE, FIESTA |
| **EPI** | Echo Planar | Ultra-fast k-space traversal | Single/multi-shot EPI |
| **MIXED** | Hybrid | Combines physics or application-specific | DWI-EPI, BOLD, ASL, GRASE |

---

## Detection Strategy

NILS uses a **three-tier detection approach** (first match wins):

### Tier 1: Exclusive Flag (95% confidence)

A single `unified_flag` that definitively identifies the technique:

```yaml
MPRAGE:
  detection:
    exclusive: is_mprage  # If True → MPRAGE
```

### Tier 2: Keywords Match (85% confidence)

Text pattern matching in series description:

```yaml
MPRAGE:
  keywords:
    - "mprage"
    - "bravo"
    - "3d tfe"
    - "ir-fspgr"
```

### Tier 3: Combination (75% confidence)

Multiple flags that must ALL be True (AND logic):

```yaml
MPRAGE:
  detection:
    combination:  # All must be True
      - has_ir
      - has_gre
      - is_3d
```

---

## Priority-Based Detection

Techniques are checked in **priority order** - more specific techniques before generic ones:

```
Specific → Generic

MPRAGE → checked before → GRE
RESOLVE → checked before → DWI-EPI → checked before → EPI
SPACE → checked before → TSE → checked before → SE
```

This ensures that MPRAGE (a specific IR-prepared 3D GRE) is detected rather than falling through to generic GRE.

---

## SE Family (Spin Echo)

Uses 180° RF refocusing pulse. Produces "true" T2 contrast without susceptibility artifacts.

### MDME (Multi-Dynamic Multi-Echo)

**Vendor names:** SyMRI, MAGiC

**Physics:** 2D TSE with multiple echoes at multiple TR/saturation times.

**Detection:**
- Exclusive flag: `is_mdme`
- Keywords: `mdme`, `symri`, `magic`

**Purpose:** Acquisition for synthetic MRI - generates T1, T2, PD maps and synthetic contrasts.

---

### SPACE (3D-TSE)

**Vendor names:** Siemens: SPACE, GE: CUBE, Philips: VISTA

**Physics:** 3D volumetric TSE with variable flip angle refocusing (reduces SAR).

**Detection:**
- Exclusive flag: `is_space`
- Keywords: `space`, `cube`, `vista`, `3d tse`
- Combination: `is_tse` + `is_3d`

**Purpose:** Isotropic high-resolution 3D imaging.

---

### HASTE (Single-Shot TSE)

**Vendor names:** Siemens: HASTE, GE: SSFSE, Philips: SSH-TSE

**Physics:** Acquires entire k-space in one TR (single-shot).

**Detection:**
- Exclusive flag: `is_haste`
- Keywords: `haste`, `ssfse`, `ss-tse`

**Characteristics:**
- Very fast acquisition
- Motion-robust
- Has T2 blurring from long echo train

---

### TIRM (IR-TSE)

**Physics:** Inversion recovery preparation + TSE readout.

**Detection:**
- Keywords: `tirm`, `stir`, `ir-tse`
- Combination: `has_ir` + `is_tse`

**Variants:**
- STIR: Short TI for fat suppression
- TIRM: Siemens IR-TSE variant

!!! note "FLAIR vs IR-TSE"
    FLAIR is a **modifier**, not a technique. FLAIR uses IR-TSE physics but is captured in the Modifier axis. The technique would be TSE or IR-TSE.

---

### MESE (Multi-Echo SE)

**Physics:** Multiple spin echoes at different TEs from single excitation.

**Detection:**
- Exclusive flag: `is_me_se`
- Keywords: `mese`, `me-se`, `dual echo se`
- Combination: `is_multi_echo` + `has_se`

**Purpose:** T2 mapping, proton density estimation.

**Note:** Different from TSE - MESE has separate echoes; TSE has an echo train.

---

### RESTORE (VFA-TSE)

**Vendor names:** Siemens: RESTORE, GE: FRFSE, Philips: DRIVE

**Physics:** Variable flip angle within echo train + driven equilibrium.

**Detection:** Keywords only (`restore`, `frfse`, `drive`)

**Purpose:** Optimized T2 contrast with improved SNR.

---

### TSE (Turbo Spin Echo)

**Vendor names:** Siemens: TSE, GE: FSE, Philips: TSE

**Physics:** Echo train of refocusing pulses - multiple echoes per TR.

**Detection:**
- Exclusive flag: `is_tse`
- Keywords: `tse`, `fse`, `turbo spin echo`, `rare`

**Clinical role:** The workhorse of clinical MRI. Used for T1w, T2w, PD, FLAIR.

---

### SE (Basic Spin Echo)

**Physics:** Simple 90°-180° pulse pair, single echo.

**Detection:**
- Exclusive flag: `has_se` (fallback)
- Keywords: `spin-echo`

**Note:** Fallback for SE family when no specific variant matches.

---

## GRE Family (Gradient Echo)

Uses gradient reversal only (no 180° pulse). Faster than SE but susceptible to field inhomogeneities.

### Quantitative/Special Techniques

#### QALAS (3D-QALAS)

**Vendor:** GE

**Physics:** Interleaved IR-GRE for multiparametric mapping.

**Detection:**
- Exclusive flag: `is_qalas`
- Keywords: `qalas`, `3d qalas`

**Purpose:** T1, T2, PD quantification.

---

#### MP2RAGE

**Physics:** Two-inversion MPRAGE - acquires at two TI times.

**Detection:**
- Keywords: `mp2rage`
- Combination: `is_multi_ti` + `has_gre` + `has_ir` + `is_3d`

**Purpose:** Bias-reduced T1 mapping, uniform T1w images.

---

#### MEMPRAGE (Multi-Echo MPRAGE)

**Physics:** MPRAGE with multiple GRE echoes.

**Detection:**
- Keywords: `memprage`, `me-mprage`
- Combination: `is_multi_echo` + `has_ir` + `has_gre` + `is_3d`

**Purpose:** T2* mapping alongside structural T1w.

---

#### MPRAGE

**Vendor names:** Siemens: MPRAGE, GE: BRAVO/IR-SPGR, Philips: 3D-TFE

**Physics:** IR-prepared 3D spoiled GRE.

**Detection:**
- Exclusive flag: `is_mprage`
- Keywords: `mprage`, `bravo`, `3d tfe`
- Combination: `has_ir` + `has_gre` + `is_3d`

**Clinical role:** Standard 3D T1-weighted structural sequence.

---

### MRA/Flow Techniques

#### TOF-MRA (Time-of-Flight)

**Physics:** Inflow enhancement - fresh spins appear bright.

**Detection:**
- Exclusive flag: `is_tof`
- Keywords: `tof`, `time of flight`

**Purpose:** Bright-blood angiography without contrast.

---

#### PC-MRA (Phase Contrast)

**Physics:** Bipolar gradients encode velocity.

**Detection:**
- Exclusive flag: `is_pc`
- Keywords: `pc-mra`, `phase contrast`, `velocity`

**Purpose:** Flow quantification, 4D flow imaging.

---

### Multi-Echo GRE

#### MEDIC (Combined ME-GRE)

**Vendor names:** Siemens: MEDIC, GE: MERGE, Philips: mFFE

**Physics:** Fused multi-echo GRE.

**Detection:** Keywords: `medic`, `merge`, `mffe`

**Implied base:** T2*w (85% confidence)

---

#### MEGRE (Multi-Echo GRE)

**Physics:** Multiple gradient echoes at different TEs.

**Detection:**
- Keywords: `megre`, `me-gre`, `multi echo gre`
- Combination: `is_multi_echo` + `has_gre`

**Purpose:** T2* mapping, QSM, Dixon fat/water separation.

**Implied base:** T2*w (80% confidence)

---

### SSFP Variants

#### CISS

**Vendor names:** Siemens: CISS, GE: FIESTA-C, Philips: 3D-DRIVE

**Physics:** Phase-cycled balanced SSFP.

**Detection:**
- Exclusive flag: `is_ciss`
- Keywords: `ciss`, `fiesta-c`

**Purpose:** High-SNR imaging of cisterns, cranial nerves.

---

#### FIESTA (bSSFP)

**Vendor names:** Siemens: TrueFISP, GE: FIESTA, Philips: bFFE

**Physics:** Fully balanced gradients - high SNR, mixed T2/T1 contrast.

**Detection:**
- Exclusive flag: `is_ssfp`
- Keywords: `bssfp`, `truefisp`, `fiesta`, `balanced ffe`
- Combination: `has_steady_state` + `has_gre`

---

#### DESS (Dual-Echo SSFP)

**Physics:** Combines FID and echo signals.

**Detection:** Keywords: `dess`, `mensa`

**Purpose:** Musculoskeletal/cartilage imaging.

---

#### PSIF (Generic SSFP)

**Physics:** Reversed echo SSFP (T2-like contrast).

**Detection:**
- Keywords: `ssfp`, `psif`, `t2-ffe`
- Combination: `has_steady_state` + `has_gre`

---

### Volumetric GRE

#### VIBE (3D Volumetric Spoiled GRE)

**Vendor names:** Siemens: VIBE, GE: LAVA, Philips: THRIVE

**Physics:** 3D spoiled GRE for volumetric T1 imaging.

**Detection:**
- Keywords: `vibe`, `lava`, `thrive`
- Combination: `has_spoiled` + `is_3d` + `has_gre`

---

#### VFA-GRE (Variable Flip Angle)

**Physics:** Multiple flip angles for T1 mapping.

**Detection:**
- Keywords: `vfa`, `despot`
- Combination: `is_multi_fa` + `has_gre` + `is_3d`

---

### Fast/Basic GRE

#### TurboFLASH (Fast Spoiled GRE)

**Vendor names:** Siemens: TurboFLASH, GE: FSPGR, Philips: TFE

**Detection:**
- Exclusive flag: `is_tfl`
- Keywords: `turboflash`, `fspgr`, `tfe`

**Purpose:** Rapid T1w for dynamic imaging.

---

#### FLASH (Spoiled GRE)

**Vendor names:** Siemens: FLASH, GE: SPGR, Philips: T1-FFE

**Physics:** RF/gradient spoiling for pure T1 contrast.

**Detection:**
- Exclusive flag: `is_flash`
- Keywords: `flash`, `spgr`, `t1-ffe`
- Combination: `has_spoiled` + `has_gre`

---

#### GRE (Generic)

**Detection:**
- Exclusive flag: `has_gre` (fallback)
- Keywords: `gre`, `gradient-echo`, `ffe`

**Note:** Fallback for GRE family.

---

## EPI Family (Echo Planar Imaging)

Ultra-fast k-space traversal. Backbone for functional and diffusion imaging.

### RESOLVE (Multi-Shot EPI)

**Vendor names:** Siemens: RESOLVE, GE: MUSE

**Physics:** Splits k-space over several shots - reduces distortion.

**Detection:**
- Exclusive flag: `is_epi_diff_resolve`
- Keywords: `resolve`, `muse`, `multishot epi`
- Combination: `has_segmented_kspace` + `has_epi`

---

### EPI (Generic)

**Detection:**
- Exclusive flag: `has_epi` (fallback)
- Keywords: `epi`, `echo planar`

---

## MIXED Family (Hybrid Physics)

Combines SE+GRE, SE+EPI physics, or application-specific sequences.

### Diffusion

#### DWI-EPI

**Physics:** SE-EPI with diffusion-sensitizing gradients.

**Detection:**
- Exclusive flag: `is_dwi`
- Keywords: `dwi`, `dti`, `diffusion`, `hardi`

**Implied base:** DWI (95% confidence)

---

#### DWI-STEAM

**Physics:** Stimulated echo diffusion.

**Detection:** Keywords: `steam`, `dwi-steam`

---

### Functional/Perfusion

#### BOLD (BOLD-EPI)

**Physics:** GRE-EPI sensitive to deoxyhemoglobin.

**Detection:**
- Exclusive flag: `is_bold`
- Keywords: `bold`, `fmri`, `functional`, `resting state`

**Note:** No tissue contrast - functional imaging.

---

#### ASL (Arterial Spin Labeling)

**Physics:** RF labeling for non-contrast perfusion.

**Detection:**
- Exclusive flag: `is_asl`
- Keywords: `asl`, `pcasl`, `pasl`, `arterial spin`

**Implied base:** PWI

---

#### Perfusion-EPI

**Physics:** DSC (T2*-based) or DCE (T1-based) perfusion.

**Detection:**
- Exclusive flag: `is_perfusion`
- Keywords: `perfusion`, `dsc`, `dce`, `pwi`

---

### Hybrid Physics

#### GRASE

**Physics:** SE refocusing with GRE readouts.

**Detection:**
- Keywords: `grase`, `turbogse`, `tgse`
- Combination: `has_se` + `has_gre`

**Characteristics:** Faster than pure TSE, less T2* than EPI.

---

#### SE-EPI

**Physics:** 180° SE preparation + EPI readout.

**Detection:**
- Keywords: `se-epi`, `spin echo epi`
- Combination: `has_se` + `has_epi`

**Benefit:** Less T2* artifact than GRE-EPI.

---

#### GRE-EPI

**Physics:** GRE preparation + EPI readout.

**Detection:**
- Keywords: `gre-epi`, `epi-gre`
- Combination: `has_gre` + `has_epi`

**Note:** Backbone for BOLD and DSC perfusion.

---

#### MRF (MR Fingerprinting)

**Physics:** Pseudo-randomized parameters + dictionary matching.

**Detection:**
- Exclusive flag: `is_mrf`
- Keywords: `mrf`, `fingerprinting`

**Purpose:** Quantitative T1/T2 mapping.

---

## Important Notes

### SWI is NOT a Technique

SWI (Susceptibility-Weighted Imaging) is a **provenance/processing method**, not an acquisition technique. The actual acquisition is GRE or EPI. SWI processing is captured in `provenance="SWIRecon"`.

### Radial/Spiral are Modifiers

Radial and Spiral are **trajectory modifiers**, not techniques:
- Radial GRE = GRE technique + Radial modifier
- Spiral EPI = EPI technique + Spiral modifier

These are captured in the Modifier axis.

---

## Confidence Levels

| Detection Method | Confidence | Rationale |
|-----------------|------------|-----------|
| Exclusive flag | 95% | Definitive single flag |
| Keywords match | 85% | Text-based identification |
| Combination (AND) | 75% | Multiple flags required |
| Family fallback | 60% | Generic family detection |

---

## Conflict Detection

NILS checks for physics family conflicts:

- GRE technique detected but SE flags present → **conflict**
- SE technique detected but GRE keywords in text → **conflict**

Conflicts trigger manual review. MIXED family techniques skip conflict checks (they inherently combine physics).

---

## Technique-to-Base Inference

Some techniques strongly imply a base contrast:

| Technique | Implied Base | Confidence |
|-----------|--------------|------------|
| MPRAGE | T1w | 95% |
| DWI-EPI | DWI | 95% |
| TOF-MRA | T1w | 90% |
| ME-GRE | T2*w | 85% |
| ASL-EPI | PWI | 95% |
| BOLD-EPI | *None* | - |

BOLD-EPI has no tissue contrast inference (functional imaging).

---

## YAML Configuration

Techniques are configured in `backend/src/classification/detection_yaml/technique-detection.yaml`:

```yaml
techniques:
  MPRAGE:
    name: "MPRAGE"
    family: "GRE"
    description: "IR-prepped 3D T1w"
    keywords:
      - "mprage"
      - "bravo"
    detection:
      exclusive: is_mprage
      combination:
        - has_ir
        - has_gre
        - is_3d

rules:
  priority_order:
    - "MS-EPI"      # Specific first
    - "DWI-EPI"
    - "MPRAGE"
    # ... more specific ...
    - "GRE"         # Generic last
    - "EPI"
```
