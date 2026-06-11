# STAGE Branch

The **STAGE branch** handles **STrategically Acquired Gradient Echo** (STAGE) outputs — a rapid multi-parametric 3D GRE protocol.

---

## Overview

| Aspect | Description |
|--------|-------------|
| **Provenance** | `STAGE` |
| **Classification Branch** | `stage` |
| **Source Module** | `backend/src/classification/branches/stage.py` |
| **Output Types** | Magnitude, Phase (raw outputs) |

STAGE acquires two dual-echo 3D GRE scans at different flip angles (e.g. 6° and 24°, TR ≈ 25 ms). From these few raw acquisitions, downstream processing can derive many maps — enhanced T1 contrast, SWI/tSWI, QSM, and T1/PD/R2\* maps. NILS classifies the **raw DICOM outputs the scanner stores**: magnitude and phase images at each flip-angle/echo combination.

---

## What Makes STAGE Different

Unlike the SWI and SyMRI branches, the STAGE branch **does not override base contrast**. The raw echoes genuinely carry tissue contrast — proton-density weighting at the low flip angle, T1 weighting at the high flip angle — which the standard `BaseContrastDetector` resolves correctly from text keywords. The branch sets only the technique and construct:

| Axis | STAGE behavior |
|------|----------------|
| **Base** | **Not overridden** — standard detector resolves PDw vs T1w |
| **Technique** | Overridden to `STAGE` (replacing what would be VIBE/FLASH) |
| **Construct** | Overridden to `Magnitude` or `Phase` (from ImageType flags) |
| **Intent** | `anat` |

This is the key design distinction:

| Branch | Overrides base? | Overrides construct? | Overrides technique? |
|--------|:---------------:|:--------------------:|:--------------------:|
| **STAGE** | No | Yes | Yes |
| **SWI** | Yes (`base=SWI`) | Yes | Yes |
| **SyMRI** | Yes (often `base=NULL`) | Yes | No |

---

## Detection

STAGE is its own provenance, detected by the keyword `stage` in the text search blob (confidence 0.90). In the provenance priority order it is placed **before SWIRecon** so that STAGE's own SWI/QSM-named derived echoes are not grabbed by the SWI branch.

### Construct Selection

| Condition | Construct | Meaning |
|-----------|-----------|---------|
| `has_phase` and not `has_magnitude` | `Phase` | Phase image (for SWI/QSM processing) |
| `has_magnitude` | `Magnitude` | Tissue-weighted magnitude image |
| neither | `Magnitude` (fallback) | Most STAGE stacks are magnitude |

---

## Example

**DICOM Fields:**
```
SeriesDescription: STAGE_FA6_TE1 magnitude proton-density
ScanningSequence: GR
ImageType: ORIGINAL\PRIMARY\M
```

**Classification:**
```python
provenance = "STAGE"
base = "PDw"          # resolved by the standard base detector
construct = "Magnitude"
technique = "STAGE"
directory_type = "anat"
```

At the high flip angle the same logic resolves `base = "T1w"`.

---

## See Also

- [Branches Overview](index.md) - Why branches exist
- [SWI Branch](swi.md) - Susceptibility-weighted imaging (overrides base)
- [Provenance Axis](../provenance.md) - How STAGE provenance is detected
- [Base Axis](../base.md) - PDw vs T1w resolution
