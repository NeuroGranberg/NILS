# Export

**Export** generates BIDS-compliant output from classified data. It supports multiple output formats and layouts.

## What Export Does

1. **Filters by Intent** - Selects series by BIDS intent (anat, dwi, func, fmap)
2. **Provenance Routing** - Organizes by processing pipeline
3. **Format Conversion** - Generates DICOM or NIfTI output
4. **Naming Convention** - Collision-safe BIDS naming
5. **Parallel Processing** - Multi-threaded copy and conversion

---

## Unified Export Path

Whole-cohort export and stack-subset export share a **single export path** and the same `export` job stage. They differ only in scope:

| Mode | Scope | How it's invoked |
|------|-------|------------------|
| **Cohort export** | Every classified stack in a cohort | Run from the cohort's BIDS stage |
| **Subset export** | A specific set of stacks resolved from a selection manifest | Run from the standalone export endpoint |

Both feed the same configuration builder and run logic, so naming, conversion, and provenance routing behave identically regardless of scope. The analysis-pipeline `db_subset` input materializes its BIDS tree through this exact path too.

!!! note "Legacy `subset_export` stage"
    The standalone export stage was previously named `subset_export`. It is now unified under `export`, and `subset_export` is kept as a **compatibility alias** so older job history still displays correctly.

---

## Asynchronous Execution

Exports run as **tracked background jobs**. Starting an export returns immediately with a job you can monitor and cancel, rather than blocking until the export finishes.

- **Monitor** — poll the returned job for progress (1→100%) and metrics.
- **Cancel** — request cancellation; the job flips to `canceled`.

Database backups and restores behave the same way — see [Database Management](index.md#job-management).

---

## Output Formats

### DICOM (`dcm`)

Copies DICOM files to organized structure. No conversion.

**Use when:**

- Downstream tools need DICOM
- Maximum data fidelity required
- Faster processing (no conversion)

### NIfTI (`nii`)

Converts DICOM to uncompressed NIfTI using `dcm2niix`.

**Use when:**

- Analysis tools need NIfTI
- JSON sidecars are needed
- Standard neuroimaging workflows

### Compressed NIfTI (`nii.gz`)

Converts DICOM to gzip-compressed NIfTI.

**Use when:**

- Storage space is limited
- Data will be archived
- Compression overhead acceptable

---

## Output Layouts

### BIDS Layout

Standard Brain Imaging Data Structure:

```
dataset/
├── dataset_description.json
├── participants.tsv
├── sub-001/
│   └── ses-M00/
│       ├── anat/
│       │   ├── sub-001_ses-M00_T1w.nii.gz
│       │   ├── sub-001_ses-M00_T1w.json
│       │   ├── sub-001_ses-M00_T2w.nii.gz
│       │   └── sub-001_ses-M00_FLAIR.nii.gz
│       ├── dwi/
│       │   ├── sub-001_ses-M00_dwi.nii.gz
│       │   ├── sub-001_ses-M00_dwi.bval
│       │   └── sub-001_ses-M00_dwi.bvec
│       └── func/
│           └── sub-001_ses-M00_task-rest_bold.nii.gz
└── sub-002/
    └── ...
```

### Flat Layout

Simple flat directory with descriptive filenames:

```
output/
├── sub-001_ses-M00_T1w.nii.gz
├── sub-001_ses-M00_T2w.nii.gz
├── sub-001_ses-M00_FLAIR.nii.gz
├── sub-001_ses-M00_dwi.nii.gz
├── sub-002_ses-M00_T1w.nii.gz
└── ...
```

---

## Filtering Options

### By Intent (directory_type)

Include or exclude by BIDS intent:

| Intent | Description | Typical Sequences |
|--------|-------------|-------------------|
| `anat` | Anatomical | T1w, T2w, FLAIR, SWI |
| `dwi` | Diffusion | DWI, DTI |
| `func` | Functional | BOLD, ASL |
| `fmap` | Fieldmaps | B0 maps, phase maps |

**Example:** Export only anatomical data:

```
include_intents: ["anat"]
```

### By Provenance

Include or exclude by processing pipeline:

| Provenance | Description |
|------------|-------------|
| `RawRecon` | Standard reconstructions |
| `SyMRI` | Synthetic MRI |
| `SWIRecon` | SWI processing |
| `DTIRecon` | DTI maps |
| `ProjectionDerived` | MIP, MinIP projections |

**Example:** Exclude derived projections:

```
exclude_provenance: ["ProjectionDerived"]
```

---

## Provenance Routing

Different provenances get special organization:

### SyMRI

Synthetic MRI outputs grouped in subdirectory:

```
anat/
├── sub-001_T1w.nii.gz          # Raw T1w
└── SyMRI/
    ├── sub-001_T1w_synth.nii.gz
    ├── sub-001_T2w_synth.nii.gz
    └── sub-001_FLAIR_synth.nii.gz
```

### SWI

Susceptibility-weighted outputs with special naming:

```
anat/
├── sub-001_swi.nii.gz
├── sub-001_part-mag_swi.nii.gz
├── sub-001_part-phase_swi.nii.gz
└── sub-001_acq-qsm_T2starw.nii.gz
```

---

## Collision Handling

When multiple series have the same classification:

1. **Time-ordered suffixes** - Earlier acquisition first
2. **Run numbers** - `_run-01`, `_run-02`, etc.
3. **Unique identifiers** - Guaranteed uniqueness

**Example:**

```
sub-001_ses-M00_run-01_T1w.nii.gz  # First T1w
sub-001_ses-M00_run-02_T1w.nii.gz  # Second T1w
```

---

## Subject Identifier

### Default: subject_code

Uses the `subject_code` from Subject table.

### Alternative: Other Identifier

Use an alternative ID from `subject_other_identifiers`:

```
subject_identifier_source: 3  # id_type_id for STUDY_ID
```

This allows subjects to be named by study-specific IDs rather than database IDs.

---

## Overwrite Modes

| Mode | Description |
|------|-------------|
| `SKIP` | Skip existing files (default) |
| `CLEAN` | Delete target directory before export |
| `OVERWRITE` | Overwrite existing files in-place |

---

## dcm2niix Configuration

For NIfTI conversion:

| Option | Description | Default |
|--------|-------------|---------|
| `dcm2niix_path` | Path to dcm2niix executable | auto-detect |
| `convert_workers` | Parallel conversion jobs | 8 |
| `copy_workers` | Parallel copy threads | 8 |

---

## Running Export

### From Web Interface

1. Navigate to the cohort
2. Click **Export**
3. Configure:
   - Output directory
   - Format (dcm/nii/nii.gz)
   - Layout (BIDS/flat)
   - Intent filters
   - Provenance filters
4. Click **Start**

### Example Configuration

```yaml
output_dir: /data/bids_output
output_mode: nii.gz
layout: bids
include_intents:
  - anat
  - dwi
exclude_provenance:
  - ProjectionDerived
  - SubtractionDerived
overwrite: skip
```

---

## BIDS Validation

After export, validate your dataset:

```bash
# Install BIDS Validator
npm install -g bids-validator

# Validate
bids-validator /path/to/bids/dataset
```

Or use Docker:

```bash
docker run -v /path/to/bids:/data:ro bids/validator /data
```

---

## Troubleshooting

### "No series to export"

- Check sorting completed successfully
- Verify intent filter matches available data
- Check provenance filters aren't too restrictive

### Conversion Errors

- Verify dcm2niix is installed and accessible
- Check DICOM files are valid
- Review error messages in job log

### Missing Sidecars

- Ensure dcm2niix version supports JSON sidecar generation
- Check for DICOM metadata completeness
