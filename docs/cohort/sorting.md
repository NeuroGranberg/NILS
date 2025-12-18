# Sorting

**Sorting** runs the classification pipeline on all extracted series. It consists of four steps that must run in sequence.

## The Four Steps

```mermaid
flowchart LR
    A["Step 1: Checkup"] --> B["Step 2: Stack Fingerprint"]
    B --> C["Step 3: Classification"]
    C --> D["Step 4: Output Generation"]
```

---

## Step 1: Checkup

**Purpose:** Validate data and prepare series for processing.

### What It Does

1. **Cohort Subject Resolution**
   - Gets all subjects in the cohort
   - Validates subject membership

2. **Study Discovery**
   - Finds all studies for these subjects
   - Validates study-subject relationships

3. **Study Date Validation & Repair**
   - Checks for missing `study_date`
   - Attempts recovery from `acquisition_date` or `content_date`
   - Flags studies with unrecoverable dates

4. **Series Collection**
   - Gets all series from valid studies
   - Filters by modality if configured

5. **Existing Classification Filter**
   - Checks if series already classified
   - Skip or reprocess based on configuration

### Output

`Step1Handover` containing:

- List of `SeriesForProcessing` (series_id, study_id, subject_id)
- Validation results
- Excluded series with reasons

---

## Step 2: Stack Fingerprint

**Purpose:** Build classification-ready feature vectors for each stack.

### What It Does

1. **Load Handover**
   - Receives series IDs from Step 1

2. **Query Stack Data**
   - Fetches all SeriesStack records
   - Joins with Series, Study, and modality-specific details

3. **Build Fingerprints (Polars)**
   - Vectorized transformations using Polars
   - Normalizes values across modalities
   - Aggregates text fields into searchable blobs
   - Computes geometry features (FOV, aspect ratio)

4. **Database Upsert**
   - Bulk COPY into `stack_fingerprint` table
   - UPSERT for existing fingerprints

5. **Batched Commits**
   - Commits in batches to prevent OOM
   - Enables progress tracking

### Performance

- Processes ~450K stacks in 45-60 seconds
- Previous ORM-based approach caused OOM on large datasets
- Polars vectorization provides 10-50x speedup

### Output

`Step2Handover` containing:

- List of `fingerprint_id` values
- Processing statistics

---

## Step 3: Classification

**Purpose:** Run the 10-stage classification pipeline on each fingerprint.

### What It Does

For each StackFingerprint:

1. **Stage 0: Exclusion Check**
   - Filters screenshots, secondary reformats
   - Checks ImageType flags

2. **Stage 1: Provenance Detection**
   - Determines processing pipeline
   - Routes to appropriate branch

3. **Stage 2: Technique Detection**
   - Identifies pulse sequence family

4. **Stage 3: Branch Logic**
   - Executes provenance-specific logic:
     - `SWI Branch` → SWI/QSM classification
     - `SyMRI Branch` → Synthetic MRI classification
     - `EPIMix Branch` → Multi-contrast EPI
     - `RawRecon Branch` → Standard detection

5. **Stage 4: Modifier Detection**
   - Detects FLAIR, FatSat, MT, etc.

6. **Stage 5: Acceleration Detection**
   - Detects GRAPPA, SMS, etc.

7. **Stage 6: Contrast Agent Detection**
   - Pre/post contrast determination

8. **Stage 7: Body Part Detection**
   - Spinal cord flagging

9. **Stage 8: Intent Synthesis**
   - Maps to BIDS directory_type (anat, dwi, func, fmap)

10. **Stage 9: Review Flag Aggregation**
    - Combines all review triggers
    - Sets `manual_review_required`

### Output

`SeriesClassificationCache` records containing:

- All six classification axes
- Flags (post_contrast, localizer, spinal_cord)
- BIDS intent (directory_type)
- Review requirements

---

## Step 4: Output Generation

**Purpose:** Export classified data to target structure.

### What It Does

1. **Filter by Classification**
   - Include/exclude by provenance
   - Include/exclude by intent

2. **Organize Output**
   - BIDS structure or flat layout
   - Provenance-specific routing

3. **Copy/Convert Files**
   - DICOM copy or NIfTI conversion
   - Parallel processing

### Output Modes

| Mode | Description |
|------|-------------|
| `dcm` | Copy DICOM files |
| `nii` | Convert to NIfTI |
| `nii.gz` | Convert to compressed NIfTI |

---

## Running Sorting

### From Web Interface

1. Navigate to the cohort
2. Click **Sort**
3. Steps run automatically in sequence
4. Monitor in Jobs tab

### Step-by-Step Execution

You can also run steps individually:

1. Run Step 1 (Checkup)
2. Review validation results
3. Run Step 2 (Fingerprint)
4. Run Step 3 (Classification)
5. Run Step 4 (Output) when ready

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `reprocess` | Reclassify already-classified series | false |
| `include_modalities` | Filter to specific modalities | all |
| `parallel_workers` | Classification workers | 4 |

---

## Date Recovery

Step 1 attempts to repair missing `study_date`:

1. Check `acquisition_date` from Instance
2. Check `content_date` from Instance
3. Mark as excluded if unrecoverable

This handles DICOM files with missing or corrupted dates.

---

## Stack Key

Each SeriesStack has a deterministic `stack_key`:

```
MR|TE=2.46|TI=900|FA=9|ECHO=1|TYPE=M|ORIENT=AX
```

This enables:

- Duplicate detection across reruns
- Idempotent classification
- Stack grouping within series

---

## Fingerprint Features

StackFingerprint contains normalized features:

### General Features

- `modality` - MR, CT, PET
- `manufacturer` - Normalized (GE, SIEMENS, PHILIPS, etc.)
- `text_search_blob` - Concatenated descriptions

### Geometry Features

- `stack_orientation` - Axial, Coronal, Sagittal
- `fov_x`, `fov_y` - Field of view in mm
- `aspect_ratio` - FOV ratio

### MR Features

- `mr_te`, `mr_tr`, `mr_ti` - Timing parameters (ms)
- `mr_flip_angle` - Flip angle (degrees)
- `mr_acquisition_type` - 2D or 3D
- `mr_diffusion_b_value` - Diffusion b-value

### CT Features

- `ct_kvp` - Tube voltage
- `ct_tube_current` - Tube current
- `ct_convolution_kernel` - Reconstruction kernel

### PET Features

- `pet_tracer` - Radiopharmaceutical
- `pet_reconstruction_method` - Recon algorithm
- `pet_suv_type` - SUV calculation type

---

## Troubleshooting

### "No series to process"

- Check extraction completed successfully
- Verify series exist in database
- Check modality filters

### Classification Issues

- Review `manual_review_required` flags
- Check `manual_review_reasons_csv` for details
- Use QC interface to review flagged series

### Performance

- Step 2 is typically the bottleneck
- Ensure adequate RAM (8GB+ for large datasets)
- Reduce batch size if memory issues occur
