# Extraction

**Extraction** scans DICOM files and imports metadata into the NILS database. This is the first step in processing a cohort.

## What Extraction Does

1. **Subject Discovery** - Scans the raw directory for folder structure
2. **Series Grouping** - Organizes files by Series/Study/Subject
3. **DICOM Parsing** - Extracts metadata from each DICOM file
4. **Database Insertion** - Writes to metadata database tables
5. **Stack Detection** - Groups instances into homogeneous SeriesStacks
6. **Progress Tracking** - Supports resumable extraction

---

## Extraction Phases

### Phase 1: Subject Discovery

- Scans `source_path` (raw root directory)
- Identifies subject folders based on naming convention
- Builds list of `SubjectFolder` objects

### Phase 2: Series Planning

- Enumerates all DICOM files per subject
- Groups files by SeriesInstanceUID
- Groups series by StudyInstanceUID
- Groups studies by Subject

### Phase 3: Parallel Extraction

- Multiple workers parse DICOM files in parallel (ProcessPoolExecutor)
- Extracts full DICOM metadata from each file
- Writes results to batch buffers

### Phase 4: Batch Database Insertion

- Bulk inserts to database with batching
- Atomic transactions per batch to prevent data corruption
- Prevents out-of-memory errors on large datasets

---

## What Gets Extracted

### Study-Level Metadata

| Field | DICOM Tag |
|-------|-----------|
| `study_instance_uid` | (0020,000D) |
| `study_date` | (0008,0020) |
| `study_time` | (0008,0030) |
| `study_description` | (0008,1030) |
| `modality` | (0008,0060) |
| `manufacturer` | (0008,0070) |
| `manufacturer_model_name` | (0008,1090) |
| `station_name` | (0008,1010) |
| `institution_name` | (0008,0080) |

### Series-Level Metadata

| Field | DICOM Tag |
|-------|-----------|
| `series_instance_uid` | (0020,000E) |
| `series_description` | (0008,103E) |
| `protocol_name` | (0018,1030) |
| `sequence_name` | (0018,0024) |
| `body_part_examined` | (0018,0015) |
| `scanning_sequence` | (0018,0020) |
| `sequence_variant` | (0018,0021) |
| `scan_options` | (0018,0022) |

### MR-Specific Metadata

| Field | DICOM Tag |
|-------|-----------|
| `repetition_time` | (0018,0080) |
| `echo_time` | (0018,0081) |
| `inversion_time` | (0018,0082) |
| `flip_angle` | (0018,1314) |
| `echo_train_length` | (0018,0091) |
| `mr_acquisition_type` | (0018,0023) |
| `diffusion_b_value` | (0018,9087) |
| `parallel_acquisition_technique` | (0018,9078) |

### CT-Specific Metadata

| Field | DICOM Tag |
|-------|-----------|
| `kvp` | (0018,0060) |
| `exposure_time` | (0018,1150) |
| `x_ray_tube_current` | (0018,1151) |
| `convolution_kernel` | (0018,1210) |
| `spiral_pitch_factor` | (0018,9311) |
| `ctdi_vol` | (0018,9345) |

### PET-Specific Metadata

| Field | DICOM Tag |
|-------|-----------|
| `radiopharmaceutical` | (0018,0031) |
| `radionuclide_total_dose` | (0018,1074) |
| `radionuclide_half_life` | (0018,1075) |
| `reconstruction_method` | (0054,1103) |
| `attenuation_correction_method` | (0054,1101) |
| `suv_type` | (0054,1006) |

---

## Stack Detection

After parsing instances, NILS groups them into **SeriesStacks**.

### What Defines a Stack?

Instances are grouped by matching:

**MR Stacks:**

- Echo Time (TE)
- Inversion Time (TI)
- Flip Angle
- Echo Number
- Image Type (MAGNITUDE, PHASE, etc.)
- Image Orientation

**CT Stacks:**

- kVp
- Tube Current
- Exposure

**PET Stacks:**

- Bed Index
- Frame Type

### Stack Key

Each stack gets a deterministic `stack_key` - a string combining all defining parameters:

```
MR|TE=2.5|TI=900|FA=9|ECHO=1|TYPE=M|ORIENT=SAG
```

This enables idempotent classification across reruns.

---

## Resume Capability

Extraction supports **resumable processing**:

- Tracks previously extracted subjects/series
- On restart, skips already-processed data
- Enables recovery from interruptions

The resume index stores:

- Subject folder paths already processed
- Series UIDs already in database

---

## Running Extraction

### From Web Interface

1. Navigate to the cohort
2. Click **Extract**
3. Monitor progress in the Jobs tab

### From CLI

```bash
docker compose exec backend neuro-backend extract \
  --cohort my_cohort \
  --workers 4
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--workers` | Parallel parsing workers | 4 |
| `--batch-size` | DB insert batch size | 1000 |
| `--resume` | Resume from last checkpoint | true |

---

## After Extraction

Once extraction completes, the database contains:

- **Subject** records for each patient
- **Study** records for each imaging session
- **Series** records for each acquisition
- **Instance** records for each image
- **SeriesStack** records grouping instances

The cohort is now ready for **Sorting**.

---

## Troubleshooting

### "No DICOM files found"

- Check that `source_path` points to actual DICOM files
- NILS scans recursively - files can be in subdirectories
- Verify files have .dcm extension or no extension

### "Duplicate series"

- This is normal for re-extractions
- Enable resume mode to skip already-imported data
- Or clear existing data before re-extraction

### Memory Issues

- Reduce `--workers` count
- Reduce `--batch-size`
- Ensure adequate system RAM (8GB+ recommended)
