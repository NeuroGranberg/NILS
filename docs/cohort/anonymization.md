# Anonymization

**Anonymization** (pseudo-anonymization) de-identifies DICOM files for research use. NILS provides comprehensive privacy protection with audit logging.

## What Anonymization Does

1. **DICOM Tag Scrubbing** - Removes or modifies 80+ patient-identifiable tags
2. **Patient ID Remapping** - Multiple strategies for ID substitution
3. **Date Manipulation** - Maps study dates to relative timepoints
4. **Audit Logging** - Records all modifications for compliance
5. **Folder Renaming** - Renames directories to new IDs
6. **Output Export** - Generates CSV/Excel audit reports

---

## Tag Categories

Tags are organized into categories that can be enabled/disabled:

### Patient Information

- Patient Name
- Patient Birth Date
- Patient Age
- Patient Address
- Patient Phone
- Patient Email
- Patient Occupation
- Patient ID numbers

### Clinical Trial Information

- Protocol ID
- Enrollment Date
- Subject Number
- Site Information

### Healthcare Provider Information

- Physician Names
- Referring Physician
- Performing Physician
- Department
- Facility Codes

### Institution Information

- Institution Name
- Institution Address
- Institution Contact

### Time and Date Information

- Study Date/Time
- Series Date/Time
- Acquisition Date/Time
- Content Date/Time

---

## Patient ID Strategies

NILS supports five patient ID remapping strategies:

### 1. NONE

No ID remapping. Original Patient IDs are preserved.

!!! warning
    Use only when original IDs are already anonymized.

### 2. SEQUENTIAL

Discover patients and assign sequential IDs.

| Option | Description |
|--------|-------------|
| `starting_number` | First ID number (default: 1) |
| `prefix` | ID prefix (default: "P") |
| `discovery_mode` | How to discover patients |

**Discovery Modes:**

- `per_top_folder` - Each top-level folder is one patient
- `one_per_study` - Each StudyInstanceUID is one patient
- `all` - Scan all DICOM files to discover unique PatientIDs

**Example output:** P001, P002, P003, ...

### 3. FOLDER

Extract ID from folder path using regex.

| Option | Description |
|--------|-------------|
| `regex_pattern` | Pattern to extract ID |
| `folder_depth` | Directory depth to match |
| `fallback_template` | Template for unmatched folders |

**Example:**

- Path: `/data/SUBJECT_001/session1/`
- Pattern: `SUBJECT_(\d+)`
- Result: Patient ID = "001"

### 4. DETERMINISTIC

Hash-based consistent ID mapping.

| Option | Description |
|--------|-------------|
| `salt` | Salt for hash computation |
| `prefix` | ID prefix |

**How it works:**

- Uses blake2b hash with configurable salt
- Same original ID always produces same new ID
- Reproducible across runs with same salt

**Example:** Original "PAT001" → Hash "A3F7B2C1"

### 5. CSV

Load mapping from external CSV file.

| Option | Description |
|--------|-------------|
| `csv_path` | Path to mapping CSV |
| `source_column` | Column with original IDs |
| `target_column` | Column with new IDs |
| `missing_mode` | How to handle unmapped IDs |

**Missing Modes:**

- `SEQUENTIAL` - Assign sequential ID to unmapped
- `HASH` - Use deterministic hash for unmapped

---

## Study Date Mapping

NILS can map actual dates to relative timepoints:

| Original Date | Mapped Value | Meaning |
|---------------|--------------|---------|
| 2020-01-15 | M00 | Baseline |
| 2020-07-20 | M06 | 6 months |
| 2021-01-18 | M12 | 12 months |
| 2022-01-22 | M24 | 24 months |

**How it works:**

1. First scan date becomes anchor (M00)
2. Subsequent dates mapped to nearest timepoint
3. Actual dates removed from DICOM

This enables longitudinal tracking without exposing actual dates.

---

## Running Anonymization

### From Web Interface

1. Navigate to the cohort
2. Click **Anonymize**
3. Configure options:
   - Select tag categories to scrub
   - Choose ID strategy
   - Enable/disable date mapping
4. Click **Start**

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `patient_id_strategy` | ID remapping method | SEQUENTIAL |
| `map_study_dates` | Map to relative timepoints | true |
| `categories` | Tag categories to scrub | all |
| `output_dir` | Where to write anonymized files | required |

---

## Audit System

NILS maintains comprehensive audit logs:

### Per-File Tracking

- Tags removed
- Tags modified
- Original → New values
- Processing timestamp

### Per-Cohort Summary

- Total files processed
- Total tags modified
- Files with errors
- Processing duration

### Export Formats

- **CSV** - Plain text audit log
- **Excel** - Formatted with encryption option

---

## Resume Capability

Anonymization supports resumable processing:

- Tracks completed StudyInstanceUIDs
- Skips already-processed studies on restart
- Enables recovery from interruptions

### Leaf-Level Granularity

Each "leaf" (unique StudyInstanceUID) is tracked:

- `files_written` - Count of files processed
- `files_reused` - Count of files skipped (already done)
- `errors` - Processing errors

---

## Output Structure

Anonymized files are written with new structure:

**Input:**
```
/raw/PATIENT_001/Study_2020/series1/file.dcm
```

**Output (Sequential IDs):**
```
/anonymized/P001/M00/series1/file.dcm
```

**Output (Folder-based):**
```
/anonymized/001/M00/series1/file.dcm
```

---

## Best Practices

### Before Anonymization

1. **Backup original data** - Anonymization modifies files
2. **Verify extraction** - Ensure all data is in database
3. **Review ID strategy** - Choose appropriate method for your study

### During Anonymization

1. **Monitor progress** - Check Jobs tab for status
2. **Check errors** - Review any failed files
3. **Verify output** - Spot-check anonymized files

### After Anonymization

1. **Review audit log** - Verify all expected tags were removed
2. **Validate output** - Check files can still be read
3. **Secure mapping file** - Protect ID mapping for re-identification

---

## Security Considerations

!!! warning "Protect the Mapping"
    The ID mapping file (for SEQUENTIAL, CSV strategies) can be used to re-identify subjects. Store it securely and separately from anonymized data.

!!! note "Pseudo-anonymization"
    NILS performs pseudo-anonymization, not full anonymization. Data can be re-linked using the mapping file if properly authorized.

### What's NOT Removed

- Imaging pixel data
- Acquisition parameters (TR, TE, etc.)
- Scanner information (can be identifying in small studies)
- Burned-in annotations (if present in pixel data)

For stricter anonymization needs, consider:

- Additional pixel-level processing (face removal)
- Scanner model obfuscation
- Custom tag handling
