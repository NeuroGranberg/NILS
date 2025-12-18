# Core Concepts

This section explains the fundamental data models and terminology used in NILS.

## Data Hierarchy

NILS implements a hierarchical data model that mirrors the DICOM structure:

```
Subject
├── Study
│   ├── Series
│   │   ├── SeriesStack (homogeneous instance group)
│   │   │   ├── Instance
│   │   │   └── Instance
│   │   └── SeriesStack
│   │       └── Instance
│   └── Series
└── Study
```

## Entities

### Subject

A **Subject** represents an individual patient or participant in the study.

| Field | Description |
|-------|-------------|
| `subject_code` | Unique identifier for the subject |
| `patient_name` | Original DICOM patient name (optional) |
| `patient_birth_date` | Date of birth |
| `patient_sex` | Sex (M/F/O) |
| `ethnic_group` | Ethnicity information |
| `occupation` | Patient occupation |
| `additional_patient_history` | Clinical history notes |

Subjects can be linked to:

- **Cohorts** via `SubjectCohort` (many-to-many)
- **Diseases** via `SubjectDisease` with onset/diagnosis events
- **Other identifiers** via `SubjectOtherIdentifier` (multiple ID types)

### Study

A **Study** represents a single imaging session (one visit to the scanner).

| Field | Description |
|-------|-------------|
| `study_instance_uid` | Unique DICOM Study Instance UID |
| `study_date` | Date of the imaging session |
| `study_time` | Time of the session |
| `study_description` | Description of the study |
| `modality` | Primary modality (MR, CT, PET) |
| `manufacturer` | Scanner manufacturer |
| `manufacturer_model_name` | Scanner model |
| `station_name` | Scanner station name |
| `institution_name` | Institution where scan was performed |

### Series

A **Series** represents a single acquisition within a study.

| Field | Description |
|-------|-------------|
| `series_instance_uid` | Unique DICOM Series Instance UID |
| `modality` | Acquisition modality |
| `series_description` | Series description from DICOM |
| `protocol_name` | Protocol name |
| `sequence_name` | Sequence name |
| `body_part_examined` | Anatomical region |
| `scanning_sequence` | DICOM scanning sequence |
| `sequence_variant` | DICOM sequence variant |
| `scan_options` | Additional scan options |

**Modality-specific details** are stored in separate tables:

- `MRISeriesDetails` - MR-specific parameters (TR, TE, TI, flip angle, etc.)
- `CTSeriesDetails` - CT-specific parameters (kVp, exposure, kernel, etc.)
- `PETSeriesDetails` - PET-specific parameters (tracer, SUV, corrections, etc.)

### SeriesStack

A **SeriesStack** is a critical concept in NILS. It represents a **homogeneous group of instances** within a single Series that share identical acquisition parameters.

!!! info "Why SeriesStack?"
    A single DICOM Series can contain multiple logically distinct stacks. For example:

    - Multi-echo acquisitions (different TE values)
    - Multi-flip-angle acquisitions
    - Dixon fat/water separation (in-phase, out-of-phase, water, fat images)
    - SWI magnitude and phase components

    SeriesStack allows NILS to classify each stack independently.

| Field | Description |
|-------|-------------|
| `stack_index` | Numeric index within the series |
| `stack_key` | Deterministic string combining defining parameters |
| `stack_modality` | MR, CT, or PET |
| `stack_n_instances` | Number of instances in this stack |

**MR-specific stack fields:**

- `stack_echo_time`, `stack_repetition_time`, `stack_inversion_time`
- `stack_flip_angle`, `stack_echo_train_length`
- `stack_receive_coil_name`, `stack_image_orientation`
- `stack_image_type` (e.g., MAGNITUDE, PHASE, REAL, IMAGINARY)

**CT-specific stack fields:**

- `stack_kvp`, `stack_tube_current`, `stack_xray_exposure`

**PET-specific stack fields:**

- `stack_pet_bed_index`, `stack_pet_frame_type`

### StackFingerprint

A **StackFingerprint** is a flattened, feature-rich representation of a SeriesStack designed for the classification algorithm.

It normalizes parameters across modalities and aggregates text fields for easy searching:

| Category | Fields |
|----------|--------|
| **General** | modality, manufacturer, manufacturer_model |
| **Text** | text_search_blob (concatenated descriptions), contrast_search_blob |
| **Geometry** | stack_orientation, fov_x, fov_y, aspect_ratio |
| **MR** | mr_te, mr_tr, mr_ti, mr_flip_angle, mr_acquisition_type (2D/3D), mr_angio_flag |
| **CT** | ct_kvp, ct_exposure_time, ct_tube_current, ct_convolution_kernel |
| **PET** | pet_tracer, pet_reconstruction_method, pet_suv_type, pet_units |

### SeriesClassificationCache

Stores the classification results for each SeriesStack:

| Field | Description |
|-------|-------------|
| `base` | Base contrast weighting (T1w, T2w, etc.) |
| `technique` | Pulse sequence technique (MPRAGE, TSE, etc.) |
| `modifier_csv` | Comma-separated modifiers (FLAIR, FatSat, etc.) |
| `construct_csv` | Comma-separated derived types (ADC, FA, etc.) |
| `provenance` | Processing pipeline (SyMRI, SWIRecon, etc.) |
| `acceleration_csv` | Comma-separated acceleration methods |
| `post_contrast` | Contrast agent status (1=post, 0=pre, NULL=unknown) |
| `localizer` | Is this a localizer/scout? |
| `spinal_cord` | Spinal cord involvement flag |
| `directory_type` | BIDS intent (anat, dwi, func, fmap) |
| `manual_review_required` | Needs QC review? |
| `manual_review_reasons_csv` | Why review is needed |

---

## Clinical Data Models

NILS supports comprehensive clinical metadata:

### Disease & DiseaseType

```
Disease (e.g., Multiple Sclerosis)
└── DiseaseType (e.g., RRMS, SPMS, PPMS)
```

### SubjectDisease

Links subjects to diseases with:

- `diagnosis_notes` - Clinical notes
- `family_history` - Family history information
- `onset_event_id` - Reference to disease onset event
- `diagnosis_event_id` - Reference to diagnosis event

### Events

Clinical events with:

- `event_type` (category + name)
- `event_date` and `event_time`
- `notes`

### Clinical Measures

Four measure types based on value type:

- **NumericMeasure** - Numeric values with units (e.g., EDSS score)
- **TextMeasure** - Text values (e.g., medication names)
- **BooleanMeasure** - Yes/No values (e.g., relapse occurred)
- **JsonMeasure** - Complex structured data

Each measure links to a **ClinicalMeasureType** that defines:

- Category and name
- Unit of measurement
- Value type
- Min/max valid values
- Whether it's a primary outcome measure

---

## Cohorts

A **Cohort** is a managed group of subjects with associated imaging data.

| Field | Description |
|-------|-------------|
| `name` | Unique cohort name |
| `source_path` | Root directory containing DICOM files |
| `description` | Text description |
| `owner` | Owner/creator |
| `tags` | Categorization tags |

Cohorts track pipeline progress:

- `status`: idle, in_progress, completed
- `completion_percentage`: 0-100
- `total_subjects`, `total_sessions`, `total_series`

See [Cohort Operations](../cohort/index.md) for details on cohort processing.

---

## Next: Classification

Learn how NILS classifies series using the [Six-Axis Classification System](../classification/index.md).
