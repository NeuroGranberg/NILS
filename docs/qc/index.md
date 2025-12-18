# Quality Control & Viewer

NILS includes a comprehensive QC system for reviewing and correcting classification results, featuring automatic flagging, rules-based validation, and an integrated DICOM viewer.

---

## QC System Overview

The Quality Control system provides:

1. **Automatic Flagging** - Series requiring review are flagged during classification
2. **Priority Scoring** - Issues ranked by severity for efficient workflow
3. **Rules Engine** - Configurable validation rules detect inconsistencies
4. **Draft Pattern** - Non-destructive edits until explicitly confirmed
5. **Integrated Viewer** - WebGL-accelerated DICOM viewing with metadata overlays

---

## Flagging System

### How Flagging Works

During classification, NILS automatically flags series when:

1. **Detection confidence is low** - Detector uncertain about result
2. **Conflicting signals exist** - Multiple detectors disagree
3. **Values are missing** - Expected classification absent
4. **Ambiguous interpretation** - Multiple valid interpretations

### Flag Types

| Flag Type | Color | Icon | Description |
|-----------|-------|------|-------------|
| `missing` | Red | `?` | Value should be present but isn't |
| `conflict` | Orange | `⚠` | Conflicting signals from detectors |
| `low_confidence` | Yellow | `↓` | Detection confidence below threshold |
| `ambiguous` | Purple | `❓` | Multiple equally valid interpretations |
| `review` | Gray | `👁` | General flag for manual review |

### Flag Format

Flags are stored as comma-separated reason codes:

```
{axis}:{flag_type}
```

**Examples:**
- `base:missing` - Base contrast not detected
- `technique:conflict` - Technique detectors disagree
- `provenance:low_confidence` - Provenance detection uncertain
- `body_part:ambiguous` - Brain vs spine unclear

### Database Storage

Flags are stored in `series_classification_cache`:

| Field | Type | Description |
|-------|------|-------------|
| `manual_review_required` | Integer (0/1) | Binary flag indicating review needed |
| `manual_review_reasons_csv` | Text | Comma-separated flag codes |

---

## Priority Scoring

Items are prioritized for efficient review workflow:

### Priority Calculation

```python
priority = 0
if "low_confidence" in review_reasons: priority += 1
if "missing" in review_reasons: priority += 2
if "ambiguous" in review_reasons: priority += 2
if "conflict" in review_reasons: priority += 3
```

### Priority Interpretation

| Priority | Meaning | Action |
|----------|---------|--------|
| 3+ | Critical | Review immediately (conflicts) |
| 2 | High | Missing or ambiguous values |
| 1 | Medium | Low confidence detection |
| 0 | Low | General review flag |

---

## Rules Engine

The rules engine provides configurable validation that evaluates classification results against expected patterns.

### Rule Severity Levels

| Level | Meaning | Action Required |
|-------|---------|-----------------|
| **ERROR** | Definite mistake | Must be fixed |
| **WARNING** | Likely issue | Should review |
| **INFO** | Informational | May not need action |

### Rule Categories

| Category | Purpose |
|----------|---------|
| `base` | Validate base contrast classification |
| `technique` | Validate technique assignment |
| `provenance` | Validate provenance-construct consistency |
| `body_part` | Validate anatomy classification |
| `contrast` | Validate contrast agent status |

### Built-in Rules

#### Technique Rules

**TechniqueFamilyMismatchRule** (ERROR)
- Validates technique matches expected echo family
- SE techniques (TSE, SPACE, HASTE) should not have GRE constructs
- SWIRecon provenance expects GRE family

**TechniqueMissingRule** (WARNING)
- Flags when base is classified but technique is missing
- Skips localizers and special provenances (SyMRI, SWI)

#### Body Part Rules

**BrainAspectRatioRule** (WARNING)
- Brain scans should have ~1:1 aspect ratio (0.7-1.4)
- Elongated ratios (>1.4) suggest spine misclassification

**SpineAspectRatioRule** (WARNING)
- Spine scans should have elongated ratio (>1.3)
- Near-square ratio suggests brain, not spine

**LocalizerSliceCountRule** (WARNING)
- Localizers should have <20 slices
- High slice count suggests full acquisition misclassified

**NonLocalizerLowSliceCountRule** (INFO)
- Anatomical scans should have >10 slices
- Low count may indicate misclassified localizer

#### Provenance Rules

**ProvenanceMismatchRule** (WARNING)
- Validates provenance matches expected constructs
- SWIRecon → SWI, QSM, Phase, Magnitude
- DTIRecon → ADC, FA, MD, Trace
- SyMRI → T1map, T2map, PDmap, Myelin
- PerfusionRecon → CBF, CBV, MTT, Tmax

#### Contrast Rules

**ContrastUndeterminedRule** (INFO)
- T1w anatomical scans should have known contrast status
- Unknown pre/post gadolinium status flagged

#### Base Rules

**BaseMissingRule** (WARNING)
- Anatomical scans should have base contrast
- Skips derived maps and special provenances

### Rule Context

Rules evaluate against a context containing:

```python
@dataclass
class RuleContext:
    # Classification fields
    base: Optional[str]
    technique: Optional[str]
    provenance: Optional[str]
    modifier_csv: Optional[str]
    construct_csv: Optional[str]
    directory_type: Optional[str]
    post_contrast: Optional[int]
    localizer: Optional[int]
    spinal_cord: Optional[int]

    # Geometry fields
    aspect_ratio: Optional[float]
    fov_x_mm: Optional[float]
    fov_y_mm: Optional[float]
    slices_count: Optional[int]

    # Series info
    series_description: Optional[str]
    modality: Optional[str]
```

---

## Review Categories

Items are categorized for focused review:

| Category | Description | What to Check |
|----------|-------------|---------------|
| `base` | Base contrast weighting | Is T1w/T2w/FLAIR correct? |
| `provenance` | Processing pipeline | Is SyMRI/SWI detection correct? |
| `technique` | Pulse sequence family | Is MPRAGE/TSE/EPI correct? |
| `body_part` | Anatomical region | Spinal cord vs brain? |
| `contrast` | Pre/post contrast | Contrast agent status? |
| `modifier` | Acquisition modifiers | FLAIR/FatSat detection? |
| `construct` | Derived maps | ADC/FA map detection? |
| `axes` | All axes combined | Full classification review |

---

## QC Workflow

### 1. Session Creation

Start a QC session for a cohort:

```
User selects cohort → API creates QCSession
→ Query metadata_db for manual_review_required = 1
→ Parse manual_review_reasons_csv by axis
→ Create QCItem records with priority scores
```

### 2. Filter and Navigate

Focus on specific issues using filters:

- **By Axis**: base, technique, modifier, provenance, construct
- **By Flag Type**: missing, conflict, low_confidence, ambiguous
- **Sorted by**: subject_code, study_date, field_strength, manufacturer

### 3. Review Item

For each item:

1. **View DICOM** - Examine image with metadata overlays
2. **See current classification** - Check base, technique, etc.
3. **Review flags** - Understand why item was flagged
4. **Check rule violations** - See what rules are violated

### 4. Make Corrections (Draft Pattern)

Edits are saved as **drafts** in the application database:

```
User selects axis → picks new value
→ API: PATCH /api/qc/cohorts/{id}/axes/items/{stack_id}
→ Creates QCDraftChange in app_db (NOT metadata_db)
→ UI immediately shows draft status
```

**Key Benefit**: Changes are reversible until explicitly confirmed.

### 5. Confirm or Discard

**Confirm** - Push all drafts to metadata database:
```
User clicks "Submit"
→ API: POST /api/qc/cohorts/{id}/axes/confirm
→ For each draft: UPDATE series_classification_cache
→ Clear manual_review_required flag
→ Delete draft records
```

**Discard** - Revert all pending changes:
```
User clicks "Discard"
→ API: POST /api/qc/cohorts/{id}/axes/discard
→ Delete all draft records
→ No changes to metadata_db
```

### 6. Complete Session

When all items reviewed:
- Session status → `completed`
- Corrections persisted in metadata_db
- Re-export to apply corrections to output

---

## DICOM Viewer

### Features

The integrated viewer supports visual QC:

| Feature | Description |
|---------|-------------|
| **Window/Level** | Adjust contrast and brightness |
| **Zoom/Pan** | Navigate within image |
| **Scroll** | Navigate through slices |
| **Metadata Overlays** | Semi-transparent HUD on image |

### HUD Overlays

The viewer displays acquisition info directly on the image:

**Acquisition HUD (Top-left)**
- Modality and acquisition type
- Timing parameters: TE, TR, TI, FA
- ImageType tokens

**Sequence HUD (Bottom-left)**
- Sequence name and protocol
- Scanning sequence
- Scan options

**Classification HUD**
- Current axis values
- Flagged axes with color-coded badges

**FOV HUD**
- Field of view dimensions
- Aspect ratio

### Badge Colors

| Color | Meaning |
|-------|---------|
| Red | Missing value |
| Orange | Conflict detected |
| Yellow | Low confidence |
| Purple | Ambiguous |
| Gray | General review |
| Green | Value present (no issues) |

### Viewer Technology

Built on **Cornerstone.js**:
- WebGL-accelerated rendering
- Supports common DICOM transfer syntaxes
- Runs entirely in browser
- Fallback to server-side PNG rendering

---

## QC Data Models

### QCSession

Tracks overall QC progress for a cohort:

| Field | Type | Description |
|-------|------|-------------|
| `cohort_id` | Integer | Associated cohort |
| `status` | Enum | pending, in_progress, completed, abandoned |
| `total_items` | Integer | Total items requiring review |
| `reviewed_items` | Integer | Items reviewed so far |
| `confirmed_items` | Integer | Items confirmed correct |
| `created_at` | Timestamp | Session creation time |
| `started_at` | Timestamp | When review began |
| `completed_at` | Timestamp | When review finished |

### QCItem

Individual item requiring review:

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | Integer | Parent session |
| `series_instance_uid` | String | Series identifier |
| `stack_index` | Integer | Stack within series |
| `category` | Enum | Review category (base, technique, etc.) |
| `status` | Enum | pending, reviewed, confirmed, skipped |
| `priority` | Integer | Prioritization score |
| `review_reasons_csv` | Text | Flags triggering review |

### QCDraftChange

Pending corrections (draft pattern):

| Field | Type | Description |
|-------|------|-------------|
| `item_id` | Integer | Parent QCItem |
| `field_name` | String | Column being changed |
| `original_value` | Text | Value before change |
| `new_value` | Text | Proposed new value |
| `change_reason` | Text | Why change was made |

---

## API Reference

### Session Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/qc/cohorts/{id}/axes/session` | GET | Get or create axes session |
| `/api/qc/sessions/{id}` | GET | Get session details |
| `/api/qc/sessions/{id}/summary` | GET | Stats by category/status |
| `/api/qc/sessions/{id}/refresh` | POST | Reload from metadata DB |

### Item Operations

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/qc/cohorts/{id}/axes/items` | GET | Paginated items with filters |
| `/api/qc/axes/items/{stack_id}` | GET | Single stack details |
| `/api/qc/cohorts/{id}/axes/items/{stack_id}` | PATCH | Save axis draft |

### Confirmation

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/qc/cohorts/{id}/axes/confirm` | POST | Confirm all drafts |
| `/api/qc/cohorts/{id}/axes/discard` | POST | Discard all drafts |

### Options

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/qc/axes/options` | GET | Available values per axis |
| `/api/qc/cohorts/{id}/axes/filters` | GET | Available filters for cohort |

### DICOM Viewer

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/qc/dicom/{series_uid}/metadata` | GET | Cornerstone.js metadata |
| `/api/qc/dicom/{series_uid}/instances` | GET | Instance IDs for navigation |
| `/api/qc/dicom/{series_uid}/thumbnail` | GET | Middle slice thumbnail |
| `/api/qc/dicom/image/{instance_id}` | GET | PNG rendering |
| `/api/qc/dicom/wado` | GET | WADO-URI endpoint |

---

## Best Practices

### Before QC

1. **Complete sorting** - Ensure all series classified
2. **Review summary** - Check overall classification distribution
3. **Identify patterns** - Common misclassifications?

### During QC

1. **Use filters** - Focus on one category at a time
2. **Trust the system** - Most classifications are correct
3. **Start with conflicts** - Highest priority issues first
4. **Use keyboard navigation** - Arrow keys for efficiency

### After QC

1. **Confirm changes** - Push drafts to metadata DB
2. **Re-export** - Apply corrections to output
3. **Report issues** - Note recurring problems
4. **Refine rules** - Update YAML detection rules if needed

---

## Troubleshooting

### "No items to review"

- Check cohort has been sorted
- Verify `manual_review_required` flags exist
- Check filter settings (axis, flag_type)

### Viewer Not Loading

- Check browser supports WebGL
- Verify DICOM file accessibility
- Check browser console for errors
- Try PNG fallback mode

### Corrections Not Saving

- Ensure you clicked "Submit" to confirm
- Check API response for errors
- Verify database connection
- Drafts are only in app_db until confirmed

### Rule Violations Not Showing

- Check rules are enabled in rules_engine
- Verify classification data is complete
- Check rule category matches review category

---

## Extending the Rules Engine

To add custom validation rules:

```python
from backend.src.qc.rules_engine import QCRule, RuleSeverity, RuleCategory

class MyCustomRule(QCRule):
    rule_id = "my_custom_rule"
    category = RuleCategory.BASE
    name = "My Custom Rule"
    description = "Validates something specific"
    severity = RuleSeverity.WARNING

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        if some_condition(ctx):
            return self._create_violation(
                "Rule violated because...",
                {"field": ctx.some_field}
            )
        return None

# Register in rules_engine
rules_engine.register_rule(MyCustomRule())
```

---

## See Also

- [Classification System](../classification/index.md) - How classification works
- [Branches](../classification/branches/index.md) - Multi-output classification
- [Sorting Workflow](../cohort/sorting.md) - Pre-QC workflow
