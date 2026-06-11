# Main Acquisition QC (MASQC)

**Main Acquisition QC** (MASQC) is a cohort-wide review pipeline that identifies the single representative ("main") acquisition per session and axis. When a session contains several competing stacks of the same contrast — a full acquisition, a retake, a partial-volume abort, Dixon outputs — MASQC picks the one that should be tagged as the main acquisition and lets reviewers confirm or override that choice.

---

## Why MASQC?

A single imaging session often produces multiple stacks of the same contrast:

- The technologist repeated a scan after motion (a **retake**)
- A scan was aborted partway and re-run (**partial volume**)
- A Dixon acquisition emits in-phase, opposed-phase, water, and fat as separate stacks
- MP2RAGE emits inversion images plus a uniform image

For many downstream uses you want exactly **one** canonical stack per contrast per session. MASQC automates that selection across the whole cohort, then surfaces the borderline cases for human review. The chosen stack is tagged with a `main_acquisition` marker in the classification cache.

MASQC has **two complementary halves**:

| Half | Purpose |
|------|---------|
| **Auto-pick + heatmap** | Algorithmically pick the main acquisition per session/axis across the cohort, with a green/amber/red heatmap |
| **Session bundle review** | Walk the cohort session-by-session and set main/pre/post roles manually |

Both write the same `main_acquisition` marker.

---

## Automated Session Picking

For the whole cohort, the auto-pick engine computes — **per session, per axis** (T1w and T2w-FLAIR) — which stack(s) should be the main acquisition.

### Sessions Span Studies

A session is keyed by `(subject_id, session_date)`. A single calendar visit can span multiple DICOM studies (e.g. a brain study and a spine study acquired the same day); MASQC groups them as one session so the choice is made across the whole visit.

### Bundle Grouping

Stacks are grouped into **bundles** that represent one acquisition:

1. **Fingerprint grouping** — stacks with the same full acquisition fingerprint (orientation, base, technique, modifiers, TR/TE/TI/FA, contrast) are bundled together.
2. **Dixon family merge** — Dixon outputs (in/opposed/water/fat) of one acquisition are merged into a single family so they stay together.

### Canonical Selection Within a Bundle

Within each bundle the engine picks the canonical stack(s):

- **Dixon families** prefer In-Phase, then Water (the family is dropped if neither is present).
- **MP2RAGE** prefers the denoised uniform image, then the uniform image.
- **Generic bundles** tag all stacks, but **auto-demote** partial-volume / aborted short stacks (slice count well below the bundle maximum).

### Scoring

Each bundle is scored as a weighted sum of eight components — acquisition dimension (2D/3D), technique tier, modifiers, slice count, field of view, cohort share, orientation, and completeness — with a penalty for EPIMix-derived data. All weights and tiers live in a YAML config (`main_qc_weights.yaml`) and can be tuned without code changes.

### Heatmap

Each session/axis cell is bucketed by the winner's score:

| Color | Meaning |
|-------|---------|
| **Green** | High-confidence pick (score ≥ 0.80) |
| **Amber** | Moderate confidence (≥ 0.60) |
| **Red** | Low confidence, or no eligible stack |

The heatmap gives an at-a-glance view of which sessions need attention, plus a `needs_check` count.

---

## Reason Banners

Every auto-pick carries the reasons it may need review, rendered as banners in the QC modal. Review-triggering reasons include:

| Reason | Explanation |
|--------|-------------|
| `no_canonical_construct` | Bundle had only Fat/OutPhase/Phase — nothing taggable |
| `retake` / `retake_dixon_canonical` / `retake_mp2rage` | Multiple candidates where only one main is expected |
| `close_runner_up` | The top two bundles scored within ~5% of each other |
| `unknown_dim` | The winner has no MR acquisition type (2D/3D) |
| `slice_count_outlier` | Winner's slice count is an outlier for its dimension bin |
| `pre_post_twin` | Another strong bundle differs only in contrast status |
| `epimix_fallback` | The winner is EPIMix-derived |
| `rare_technique` | The winning technique is rare in this cohort |
| `dixon_vs_plain` | A Dixon family won but a non-Dixon bundle is close behind |

Informational reasons (shown but **not** review-triggering) include `dropped_short_partial_volume` (a short stack was auto-demoted) and `acknowledged` (a reviewer confirmed the pick).

---

## Session Bundle Review

The manual half lets a reviewer walk the cohort one session at a time.

### Cascading Filter Bar

Seven facets narrow the session list: `directory_type`, `provenance`, `orientation`, `base`, `mr_acquisition_type`, `technique`, and `modifier_csv`. The facets **cascade** — each dropdown only offers values still compatible with the other active filters.

### Multi-Stack Filter

The `only_multi_stack` filter restricts the list to sessions that actually contain a choice — i.e. at least one bundle with more than one stack. This focuses review on sessions where a main-vs-others decision exists.

### Bundles and Suggested Main

For a selected session, stacks are grouped into bundles, each with a human-readable BIDS-style title and a **suggested main** (latest acquisition time, breaking ties by largest slice count).

### Setting Roles

Reviewers set roles per stack:

- **Main** — setting a stack as main clears the `main_acquisition` marker from its sibling stacks in the same bundle (one main per bundle).
- **Pre / Post** — a tri-state contrast toggle on `post_contrast` (pre / post / unknown) that also clears stale `contrast:*` review flags.

---

## Saved Sessions, Display IDs, and Acknowledgement

### Saved Review Sessions

A review can be saved with a name, capturing its filters, current position, progress (which sessions you've seen), display-ID type, and the multi-stack toggle. You can resume exactly where you left off across logins.

### Display IDs

Sessions can be sorted and labeled by an alternative subject identifier (e.g. a study-specific ID like BROMS) instead of the database `subject_code`. The available identifier types are discovered per cohort, and any identifier can be resolved back to the canonical subject code.

### Acknowledgement ("In NILS we trust")

A reviewer can acknowledge an auto-pick for a session/axis. The acknowledgement is stored durably, keyed by `(cohort_id, subject_id, session_date, axis)`, so it **survives a later Apply** that recomputes picks — confirming a pick once means it stays confirmed.

---

## Caching

To keep "next session" instant on large cohorts, MASQC uses two in-process TTL caches (no external dependencies):

- The expensive cohort-wide **session-list** query is cached (60 s TTL) and invalidated immediately for a cohort on any role write.
- The per-cohort alternative-identifier map used by auto-pick is cached (60 s TTL), invalidated on Apply/Restore.

The frontend additionally prefetches neighboring sessions for instant navigation.

---

## API Reference

### Auto-Pick & Heatmap

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/cohorts/{cohort_id}/main-qc` | Get the auto-pick heatmap state |
| POST | `/api/cohorts/{cohort_id}/main-qc/apply` | Run cohort-wide auto-pick and rewrite markers |
| POST | `/api/cohorts/{cohort_id}/main-qc/restore-previous` | Undo the last auto-pick run |
| POST | `/api/cohorts/{cohort_id}/main-qc/session-pick` | Manually override the pick for a session/axis |
| POST | `/api/cohorts/{cohort_id}/main-qc/session-reset` | Reset a session/axis to the algorithm's choice |
| POST | `/api/cohorts/{cohort_id}/main-qc/session-acknowledge` | Acknowledge a session/axis pick |

### Session Review & Saved Sessions

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/cohorts/{cohort_id}/main-acq/filter-options` | Cascading filter-bar options |
| POST | `/api/cohorts/{cohort_id}/main-acq/sessions` | List sessions for a filter state |
| POST | `/api/cohorts/{cohort_id}/main-acq/sessions/{session_index}/bundles` | Bundles for one session |
| GET | `/api/main-acq/resolve-subject?identifier=` | Resolve any identifier to a subject code |
| POST | `/api/main-acq/stacks/{series_stack_id}/role` | Set/clear main/pre/post for a stack |
| GET | `/api/cohorts/{cohort_id}/main-acq/saved-sessions` | List saved review sessions |
| POST | `/api/cohorts/{cohort_id}/main-acq/saved-sessions` | Create a saved review session |
| PATCH | `/api/main-acq/saved-sessions/{session_id}` | Update position/progress/filters/name |
| DELETE | `/api/main-acq/saved-sessions/{session_id}` | Delete a saved session |

---

## Tuning the Scoring

All scoring behavior is configurable in `backend/src/qc/main_qc_weights.yaml` without redeploying:

- Component **weights** (dimension, technique, modifier, slices, FOV, share, orientation, completeness)
- **Provenance penalty** (RawRecon 1.0, EPIMix 0.5)
- **Technique tiers** per axis
- **Border thresholds** (runner-up percentage, pre/post twin threshold, rare-technique share, partial-volume cutoffs, slice-count percentile bounds)
- **Canonical priorities** for Dixon and MP2RAGE families

---

## See Also

- [Quality Control](index.md) — axes QC and the rules engine
- [Body Part QC](body-part.md) — image-based body-part classification
- [Cohort Operations](../cohort/index.md) — where QC fits in the pipeline
