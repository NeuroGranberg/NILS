# Changelog

All notable changes to NILS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-06-08

### Added

- **Analysis Pipelines** — Register external neuroimaging analysis pipelines (e.g. MRIQC, fMRIPrep, dcm2niix, hd-bet) by repository URL and run them on a cohort subset or an existing BIDS dataset, with results flowing back into NILS
  - Pipeline inventory: add a pipeline by pasting a Git URL; each renders as a card built from its descriptor — work-unit, runtime needs (GPU / FreeSurfer license / TemplateFlow), pinned image digest and repo commit, and version
  - Activated detail page with a configuration form **generated automatically from the pipeline descriptor** — every parameter with its description, type, choices, and default — so a new tool or option needs no NILS UI code
  - Input selection reuses the export flow: pick a stack subset (resolved from a selection manifest) or point at an existing BIDS tree
  - **Local execution via `docker` or `apptainer`** — runs on a single machine with no Prefect server or cluster required; the container runtime is auto-detected
  - Per-run tracking: live status, streamed logs, and a per-unit results table showing which subjects/stacks succeeded or failed and their derivative files
  - Immutable-per-run provenance: each run pins the repository commit and image digest so it is exactly reproducible
  - Descriptors follow the **Boutiques** standard plus a small NILS extension, so existing BIDS-App descriptors are reusable and the configuration form is generated for free

- **Asynchronous Database Backups & Exports** — Database backups and BIDS/subset exports now run as tracked background jobs: the request returns immediately with a job you can monitor and cancel, instead of blocking until completion

### Changed

- **Unified Export** — Whole-cohort export and stack-subset export now share a single export path and `export` job stage; the previous `subset_export` stage is kept as a compatibility alias so existing job history still displays

### Fixed

- **Faster cohort export on large databases** — Reworked the stack-selection query to avoid row explosion, removing export timeouts on large cohorts
- **Container startup reliability** — Fixed several issues that prevented the backend and body-part-QC worker containers from starting on a clean build (notably under Podman): the API package is now correctly included in the installed distribution and the backend launches via its installed entry point instead of rebuilding an editable environment on every start; the worker pins a compatible `transformers` version and declares its `httpx` dependency

## [0.4.0] - 2026-05-26

### Added

- **Body Part Quality Control** — Cohort-scoped QC subsystem that learns per-cohort body-part labels from DICOM thumbnails and protects them across re-runs
  - Two-stage seeding pipeline: keyword-prior candidate selection followed by stratified zero-shot label assignment, producing a balanced training set without manual annotation
  - Stage-and-commit workflow with per-stack pick/destage actions, partial commits, and override conflict detection so reviewers can iterate without blowing away prior work
  - Orientation-aware inference combining axial composition with center-weighted aggregation across sagittal and coronal slices; sagittal portrait spine heuristic flags portrait-aspect sagittal acquisitions as spinal cord
  - Multi-slice embedding averaging with automated hyperparameter tuning during training; configurable classifiers with optional PCA dimensionality reduction
  - Global body-part model registry: train once, reuse across cohorts; per-cohort model selection from the registry
  - Durable label protection — committed body-part labels persist through a profile registry and survive reclassification runs
  - Cohort-scoped reset action to clear QC state and start over
  - Expanded body part keyword set; middle-slice thumbnail URL included in cohort model metadata for quick visual sanity-checks

- **Main Acquisition QC (MASQC)** — Cohort-wide review pipeline for the single representative ("main") acquisition per session
  - Automated session picking with bundle-based grouping and heatmap visualization across the cohort
  - Session-based bundle review with automated classification tagging applied on commit
  - Persistent saved sessions with custom display IDs (configurable per ID type), adjustable grid layouts, and per-session acknowledgement that survives across logins
  - Multi-stack filter to focus on sessions containing multiple competing stacks
  - In-process TTL cache for session lists plus frontend prefetching of neighbor sessions for instant navigation
  - Categorized reason banners in the QC modal explain why each candidate was picked or skipped

- **Cohort-Level Classification Overrides** — Per-cohort keyword override service and management UI lets sites tune detection without touching global YAML configs

- **STAGE Classification Branch** — Dedicated provenance branch for STrategically Acquired Gradient Echo data, routing STAGE outputs through specialized base-contrast and construct logic

- **SWI Branch — QSM and R2\* outputs** — Quantitative susceptibility maps and R2\* maps now classified as first-class SWI outputs with documentation

- **Recon-Variant Constructs** — Three new constructs track scanner reconstruction variants
  - `ORIG` (GE raw/unprocessed) and `Filtered` (GE noise-filtered) detected via text-search prefixes
  - `ND` (Siemens no-distortion-correction) detected from the `ND` ImageType token

- **Multi-Frame DICOM Rendering** — Backend frame-specific extraction and frontend rendering for Enhanced MR / multi-frame DICOMs, with optimized scrolling for large frame counts

- **Enhanced MR Metadata Extraction** — Extractor now reads `SharedFunctionalGroupsSequence` and `PerFrameFunctionalGroupsSequence`, recovering physics parameters (TE/TR/TI/FA) for Enhanced MR Image Storage objects that previously came through with empty metadata

- **Intent-Scoped Reference Databases** — Step 4 gap-filling now isolates the physics-similarity reference pool per intent, preventing cross-intent matches (e.g., a FLAIR-like stack will no longer borrow parameters from a perfusion reference) during stuck-stack recovery

- **Session-Aware Rescue** — Sessions that contain only `ORIGINAL\SECONDARY` images (no `ORIGINAL\PRIMARY`) are no longer dropped wholesale; secondary stacks are rescued so the session still gets classified

- **`nils extract` CLI** — New top-level command replacing `metadata ingest` under a stable CLI contract; flag surface shrunk from 22 to ~6 by moving knobs into a config file, with extraction logic relocated into a typed `ExtractionConfig`

- **Detection Vocabulary Expansion**
  - `t2w-cube` token mapping for GE T2 CUBE volumetric acquisitions
  - `t1`, `t2`, and `asset cal` labels in detection configs (catches scanner calibration scans)
  - `bffe` keyword support for balanced-FFE sequences
  - Additional positive and negative keywords for contrast detection

- **`slice_thickness_mm`** — Added to the metadata schema and surfaced in QC modal display

- **Acquisition Parameters in BIDS Export** — Stack export filenames now include configurable acquisition-parameter naming, with improved collision handling options

### Changed

- **Default Backend Port** — Default backend port changed from `8000` to `8010` to allow coexistence with other NILS services on the same host. Existing `docker-compose.yml` overrides and client URLs hitting `:8000` should be updated.

- **Cohort Scoping** — Standardized on `dicom_origin_cohort` across QC and classification services; removed the subject-cohort mapping dependency so cohort scoping is consistent end-to-end

- **Session Identification** — Migrated session identity from `study_id` to `(subject_id, session_date)` across backend and frontend QC services, eliminating duplicate-session artifacts when a subject has multiple studies on the same date

- **Sequence Taxonomy Cleanup**
  - Siemens MEDIC sequences decoupled from generic multi-echo SE detection (MEDIC is now its own concept with explicit documentation)
  - Removed generic steady-state detection rules in favor of more specific keyword-based detection

- **MTw Contrast Priority** — Magnetization-transfer-weighted contrast detection priority adjusted so MTw isn't shadowed by other modifiers

- **Frontend QC Architecture**
  - Stack image rendering extracted into a reusable `StackImagePane` with caching for cohort QC data
  - Legacy heatmap components replaced with a modular `AxisHeatmap` + `SubjectStrip` pair
  - Thumbnail rendering backed by `lru_cache` with longer cache-control duration for faster repeat views

- **Available ID Types** — Session response now includes the list of available identifier types so frontend can offer them as display options without an extra round-trip

### Fixed

- **IR-TSE and MPRAGE Detection** — Vendor-specific logic added for Philips and GE scanners; previously Philips IR-TSE and GE MPRAGE variants were misclassified due to differing keyword conventions

- **Body Part Label Sanitization** — Labels normalized to lowercase during sanitization so case-only variants (e.g., `Brain` vs `brain`) no longer create duplicate buckets

- **SubjectStrip Label Truncation** — Width calculation fixed to prevent subject labels from being clipped; theme-consistent styling applied across the strip

## [0.3.0] - 2026-03-20

### Added

- **Classification Engine** — Complete rewrite of MRI sequence classification as a modular, YAML-driven 10-stage pipeline
  - Six orthogonal detection axes (base contrast, technique, modifier, construct, provenance, acceleration) each backed by its own YAML config and detector class
  - Semantic text normalizer that tokenizes DICOM descriptions — handles `*` → `star`, vendor-specific abbreviations (`pha` → `phase`, `mag` → `magnitude`), and context-aware replacements (`mt` only maps to magnetization-transfer when not inside "metric")
  - Branch-based routing: provenance detection runs first and routes multi-output sequences (SWI, SyMRI, EPIMix/NeuroMix, MP2RAGE) into specialized sub-pipelines that override only base contrast and construct — all other axes still run the standard detectors
  - SWI branch distinguishes 6 output types (QSM, MinIP, MIP, Phase, SWI Processed, Magnitude) using ImageType flags and text keywords with per-type confidence scores
  - SyMRI branch classifies 16+ outputs across quantitative maps (T1map, T2map, PDmap, Myelin, B1map), synthetic weighted images (SyntheticT1w, SyntheticFLAIR, etc.), and raw source components
  - EPIMix/NeuroMix branch handles 11 output types with physics-based fallback: uses TI thresholds (T1-FLAIR vs T2-FLAIR), TE ranges (T2*-w vs T2-w), and readout type (EPI vs SSFSE) when text keywords are ambiguous
  - Technique detector covers 30+ sequences across SE, GRE, EPI, and MIXED physics families — detection via exclusive flags, combination logic (AND of multiple flags), and keyword fallback
  - Modifier detector with mutual exclusion groups: IR contrasts pick highest priority (FLAIR > STIR > DIR > PSIR > IR), trajectory picks one (Radial > Spiral), independent modifiers always additive (FatSat, WaterExcitation, MT)
  - Construct detector additively collects derived maps — diffusion (ADC, FA, MD, Trace), perfusion (CBF, CBV, MTT), quantitative (T1map, T2map), SWI (QSM, Phase), projection (MIP, MinIP, MPR), Dixon (Water, Fat, InPhase, OutPhase)
  - Acceleration detector with bounded regex to avoid false positives — e.g. `\bmb\d` matches "mb2" but not "combat"
  - Body part detector classifying brain, spine, neck, and brain-neck from DICOM keywords, used for BIDS directory naming (SC_ prefix for spinal cord)
  - Intent synthesis maps detected axes to BIDS directory types (anat/dwi/func/fmap/perf/misc) using a priority chain: provenance → construct → functional keywords → base+modifier → fallback
  - 55+ unified boolean flags extracted from DICOM headers, scanner-agnostic
  - Confidence tracking per axis — stacks below 0.6 are automatically flagged for manual review

- **Sorting Pipeline Rebuild** — Four-step pipeline with independent execution, typed handovers, and real-time progress streaming
  - Step 1 (Checkup): validates subjects/studies, repairs missing study dates from 4 fallback sources (series_date → acquisition_date → content_date → UID date extraction), filters by modality (MR/CT/PET), supports incremental mode (skip already-classified series)
  - Step 2 (Stack Fingerprint): single JOIN query loads all stack data, Polars vectorized transforms compute FOV, orientation confidence, text/contrast search blobs, manufacturer normalization; bulk COPY + UPSERT in 50K-row batches — 10-100x faster than v0.2 row-by-row approach
  - Step 3 (Classification): runs the classification engine on each fingerprint in batches of 1000, bulk upserts results to `series_classification_cache`
  - Step 4 (Completion): 5-phase post-processing — normalizes field strength (handles Gauss scale, ±tolerance), flags low orientation confidence (<0.85), fills missing 2D/3D acquisition type from scan options/text/technique inference, fills missing base/technique via physics-similarity matching against all previously classified stacks in the database (binned by TR/TE/TI/FA/slice count), re-routes newly-detected SWI through the SWI branch, re-synthesizes intent for stacks stuck in "misc"
  - Handover mechanism: each step produces a typed dataclass persisted to `nils_dataset_pipeline_step` — stores IDs (not full data) so downstream steps re-query fresh state
  - Step-wise execution: any step can run independently by loading the previous step's persisted handover, enabling re-runs with different config without starting from scratch
  - Preview mode: run steps without committing results to database
  - SSE progress streaming with rolling 100-line log buffer displayed in frontend

- **Quality Control Pipeline** — Full QC review system with draft-based workflow and DICOM viewer
  - Axes QC Page: image-centric view with Cornerstone.js WebGL rendering, HUD overlays showing acquisition parameters (TE/TR/TI/FA/ImageType) and current classification, keyboard navigation (arrow keys to browse stacks, number keys to select correction options)
  - QC Viewer Page: three-level hierarchy (subjects → sessions → stacks) with searchable subject list, sessions grouped by study date, stacks grouped by intent with provenance sub-grouping (SyMRI in purple, SWI in green, EPIMix in orange)
  - Draft-based workflow: changes saved to app_db as drafts, not touching metadata_db until user confirms — discard reverts everything, confirm pushes all drafts atomically and clears manual_review_required flag
  - Rules engine with 9 configurable rules: TechniqueFamilyMismatch (validates physics family), BrainAspectRatio (flags elongated FOV on brain scans), SpineAspectRatio, LocalizerSliceCount (>20 slices suspicious), ProvenanceMismatch (SWI constructs must have SWI provenance), ContrastUndetermined (T1w without known gadolinium status), BaseMissing
  - 5 flag severities: missing (red), conflict (orange), low_confidence (yellow), ambiguous (purple), review (gray) — priority scoring determines QC item ordering
  - Dynamic filtering by axis and flag type, with filters only showing options that have items

- **BIDS Export** — Background job processing with cross-cohort resolution and field strength filtering
  - Stack naming includes body part prefix (SC_, Neck, BrainNeck), orientation (Ax/Cor/Sag), base contrast, 2D/3D, modifiers, technique, acceleration, constructs, and contrast suffix (_CE)
  - DWI stacks self-describe: `Ax_DWI_EPI_b1000_AP_32dir` includes b-value, phase encoding direction, and number of gradient directions extracted from vendor-private DICOM tags (Siemens, GE, Philips)
  - Multi-stack series handling: echo suffixes (_e1, _e2), TI suffixes (_ti1, _ti2), plus collision resolution with numbered suffixes only when names actually collide
  - Cross-cohort DICOM path resolution: when a subject's files live in a different cohort's dcm-raw folder, the exporter falls back through all known cohort paths
  - SQL-level field strength filtering (0.5/1.0/1.5/3.0/7.0T) — avoids loading irrelevant stacks from large databases
  - Provenance filtering with allow-list/block-list: include specific provenances (SyMRI, SWIRecon) or exclude others (ProjectionDerived)
  - `completed_with_warnings` job status — exports that succeed but had skipped files or NIfTI conversion errors are flagged separately from clean completions
  - Standalone export system: define a manifest (CSV/JSON) of subject/session/stack selections, resolve to stack IDs, run as a named export job independent of any cohort
  - `sub-` prefix guard prevents `sub-sub-XXXX` directory names when PatientID already contains the prefix
  - Process pool uses `spawn` context (not `fork`) to avoid virtual memory exhaustion on systems with strict overcommit

- **Data Import System** — CSV import for 10+ entities with preview/apply pattern
  - Subjects, cohorts, subject-cohort memberships, subject identifiers (alternative IDs)
  - Events with observation type registry and imaging session backfill
  - Diseases, disease types, subject diseases, subject disease types — full longitudinal clinical metadata
  - Every import has a preview endpoint (dry-run with sample rows + validation errors) and an apply endpoint (commits to database)
  - Validation: column existence, type coercion, required field enforcement, foreign key checking, duplicate handling policy (skip/update/error)

- **Database Management UI** — Tabbed interface organizing tables by domain (subjects, events, clinical measures, imaging, system)
  - Identifier type creation with validation
  - Application and metadata backup/restore with optional user notes
  - 3-phase parallel restore: schema (sequential) → data (parallel, 4 workers) → indexes/constraints (parallel)
  - Post-restore migrations automatically apply schema changes from newer versions to older backups

- **Authentication** — Token-based middleware with login page and asset caching
- **Podman Support** — `--podman` flag in `manage.sh` with `:Z` SELinux labels on all volume mounts
- **Docker Health Checks** — Startup ordering with health check dependencies to prevent connection errors

### Changed

- **Backend Architecture** — `server.py` refactored from ~2000 lines to ~120 lines
  - 12+ route modules (cohorts, imports, backups, jobs, qc, export, csv, metadata, database, system, etc.)
  - `api/schemas` renamed to `api/models`, utility functions extracted to `api/utils/`
  - GZip middleware added (60-80% response size reduction)
- **Stack Creation** — Stacks now created during extraction via per-instance signature hashing, eliminating the 30-minute post-extraction UPDATE bottleneck
  - Signature computed from series UID + modality-specific fields (echo time, inversion time, flip angle, b-value, orientation, image type)
  - Cache tracks signature → stack_id; new signature triggers immediate stack row creation
- **Extraction Performance** — Adaptive batch sizing based on execution timing (target: 1000ms/batch), series-level processing with configurable worker pool, comprehensive MRI/CT/PET field mappings
- **Database** — Date and time columns migrated from text to native PostgreSQL types; new indexes on frequently queried columns; API response caching
- **Anonymization** — V2 pipeline with compression, audit resume capability, multiprocessing with streaming, leaf event management
- **Frontend** — Complete redesign with NILS branding, dark-theme-first flat design, Mantine UI components; cohort detail page with pipeline stage stepper and run button loading state; job center with cohort-centric view and progress animations
- **Observation Types** — Event types and clinical measure types unified into a single observation type taxonomy
- **SWI/Provenance Reclassification** — SWI reclassified as provenance (not technique), Radial/Spiral reclassified as modifiers (not standalone techniques)
- **Semantic Token Map** — Expanded to v1.2.0 with vendor-specific mappings for Swedish/Scandinavian protocols (`gd` suffixes, `da-fl` for FLAIR, `direkt`, `syntetisk`)

### Fixed

- **Series time formatting** — `series_time` serialized as ISO string, preventing sorting/export crashes when datetime.time objects were passed raw
- **Extraction writer crash handling** — Writer task crash now surfaces immediately instead of hanging forever on an undrainable queue
- **Missing measurement values** — Event import preview handles NULL measurements gracefully
- **Semantic normalizer** — Correctly tokenizes `+`/`-` characters (contrast notation like `+Gd`) and improves `mp2rage` keyword matching
- **Diffusion b-values** — Upper bound validation filters garbage data from vendor-private tags
- **Localizer detection priority** — Scout MPR reformats correctly classified before other detectors claim them
- **SWI classification** — Robust ImageType-based detection prevents technique override from misclassifying SWI outputs
- **Bulk classification OOM** — Batched upserts prevent PostgreSQL out-of-memory on large cohorts (450K+ stacks)
- **Study date format** — Hyphens removed for consistent BIDS session naming (`ses-20250315` not `ses-2025-03-15`)
- **PostgreSQL cast syntax** — `CAST()` used instead of `::` for migration compatibility
- **Migration transaction nesting** — Per-index transactions with column validation prevent partial migration failures
- **Warning handling** — Cohort path resolution no longer raises on non-critical warnings
- **CHOKIDAR polling** — Enabled in Docker to resolve inotify issues causing phantom file change detection

## [0.2.1] - 2025-12-29

### Fixed

- **BIDS Export Memory Error**: Fixed `[Errno 12] Cannot allocate memory` when running NIfTI conversion
  - Affected systems with strict memory overcommit (`vm.overcommit_memory=2`)
  - Now uses `spawn` instead of `fork` for process pool to avoid virtual memory exhaustion
- **Database Restore Failures**: Fixed `pg_restore` failing due to foreign key constraint errors
  - Tables are now dropped in correct dependency order before restore

### Changed

- **Cohort Detail API Performance**: Response payload reduced by ~90%
  - Job history now uses slim serialization (full details available via `/jobs/{id}`)
  - Metrics cache extended from 30 seconds to 2 minutes
- **Frontend Code Organization**: Extracted BIDS configuration into dedicated component
  - Centralized status colors and configuration
  - Added cohort prefetching on hover for faster navigation

## [0.2.0] - 2025-12-25

### Added

- **Extraction Retry with Exponential Backoff**: Transient database errors (OOM, timeouts) now trigger automatic retry
  - Retries indefinitely until all data is written - never skips data
  - Adaptive batch size reduction during memory pressure
  - Initial delay of 2s, max delay capped at 2 minutes
- **Periodic Cache Pruning**: In-memory lookup caches are pruned during long-running extractions
  - Prevents unbounded memory growth over multi-day extractions (previously could reach several GB)
  - Prunes after every 100 subjects processed
- **Orphaned Job Recovery on Startup**: Jobs that were running when backend crashed/restarted are now marked as failed
  - Clear error message explaining the interruption and how to resume
  - Enables resume from where extraction left off
- **Metrics Caching**: Cohort metrics cached for 30 seconds to avoid repeated expensive COUNT queries
  - Fast approximate counts using PostgreSQL statistics for instant response
  - Cache invalidation after extraction completes
- **Parents-First Write Pattern**: New insertion strategy that prevents orphan database records
  - Pre-filters duplicates before creating parent records (subject/study/series)
  - Eliminates dead rows from PostgreSQL MVCC overhead (~50% storage savings on large extractions)
  - Comprehensive test suite validates no orphan records are created
- **Database Foreign Key Constraints**: Added explicit FK constraints with CASCADE delete
  - Ensures referential integrity across subject → study → series → instance hierarchy
- **Frontend Query Garbage Collection**: Unused cached queries now garbage collected after 5 minutes

### Fixed

- **PostgreSQL Out-of-Memory During Large Extractions** (30M+ instances)
  - Reduced work memory from 256MB to 32MB per query
  - Disabled parallel query workers during extraction
  - Added 48GB memory limit to metadata database container
  - Increased shared memory allocation to 4GB
- **Memory Growth in Extraction Writer**
  - Eliminated reverse lookup cache that could grow to ~850MB for large cohorts
  - Stack queries now use efficient JOIN instead of in-memory lookup
- **Frontend Memory Growth**
  - Removed aggressive polling on cohorts list (now manual refresh)
  - Reduced job list polling from 5s to 15s
  - Disabled automatic polling on administrative pages (backups, database info)
  - Disabled polling on health/readiness endpoints
- **Cohort Detail API Performance**: Metrics now fetched once and reused for all job history entries
- **Modality Details Conflict Handling**: Fixed edge case where series processed after rollback could fail

### Changed

- **PostgreSQL Configuration** optimized for large extraction workloads
  - Shared buffers: 2GB → 4GB
  - Work memory: 256MB → 32MB (conservative for concurrent writes)
  - Effective cache size: 4GB → 32GB
  - Added connection limit of 50
  - Added query timeout of 120s to kill runaway queries
  - Added idle transaction timeout of 5 minutes
  - Added slow query logging (>10s)
- **Frontend Independence**: Frontend container no longer waits for backend to be healthy
  - Prevents frontend restarts from interrupting long-running backend extraction jobs
  - Frontend gracefully handles backend unavailability
- **Production Build Optimization**: Removes debugger statements and console.log in production builds

## [0.1.0] - 2025-12-18

### Added

- Initial release of NILS - Neuroimaging Intelligent Linked System
- **DICOM Classification System**: Rule-based classification with YAML configuration
  - Base sequence detection (T1w, T2w, FLAIR, DWI, etc.)
  - Technique detection (acceleration, contrast, orientation)
  - Special case handling (EPIMix, SWI, SyMRI, Dixon, MP2RAGE)
- **Sorting Pipeline**: Automated DICOM organization and file management
- **Pseudo-anonymization**: Secure patient data de-identification
- **Metadata Extraction**: DICOM tag extraction and CSV/Excel import
- **BIDS Export**: Brain Imaging Data Structure compliant export
- **Quality Control**: Visual QC workflow with DICOM viewer integration
- **Web Interface**: React-based UI with dark theme
  - Dashboard overview
  - Database browser
  - Cohort management
  - Job monitoring
- **Docker Compose Deployment**: Containerized full-stack application
- **Dual Database System**: Separate application and metadata PostgreSQL databases
