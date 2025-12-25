# Changelog

All notable changes to NILS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
