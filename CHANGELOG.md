# Changelog

All notable changes to NILS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
