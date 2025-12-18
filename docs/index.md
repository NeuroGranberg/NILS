# NILS - Neuroimaging Intelligent Linked System

<p align="center">
  <img src="assets/nils-logo.svg" alt="NILS Logo" width="150">
</p>

<p align="center">
  <strong>A comprehensive system for DICOM classification, sorting, anonymization, and BIDS export</strong>
</p>

<p align="center">
  <em>Developed at <a href="https://ki.se">Karolinska Institutet</a></em><br>
  Department of Clinical Neuroscience, Neuroradiology
</p>

---

## What is NILS?

NILS (Neuroimaging Intelligent Linked System) is a full-stack application designed for research institutions to efficiently manage neuroimaging data. It provides a complete pipeline from raw DICOM ingestion to BIDS-compliant export.

## Core Capabilities

### Six-Axis Classification System

NILS classifies MRI series using six orthogonal axes:

| Axis | Description | Examples |
|------|-------------|----------|
| **Base** | Contrast weighting | T1w, T2w, PD, DWI, BOLD, SWI |
| **Technique** | Pulse sequence family | MPRAGE, TSE, FLASH, EPI, GRASE |
| **Modifier** | Acquisition enhancements | FLAIR, FatSat, MT, IR, PhaseContrast |
| **Construct** | Derived/map type | ADC, FA, MD, T1Map, T2Map, CBF |
| **Provenance** | Processing pipeline | SyMRI, SWIRecon, DTIRecon, RawRecon |
| **Acceleration** | Parallel imaging | GRAPPA, SMS, CAIPIRINHA |

### Data Hierarchy

NILS organizes imaging data in a 4-level hierarchy:

```
Subject (Patient)
└── Study (Imaging Session)
    └── Series (Acquisition)
        └── SeriesStack (Homogeneous Instance Group)
```

**SeriesStack** is a key concept - it represents a group of instances within a series that share identical acquisition parameters. This handles multi-echo, multi-flip-angle, and other complex acquisitions.

## Documentation

- [**Concepts**](concepts/index.md) - Core data models, entities, and terminology
- [**Cohort Operations**](cohort/index.md) - Extraction, Sorting, Anonymization, Export
- [**Classification**](classification/index.md) - The six-axis detection system
- [**QC & Viewer**](qc/index.md) - Quality control and image review

## Quick Start

```bash
# Clone and start
git clone https://github.com/NeuroGranberg/NILS.git
cd NILS
./scripts/manage.sh start

# Access web interface
open http://localhost:5173
```

## Requirements

- Docker & Docker Compose
- 4GB RAM minimum (8GB recommended)
- Modern web browser

## License

MIT License - See [LICENSE](https://github.com/NeuroGranberg/NILS/blob/main/LICENSE)

## Citation

If you use NILS in your research, please cite:

> Chamyani, N. (2025). NILS - Neuroimaging Intelligent Linked System.
> Karolinska Institutet, Department of Clinical Neuroscience.
> [https://github.com/NeuroGranberg/NILS](https://github.com/NeuroGranberg/NILS)
