# NILS - Neuroimaging Intelligent Linked System

<p align="center">
  <img src="frontend/public/nils-icon.svg" alt="NILS Logo" width="120">
</p>

<p align="center">
  <strong>A comprehensive system for DICOM classification, sorting, anonymization, and BIDS export</strong>
</p>

<p align="center">
  <b>Developed at <a href="https://ki.se">Karolinska Institutet</a></b><br>
  Department of Clinical Neuroscience, Neuroradiology
</p>

<p align="center">
  <a href="https://neurogranberg.github.io/NILS/">
    <img src="https://img.shields.io/badge/docs-neurogranberg.github.io-blue" alt="Documentation">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
  </a>
  <a href="CHANGELOG.md">
    <img src="https://img.shields.io/badge/version-0.1.0-green.svg" alt="Version">
  </a>
</p>

---

## Features

### Six-Axis Classification System

NILS classifies MRI series using six orthogonal axes:

| Axis | Description | Examples |
|------|-------------|----------|
| **Base** | Contrast weighting | T1w, T2w, PD, DWI, BOLD, SWI |
| **Technique** | Pulse sequence family | MPRAGE, TSE, FLASH, EPI |
| **Modifier** | Acquisition enhancements | FLAIR, FatSat, MT, IR |
| **Construct** | Derived/map type | ADC, FA, T1Map, QSM |
| **Provenance** | Processing pipeline | SyMRI, SWIRecon, DTIRecon |
| **Acceleration** | Parallel imaging | GRAPPA, SMS, CAIPIRINHA |

### Complete Pipeline

- **Extraction** - Import DICOM metadata into database
- **Sorting** - Classify all series with 4-step pipeline
- **Anonymization** - De-identify with multiple ID strategies
- **Export** - Generate BIDS-compliant output

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- 4GB RAM minimum (8GB recommended)

### Start NILS

```bash
# Clone the repository
git clone https://github.com/NeuroGranberg/NILS.git
cd NILS

# Start services with your DICOM data
./scripts/manage.sh start --data /path/to/your/dicom/data

# Access the web interface
open http://localhost:5173
```

### Network Options

| Mode | Command | Access |
|------|---------|--------|
| **Default** | `start` | Localhost only (secure) |
| External | `start --forward` | Network/Tailscale accessible |

---

## Documentation

Full documentation available at: **[neurogranberg.github.io/NILS](https://neurogranberg.github.io/NILS/)**

- [**Concepts**](https://neurogranberg.github.io/NILS/concepts/) - Core data models and terminology
- [**Cohort Operations**](https://neurogranberg.github.io/NILS/cohort/) - Extraction, Sorting, Anonymization, Export
- [**Classification**](https://neurogranberg.github.io/NILS/classification/) - The six-axis detection system
- [**QC & Viewer**](https://neurogranberg.github.io/NILS/qc/) - Quality control and image review

---

## Usage

### Options

| Option | Description |
|--------|-------------|
| `--data PATH` | Mount DICOM directory (can specify multiple) |
| `--forward` | Expose ports externally (default: localhost only) |
| `--clean` | Remove containers and volumes before starting |
| `--db-dir PATH` | Override database storage directory |

### Examples

```bash
# Start with localhost access (default - secure)
./scripts/manage.sh start --data /srv/dicom

# Mount multiple data directories
./scripts/manage.sh start \
  --data /srv/dicom/ct \
  --data /srv/dicom/mr

# Clean start with network access
./scripts/manage.sh start --clean --forward --data /srv/dicom

# Stop services
./scripts/manage.sh stop
```

### Remote Access

**SSH tunnel (recommended for default mode):**
```bash
ssh -L 5173:localhost:5173 user@server
# Then open http://localhost:5173 locally
```

**Tailscale (with `--forward` mode):**
```
http://your-server.ts.net:5173
```

---

## Configuration

Environment variables in `.env`:

| Variable | Description |
|----------|-------------|
| `APP_ACCESS_TOKEN` | Secret key for login protection |
| `DB_DATA_DIR` | Database storage directory |
| `METADATA_DB_DATA_DIR` | Metadata database directory |

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Docker Network                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │    db    │  │ metadata │  │     backend      │  │
│  │ postgres │  │    db    │  │   FastAPI API    │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│                                       ▲             │
│                              ┌────────┴─────────┐  │
│                              │     frontend     │  │
│                              │   Vite + React   │  │
│                              └──────────────────┘  │
└─────────────────────────────────────────────────────┘
```

---

## Testing

```bash
# Frontend unit tests
./scripts/manage.sh test-frontend

# Backend unit tests
./scripts/manage.sh test-backend
```

---

## License

MIT License - see [LICENSE](LICENSE) file.

---

## Citation

If you use NILS in your research, please cite:

> Chamyani, N. (2025). NILS - Neuroimaging Intelligent Linked System.
> Karolinska Institutet, Department of Clinical Neuroscience.
> https://github.com/NeuroGranberg/NILS

---

Karolinska Institutet, Department of Clinical Neuroscience, Neuroradiology
