"""Configuration models for the anonymization stage."""

from __future__ import annotations

import shutil
from enum import Enum
from pathlib import Path
from typing import NamedTuple, Optional

from pydantic import BaseModel, Field, FilePath, field_validator, model_validator


class DerivativesStatus(str, Enum):
    """State of the derivatives folder prior to anonymization."""

    FRESH = "fresh"
    RAW_EXISTS_EMPTY = "raw_exists_empty"
    RAW_EXISTS_WITH_CONTENT = "raw_exists_with_content"


class DerivativesSetup(NamedTuple):
    source_path: Path
    output_path: Path
    status: DerivativesStatus


def _has_contents(path: Path) -> bool:
    try:
        next(path.iterdir())
    except StopIteration:
        return False
    except FileNotFoundError:
        return False
    return True


def setup_derivatives_folders(selected_root: Path) -> DerivativesSetup:
    """Prepare derivatives structure and report current status.

    The helper handles three scenarios:

    1. A fresh dataset with raw files at the cohort root (creates ``derivatives``
       and migrates originals into ``dcm-original``).
    2. A cohort root that already contains ``derivatives/dcm-original`` and
       ``derivatives/dcm-raw`` (no migration, just reuse existing paths).
    3. A user-selected folder that *is* already ``derivatives`` or ``dcm-``
       subdirectories (maps to the appropriate paths without nesting).
    """

    root = selected_root.resolve()

    derivatives_root: Path
    source_path: Path
    output_path: Path
    perform_move = False

    if root.name == "dcm-original":
        derivatives_root = root.parent
        source_path = root
        output_path = derivatives_root / "dcm-raw"
    elif root.name == "dcm-raw":
        derivatives_root = root.parent
        output_path = root
        source_path = derivatives_root / "dcm-original"
    elif root.name == "derivatives" and (root / "dcm-original").exists():
        derivatives_root = root
        source_path = root / "dcm-original"
        output_path = root / "dcm-raw"
    elif (root / "derivatives" / "dcm-original").exists():
        derivatives_root = root / "derivatives"
        source_path = derivatives_root / "dcm-original"
        output_path = derivatives_root / "dcm-raw"
    elif (root / "dcm-original").exists() and (root / "dcm-raw").exists():
        derivatives_root = root
        source_path = root / "dcm-original"
        output_path = root / "dcm-raw"
    else:
        derivatives_root = root / "derivatives"
        source_path = derivatives_root / "dcm-original"
        output_path = derivatives_root / "dcm-raw"
        perform_move = True

    raw_existed = output_path.exists()

    source_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)

    if perform_move and not _has_contents(source_path):
        for item in root.iterdir():
            if item == derivatives_root:
                continue
            destination = source_path / item.name
            if destination.exists():
                continue
            shutil.move(str(item), str(destination))

    raw_has_content = _has_contents(output_path)
    if raw_has_content:
        status = DerivativesStatus.RAW_EXISTS_WITH_CONTENT
    elif raw_existed:
        status = DerivativesStatus.RAW_EXISTS_EMPTY
    else:
        status = DerivativesStatus.FRESH

    return DerivativesSetup(source_path, output_path, status)


def clean_dcm_raw(raw_path: Path) -> None:
    """Remove contents of the anonymized output folder without touching originals."""

    if not raw_path.exists():
        raw_path.mkdir(parents=True, exist_ok=True)
        return
    if not raw_path.is_dir():  # pragma: no cover - defensive
        raise ValueError(f"Invalid dcm-raw path: {raw_path}")

    for child in raw_path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


class PatientIdStrategyType(str, Enum):
    NONE = "none"
    FOLDER = "folder"
    CSV = "csv"
    DETERMINISTIC = "deterministic"
    SEQUENTIAL = "sequential"


class FolderStrategy(str, Enum):
    DEPTH = "depth"
    REGEX = "regex"


class CsvMissingMode(str, Enum):
    HASH = "hash"
    PER_TOP_FOLDER_SEQ = "per_top_folder_seq"


class CsvMappingConfig(BaseModel):
    path: FilePath
    source_column: str = Field(..., min_length=1)
    target_column: str = Field(..., min_length=1)
    missing_mode: CsvMissingMode = CsvMissingMode.HASH
    missing_pattern: str = Field("MISSEDXXXXX", min_length=3)
    missing_salt: str = Field("csv-missed", min_length=1)
    preserve_top_folder_order: bool = True

    def starting_number_for_missing(self, mapping: dict[str, str]) -> int:
        if self.missing_mode != CsvMissingMode.PER_TOP_FOLDER_SEQ:
            return 1
        width = self.missing_pattern.count("X")
        static_prefix = self.missing_pattern.replace("X" * width, "") if width else self.missing_pattern
        existing_numbers: list[int] = []
        for value in mapping.values():
            if not value:
                continue
            suffix = value
            if static_prefix and suffix.startswith(static_prefix):
                suffix = suffix[len(static_prefix):]
            if suffix.isdigit():
                existing_numbers.append(int(suffix))
        return max(existing_numbers) + 1 if existing_numbers else 1


class FolderIdConfig(BaseModel):
    strategy: FolderStrategy = FolderStrategy.DEPTH
    depth_after_root: int = Field(ge=1, le=10, default=2)
    regex: str = Field(r"\b(\d+)[-_](?:[Mm]\d+|\d+)", min_length=1)
    fallback_template: str = Field(..., min_length=1)

    @model_validator(mode="after")
    def ensure_regex_for_regex_strategy(self) -> "FolderIdConfig":
        if self.strategy == FolderStrategy.REGEX and not self.regex:
            raise ValueError("regex must be provided when folder strategy is 'regex'")
        return self


class DeterministicIdConfig(BaseModel):
    pattern: str = Field(..., min_length=1)
    salt: str = Field(..., min_length=1)


class SequentialDiscoveryMode(str, Enum):
    PER_TOP_FOLDER = "per_top_folder"
    ONE_PER_STUDY = "one_per_study"
    ALL = "all"


class SequentialIdConfig(BaseModel):
    pattern: str = Field(..., min_length=1)
    starting_number: int = Field(1, ge=0)
    discovery: SequentialDiscoveryMode = SequentialDiscoveryMode.PER_TOP_FOLDER


class PatientIdConfig(BaseModel):
    enabled: bool = True
    strategy: PatientIdStrategyType = PatientIdStrategyType.SEQUENTIAL
    folder: Optional[FolderIdConfig] = None
    csv_mapping: Optional[CsvMappingConfig] = None
    deterministic: Optional[DeterministicIdConfig] = None
    sequential: Optional[SequentialIdConfig] = None
    number_ranges: list[tuple[int, int]] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_strategy_payload(self) -> "PatientIdConfig":
        if not self.enabled:
            return self

        if self.strategy == PatientIdStrategyType.NONE:
            return self
        elif self.strategy == PatientIdStrategyType.FOLDER:
            if not self.folder:
                raise ValueError("folder strategy requires 'folder' configuration")
        elif self.strategy == PatientIdStrategyType.CSV:
            if not self.csv_mapping:
                raise ValueError("csv strategy requires 'csv_mapping' configuration")
        elif self.strategy == PatientIdStrategyType.DETERMINISTIC:
            if not self.deterministic:
                raise ValueError("deterministic strategy requires 'deterministic' configuration")
        elif self.strategy == PatientIdStrategyType.SEQUENTIAL:
            if not self.sequential:
                raise ValueError("sequential strategy requires 'sequential' configuration")
        return self


class StudyDateConfig(BaseModel):
    enabled: bool = False
    snap_to_six_months: bool = True
    minimum_offset_months: int = Field(0, ge=0)


class AuditExportFormat(str, Enum):
    CSV = "csv"
    ENCRYPTED_EXCEL = "encrypted_excel"


class AuditExportConfig(BaseModel):
    enabled: bool = True
    format: AuditExportFormat = AuditExportFormat.ENCRYPTED_EXCEL
    filename: Optional[str] = None
    excel_password: Optional[str] = None

    @model_validator(mode="after")
    def validate_excel_requirements(self) -> "AuditExportConfig":
        if not self.enabled:
            return self
        if self.format == AuditExportFormat.ENCRYPTED_EXCEL and not self.excel_password:
            raise ValueError("excel_password is required for encrypted excel export")
        return self


class AnonymizeConfig(BaseModel):
    source_root: Path
    output_root: Path
    
    # Explicit tag list (preferred method)
    scrub_tags: Optional[list[tuple[int, int]]] = None
    
    # Category-based (for backward compatibility)
    anonymize_categories: list[str] = Field(default_factory=list)
    
    patient_id: PatientIdConfig = Field(default_factory=PatientIdConfig)
    study_dates: StudyDateConfig = Field(default_factory=StudyDateConfig)
    scrub_exclude_tags: list[str] = Field(default_factory=list)
    concurrent_processes: int = Field(32, ge=1)
    worker_threads: int = Field(32, ge=1)
    audit_export: AuditExportConfig = Field(default_factory=AuditExportConfig)
    preserve_uids: bool = True
    rename_patient_folders: bool = False
    resume: bool = False
    audit_resume_per_leaf: bool = True
    total_subjects: Optional[int] = None

    cohort_name: Optional[str] = None

    @model_validator(mode="after")
    def validate_scrub_config(self) -> "AnonymizeConfig":
        """Ensure either scrub_tags or anonymize_categories is provided."""
        if not self.scrub_tags and not self.anonymize_categories:
            raise ValueError("Either scrub_tags or anonymize_categories must be provided")
        return self

    @model_validator(mode="after")
    def ensure_paths(self) -> "AnonymizeConfig":
        if not self.source_root.exists() or not self.source_root.is_dir():
            raise ValueError(f"source_root '{self.source_root}' must exist and be a directory")
        
        # Create output directory
        self.output_root.mkdir(parents=True, exist_ok=True)
        
        return self


class AnonymizeResult(BaseModel):
    total_files: int
    updated_files: int
    skipped_files: int
    duration_seconds: float
    audit_rows_written: int
    export_path: Optional[Path]
    job_id: Optional[int]
    errors: list[str] = Field(default_factory=list)
    leaves_skipped: int = 0
    leaves_completed: int = 0
    files_reused: int = 0
    files_output_only: int = 0


def load_config(path: Path) -> AnonymizeConfig:
    """Load an anonymization config from a JSON or YAML file."""

    import json

    text = path.read_text()
    if path.suffix.lower() in {".yaml", ".yml"}:
        import yaml  # type: ignore

        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    return AnonymizeConfig.model_validate(data)
