"""Configuration models for metadata extraction."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class ExtensionMode(str, Enum):
    DCM = "dcm"
    DCM_UPPER = "DCM"
    ALL_DCM = "all_dcm"
    NO_EXT = "no_ext"
    ALL = "all"


class DuplicatePolicy(str, Enum):
    SKIP = "skip"
    OVERWRITE = "overwrite"
    APPEND_SERIES = "append_series"


class ExtractionConfig(BaseModel):
    cohort_id: int
    cohort_name: str
    raw_root: Path
    max_workers: int = Field(
        default=4,
        ge=1,
        le=128,
        description="Maximum number of top-level folders (SubjectFolders) to process concurrently",
    )
    batch_size: int = Field(default=100, ge=10, le=5000)
    queue_size: int = Field(default=10, ge=1, le=500)
    extension_mode: ExtensionMode = ExtensionMode.ALL
    duplicate_policy: DuplicatePolicy = DuplicatePolicy.SKIP
    resume: bool = True
    resume_by_path: bool = Field(
        default=False,
        description="Skip files whose relative path already exists in metadata.instance (requires stable paths)",
    )
    series_workers_per_subject: int = Field(default=1, ge=1, le=16)
    adaptive_batching_enabled: bool = False
    target_tx_ms: int = Field(default=200, ge=50, le=2000)
    min_batch_size: int = Field(default=50, ge=10, le=10000)
    max_batch_size: int = Field(default=1000, ge=50, le=20000)
    use_specific_tags: bool = Field(default=True)
    use_process_pool: bool = Field(
        default=True,
        description="Use ProcessPoolExecutor for true CPU parallelism (recommended)",
    )
    process_pool_workers: Optional[int] = Field(
        default=None,
        ge=1,
        le=128,
        description="Number of worker processes (defaults to max_workers if None)",
    )
    db_writer_pool_size: int = Field(
        default=1,
        ge=1,
        le=16,
        description="Number of concurrent database writers (1-16, use 2-4 for best results)",
    )
    subject_id_type_id: Optional[int] = None
    subject_code_map: dict[str, str] = Field(default_factory=dict, exclude=True)
    subject_code_seed: Optional[str] = None
    subject_code_map_name: Optional[str] = None

    def resolved_subject_code_seed(self) -> str:
        base = (self.subject_code_seed or self.cohort_name or "default-seed").strip()
        normalized = base.upper() if base else "DEFAULT-SEED"
        return normalized or "DEFAULT-SEED"
