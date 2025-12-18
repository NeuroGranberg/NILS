"""Helpers for skipping files already stored in the metadata DB."""

from __future__ import annotations

import hashlib
import logging
import math
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, Optional, Tuple

from sqlalchemy import select

from metadata_db.schema import Instance, Series, Study, Subject, SubjectCohort
from metadata_db.session import SessionLocal


logger = logging.getLogger(__name__)


def _hash_bytes(value: str, seed: int) -> int:
    data = f"{seed}|{value}".encode("utf-8")
    digest = hashlib.blake2b(data, digest_size=8).digest()
    return int.from_bytes(digest, "big")


@dataclass
class BloomFilter:
    bit_count: int
    hash_count: int
    bits: bytearray
    seeds: Tuple[int, ...]

    @classmethod
    def from_capacity(cls, capacity: int, error_rate: float) -> "BloomFilter":
        capacity = max(1, capacity)
        error_rate = min(max(error_rate, 1e-6), 0.5)
        bit_count = int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))
        bit_count = max(bit_count, 8)
        hash_count = int((bit_count / capacity) * math.log(2)) or 1
        seeds = tuple(range(hash_count))
        byte_len = (bit_count + 7) // 8
        return cls(bit_count=bit_count, hash_count=hash_count, bits=bytearray(byte_len), seeds=seeds)

    def add(self, value: str) -> None:
        for seed in self.seeds:
            digest = _hash_bytes(value, seed)
            bit = digest % self.bit_count
            self.bits[bit >> 3] |= 1 << (bit & 7)

    def contains(self, value: str) -> bool:
        for seed in self.seeds:
            digest = _hash_bytes(value, seed)
            bit = digest % self.bit_count
            if not (self.bits[bit >> 3] & (1 << (bit & 7))):
                return False
        return True


class SubjectPathEntry:
    """Resume filter for a single subject."""

    def __init__(self, threshold: int = 50_000, error_rate: float = 0.01) -> None:
        self._threshold = threshold
        self._error_rate = error_rate
        self._paths: set[str] | None = set()
        self._bloom: BloomFilter | None = None
        self._count = 0

    def add(self, relative_path: str) -> None:
        relative = relative_path or ""
        if self._bloom is not None:
            self._bloom.add(relative)
            self._count += 1
            return
        assert self._paths is not None
        self._paths.add(relative)
        self._count = len(self._paths)
        if self._count >= self._threshold:
            bloom = BloomFilter.from_capacity(self._count, self._error_rate)
            for existing in self._paths:
                bloom.add(existing)
            self._paths = None
            self._bloom = bloom

    def contains(self, relative_path: str) -> bool:
        relative = relative_path or ""
        if self._bloom is not None:
            return self._bloom.contains(relative)
        return relative in (self._paths or set())

    def __len__(self) -> int:
        return self._count


class ExistingPathIndex:
    """Tracks paths already stored for resume-by-path."""

    def __init__(self, subject_threshold: int = 50_000, error_rate: float = 0.01) -> None:
        self._subject_threshold = subject_threshold
        self._error_rate = error_rate
        self._subjects: Dict[str, SubjectPathEntry] = {}
        self._total_paths = 0

    def add_raw_path(self, relative_path: str) -> None:
        subject_key, inner_path = split_subject_relative(relative_path)
        if not subject_key:
            return
        self.add(subject_key, inner_path)

    def add(self, subject_key: str, subject_relative: str) -> None:
        if not subject_key:
            return
        entry = self._subjects.get(subject_key)
        if entry is None:
            entry = SubjectPathEntry(self._subject_threshold, self._error_rate)
            self._subjects[subject_key] = entry
        entry.add(subject_relative)
        self._total_paths += 1

    def should_skip(self, subject_key: str, subject_relative: str) -> bool:
        entry = self._subjects.get(subject_key)
        if entry is None:
            return False
        return entry.contains(subject_relative)

    def entry_for(self, subject_key: str) -> SubjectPathEntry | None:
        return self._subjects.get(subject_key)

    def subject_map(self) -> Dict[str, SubjectPathEntry]:
        return self._subjects

    @property
    def total_paths(self) -> int:
        return self._total_paths


def split_subject_relative(file_path: str) -> Tuple[str, str]:
    cleaned = file_path.strip().replace("\\", "/")
    if not cleaned:
        return "", ""
    parts = [segment for segment in cleaned.split("/") if segment]
    if not parts:
        return "", ""
    subject_key = parts[0]
    remainder = "/".join(parts[1:]) if len(parts) > 1 else ""
    return subject_key, remainder


def build_existing_path_index(
    cohort_id: int,
    subject_keys: Optional[Iterable[str]] = None,
    *,
    chunk_size: int = 5000,
) -> ExistingPathIndex:
    """Load existing DICOM paths for the cohort into an index."""

    key_filter = set(subject_keys) if subject_keys is not None else None
    session = SessionLocal()
    index = ExistingPathIndex()
    try:
        stmt = (
            select(Instance.dicom_file_path)
            .join(Series, Series.series_id == Instance.series_id)
            .join(Study, Study.study_id == Series.study_id)
            .join(Subject, Subject.subject_id == Study.subject_id)
            .join(SubjectCohort, SubjectCohort.subject_id == Subject.subject_id)
            .where(SubjectCohort.cohort_id == cohort_id)
        )
        stream = session.execute(stmt.execution_options(stream_results=True, yield_per=chunk_size))
        loaded = 0
        for (raw_path,) in stream:
            if not raw_path:
                continue
            subject_key, remainder = split_subject_relative(raw_path)
            if key_filter and subject_key not in key_filter:
                continue
            index.add(subject_key, remainder)
            loaded += 1
        logger.info("Loaded %d existing DICOM paths for cohort %s", loaded, cohort_id)
        return index
    finally:
        session.close()
