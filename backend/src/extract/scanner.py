"""Filesystem discovery utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class SubjectFolder:
    subject_key: str
    path: Path


def discover_subjects(raw_root: Path) -> Iterable[SubjectFolder]:
    raw_root = raw_root.resolve()
    for entry in sorted(raw_root.iterdir()):
        if entry.is_dir():
            yield SubjectFolder(subject_key=entry.name, path=entry)
