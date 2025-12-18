"""Helpers for deriving subject codes and mapping identifiers."""

from __future__ import annotations

import csv
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional


logger = logging.getLogger(__name__)

DEFAULT_DIGEST_SIZE = 8


def subject_code_gen(input_string: str, key_string: str, digest_size: int = DEFAULT_DIGEST_SIZE) -> str:
    """Generate a BLAKE2b hash for the given input using the provided key."""

    key_bytes = key_string.encode("utf-8")
    hasher = hashlib.blake2b(input_string.encode("utf-8"), key=key_bytes, digest_size=digest_size)
    return hasher.hexdigest()


@dataclass(frozen=True)
class SubjectResolution:
    subject_code: str
    patient_id: Optional[str]
    patient_name: Optional[str]
    source: str


class SubjectResolver:
    """Derive subject codes from Patient IDs with optional CSV overrides."""

    def __init__(
        self,
        *,
        subject_code_map: Optional[Dict[str, str]] = None,
        seed: str,
        digest_size: int = DEFAULT_DIGEST_SIZE,
    ) -> None:
        self._seed = seed
        self._digest_size = digest_size
        normalized: Dict[str, str] = {}
        if subject_code_map:
            for raw_key, raw_value in subject_code_map.items():
                key = (raw_key or "").strip()
                value = (raw_value or "").strip()
                if not key:
                    continue
                if not value:
                    raise ValueError("Subject code CSV contains empty subject_code entries")
                normalized[key] = value
        self._map = normalized

    def resolve(
        self,
        *,
        patient_id: Optional[str],
        patient_name: Optional[str],
        study_uid: str,
    ) -> SubjectResolution:
        if patient_id:
            mapped = self._map.get(patient_id)
            if mapped:
                return SubjectResolution(subject_code=mapped, patient_id=patient_id, patient_name=patient_name, source="csv")

        fallback_key = patient_id or study_uid
        if not fallback_key:
            raise ValueError("Cannot derive subject_code without PatientID or StudyInstanceUID")
        code = subject_code_gen(fallback_key, self._seed, digest_size=self._digest_size)
        if not patient_id:
            logger.warning("Generated subject_code via hash because PatientID missing for study %s", study_uid)
        elif patient_id not in self._map:
            logger.info("Generated subject_code via hash because PatientID %s not found in mapping", patient_id)
        return SubjectResolution(subject_code=code, patient_id=patient_id, patient_name=patient_name, source="hash")


def load_subject_code_csv(path: Path, patient_column: str, subject_column: str) -> Dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Subject code CSV not found: {path}")

    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("Subject code CSV is missing a header row")
        columns = {name.strip(): name for name in reader.fieldnames if name}
        if patient_column not in columns and patient_column.strip() in columns:
            patient_column = patient_column.strip()
        if subject_column not in columns and subject_column.strip() in columns:
            subject_column = subject_column.strip()
        if patient_column not in columns:
            raise ValueError(f"Subject code CSV missing patient column '{patient_column}'")
        if subject_column not in columns:
            raise ValueError(f"Subject code CSV missing subject code column '{subject_column}'")

        patient_key = columns[patient_column]
        subject_key = columns[subject_column]

        mapping: Dict[str, str] = {}
        for row in reader:
            patient_value = (row.get(patient_key) or "").strip()
            subject_value = (row.get(subject_key) or "").strip()
            if not patient_value and not subject_value:
                continue
            if not patient_value:
                raise ValueError("Subject code CSV row missing PatientID")
            if not subject_value:
                raise ValueError(f"Subject code CSV row for PatientID '{patient_value}' missing subject_code")
            if patient_value in mapping and mapping[patient_value] != subject_value:
                raise ValueError(
                    f"Subject code CSV has conflicting subject_code values for PatientID '{patient_value}'"
                )
            mapping[patient_value] = subject_value

    return mapping
