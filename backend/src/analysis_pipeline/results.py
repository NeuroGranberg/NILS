"""``results.json`` schema + parser (frozen design §3).

A flow writes a ``results.json`` into a run's ``output_dir`` describing the
per-unit outcome (one ``UnitResult`` per work-unit). The runner consumes the
success side to mirror job status; ``ingest_derivatives`` consumes succeeded
units + their derivative paths. This module is the pure, DB-free parsing
surface for both.
"""

from __future__ import annotations

import enum
import json
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ResultsError(ValueError):
    """Raised when a ``results.json`` is present but unparseable/invalid.

    A *missing* results file is NOT an error (an empty ``RunResults`` is
    returned); only structurally broken content raises this.
    """


class UnitStatus(str, enum.Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class UnitResult(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    unit_id: str = Field(..., alias="unit_id")  # e.g. "sub-001" / stack id
    work_unit: str  # "stack" | "session" | "subject" | "group"
    status: UnitStatus
    derivatives: list[str] = Field(default_factory=list)  # rel paths under output_dir
    metrics: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class RunResults(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    schema_version: str = "1"
    units: list[UnitResult] = Field(default_factory=list)

    @property
    def succeeded(self) -> list[UnitResult]:
        return [u for u in self.units if u.status == UnitStatus.SUCCEEDED]

    @property
    def failed(self) -> list[UnitResult]:
        return [u for u in self.units if u.status == UnitStatus.FAILED]

    @property
    def skipped(self) -> list[UnitResult]:
        return [u for u in self.units if u.status == UnitStatus.SKIPPED]

    @property
    def counts(self) -> dict[str, int]:
        return {
            "succeeded": len(self.succeeded),
            "failed": len(self.failed),
            "skipped": len(self.skipped),
            "total": len(self.units),
        }


_RESULTS_FILENAME = "results.json"


def _coerce_results_payload(payload: Any) -> dict:
    """Normalize a parsed payload into the ``{"units": [...]}`` shape.

    Tolerates a bare list (wraps as ``{"units": [...]}``). A mapping is returned
    as-is. Anything else is a structural error.
    """
    if isinstance(payload, list):
        return {"units": payload}
    if isinstance(payload, dict):
        return payload
    raise ResultsError(
        f"results.json must be an object or a list, got {type(payload).__name__}."
    )


def _parse_results_text(text: str) -> RunResults:
    """Parse results text (JSON; YAML-tolerant) into ``RunResults``."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        # results.json should be JSON, but JSON is a YAML subset; fall back so a
        # hand-written fixture in YAML still loads, and surface a clean error if
        # neither parser accepts it.
        try:
            payload = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ResultsError(f"results.json is not valid JSON/YAML: {exc}") from exc
    if payload is None:
        raise ResultsError("results.json is empty.")
    data = _coerce_results_payload(payload)
    try:
        return RunResults.model_validate(data)
    except Exception as exc:  # pydantic ValidationError → ResultsError
        raise ResultsError(f"results.json failed schema validation: {exc}") from exc


def _locate_results_file(output_dir: Path) -> Path | None:
    """Find ``results.json`` at the top level first, then a shallow scan.

    Shallow = output_dir itself and its immediate subdirectories. Deterministic
    (sorted) so behavior is stable.
    """
    top = output_dir / _RESULTS_FILENAME
    if top.is_file():
        return top
    if not output_dir.is_dir():
        return None
    for child in sorted(output_dir.iterdir()):
        if child.is_dir():
            candidate = child / _RESULTS_FILENAME
            if candidate.is_file():
                return candidate
    return None


def parse_results_json(source: str | Path) -> RunResults:
    """Locate and parse a ``results.json``.

    ``source`` may be:
      * a directory → locate ``results.json`` (top-level, then a shallow scan);
        returns an empty ``RunResults`` if absent;
      * a path to a ``results.json`` file → parse it directly;
      * a JSON/YAML *string* of results content → parse it directly.

    Raises ``ResultsError`` only when content is present but unparseable or
    structurally invalid.
    """
    # Path-like inputs.
    if isinstance(source, Path):
        if source.is_dir():
            located = _locate_results_file(source)
            if located is None:
                return RunResults()
            return _parse_results_text(located.read_text(encoding="utf-8"))
        if source.is_file():
            return _parse_results_text(source.read_text(encoding="utf-8"))
        # A Path that doesn't exist is treated as a missing results dir/file.
        return RunResults()

    # String input: directory path, file path, or inline content.
    if "\n" not in source:
        try:
            p = Path(source)
        except (OSError, ValueError):
            p = None
        if p is not None:
            if p.is_dir():
                located = _locate_results_file(p)
                if located is None:
                    return RunResults()
                return _parse_results_text(located.read_text(encoding="utf-8"))
            if p.is_file():
                return _parse_results_text(p.read_text(encoding="utf-8"))

    # Treat as inline results content.
    return _parse_results_text(source)
