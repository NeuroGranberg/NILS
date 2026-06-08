"""Boutiques-subset + ``x-nils`` analysis-pipeline descriptor layer.

A descriptor (``nils.job.yml``) is the single source of truth for *what an
analysis pipeline is*: its container image, its config-form knobs (Boutiques
``inputs``/``groups``), its declared outputs, and the NILS-specific ``x-nils``
block (work-unit / analysis-level, input format expectations, runtime ``needs``,
optional inline steps, and a deferred ingest entrypoint).

Design contract (frozen design §2). Key behaviors:

* Pydantic v2 with ``extra="allow"`` so unknown keys are CARRIED, not rejected
  (forward-compatibility, spec §17.3.1). Hyphenated Boutiques keys map to
  snake_case attributes via aliases + ``populate_by_name``.
* ``parse_descriptor`` accepts a YAML/JSON string OR a path.
* ``validate_descriptor`` returns a list of WARNING strings and only *raises* in
  two hard cases: an out-of-range ``schema-version`` and a missing ``name``.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

# [min, max) — Boutiques 0.5.x is the supported descriptor schema band. A
# descriptor declaring a version below MIN or at/above MAX is the ONE hard
# refuse (see validate_descriptor rule 1).
SUPPORTED_SCHEMA_VERSION_RANGE: tuple[str, str] = ("0.5", "0.6")

# BIDS-Apps analysis levels (the raw, persisted ``x-nils.analysis-level``).
_VALID_ANALYSIS_LEVELS: frozenset[str] = frozenset(
    {"run", "session", "subject", "dataset", "meta"}
)

# Internal work-unit vocabulary that the runner slices over.
_VALID_WORK_UNITS: frozenset[str] = frozenset(
    {"stack", "session", "subject", "group"}
)


class DescriptorError(ValueError):
    """Raised when a descriptor is structurally invalid in a hard way.

    Used for the missing-``name`` case and as the base for the schema-version
    refuse. Unknown keys never raise this — they only produce warnings.
    """


class UnsupportedSchemaVersionError(DescriptorError):
    """The ONE hard refuse: ``schema-version`` outside the supported range."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def slugify(value: str) -> str:
    """Slugify a descriptor name to ``[a-z0-9-]`` (lowercase, dash-joined)."""
    text = value.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def _version_tuple(version: str) -> tuple[int, ...]:
    """Parse a dotted version string into an int tuple for comparison.

    Non-numeric trailing segments (e.g. ``"0.5.0-beta"``) are truncated at the
    first non-numeric component so comparison stays total and sane.
    """
    parts: list[int] = []
    for chunk in str(version).split("."):
        m = re.match(r"^(\d+)", chunk.strip())
        if not m:
            break
        parts.append(int(m.group(1)))
    return tuple(parts) or (0,)


def work_unit_of(level: str, formats: list[str] | None = None) -> str:
    """Map a BIDS-Apps ``analysis-level`` to an internal work-unit.

    ``run``/``session`` collapse to ``session`` unless the descriptor declares a
    non-BIDS (e.g. ``dicom``) input layout, in which case the natural unit is a
    ``stack``. ``subject`` → ``subject``; ``dataset``/``meta`` → ``group``.
    Unknown levels fall back to ``subject`` (the most common BIDS-App default).
    """
    formats = formats or []
    lowered = {f.lower() for f in formats}
    if level in ("run", "session"):
        if "dicom" in lowered:
            return "stack"
        return "session"
    if level == "subject":
        return "subject"
    if level in ("dataset", "meta"):
        return "group"
    return "subject"


# ---------------------------------------------------------------------------
# Boutiques models
# ---------------------------------------------------------------------------

class ContainerImage(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: str  # "docker" | "singularity"
    image: str  # "ghcr.io/...@sha256:..."
    container_hash: str | None = Field(None, alias="container-hash")


class DescriptorInput(BaseModel):
    """One config-form knob (a Boutiques input)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    name: str
    type: str  # "String" | "File" | "Flag" | "Number"
    description: str | None = None
    command_line_flag: str | None = Field(None, alias="command-line-flag")
    value_key: str | None = Field(None, alias="value-key")  # "[KEY]" placeholder
    optional: bool = True
    default_value: Any | None = Field(None, alias="default-value")
    value_choices: list[Any] | None = Field(None, alias="value-choices")
    # Boutiques ``list`` flag. Named ``is_list`` to avoid shadowing the ``list``
    # builtin in lazily-evaluated annotations on sibling fields; the wire key
    # stays ``list`` via the alias.
    is_list: bool = Field(False, alias="list")
    minimum: float | None = None
    maximum: float | None = None
    integer: bool = False


class DescriptorGroup(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    name: str | None = None
    members: list[str] = []
    mutually_exclusive: bool = Field(False, alias="mutually-exclusive")
    one_is_required: bool = Field(False, alias="one-is-required")
    all_or_none: bool = Field(False, alias="all-or-none")


class OutputFile(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    name: str
    path_template: str = Field(..., alias="path-template")


class SuggestedResources(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    cpu_cores: int | None = Field(None, alias="cpu-cores")
    ram: float | None = None  # GB
    walltime_estimate: float | None = Field(None, alias="walltime-estimate")


class ErrorCode(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    code: int
    description: str


# ---------------------------------------------------------------------------
# x-nils block
# ---------------------------------------------------------------------------

class XNilsInput(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    formats: list[str] = []  # ["nifti"] | ["dicom"] | ["bids"]
    layout: str = "bids"  # "bids" | "flat" | "dicom"


class XNilsNeeds(BaseModel):
    """Runtime needs. ``extra="allow"`` so unknown needs are carried forward."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    fs_license: bool = False
    templateflow: bool = False
    gpu: bool = False
    work_dir: bool = False


class XNilsStep(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    command: str  # Boutiques [KEY] template


class XNilsIngest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    enabled: bool = False
    entrypoint: str | None = None  # "nils_ingest.py:ingest"


class XNils(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    analysis_level: str = Field("subject", alias="analysis-level")  # = work_unit driver
    input: XNilsInput = Field(default_factory=XNilsInput)
    needs: XNilsNeeds = Field(default_factory=XNilsNeeds)
    steps: list[XNilsStep] = Field(default_factory=list)
    ingest: XNilsIngest = Field(default_factory=XNilsIngest)
    flow: str | None = None  # escape hatch: repo-owned Prefect flow (deferred)


# ---------------------------------------------------------------------------
# Top-level descriptor
# ---------------------------------------------------------------------------

class Descriptor(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    schema_version: str = Field("0.5", alias="schema-version")
    tool_version: str | None = Field(None, alias="tool-version")
    description: str | None = None
    command_line: str | None = Field(None, alias="command-line")
    container_image: ContainerImage | None = Field(None, alias="container-image")
    inputs: list[DescriptorInput] = Field(default_factory=list)
    groups: list[DescriptorGroup] = Field(default_factory=list)
    output_files: list[OutputFile] = Field(default_factory=list, alias="output-files")
    suggested_resources: SuggestedResources | None = Field(
        None, alias="suggested-resources"
    )
    error_codes: list[ErrorCode] = Field(default_factory=list, alias="error-codes")
    x_nils: XNils = Field(default_factory=XNils, alias="x-nils")

    @property
    def slug(self) -> str:
        """Descriptor ``name`` slugified to ``[a-z0-9-]``."""
        return slugify(self.name)

    @property
    def image_digest(self) -> str | None:
        """The pinned image digest.

        Prefers an explicit ``container-hash``; otherwise extracts an
        ``@sha256:...`` digest from the image reference if present.
        """
        if self.container_image is None:
            return None
        if self.container_image.container_hash:
            return self.container_image.container_hash
        ref = self.container_image.image or ""
        if "@" in ref:
            return ref.split("@", 1)[1]
        return None

    @property
    def analysis_level(self) -> str:
        """Raw BIDS-Apps analysis level (persisted verbatim)."""
        return self.x_nils.analysis_level

    @property
    def work_unit(self) -> str:
        """Resolved internal work-unit (``stack``/``session``/``subject``/``group``)."""
        return work_unit_of(self.x_nils.analysis_level, self.x_nils.input.formats)

    @property
    def warnings(self) -> list[str]:
        """Validation warnings attached at parse time (never blocks load)."""
        return list(getattr(self, "_warnings", []))


# ---------------------------------------------------------------------------
# Validation + parsing
# ---------------------------------------------------------------------------

# Known top-level keys (alias form). Anything else → carried + warned.
_KNOWN_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "schema-version",
        "tool-version",
        "description",
        "command-line",
        "container-image",
        "inputs",
        "groups",
        "output-files",
        "suggested-resources",
        "error-codes",
        "x-nils",
    }
)

_KNOWN_NEEDS_KEYS: frozenset[str] = frozenset(
    {"fs_license", "templateflow", "gpu", "work_dir"}
)


def validate_descriptor(data: dict) -> list[str]:
    """Validate a raw descriptor dict; return WARNING strings (forward-compat).

    Hard refuses (raise):
      1. ``schema-version`` below MIN or at/above MAX → ``UnsupportedSchemaVersionError``.
      2. Missing required-and-known ``name`` → ``DescriptorError``.

    Soft cases (warn, carry):
      3. Unknown top-level keys / unknown ``x-nils.needs`` keys.
      4. A descriptor with neither ``command-line`` nor ``x-nils.steps`` (nothing
         to run).
    """
    if not isinstance(data, dict):
        raise DescriptorError(
            f"Descriptor must be a mapping, got {type(data).__name__}"
        )

    warnings: list[str] = []

    # Rule 1 — schema-version RANGE (the one hard refuse).
    version = str(data.get("schema-version", data.get("schema_version", "0.5")))
    lo, hi = SUPPORTED_SCHEMA_VERSION_RANGE
    v = _version_tuple(version)
    if v < _version_tuple(lo) or v >= _version_tuple(hi):
        raise UnsupportedSchemaVersionError(
            f"Unsupported descriptor schema-version {version!r}; "
            f"supported range is [{lo}, {hi})."
        )

    # Rule 2 — required-and-known missing.
    name = data.get("name")
    if not name or not str(name).strip():
        raise DescriptorError("Descriptor is missing the required 'name' field.")

    # Rule 3a — unknown top-level keys.
    for key in data:
        if key in _KNOWN_TOP_LEVEL_KEYS:
            continue
        # tolerate the snake_case spelling of schema_version without warning
        if key == "schema_version":
            continue
        warnings.append(
            f"Unknown top-level descriptor key {key!r} (carried, not interpreted)."
        )

    # Rule 3b — unknown x-nils.needs keys.
    x_nils = data.get("x-nils") or data.get("x_nils") or {}
    if isinstance(x_nils, dict):
        needs = x_nils.get("needs") or {}
        if isinstance(needs, dict):
            for key in needs:
                if key not in _KNOWN_NEEDS_KEYS:
                    warnings.append(
                        f"Unknown x-nils.needs key {key!r} "
                        "(carried, will be handled forward-compatibly)."
                    )
        steps = x_nils.get("steps") or []
        # Rule 3c — unrecognized analysis-level (soft: warn, default to subject).
        level = x_nils.get("analysis-level") or x_nils.get("analysis_level")
        if level and str(level) not in _VALID_ANALYSIS_LEVELS:
            warnings.append(
                f"Unknown x-nils.analysis-level {str(level)!r} (not in "
                f"{sorted(_VALID_ANALYSIS_LEVELS)}); work-unit defaults to 'subject'."
            )
    else:
        steps = []

    # Rule 4 — nothing to run.
    has_command = bool(data.get("command-line") or data.get("command_line"))
    has_steps = bool(steps)
    if not has_command and not has_steps:
        warnings.append(
            "Descriptor declares neither 'command-line' nor 'x-nils.steps'; "
            "there is nothing to run."
        )

    for w in warnings:
        logger.warning("descriptor validation: %s", w)

    return warnings


def _load_descriptor_text(text: str) -> dict:
    """Parse descriptor text (YAML; JSON is a YAML subset) into a dict."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:  # pragma: no cover - exercised via parse tests
        raise DescriptorError(f"Descriptor is not valid YAML/JSON: {exc}") from exc
    if data is None:
        raise DescriptorError("Descriptor is empty.")
    if not isinstance(data, dict):
        raise DescriptorError(
            f"Descriptor must be a mapping, got {type(data).__name__}."
        )
    return data


def parse_descriptor(source: str | Path) -> Descriptor:
    """Parse a descriptor from a YAML/JSON *string* OR a *path*.

    If ``source`` is path-like and the file exists, it is read; otherwise the
    string is treated as descriptor text. Validation runs before the model is
    built; any non-fatal warnings are attached to ``descriptor._warnings``.
    """
    text: str
    if isinstance(source, Path):
        if not source.exists():
            raise DescriptorError(f"Descriptor path does not exist: {source}")
        text = source.read_text(encoding="utf-8")
    else:
        # A string that resolves to an existing file is read as a path; a path
        # with newlines or that does not exist is treated as inline text.
        candidate: Path | None = None
        if "\n" not in source:
            try:
                p = Path(source)
                if p.exists() and p.is_file():
                    candidate = p
            except (OSError, ValueError):
                candidate = None
        if candidate is not None:
            text = candidate.read_text(encoding="utf-8")
        else:
            text = source

    data = _load_descriptor_text(text)
    warnings = validate_descriptor(data)

    try:
        descriptor = Descriptor.model_validate(data)
    except DescriptorError:
        raise
    except Exception as exc:  # pydantic ValidationError → DescriptorError
        raise DescriptorError(f"Descriptor failed validation: {exc}") from exc

    # Attach non-field warnings for callers to log (never blocks load).
    object.__setattr__(descriptor, "_warnings", warnings)
    return descriptor
