"""Per-cohort classification keyword overrides.

This module provides the merge layer between the global detection YAML
configuration and per-cohort keyword deltas stored in
``cohort_classification_overrides``.

Design principles (see spec 2026-04-24-per-cohort-classification-keyword-overrides):
- Deltas, not snapshots. Global YAML changes propagate automatically.
- Only keyword lists are editable; physics/flags/rules stay globally locked.
- Merge contract per bucket:
      effective = (defaults + added, dedup preserving order) \\ removed
- Unknown bucket paths in the DB are silently ignored (warn-and-continue)
  so old overrides survive YAML renames without crashing the pipeline.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

logger = logging.getLogger(__name__)


# =============================================================================
# Editable bucket registry
# =============================================================================

# Keys are axis identifiers exposed to the API/UI. Each entry declares:
#   file:     YAML filename relative to detection_yaml/
#   label:    Human-friendly axis label
#   patterns: List of bucket-path templates. A ``*`` in the path is a wildcard
#             over the keys of the enclosing dict. Exact paths (no ``*``) are
#             taken as-is (used for top-level lists).
#
# Only keys listed here are editable. Anything else in the YAML files (physics,
# flags, rules, priorities) stays globally locked.
EDITABLE_BUCKETS: dict[str, dict[str, Any]] = {
    "base": {
        "file": "base-detection.yaml",
        "label": "Base contrast",
        "description": "Tissue contrast weighting (T1w, T2w, FLAIR, ...)",
        "patterns": ["bases.*.keywords"],
    },
    "construct": {
        "file": "construct-detection.yaml",
        "label": "Construct",
        "description": "Derived maps and computed images (ADC, FA, MIP, ...)",
        "patterns": ["constructs.*.keywords"],
    },
    "contrast": {
        "file": "contrast-detection.yaml",
        "label": "Contrast agent",
        "description": "Pre/post contrast detection (no category buckets - top-level lists)",
        "patterns": ["negative_keywords", "positive_keywords"],
    },
    "modifier": {
        "file": "modifier-detection.yaml",
        "label": "Modifier",
        "description": "Sequence modifiers (FatSat, FLAIR, STIR, ...)",
        "patterns": ["modifiers.*.keywords"],
    },
    "technique": {
        "file": "technique-detection.yaml",
        "label": "Technique",
        "description": "Pulse sequence family (MPRAGE, TSE, EPI, ...)",
        "patterns": ["techniques.*.keywords"],
    },
    # NOTE: "acceleration" is intentionally NOT editable. The
    # AccelerationDetector uses hard-coded class-level keyword lists rather
    # than the YAML file; exposing it here would be a silent no-op. Revisit
    # once AccelerationDetector is YAML-driven.
    "provenance": {
        "file": "provenance-detection.yaml",
        "label": "Provenance",
        "description": "Reconstruction / origin (SWIRecon, SyMRI, EPIMix, ...)",
        "patterns": ["provenances.*.detection.keywords"],
    },
    "body_part": {
        "file": "body_part-detection.yaml",
        "label": "Body part",
        "description": "Anatomical region (brain, spine, neck, ...)",
        "patterns": ["categories.*.keywords"],
    },
}

DEFAULT_YAML_DIR = Path(__file__).parent / "detection_yaml"


# =============================================================================
# Path helpers
# =============================================================================

def _split_path(path: str) -> list[str]:
    return [p for p in path.split(".") if p]


def get_by_path(doc: dict, path: str) -> Any:
    """Return the value at dotted ``path`` inside ``doc``, or None if absent."""
    node: Any = doc
    for key in _split_path(path):
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def set_by_path(doc: dict, path: str, value: Any) -> None:
    """Set dotted ``path`` in ``doc`` to ``value``, creating dicts as needed."""
    parts = _split_path(path)
    if not parts:
        return
    node: Any = doc
    for key in parts[:-1]:
        if key not in node or not isinstance(node[key], dict):
            node[key] = {}
        node = node[key]
    node[parts[-1]] = value


def expand_patterns(doc: dict, patterns: Iterable[str]) -> list[str]:
    """Expand '*' wildcards in bucket-path patterns against ``doc``.

    Example: ``bases.*.keywords`` expands to ``bases.T1w.keywords``,
    ``bases.T2w.keywords``, ... for each key under ``bases``.
    """
    resolved: list[str] = []
    for pattern in patterns:
        parts = _split_path(pattern)
        if "*" not in parts:
            resolved.append(pattern)
            continue
        # Walk the doc substituting every wildcard; support a single '*' or
        # multiple '*' segments by recursing.
        resolved.extend(_expand(doc, parts, []))
    return resolved


def _expand(node: Any, parts: list[str], acc: list[str]) -> list[str]:
    if not parts:
        return [".".join(acc)]
    head, tail = parts[0], parts[1:]
    if head == "*":
        if not isinstance(node, dict):
            return []
        out: list[str] = []
        for key in node.keys():
            out.extend(_expand(node[key], tail, acc + [str(key)]))
        return out
    if not isinstance(node, dict) or head not in node:
        return []
    return _expand(node[head], tail, acc + [head])


# =============================================================================
# YAML load & merge
# =============================================================================

def _load_yaml(path: Path) -> dict:
    if not path.exists():
        logger.warning("YAML file not found: %s", path)
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_all_yamls(yaml_dir: Optional[Path] = None) -> dict[str, dict]:
    """Load every YAML referenced by EDITABLE_BUCKETS.

    Returns a mapping ``{filename: parsed_doc}``.
    """
    yaml_dir = yaml_dir or DEFAULT_YAML_DIR
    docs: dict[str, dict] = {}
    seen: set[str] = set()
    for axis_cfg in EDITABLE_BUCKETS.values():
        filename = axis_cfg["file"]
        if filename in seen:
            continue
        seen.add(filename)
        docs[filename] = _load_yaml(yaml_dir / filename)
    return docs


def _dedup_preserving_order(items: Iterable[str]) -> list[str]:
    """Dedup case-insensitively on ``strip().lower()`` while preserving the
    FIRST occurrence verbatim.

    The original keyword string is preserved (not trimmed) because some
    global defaults intentionally include surrounding whitespace as a
    word-boundary trick (e.g. ``" -c"`` in contrast-detection.yaml to
    avoid matching ``"med-kontrast"``).
    """
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if not isinstance(item, str):
            continue
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def apply_bucket_override(
    defaults: list[str],
    added: list[str],
    removed: list[str],
) -> list[str]:
    """Compute the effective keyword list for a bucket.

    ``effective = (defaults + added, dedup case-insensitive preserving order) \\ removed``
    All comparisons are case-insensitive on ``strip().lower()``.
    Original whitespace / case of kept keywords is preserved verbatim — this
    matters because some defaults (e.g. ``" -c"``) use leading/trailing
    whitespace as a word-boundary matching trick.
    """
    defaults = list(defaults or [])
    added = list(added or [])
    removed_lower = {r.strip().lower() for r in (removed or []) if r and isinstance(r, str)}
    merged = _dedup_preserving_order(defaults + added)
    if not removed_lower:
        return merged
    return [kw for kw in merged if kw.strip().lower() not in removed_lower]


# Type alias for the override lookup passed into merge_overrides.
OverrideMap = dict[tuple[str, str], dict[str, list[str]]]


def merge_overrides(
    yaml_dir: Optional[Path],
    overrides: OverrideMap,
) -> dict[str, dict]:
    """Merge per-cohort deltas into deep-copied YAML documents.

    Args:
        yaml_dir: Directory holding the detection YAML files. If None, uses the
            default bundled location.
        overrides: Mapping ``{(axis, bucket_path): {"added": [...], "removed": [...]}}``.

    Returns:
        ``{filename: merged_doc}`` suitable for handing to detectors.
    """
    docs = load_all_yamls(yaml_dir)
    # Deep copy so we never mutate the cached defaults.
    merged = {name: deepcopy(doc) for name, doc in docs.items()}

    for (axis, bucket_path), delta in overrides.items():
        axis_cfg = EDITABLE_BUCKETS.get(axis)
        if axis_cfg is None:
            logger.warning("Ignoring override for unknown axis: %s", axis)
            continue
        filename = axis_cfg["file"]
        doc = merged.get(filename)
        if doc is None:
            logger.warning("Ignoring override for missing YAML: %s", filename)
            continue

        current = get_by_path(doc, bucket_path)
        if current is None:
            # Bucket no longer exists - YAML likely renamed this key.
            logger.warning(
                "Ignoring override for unknown bucket %s:%s (path not found in %s)",
                axis, bucket_path, filename,
            )
            continue
        if not isinstance(current, list):
            logger.warning(
                "Ignoring override for non-list bucket %s:%s (type=%s)",
                axis, bucket_path, type(current).__name__,
            )
            continue

        effective = apply_bucket_override(
            current,
            delta.get("added", []) or [],
            delta.get("removed", []) or [],
        )
        set_by_path(doc, bucket_path, effective)

    return merged


# =============================================================================
# Catalog (enumerate all editable buckets with defaults)
# =============================================================================


@dataclass(frozen=True)
class BucketSpec:
    axis: str
    bucket_path: str
    display_name: str
    group_label: Optional[str]
    description: Optional[str]
    defaults: tuple[str, ...]


def _display_name_for_bucket(
    axis: str, bucket_path: str, doc: dict
) -> tuple[str, Optional[str], Optional[str]]:
    """Derive (display_name, group_label, description) from the YAML doc.

    For nested buckets like ``bases.T1w.keywords`` we pull the category dict
    (``bases.T1w``) and use its ``name``/``description`` fields when present.
    For top-level lists (``negative_keywords``) we fall back to a derived label.
    """
    parts = _split_path(bucket_path)
    # Top-level list like "negative_keywords" or "positive_keywords"
    if len(parts) == 1:
        pretty = parts[0].replace("_", " ").strip().capitalize()
        return pretty, "Top-level", None

    # Nested: e.g. bases.T1w.keywords or accelerations.SMS.detection.keywords
    # Find the category node (everything up to and NOT including the final
    # list key). For paths like foo.X.detection.keywords we want ``foo.X``.
    list_key = parts[-1]  # "keywords" or "exclude_keywords"
    if parts[-2] == "detection":
        category_path_parts = parts[:-2]
    else:
        category_path_parts = parts[:-1]
    category_id = category_path_parts[-1] if category_path_parts else list_key
    category_node = get_by_path(doc, ".".join(category_path_parts))

    name: Optional[str] = None
    description: Optional[str] = None
    if isinstance(category_node, dict):
        name = category_node.get("name") or category_node.get("display_name")
        description = category_node.get("description")

    display_name = name or category_id
    if list_key == "exclude_keywords":
        display_name = f"{display_name} — excludes"
    group_label = parts[0].capitalize() if parts else None
    return display_name, group_label, description


def build_catalog(yaml_dir: Optional[Path] = None) -> list[dict]:
    """Return the editable-bucket catalog grouped by axis.

    Each axis dict contains ``axis``, ``label``, ``description`` and
    ``buckets``: list of ``{axis, bucket_path, display_name, group_label,
    description, defaults}``.
    """
    docs = load_all_yamls(yaml_dir)
    out: list[dict] = []
    for axis, axis_cfg in EDITABLE_BUCKETS.items():
        doc = docs.get(axis_cfg["file"], {})
        resolved_paths = expand_patterns(doc, axis_cfg["patterns"])
        buckets: list[dict] = []
        for path in resolved_paths:
            defaults = get_by_path(doc, path)
            if not isinstance(defaults, list):
                continue
            display_name, group_label, description = _display_name_for_bucket(
                axis, path, doc
            )
            buckets.append({
                "axis": axis,
                "bucket_path": path,
                "display_name": display_name,
                "group_label": group_label,
                "description": description,
                "defaults": list(defaults),
            })
        buckets.sort(key=lambda b: (b.get("group_label") or "", b["display_name"]))
        out.append({
            "axis": axis,
            "label": axis_cfg["label"],
            "description": axis_cfg.get("description"),
            "buckets": buckets,
        })
    return out


def is_valid_bucket(axis: str, bucket_path: str, yaml_dir: Optional[Path] = None) -> bool:
    """Return True if (axis, bucket_path) is in the catalog of editable buckets."""
    axis_cfg = EDITABLE_BUCKETS.get(axis)
    if axis_cfg is None:
        return False
    docs = load_all_yamls(yaml_dir)
    doc = docs.get(axis_cfg["file"], {})
    resolved = expand_patterns(doc, axis_cfg["patterns"])
    return bucket_path in resolved
