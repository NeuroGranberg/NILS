"""Audit export utilities."""

from __future__ import annotations

import io
import re
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Set, Tuple

import polars as pl
from openpyxl import Workbook
import msoffcrypto


class StudyAuditAggregator:
    """Incrementally aggregate per-tag audit events by study."""

    _TRACKED_VALUE_TAGS = {"(0010,0020)", "(0008,0020)"}

    def __init__(self, source_root: Path, cohort_name: str) -> None:
        self._source_root = source_root.resolve()
        self._cohort_name = cohort_name or "cohort"
        self._session_meta: Dict[str, Dict[str, Optional[str]]] = {}
        self._tag_changes: DefaultDict[str, Dict[str, Dict[str, Optional[str]]]] = DefaultDict(dict)
        self._all_tags: Set[Tuple[str, str]] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_event(self, event: dict) -> None:
        rel_path = event.get("rel_path") or ""
        study_uid = event.get("study_uid") or ""
        tag_code = event.get("tag") or ""
        tag_name = event.get("tag_name") or ""
        action = event.get("action") or ""
        old_value = self._normalize_value(event.get("old_value"))
        new_value = self._normalize_value(event.get("new_value"))

        if not tag_code:
            return

        key = study_uid or f"__no_uid__::{rel_path}"
        meta = self._session_meta.setdefault(
            key,
            {"study_uid": study_uid or None, "rel_path": rel_path or None},
        )
        if not meta.get("rel_path") and rel_path:
            meta["rel_path"] = rel_path

        tag_entries = self._tag_changes.setdefault(key, {})
        entry = tag_entries.setdefault(
            tag_code,
            {"tag_name": tag_name, "old_value": None, "new_value": None},
        )

        if action == "removed":
            if old_value:
                entry["old_value"] = entry.get("old_value") or old_value
        elif action in {"replaced", "added"}:
            if old_value and not entry.get("old_value"):
                entry["old_value"] = old_value
            if new_value:
                entry["new_value"] = new_value
        elif action == "retained":
            if old_value and not entry.get("old_value"):
                entry["old_value"] = old_value

        if not entry.get("tag_name") and tag_name:
            entry["tag_name"] = tag_name

        self._all_tags.add((tag_code, entry["tag_name"] or tag_name))

    def add_events(self, events: Iterable[dict]) -> None:
        for event in events:
            self.add_event(event)

    def is_empty(self) -> bool:
        return not self._session_meta

    def build_dataframe(self) -> pl.DataFrame:
        if not self._session_meta:
            return pl.DataFrame([])

        ordered_tags = sorted(
            self._all_tags,
            key=lambda item: (item[0], self._sanitize_label(item[1] or "")),
        )

        static_columns = ["study_uid", "rel_path", "DataFolder", "ParentFolder", "SubFolder"]
        dynamic_columns: List[str] = []
        tag_to_columns: Dict[str, List[str]] = {}

        for tag_code, tag_name in ordered_tags:
            prefix = self._tag_column_prefix(tag_code, tag_name or "")
            if tag_code in self._TRACKED_VALUE_TAGS:
                cols = [f"{prefix}_old_value", f"{prefix}_new_value"]
            else:
                cols = [prefix]
            tag_to_columns[tag_code] = cols
            dynamic_columns.extend(cols)

        column_order = static_columns + dynamic_columns
        rows_out: List[Dict[str, Optional[str]]] = []

        for key in sorted(self._session_meta.keys()):
            meta = self._session_meta[key]
            rel_path = meta.get("rel_path")
            parent, sub = self._parent_folders(rel_path)

            row: Dict[str, Optional[str]] = {col: None for col in column_order}
            row["study_uid"] = meta.get("study_uid")
            row["rel_path"] = rel_path
            row["DataFolder"] = self._data_folder
            row["ParentFolder"] = parent
            row["SubFolder"] = sub

            tag_entries = self._tag_changes.get(key, {})
            for tag_code, _tag_name in ordered_tags:
                entry = tag_entries.get(tag_code)
                if not entry:
                    continue
                columns = tag_to_columns[tag_code]
                if tag_code in self._TRACKED_VALUE_TAGS:
                    row[columns[0]] = entry.get("old_value")
                    row[columns[1]] = entry.get("new_value")
                else:
                    value = entry.get("old_value") or entry.get("new_value")
                    row[columns[0]] = value

            rows_out.append(row)

        if not rows_out:
            return pl.DataFrame([])

        non_empty_columns: Set[str] = set(static_columns)
        for row in rows_out:
            for col in dynamic_columns:
                value = row.get(col)
                if value is None:
                    continue
                if isinstance(value, str):
                    if value.strip():
                        non_empty_columns.add(col)
                else:
                    non_empty_columns.add(col)

        selected_columns = [col for col in column_order if col in non_empty_columns]
        schema_overrides = {col: pl.Utf8 for col in column_order}
        return (
            pl.DataFrame(rows_out, schema_overrides=schema_overrides, infer_schema_length=len(rows_out))
            .select(selected_columns)
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def _data_folder(self) -> str:
        return self._source_root.name or self._source_root.parent.name

    @staticmethod
    def _sanitize_label(text: str) -> str:
        if not text:
            return "Tag"
        cleaned = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
        return cleaned or "Tag"

    def _tag_column_prefix(self, tag_code: str, tag_name: str) -> str:
        code = self._sanitize_label(tag_code)
        name_part = self._sanitize_label(tag_name)
        return f"{name_part}_{code}"

    @staticmethod
    def _normalize_value(value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = " | ".join(value.splitlines()).strip()
            return stripped or None
        text = str(value)
        stripped = " | ".join(text.splitlines()).strip()
        return stripped or None

    @staticmethod
    def _parent_folders(rel_path: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not rel_path:
            return None, None
        parts = [p for p in rel_path.split("/") if p]
        parent = parts[0] if len(parts) >= 1 else None
        sub = parts[1] if len(parts) >= 2 else None
        return parent, sub


def process_and_aggregate_audit(
    rows: Iterable[dict],
    source_root: Path,
    cohort_name: str,
) -> pl.DataFrame:
    """Aggregate per-tag audit events into the reference workbook layout."""

    aggregator = StudyAuditAggregator(source_root, cohort_name)
    aggregator.add_events(rows)
    return aggregator.build_dataframe()


def build_dataframe(rows: Iterable[dict]) -> pl.DataFrame:
    """Build raw dataframe without aggregation."""
    return pl.DataFrame(list(rows), infer_schema_length=500000) if rows else pl.DataFrame([])


def export_csv(df: pl.DataFrame, path: Path) -> Path:
    """Export dataframe to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(path)
    return path


def export_encrypted_excel(df: pl.DataFrame, path: Path, password: str) -> Path:
    """Export dataframe to password-protected Excel."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Audit"

    if df.is_empty():
        wb.save(path)
        return path

    ws.append(df.columns)
    for row in df.to_numpy():
        ws.append(list(row))

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    encrypted = io.BytesIO()
    office_file = msoffcrypto.OfficeFile(buffer)
    office_file.encrypt(password, encrypted)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as fh:
        fh.write(encrypted.getvalue())
    return path
