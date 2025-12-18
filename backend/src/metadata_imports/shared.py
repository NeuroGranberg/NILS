from __future__ import annotations

import csv
import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Sequence

from dateutil import parser as date_parser
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy.engine import Connection


ParserName = Literal["string", "int", "float", "bool", "date"]


@dataclass(frozen=True)
class FieldDefinition:
    name: str
    label: str
    required: bool = False
    description: str | None = None
    default_parser: ParserName = "string"
    parsers: tuple[ParserName, ...] = ("string",)


class FieldMapping(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    column: str | None = Field(default=None, alias="column")
    default: str | None = Field(default=None, alias="default")
    parser: ParserName | None = Field(default=None, alias="parser")

    @model_validator(mode="after")
    def _validate_source(self) -> "FieldMapping":
        column = (self.column or "").strip()
        default = self.default
        if not column and (default is None or (isinstance(default, str) and default.strip() == "")):
            raise ValueError("Either column or default must be provided")
        self.column = column or None
        if isinstance(default, str):
            stripped = default.strip()
            self.default = stripped if stripped else None
        return self


def parse_bool(value: str) -> int:
    lowered = value.strip().lower()
    truthy = {"1", "true", "t", "yes", "y"}
    falsy = {"0", "false", "f", "no", "n"}
    if lowered in truthy:
        return 1
    if lowered in falsy:
        return 0
    try:
        return 1 if float(lowered) != 0 else 0
    except ValueError as exc:
        raise ValueError(f"Cannot parse boolean from '{value}'") from exc


def normalize_birth_date(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("Cannot parse date from empty value")

    normalized = parse_known_date_formats(raw)
    if normalized:
        return normalized

    for kwargs in ({"dayfirst": False, "yearfirst": True}, {"dayfirst": True, "yearfirst": False}):
        try:
            parsed = date_parser.parse(raw, **kwargs)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(dt.timezone.utc)
            return parsed.date().isoformat()
        except (ValueError, OverflowError):
            continue

    raise ValueError(f"Cannot parse date from '{value}'")


def parse_date(value: str) -> str:
    return normalize_birth_date(value)


_EXPLICIT_DATE_FORMATS: tuple[str, ...] = (
    "%Y%m%d",
    "%d%m%Y",
    "%m%d%Y",
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y.%m.%d",
    "%d.%m.%Y",
    "%m.%d.%Y",
    "%Y_%m_%d",
    "%d_%m_%Y",
    "%m_%d_%Y",
    "%Y %m %d",
    "%d %m %Y",
    "%m %d %Y",
    "%Y/%d/%m",
    "%Y-%d-%m",
    "%Y.%d.%m",
)


def parse_known_date_formats(value: str) -> str | None:
    digits_only = re.fullmatch(r"\d{8}", value)
    candidates = list(_EXPLICIT_DATE_FORMATS)
    raw_inputs = [value]
    if digits_only:
        raw_inputs.insert(0, value)
    else:
        sanitized = re.sub(r"[\.\/_\\]", "-", value)
        collapsed = re.sub(r"\s+", "-", sanitized)
        if collapsed != value:
            raw_inputs.append(collapsed)

    for raw in raw_inputs:
        for fmt in candidates:
            try:
                parsed = dt.datetime.strptime(raw, fmt)
                return parsed.date().isoformat()
            except ValueError:
                continue
    return None


PARSERS: dict[ParserName, Callable[[str], Any]] = {
    "string": lambda raw: raw.strip(),
    "int": lambda raw: int(raw.strip()),
    "float": lambda raw: float(raw.strip()),
    "bool": parse_bool,
    "date": parse_date,
}


def apply_parser(value: str, parser_name: ParserName) -> Any:
    parser = PARSERS[parser_name]
    return parser(value)


def normalize_headers(fieldnames: Sequence[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for name in fieldnames:
        mapping[name] = name
        stripped = name.strip()
        if stripped not in mapping:
            mapping[stripped] = name
    return mapping


def resolve_column(row: dict[str, Any], column: str, aliases: dict[str, str]) -> str | None:
    if not column:
        return None
    actual = aliases.get(column, column)
    if actual in row:
        return row.get(actual)
    return row.get(column)


def coerce_value(
    *,
    field: FieldDefinition,
    mapping: FieldMapping,
    row: dict[str, Any],
    aliases: dict[str, str],
    warnings: list[str],
    row_number: int,
) -> Any:
    raw: str | None = None
    if mapping.column:
        raw = resolve_column(row, mapping.column, aliases)
    if raw is None or str(raw).strip() == "":
        raw = mapping.default
    if raw is None or str(raw).strip() == "":
        return None
    parser_name = mapping.parser or field.default_parser
    if parser_name not in field.parsers:
        warnings.append(
            f"Row {row_number}: parser '{parser_name}' not allowed for field '{field.label}', using default"
        )
        parser_name = field.default_parser
    try:
        coerced = apply_parser(str(raw), parser_name)
    except ValueError as exc:
        warnings.append(f"Row {row_number}: {exc}")
        return None
    return coerced


def iter_csv_rows(path: Path) -> Iterator[tuple[dict[str, Any], dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        aliases = normalize_headers(reader.fieldnames or [])
        for raw in reader:
            row = {aliases.get(key, key): value for key, value in raw.items()}
            yield row, aliases


def copy_rows(
    conn: Connection,
    *,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]] | Sequence[dict[str, Any]],
) -> None:
    if not rows:
        return

    column_list = ", ".join(columns)
    raw = conn.connection
    dbapi_conn = getattr(raw, "driver_connection", raw)
    cursor = dbapi_conn.cursor()
    try:
        copy_expert = getattr(cursor, "copy_expert", None)
        if copy_expert is not None:
            import io

            buffer = io.StringIO()
            writer = csv.writer(buffer)
            for row in rows:
                if isinstance(row, dict):
                    writer.writerow([row.get(column) for column in columns])
                else:
                    writer.writerow(row)
            buffer.seek(0)
            copy_expert(f"COPY {table} ({column_list}) FROM STDIN WITH CSV", buffer)
        else:
            paramstyle = getattr(conn.dialect, "paramstyle", "pyformat")
            if paramstyle in {"qmark", "numeric"}:
                placeholder_token = "?"
                use_named = False
            elif paramstyle in {"format", "pyformat"}:
                placeholder_token = "%s"
                use_named = False
            else:
                placeholder_token = None
                use_named = True

            if use_named:
                placeholders = ", ".join(f":{column}" for column in columns)
                statement = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
                named_rows: list[dict[str, Any]] = []
                for row in rows:
                    if isinstance(row, dict):
                        named_rows.append({column: row.get(column) for column in columns})
                    else:
                        named_rows.append(
                            {column: row[idx] if idx < len(row) else None for idx, column in enumerate(columns)}
                        )
                cursor.executemany(statement, named_rows)
            else:
                placeholders = ", ".join([placeholder_token] * len(columns))
                statement = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
                positional_rows: list[tuple[Any, ...]] = []
                for row in rows:
                    if isinstance(row, dict):
                        positional_rows.append(tuple(row.get(column) for column in columns))
                    else:
                        positional_rows.append(tuple(row))
                cursor.executemany(statement, positional_rows)
    finally:
        cursor.close()


__all__ = [
    "ParserName",
    "FieldDefinition",
    "FieldMapping",
    "PARSERS",
    "apply_parser",
    "coerce_value",
    "copy_rows",
    "iter_csv_rows",
    "normalize_birth_date",
    "normalize_headers",
    "parse_bool",
    "parse_date",
    "parse_known_date_formats",
    "resolve_column",
]
