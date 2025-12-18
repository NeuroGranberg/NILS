"""Schema bootstrap helpers for the metadata database."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from sqlalchemy import Date, inspect, select, text

from dateutil import parser as date_parser

from .schema import Base, SchemaVersion
from .session import engine, session_scope

logger = logging.getLogger(__name__)


def _needs_datetime_migration(connection) -> bool:
    """Check if the datetime migration needs to be applied (e.g., after restoring old backup)."""
    inspector = inspect(connection)
    if "instance" not in inspector.get_table_names():
        return False
    
    columns = inspector.get_columns("instance")
    for col in columns:
        if col["name"] == "acquisition_date":
            col_type = str(col["type"]).upper()
            # If it's still TEXT, migration is needed
            if "TEXT" in col_type or "VARCHAR" in col_type:
                return True
            # If it's DATE, migration already done
            if "DATE" in col_type:
                return False
    return False


def _run_datetime_migration(connection) -> None:
    """Run the datetime column migration."""
    from .migrations.migrate_datetime import run_migration
    
    logger.info("Detected Text date/time columns - running automatic migration...")
    try:
        results = run_migration(engine, dry_run=False)
        if results["success"]:
            logger.info(
                "DateTime migration completed: %d columns in %d tables (%.1fs)",
                results["columns_migrated"],
                len(results["tables_migrated"]),
                results["elapsed_seconds"],
            )
        elif results["already_migrated"]:
            logger.info("DateTime migration already applied")
    except Exception as exc:
        logger.error("DateTime migration failed: %s", exc)
        raise


def _needs_instance_stack_fields_migration(connection) -> bool:
    """Check if the instance stack fields migration needs to be applied."""
    inspector = inspect(connection)
    if "instance" not in inspector.get_table_names():
        return False
    
    columns = inspector.get_columns("instance")
    column_names = {col["name"] for col in columns}
    # Check if echo_time column exists (one of the stack-defining fields)
    return "echo_time" not in column_names


def _run_instance_stack_fields_migration(connection) -> None:
    """Run the instance stack fields migration."""
    from .migrations.migrate_instance_stack_fields import run_migration
    
    logger.info("Detected missing instance stack fields - running automatic migration...")
    try:
        results = run_migration(engine, dry_run=False)
        if results["success"]:
            logger.info(
                "Instance stack fields migration completed: %d columns added (%.1fs)",
                results["columns_added"],
                results["elapsed_seconds"],
            )
        elif results["already_migrated"]:
            logger.info("Instance stack fields migration already applied")
    except Exception as exc:
        logger.error("Instance stack fields migration failed: %s", exc)
        raise


def _needs_stack_key_nullable_migration(connection) -> bool:
    """Check if the stack_key nullable migration needs to be applied."""
    from .migrations.migrate_stack_key_nullable import _needs_migration
    return _needs_migration(connection)


def _run_stack_key_nullable_migration(connection) -> None:
    """Run the stack_key nullable migration."""
    from .migrations.migrate_stack_key_nullable import run_migration
    
    logger.info("Detected stack_key NOT NULL constraint - running automatic migration...")
    try:
        results = run_migration(engine, dry_run=False)
        if results["success"]:
            logger.info(
                "stack_key nullable migration completed: %s (%.1fs)",
                ", ".join(results["changes_made"]),
                results["elapsed_seconds"],
            )
        elif results["already_migrated"]:
            logger.info("stack_key nullable migration already applied")
    except Exception as exc:
        logger.error("stack_key nullable migration failed: %s", exc)
        raise


def _needs_orientation_confidence_migration(connection) -> bool:
    """Check if the orientation confidence migration needs to be applied."""
    from .migrations.add_orientation_confidence import _needs_migration
    return _needs_migration(connection)


def _run_orientation_confidence_migration(connection) -> None:
    """Run the orientation confidence migration."""
    from .migrations.add_orientation_confidence import migrate
    
    logger.info("Detected missing stack_orientation_confidence column - running automatic migration...")
    try:
        results = migrate(engine, dry_run=False)
        if results["success"]:
            logger.info(
                "Orientation confidence migration completed: %s (%.1fs)",
                ", ".join(results["changes_made"]),
                results["elapsed_seconds"],
            )
    except Exception as exc:
        logger.error("Orientation confidence migration failed: %s", exc)
        raise


def _needs_drop_instance_stack_fields_migration(connection) -> bool:
    """Check if the instance stack fields need to be dropped (restored old backup)."""
    from .migrations.migrate_drop_instance_stack_fields import _needs_migration
    return _needs_migration(connection)


def _run_drop_instance_stack_fields_migration(connection) -> None:
    """Run the migration to drop stack fields from instance table."""
    from .migrations.migrate_drop_instance_stack_fields import run_migration
    
    logger.info("Detected stack fields on instance table - running automatic migration to drop them...")
    try:
        results = run_migration(engine, dry_run=False)
        if results["success"]:
            logger.info(
                "Drop instance stack fields migration completed: %d columns dropped (%.1fs)",
                results["columns_dropped"],
                results["elapsed_seconds"],
            )
        elif results["already_migrated"]:
            logger.info("Drop instance stack fields migration already applied")
    except Exception as exc:
        logger.error("Drop instance stack fields migration failed: %s", exc)
        raise


def _needs_rename_derived_to_construct_migration(connection) -> bool:
    """Check if derived_csv needs to be renamed to construct_csv (restored old backup)."""
    from .migrations.rename_derived_to_construct import _needs_migration
    return _needs_migration(connection)


def _run_rename_derived_to_construct_migration(connection) -> None:
    """Run the migration to rename derived_csv to construct_csv."""
    from .migrations.rename_derived_to_construct import migrate
    
    logger.info("Detected derived_csv column - running automatic migration to rename to construct_csv...")
    try:
        results = migrate(engine, dry_run=False)
        if results["success"]:
            logger.info(
                "Rename derived_csv to construct_csv migration completed: %s (%.1fs)",
                ", ".join(results["changes_made"]) if results["changes_made"] else "no changes needed",
                results["elapsed_seconds"],
            )
    except Exception as exc:
        logger.error("Rename derived_csv to construct_csv migration failed: %s", exc)
        raise


def _needs_rename_provenance_csv_to_provenance_migration(connection) -> bool:
    """Check if provenance_csv needs to be renamed to provenance (restored old backup)."""
    from .migrations.rename_provenance_csv_to_provenance import _needs_migration
    return _needs_migration(connection)


def _run_rename_provenance_csv_to_provenance_migration(connection) -> None:
    """Run the migration to rename provenance_csv to provenance."""
    from .migrations.rename_provenance_csv_to_provenance import migrate

    logger.info("Detected provenance_csv column - running automatic migration to rename to provenance...")
    try:
        results = migrate(engine, dry_run=False)
        if results["success"]:
            logger.info(
                "Rename provenance_csv to provenance migration completed: %s (%.1fs)",
                ", ".join(results["changes_made"]) if results["changes_made"] else "no changes needed",
                results["elapsed_seconds"],
            )
    except Exception as exc:
        logger.error("Rename provenance_csv to provenance migration failed: %s", exc)
        raise


def _needs_performance_indexes_migration(connection) -> bool:
    """Check if performance indexes need to be added."""
    from .migrations.add_performance_indexes import _needs_migration
    return _needs_migration(connection)


def _run_performance_indexes_migration(connection) -> None:
    """Run the migration to add performance indexes."""
    from .migrations.add_performance_indexes import migrate

    logger.info("Adding performance indexes for DICOM viewer optimization...")
    try:
        results = migrate(engine, dry_run=False)
        if results["success"]:
            if results["already_migrated"]:
                logger.info("Performance indexes already exist")
            else:
                logger.info(
                    "Performance indexes migration completed: %d created, %d skipped (%.1fs)",
                    len(results["indexes_created"]),
                    len(results["indexes_skipped"]),
                    results["elapsed_seconds"],
                )
    except Exception as exc:
        logger.error("Performance indexes migration failed: %s", exc)
        raise


SCHEMA_VERSION = "1.2.0"
SCHEMA_SQL_PATH = Path(__file__).resolve().parents[3] / "resource" / "sql" / "metadata_schema.sql"

DEPRECATED_TABLES = ("instance_header", "contrast_agents")


def _iter_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            chunk = "\n".join(buffer).rstrip(";\n ")
            if chunk:
                statements.append(chunk)
            buffer = []
    if buffer:
        chunk = "\n".join(buffer).rstrip(";\n ")
        if chunk:
            statements.append(chunk)
    return statements


def _apply_reference_schema() -> None:
    if not SCHEMA_SQL_PATH.exists():
        return

    sql_text = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    statements = _iter_statements(sql_text)
    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            if statement.lower().startswith("pragma"):
                continue
            connection.exec_driver_sql(statement)


def _database_is_empty() -> bool:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if not table_names:
        return True
    if table_names == ["schema_version"]:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM schema_version"))
            count = result.scalar() or 0
        return count == 0
    return False


def _drop_deprecated_tables() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    targets = existing_tables.intersection(DEPRECATED_TABLES)
    if not targets:
        return

    with engine.begin() as connection:
        dialect = connection.dialect.name
        for table_name in targets:
            if dialect == "postgresql":
                connection.exec_driver_sql(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
            else:
                connection.exec_driver_sql(f'DROP TABLE IF EXISTS "{table_name}"')


def _normalize_birth_date(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    if not stripped:
        return None
    try:
        from metadata_imports.subjects import normalize_birth_date as normalize_from_import
    except ImportError:  # pragma: no cover - should not happen in runtime
        normalize_from_import = None

    if normalize_from_import:
        try:
            return normalize_from_import(stripped)
        except ValueError:
            pass

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            parsed = datetime.strptime(stripped, fmt)
            return parsed.date().isoformat()
        except ValueError:
            continue

    for kwargs in ({"dayfirst": False, "yearfirst": True}, {"dayfirst": True, "yearfirst": False}):
        try:
            parsed = date_parser.parse(stripped, **kwargs)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.date().isoformat()
        except (ValueError, OverflowError):
            continue

    return None


def _normalize_existing_birth_dates(connection) -> None:
    if "subject" not in inspect(connection).get_table_names():
        return
    rows = connection.execute(text("SELECT subject_id, patient_birth_date FROM subject")).mappings().all()
    if not rows:
        return
    update_sql = text("UPDATE subject SET patient_birth_date = :birth_date WHERE subject_id = :subject_id")
    for row in rows:
        normalized = _normalize_birth_date(row["patient_birth_date"])
        connection.execute(update_sql, {"birth_date": normalized, "subject_id": row["subject_id"]})


def _rebuild_subject_table_sqlite(connection) -> None:
    inspector = inspect(connection)
    subject_columns = inspector.get_columns("subject")
    column_names = {column["name"] for column in subject_columns}
    birth_date_column = next((column for column in subject_columns if column["name"] == "patient_birth_date"), None)
    needs_rebuild = (
        "birth_year" in column_names
        or birth_date_column is None
        or not isinstance(birth_date_column["type"], Date)
    )
    if not needs_rebuild:
        return

    rows = connection.execute(
        text(
            """
            SELECT subject_id, subject_code, patient_name, patient_birth_date, patient_sex,
                   ethnic_group, occupation, additional_patient_history, is_active,
                   created_at, updated_at
            FROM subject
            """
        )
    ).mappings().all()

    connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
    connection.exec_driver_sql("ALTER TABLE subject RENAME TO subject_old")
    connection.exec_driver_sql(
        """
        CREATE TABLE subject (
            subject_id INTEGER PRIMARY KEY,
            subject_code TEXT UNIQUE NOT NULL,
            patient_name TEXT,
            patient_birth_date DATE,
            patient_sex TEXT,
            ethnic_group TEXT,
            occupation TEXT,
            additional_patient_history TEXT,
            is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_subject_is_active ON subject(is_active)")

    insert_sql = text(
        """
        INSERT INTO subject (
            subject_id,
            subject_code,
            patient_name,
            patient_birth_date,
            patient_sex,
            ethnic_group,
            occupation,
            additional_patient_history,
            is_active,
            created_at,
            updated_at
        ) VALUES (
            :subject_id,
            :subject_code,
            :patient_name,
            :patient_birth_date,
            :patient_sex,
            :ethnic_group,
            :occupation,
            :additional_patient_history,
            :is_active,
            :created_at,
            :updated_at
        )
        """
    )

    for row in rows:
        normalized = _normalize_birth_date(row["patient_birth_date"])
        params = dict(row)
        params["patient_birth_date"] = normalized
        connection.execute(insert_sql, params)

    connection.exec_driver_sql("DROP TABLE subject_old")
    connection.exec_driver_sql("PRAGMA foreign_keys=ON")


def _upgrade_subject_table(connection) -> None:
    inspector = inspect(connection)
    if "subject" not in inspector.get_table_names():
        return

    columns = inspector.get_columns("subject")
    column_names = {column["name"] for column in columns}
    birth_year_present = "birth_year" in column_names
    birth_date_column = next((column for column in columns if column["name"] == "patient_birth_date"), None)
    birth_date_is_date = isinstance(birth_date_column["type"], Date) if birth_date_column else False

    if not birth_year_present and birth_date_is_date:
        return

    _normalize_existing_birth_dates(connection)

    if connection.dialect.name == "sqlite":
        _rebuild_subject_table_sqlite(connection)
    else:
        if birth_year_present:
            connection.exec_driver_sql("ALTER TABLE subject DROP COLUMN IF EXISTS birth_year")
        if birth_date_column and not birth_date_is_date:
            connection.exec_driver_sql(
                "ALTER TABLE subject ALTER COLUMN patient_birth_date TYPE DATE USING NULLIF(patient_birth_date, '')::date"
            )


def _apply_schema_upgrades() -> None:
    with engine.begin() as connection:
        _upgrade_subject_table(connection)
    
    # Check and apply datetime migration if needed (e.g., after restoring old backup)
    with engine.connect() as connection:
        if _needs_datetime_migration(connection):
            _run_datetime_migration(connection)
    
    # Check and apply instance stack fields migration if needed
    with engine.connect() as connection:
        if _needs_instance_stack_fields_migration(connection):
            _run_instance_stack_fields_migration(connection)
    
    # Check and apply stack_key nullable migration if needed
    with engine.connect() as connection:
        if _needs_stack_key_nullable_migration(connection):
            _run_stack_key_nullable_migration(connection)
    
    # Check and apply orientation confidence migration if needed
    with engine.connect() as connection:
        if _needs_orientation_confidence_migration(connection):
            _run_orientation_confidence_migration(connection)
    
    # Check and drop instance stack fields if present (restored old backup)
    with engine.connect() as connection:
        if _needs_drop_instance_stack_fields_migration(connection):
            _run_drop_instance_stack_fields_migration(connection)
    
    # Check and rename derived_csv to construct_csv if needed (restored old backup)
    with engine.connect() as connection:
        if _needs_rename_derived_to_construct_migration(connection):
            _run_rename_derived_to_construct_migration(connection)
    
    # Check and rename provenance_csv to provenance if needed (restored old backup)
    with engine.connect() as connection:
        if _needs_rename_provenance_csv_to_provenance_migration(connection):
            _run_rename_provenance_csv_to_provenance_migration(connection)

    # Add performance indexes for DICOM viewer optimization
    with engine.connect() as connection:
        if _needs_performance_indexes_migration(connection):
            _run_performance_indexes_migration(connection)


def ensure_schema() -> str:
    _drop_deprecated_tables()
    Base.metadata.create_all(engine)
    _apply_schema_upgrades()

    with session_scope() as session:
        existing_version = session.execute(select(SchemaVersion).limit(1)).scalar_one_or_none()
        if existing_version and existing_version.version == SCHEMA_VERSION:
            return existing_version.version

    _apply_reference_schema()

    with session_scope() as session:
        version_row = session.execute(select(SchemaVersion).where(SchemaVersion.version == SCHEMA_VERSION)).scalar_one_or_none()
        if version_row:
            return version_row.version
        session.add(SchemaVersion(version=SCHEMA_VERSION))
        return SCHEMA_VERSION


def bootstrap(auto_restore: bool | None = None) -> str:
    from .backup import MetadataBackupManager

    manager = MetadataBackupManager()
    target_auto_restore = manager.backup_settings.auto_restore if auto_restore is None else auto_restore
    if target_auto_restore:
        if manager.auto_restore_if_empty(_database_is_empty()):
            logger.info("Metadata auto-restore applied from latest backup")
        else:
            logger.info("Metadata auto-restore requested but no backup was restored")
    return ensure_schema()
