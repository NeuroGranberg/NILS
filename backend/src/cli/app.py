"""Typer application entrypoint."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, List

import typer
from rich import print as rprint
from rich.table import Table

from anonymize import run_anonymization
from anonymize.config import (
    AnonymizeConfig,
    AnonymizeResult,
    DerivativesStatus,
    clean_dcm_raw,
    load_config,
    setup_derivatives_folders,
)
from compress.config import CompressionConfig
from compress.engine import run_compression, build_chunk_plan, bytes_to_human
from cohorts.service import cohort_service
from extract import DuplicatePolicy, ExtensionMode, ExtractionConfig, run_extraction
from extract.progress import ExtractionProgressTracker
from extract.subject_mapping import load_subject_code_csv
from jobs.runner import run_anonymize_job, run_compress_job
from jobs.service import job_service
from metadata_db.backup import BackupError, MetadataBackupManager
from metadata_db.lifecycle import bootstrap as metadata_bootstrap
from metadata_db.migrations.migrate_datetime import run_migration, check_migration_status
from metadata_db.migrations.migrate_instance_stack_fields import (
    run_migration as run_stack_migration,
    check_migration_status as check_stack_migration_status,
)
from metadata_db.session import engine as metadata_engine
from logging_config import configure_logging


configure_logging()


app = typer.Typer(help="Neuroimaging toolkit backend CLI")
anonymize_app = typer.Typer(help="Run anonymization workflows")
compress_app = typer.Typer(help="Compress anonymized datasets")
metadata_app = typer.Typer(help="Manage DICOM metadata database")

app.add_typer(anonymize_app, name="anonymize")
app.add_typer(compress_app, name="compress")
app.add_typer(metadata_app, name="metadata")


@metadata_app.command("init")
def metadata_init(auto_restore: bool = typer.Option(True, help="Attempt auto-restore from latest backup")) -> None:
    version = metadata_bootstrap(auto_restore=auto_restore)
    typer.echo(f"Metadata schema ready (version {version}).")


@metadata_app.command("backup")
def metadata_backup() -> None:
    manager = MetadataBackupManager()
    try:
        path = manager.create_backup()
    except BackupError as exc:
        typer.echo(f"Backup failed: {exc}")
        raise typer.Exit(code=1)
    typer.echo(f"Backup created at {path}")


@metadata_app.command("restore")
def metadata_restore(file: Path = typer.Option(None, help="Path to dump; defaults to latest backup")) -> None:
    manager = MetadataBackupManager()
    dump = file
    if dump is None:
        dump = manager.latest_backup()
        if dump is None:
            typer.echo("No backups found.")
            raise typer.Exit(code=1)
    dump = dump.resolve()
    try:
        manager.restore(dump)
    except BackupError as exc:
        typer.echo(f"Restore failed: {exc}")
        raise typer.Exit(code=1)
    typer.echo(f"Restore completed from {dump}")


@metadata_app.command("list")
def metadata_list() -> None:
    manager = MetadataBackupManager()
    backups = list(manager.list_backups())
    if not backups:
        typer.echo("No backups found.")
        return
    for dump in backups:
        typer.echo(str(dump.resolve()))


@metadata_app.command("migrate-datetime")
def metadata_migrate_datetime(
    dry_run: bool = typer.Option(False, "--dry-run", help="Run migration without committing changes"),
    status_only: bool = typer.Option(False, "--status", help="Check migration status without running"),
) -> None:
    """
    One-time migration to convert Text date/time columns to proper DATE/TIME types.
    
    This converts DICOM date columns (YYYYMMDD) and time columns (HHMMSS) to proper
    PostgreSQL DATE and TIME types. Run this once after updating to avoid re-ingesting
    millions of DICOM files.
    """
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    if status_only:
        status = check_migration_status(metadata_engine)
        if status["migrated"]:
            typer.echo("✅ Migration already applied")
        else:
            typer.echo("⚠️  Migration not yet applied")
        
        typer.echo("\nDATE columns:")
        for col, col_type in status["date_columns"].items():
            marker = "✅" if col_type and "DATE" in col_type and "TEXT" not in col_type else "❌"
            typer.echo(f"  {marker} {col}: {col_type}")
        
        typer.echo("\nTIME columns:")
        for col, col_type in status["time_columns"].items():
            marker = "✅" if col_type and "TIME" in col_type and "TEXT" not in col_type else "❌"
            typer.echo(f"  {marker} {col}: {col_type}")
        return
    
    typer.echo("=" * 60)
    typer.echo("Date/Time Column Migration")
    typer.echo("=" * 60)
    
    if dry_run:
        typer.echo("Mode: DRY RUN (changes will not be committed)")
    else:
        typer.echo("Mode: LIVE (changes will be committed)")
    
    typer.echo("")
    
    try:
        results = run_migration(metadata_engine, dry_run=dry_run)
    except Exception as exc:
        typer.echo(f"\n❌ Migration failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    
    if results["already_migrated"]:
        typer.echo("✅ Migration already applied - no changes needed")
        return
    
    if results["success"]:
        typer.echo("")
        typer.echo("=" * 60)
        typer.echo("Migration Results")
        typer.echo("=" * 60)
        typer.echo(f"Tables migrated: {len(results['tables_migrated'])}")
        typer.echo(f"Columns migrated: {results['columns_migrated']}")
        typer.echo(f"Duration: {results['elapsed_seconds']:.1f} seconds")
        
        if dry_run:
            typer.echo("\n⚠️  DRY RUN - Changes were rolled back")
            typer.echo("Run without --dry-run to apply changes")
        else:
            typer.echo("\n✅ Migration completed successfully!")
            typer.echo("\nRecommended next step:")
            typer.echo("  neuro-api metadata backup")
    else:
        typer.echo(f"\n❌ Migration failed: {results.get('error', 'Unknown error')}", err=True)
        raise typer.Exit(code=1)


@metadata_app.command("migrate-instance-stack-fields")
def metadata_migrate_instance_stack_fields(
    dry_run: bool = typer.Option(False, "--dry-run", help="Run migration without committing changes"),
    status_only: bool = typer.Option(False, "--status", help="Check migration status without running"),
) -> None:
    """
    Add stack-defining fields to the instance table for per-stack classification.
    
    This adds columns for MR (echo_time, echo_number, etc.), CT (kvp, tube_current),
    and PET (pet_bed_index, pet_frame_type) fields to enable grouping instances
    into stacks within a SeriesInstanceUID.
    """
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    if status_only:
        status = check_stack_migration_status(metadata_engine)
        if status["migrated"]:
            typer.echo("✅ Instance stack fields migration already applied")
        else:
            typer.echo("⚠️  Instance stack fields migration not yet applied")
        
        typer.echo("\nInstance stack columns:")
        for col, info in status["instance_columns"].items():
            marker = "✅" if info["present"] else "❌"
            typer.echo(f"  {marker} {col}: {info['type']}")
        
        if status["missing_columns"]:
            typer.echo(f"\nMissing columns: {', '.join(status['missing_columns'])}")
        return
    
    typer.echo("=" * 60)
    typer.echo("Instance Stack Fields Migration")
    typer.echo("=" * 60)
    
    if dry_run:
        typer.echo("Mode: DRY RUN (changes will not be committed)")
    else:
        typer.echo("Mode: LIVE (changes will be committed)")
    
    typer.echo("")
    
    try:
        results = run_stack_migration(metadata_engine, dry_run=dry_run)
    except Exception as exc:
        typer.echo(f"\n❌ Migration failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    
    if results["already_migrated"]:
        typer.echo("✅ Migration already applied - no changes needed")
        return
    
    if results["success"]:
        typer.echo("")
        typer.echo("=" * 60)
        typer.echo("Migration Results")
        typer.echo("=" * 60)
        typer.echo(f"Columns added: {results['columns_added']}")
        typer.echo(f"Duration: {results['elapsed_seconds']:.1f} seconds")
        
        if dry_run:
            typer.echo("\n⚠️  DRY RUN - Changes were rolled back")
            typer.echo("Run without --dry-run to apply changes")
        else:
            typer.echo("\n✅ Migration completed successfully!")
            typer.echo("\nRecommended next step:")
            typer.echo("  neuro-api metadata backup")
    else:
        typer.echo(f"\n❌ Migration failed: {results.get('error', 'Unknown error')}", err=True)
        raise typer.Exit(code=1)


@metadata_app.command("instance-stack-import")
def metadata_instance_stack_import(
    csv_file: Path = typer.Argument(..., help="Path to CSV file with stack field values"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without applying changes"),
) -> None:
    """
    Import stack-defining field values from a CSV into existing instance records.
    
    Uses PostgreSQL COPY for maximum performance (10-50x faster than batch inserts).
    
    The CSV must have 'instance_id' column and any of these optional columns:
    echo_time, echo_numbers, repetition_time, flip_angle, inversion_time,
    echo_train_length, receive_coil_name, image_orientation_patient, image_type,
    kvp, xray_exposure, tube_current, pet_bed_index, pet_frame_type
    """
    import csv
    import logging
    import tempfile
    import time
    from sqlalchemy import text
    from metadata_db.session import SessionLocal
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    csv_path = csv_file.resolve()
    if not csv_path.exists():
        typer.echo(f"CSV file not found: {csv_path}", err=True)
        raise typer.Exit(code=1)
    
    # Define mappable columns and their PostgreSQL types
    stack_columns = {
        "echo_time": "DOUBLE PRECISION",
        "echo_numbers": "TEXT",
        "repetition_time": "DOUBLE PRECISION",
        "flip_angle": "DOUBLE PRECISION",
        "inversion_time": "DOUBLE PRECISION",
        "echo_train_length": "INTEGER",
        "receive_coil_name": "TEXT",
        "image_orientation_patient": "TEXT",
        "image_type": "TEXT",
        "kvp": "DOUBLE PRECISION",
        "xray_exposure": "DOUBLE PRECISION",
        "tube_current": "DOUBLE PRECISION",
        "pet_bed_index": "INTEGER",
        "pet_frame_type": "TEXT",
    }
    
    # Columns that need list-to-backslash conversion
    list_columns = {"image_orientation_patient", "image_type", "echo_numbers"}
    
    # Numeric columns that may contain list values
    integer_columns = {"echo_train_length", "pet_bed_index"}
    float_columns = {"echo_time", "repetition_time", "flip_angle", "inversion_time", "kvp", "xray_exposure", "tube_current"}
    
    # Map CSV column names to database column names
    column_name_map = {
        "echo_number": "echo_numbers",
    }
    
    typer.echo("=" * 60)
    typer.echo("Instance Stack Field Import (COPY method)")
    typer.echo("=" * 60)
    typer.echo(f"CSV file: {csv_path}")
    typer.echo(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    typer.echo("")
    
    # Read CSV header
    with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = [col.strip().lower() for col in next(reader)]
    
    # Must have instance_id for COPY method
    if 'instance_id' not in header:
        typer.echo("❌ CSV must have 'instance_id' column for COPY method", err=True)
        raise typer.Exit(code=1)
    
    # Find columns to import
    csv_to_db_columns = {}
    for csv_col in header:
        if csv_col in stack_columns:
            csv_to_db_columns[csv_col] = csv_col
        elif csv_col in column_name_map:
            db_col = column_name_map[csv_col]
            if db_col in stack_columns:
                csv_to_db_columns[csv_col] = db_col
    
    if not csv_to_db_columns:
        typer.echo("❌ No stack columns found in CSV", err=True)
        typer.echo(f"Expected columns: {', '.join(sorted(stack_columns.keys()))}")
        raise typer.Exit(code=1)
    
    db_columns = list(set(csv_to_db_columns.values()))
    typer.echo(f"Columns to import: {', '.join(sorted(db_columns))}")
    typer.echo("")
    
    # Count total rows
    with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
        total_rows = sum(1 for _ in f) - 1
    
    typer.echo(f"Total rows in CSV: {total_rows:,}")
    
    start_time = time.time()
    
    # Step 1: Create a cleaned CSV with only the columns we need, properly converted
    typer.echo("\nStep 1: Preparing data...")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp_file:
        tmp_path = tmp_file.name
        writer = csv.writer(tmp_file)
        
        # Write header: instance_id + db columns
        out_columns = ['instance_id'] + db_columns
        writer.writerow(out_columns)
        
        rows_written = 0
        with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                instance_id = row.get('instance_id', '').strip()
                if not instance_id:
                    continue
                
                out_row = [instance_id]
                for db_col in db_columns:
                    # Find the CSV column that maps to this DB column
                    csv_col = None
                    for c, d in csv_to_db_columns.items():
                        if d == db_col:
                            csv_col = c
                            break
                    
                    raw_value = row.get(csv_col, '').strip() if csv_col else ''
                    
                    if raw_value and raw_value.lower() not in ('', 'none', 'null', 'nan'):
                        # Convert based on column type
                        if db_col in list_columns:
                            raw_value = _convert_python_list_to_backslash(raw_value)
                        elif db_col in integer_columns:
                            converted = _extract_first_integer(raw_value)
                            raw_value = str(converted) if converted is not None else ''
                        elif db_col in float_columns:
                            converted = _extract_first_float(raw_value)
                            raw_value = str(converted) if converted is not None else ''
                    else:
                        raw_value = ''  # Will be NULL in PostgreSQL
                    
                    out_row.append(raw_value)
                
                writer.writerow(out_row)
                rows_written += 1
                
                if rows_written % 1000000 == 0:
                    typer.echo(f"  Prepared {rows_written:,}/{total_rows:,} rows...")
    
    prep_time = time.time() - start_time
    typer.echo(f"  Prepared {rows_written:,} rows in {prep_time:.1f}s")
    
    if dry_run:
        import os
        os.unlink(tmp_path)
        typer.echo("\n⚠️  DRY RUN - No database changes applied")
        typer.echo(f"\nDuration: {prep_time:.1f} seconds")
        return
    
    # Step 2: COPY to temp table and UPDATE
    typer.echo("\nStep 2: Loading into database with COPY...")
    
    db_start = time.time()
    
    with SessionLocal() as session:
        conn = session.connection()
        
        # Create temp table with same structure
        col_defs = ["instance_id INTEGER"]
        for col in db_columns:
            col_defs.append(f"{col} {stack_columns[col]}")
        
        create_temp = f"""
            CREATE TEMP TABLE tmp_stack_import (
                {', '.join(col_defs)}
            ) ON COMMIT DROP
        """
        conn.execute(text(create_temp))
        
        # COPY data into temp table
        # Need to use raw connection for COPY (psycopg3 syntax)
        raw_conn = conn.connection.dbapi_connection
        with raw_conn.cursor() as cur:
            with open(tmp_path, 'r') as f:
                # Skip header
                next(f)
                # psycopg3 uses copy() method with COPY command
                with cur.copy(f"COPY tmp_stack_import FROM STDIN WITH (FORMAT CSV, NULL '')") as copy:
                    while data := f.read(8192):
                        copy.write(data)
        
        copy_time = time.time() - db_start
        typer.echo(f"  COPY completed in {copy_time:.1f}s")
        
        # Step 3: UPDATE from temp table
        typer.echo("\nStep 3: Updating instance table...")
        
        update_start = time.time()
        
        set_clauses = [f"{col} = tmp_stack_import.{col}" for col in db_columns]
        update_sql = f"""
            UPDATE instance
            SET {', '.join(set_clauses)}
            FROM tmp_stack_import
            WHERE instance.instance_id = tmp_stack_import.instance_id
        """
        
        result = conn.execute(text(update_sql))
        rows_updated = result.rowcount
        
        session.commit()
        
        update_time = time.time() - update_start
        typer.echo(f"  UPDATE completed in {update_time:.1f}s")
    
    # Cleanup temp file
    import os
    os.unlink(tmp_path)
    
    elapsed = time.time() - start_time
    
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("Import Results")
    typer.echo("=" * 60)
    typer.echo(f"Rows processed: {rows_written:,}")
    typer.echo(f"Rows updated: {rows_updated:,}")
    typer.echo(f"Duration: {elapsed:.1f} seconds")
    typer.echo(f"Rate: {rows_written / elapsed:,.0f} rows/sec")
    typer.echo(f"  - Prep: {prep_time:.1f}s")
    typer.echo(f"  - COPY: {copy_time:.1f}s")
    typer.echo(f"  - UPDATE: {update_time:.1f}s")
    typer.echo("\n✅ Import completed successfully!")


def _apply_stack_import_batch(batch: list[dict], columns: list[str], use_instance_id: bool = False) -> int:
    """Apply a batch of stack field updates to the instance table."""
    from sqlalchemy import text
    from metadata_db.session import SessionLocal
    
    if not batch:
        return 0
    
    # Build UPDATE statement with parameterized values
    set_clauses = ", ".join([f"{col} = :{col}" for col in columns])
    
    if use_instance_id:
        sql = text(f"""
            UPDATE instance
            SET {set_clauses}
            WHERE instance_id = :instance_id
        """)
        key_field = 'instance_id'
    else:
        sql = text(f"""
            UPDATE instance
            SET {set_clauses}
            WHERE sop_instance_uid = :sop_instance_uid
        """)
        key_field = 'sop_instance_uid'
    
    updated = 0
    with SessionLocal() as session:
        for row in batch:
            # Only include columns that have values in this row
            params = {key_field: row[key_field]}
            for col in columns:
                params[col] = row.get(col)
            
            result = session.execute(sql, params)
            if result.rowcount > 0:
                updated += result.rowcount
        
        session.commit()
    
    return updated


def _convert_python_list_to_backslash(value: str) -> str:
    """Convert Python list string like \"['A', 'B']\" to backslash-separated \"A\\B\"."""
    if not value:
        return value
    value = value.strip()
    if value.startswith('[') and value.endswith(']'):
        # Parse Python list format
        import ast
        try:
            items = ast.literal_eval(value)
            if isinstance(items, (list, tuple)):
                return '\\'.join(str(item) for item in items)
        except (ValueError, SyntaxError):
            pass
    return value


def _extract_first_integer(value: str) -> int | None:
    """Extract first integer from a value that may be a Python list string like \"[1, '']\"."""
    if not value:
        return None
    value = value.strip()
    
    # If it's a list format, parse and extract first valid integer
    if value.startswith('[') and value.endswith(']'):
        import ast
        try:
            items = ast.literal_eval(value)
            if isinstance(items, (list, tuple)):
                for item in items:
                    if isinstance(item, int):
                        return item
                    if isinstance(item, str) and item.strip().isdigit():
                        return int(item.strip())
                return None  # No valid integer found in list
        except (ValueError, SyntaxError):
            pass
    
    # Try to parse as plain integer
    try:
        return int(float(value))  # Handle "1.0" format
    except (ValueError, TypeError):
        return None


def _extract_first_float(value: str) -> float | None:
    """Extract first float from a value that may be a Python list string like \"[1.5, '']\"."""
    if not value:
        return None
    value = value.strip()
    
    result = None
    
    # If it's a list format, parse and extract first valid float
    if value.startswith('[') and value.endswith(']'):
        import ast
        try:
            items = ast.literal_eval(value)
            if isinstance(items, (list, tuple)):
                for item in items:
                    if isinstance(item, (int, float)):
                        result = float(item)
                        break
                    if isinstance(item, str) and item.strip():
                        try:
                            result = float(item.strip())
                            break
                        except ValueError:
                            continue
        except (ValueError, SyntaxError):
            pass
    else:
        # Try to parse as plain float
        try:
            result = float(value)
        except (ValueError, TypeError):
            pass
    
    # Handle PostgreSQL float range limits (avoid underflow/overflow)
    # PostgreSQL float8 range: ~1e-308 to ~1e+308
    if result is not None:
        if abs(result) < 1e-300 and result != 0.0:
            result = 0.0  # Underflow - treat as zero
        elif abs(result) > 1e+300:
            return None  # Overflow - skip this value
    
    return result


@metadata_app.command("ingest")
def metadata_ingest(
    cohort_id: int = typer.Argument(..., help="Cohort identifier"),
    raw_root: Optional[Path] = typer.Option(None, help="Override path to derivatives/dcm-raw"),
    extension_mode: ExtensionMode = typer.Option(ExtensionMode.ALL.value, case_sensitive=False),
    max_workers: int = typer.Option(4, min=1, max=128),
    batch_size: int = typer.Option(100, min=10, max=5000),
    queue_size: int = typer.Option(10, min=1, max=500),
    series_workers_per_subject: int = typer.Option(1, min=1, max=16, help="Concurrent series workers per subject"),
    duplicate_policy: DuplicatePolicy = typer.Option(DuplicatePolicy.SKIP.value, case_sensitive=False),
    resume: bool = typer.Option(True, help="Skip SOP instances already stored in the metadata DB"),
    resume_by_path: bool = typer.Option(
        False,
        "--resume-by-path/--no-resume-by-path",
        help="Avoid reopening files whose relative path already exists in the metadata DB",
    ),
    adaptive_batching: bool = typer.Option(False, help="Enable adaptive writer batching"),
    adaptive_target_tx_ms: int = typer.Option(200, min=50, max=2000, help="Target transaction duration (ms)"),
    adaptive_min_batch_size: int = typer.Option(50, min=10, max=10000, help="Minimum adaptive batch size"),
    adaptive_max_batch_size: int = typer.Option(1000, min=50, max=20000, help="Maximum adaptive batch size"),
    subject_id_type_id: Optional[int] = typer.Option(None, "--subject-id-type", help="ID type to use when storing PatientID values in metadata.subject_other_identifiers"),
    subject_code_csv: Optional[Path] = typer.Option(None, "--subject-code-csv", help="Optional CSV mapping PatientID to subject_code"),
    subject_code_csv_patient_column: str = typer.Option("PatientID", "--subject-code-csv-patient-column", help="Patient ID column in the subject code CSV"),
    subject_code_csv_subject_column: str = typer.Option("subject_code", "--subject-code-csv-subject-column", help="Subject code column in the subject code CSV"),
    subject_code_seed: Optional[str] = typer.Option(None, "--subject-code-seed", help="Seed used when hashing PatientIDs or StudyInstanceUIDs into subject codes"),
    job_name: Optional[str] = typer.Option(None, help="Optional job name"),
    no_job: bool = typer.Option(False, help="Run without creating a job record"),
) -> None:
    cohort = cohort_service.get_cohort(cohort_id)
    if not cohort:
        typer.echo(f"Cohort {cohort_id} not found", err=True)
        raise typer.Exit(code=1)

    selected_root = Path(cohort.source_path)
    raw_root_path: Path
    if raw_root is not None:
        raw_root_path = raw_root.resolve()
    else:
        setup = setup_derivatives_folders(selected_root)
        raw_root_path = setup.output_path

    subject_code_map = {}
    subject_code_map_name = None
    if subject_code_csv is not None:
        try:
            subject_code_map = load_subject_code_csv(
                subject_code_csv.resolve(),
                subject_code_csv_patient_column,
                subject_code_csv_subject_column,
            )
            subject_code_map_name = subject_code_csv.name
        except Exception as exc:
            typer.echo(f"Failed to load subject code CSV: {exc}", err=True)
            raise typer.Exit(code=1) from exc

    config = ExtractionConfig(
        cohort_id=cohort.id,
        cohort_name=cohort.name,
        raw_root=raw_root_path,
        max_workers=max_workers,
        batch_size=batch_size,
        queue_size=queue_size,
        extension_mode=extension_mode,
        duplicate_policy=duplicate_policy,
        resume=resume,
        resume_by_path=resume and resume_by_path,
        subject_id_type_id=subject_id_type_id,
        subject_code_map=subject_code_map,
        subject_code_seed=subject_code_seed,
        subject_code_map_name=subject_code_map_name,
        series_workers_per_subject=series_workers_per_subject,
        adaptive_batching_enabled=adaptive_batching,
        target_tx_ms=adaptive_target_tx_ms,
        min_batch_size=adaptive_min_batch_size,
        max_batch_size=adaptive_max_batch_size,
    )

    if no_job:
        typer.echo("Running metadata ingestion without job tracking...")

        latest: dict[str, int] = {"processed": 0, "total": 0}
        tracker = ExtractionProgressTracker(
            lambda percent: typer.echo(
                f"Progress: {percent}% ({latest['processed']}/{latest['total']})",
                err=True,
            )
        )

        def progress_cb(processed: int, total: int) -> None:
            latest["processed"] = processed
            latest["total"] = total
            tracker.update(processed, total)

        result = run_extraction(config, progress=progress_cb, job_id=None)
        latest["processed"] = result.completed_total
        latest["total"] = result.total_subjects
        tracker.finalize()
        if result.metrics:
            typer.echo(
                "Run summary: "
                + ", ".join(f"{key}={value}" for key, value in result.metrics.items()),
                err=True,
            )
        typer.echo("Metadata ingestion completed.")
        return

    job = job_service.create_job(
        stage="metadata_ingest",
        config=config.model_dump(mode="json"),
        name=job_name or f"{cohort.name} - metadata ingest",
    )
    job_service.mark_running(job.id)

    tracker = ExtractionProgressTracker(lambda percent: job_service.update_progress(job.id, percent))

    def progress_cb(processed: int, total: int) -> None:
        tracker.update(processed, total)

    try:
        result = run_extraction(config, progress=progress_cb, job_id=job.id)
    except Exception as exc:  # pragma: no cover - CLI reporting
        job_service.mark_failed(job.id, str(exc))
        typer.echo(f"Metadata ingestion failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    tracker.finalize()
    if result.metrics:
        job_service.update_metrics(job.id, result.metrics)
    job_service.mark_completed(job.id)
    typer.echo("Metadata ingestion completed successfully.")


@anonymize_app.command("run")
def anonymize_run(config_path: Path, job_name: Optional[str] = None, no_job: bool = False) -> None:
    """Run anonymization using the unified engine."""

    config = load_config(config_path)
    if no_job:
        result = run_anonymization(config, progress=None, job_id=None)
        typer.echo(f"Processed {result.updated_files}/{result.total_files} files")
        return

    job = job_service.create_job(stage="anonymize", config=config.model_dump(mode="json"), name=job_name)
    typer.echo(f"Created job {job.id}. Starting anonymization...")
    run_anonymize_job(job.id, config)
    typer.echo("Anonymization completed.")


@anonymize_app.command("dry-run")
def anonymize_dry_run(config_path: Path) -> None:
    """Execute anonymization without writing datasets (for inspection only)."""

    config = load_config(config_path)
    result: AnonymizeResult = run_anonymization(config, progress=None, job_id=None)
    table = Table(title="Dry-run summary")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Total files", str(result.total_files))
    table.add_row("Updated", str(result.updated_files))
    table.add_row("Skipped", str(result.skipped_files))
    table.add_row("Audit rows", str(result.audit_rows_written))
    table.add_row("Duration (s)", f"{result.duration_seconds:.2f}")
    if result.export_path:
        table.add_row("Export", str(result.export_path))
    rprint(table)


@anonymize_app.command("quick-test")
def anonymize_quick_test(selected_root: Path = Path("./sample_data/raw/mr")) -> None:
    """Run a quick anonymization test using the sample dataset."""

    setup = setup_derivatives_folders(selected_root)
    if setup.status == DerivativesStatus.RAW_EXISTS_WITH_CONTENT:
        typer.echo("Existing anonymized files detected in derivatives/dcm-raw. Clearing for quick test...")
        clean_dcm_raw(setup.output_path)
        setup = setup_derivatives_folders(selected_root)
    source_path, output_path, _ = setup
    config = AnonymizeConfig(
        source_root=source_path,
        output_root=output_path,
        anonymize_categories=[
            "Patient_Information",
            "Healthcare_Provider_Information",
            "Clinical_Trial_Information",
            "Institution_Information",
        ],
        cohort_name="Sample",
        patient_id={
            "enabled": True,
            "strategy": "sequential",
            "sequential": {"pattern": "TESTXXXX", "starting_number": 1, "discovery": "per_top_folder"},
        },
        audit_export={
            "enabled": True,
            "format": "encrypted_excel",
            "filename": "sample_audit.xlsx",
            "excel_password": "neuroimaging2025",
        },
    )
    result = run_anonymization(config)
    typer.echo(f"Processed {result.updated_files}/{result.total_files} files. Output: {output_path}")


@compress_app.command("run")
def compress_run(
    cohort_root: Path,
    password: Optional[str] = typer.Option(None, prompt=True, hide_input=True, confirmation_prompt=True),
    chunk: str = typer.Option("100GB", help="Max archive size"),
    strategy: str = typer.Option("ordered", help="Packing strategy: ordered or ffd"),
    compression_level: int = typer.Option(3, min=0, max=9, help="7z compression level"),
    workers: int = typer.Option(2, min=1, max=16, help="Parallel archives"),
    verify: bool = typer.Option(True, help="Run 7z verification"),
    par2: int = typer.Option(0, min=0, max=50, help="PAR2 redundancy percent"),
) -> None:
    """Archive original DICOMs under derivatives/archives."""

    cohort_root = cohort_root.resolve()
    originals = cohort_root / "derivatives" / "dcm-original"
    if not originals.exists() or not originals.is_dir():
        typer.echo("No dcm-original folder found; run anonymization setup first.")
        raise typer.Exit(code=1)

    archives = cohort_root / "derivatives" / "archives"
    strategy_value = strategy.lower()
    if strategy_value not in {"ordered", "ffd"}:
        typer.echo("Strategy must be 'ordered' or 'ffd'.")
        raise typer.Exit(code=1)
    config = CompressionConfig(
        root=originals,
        out_dir=archives,
        chunk=chunk,
        strategy=strategy_value,  # type: ignore[arg-type]
        compression=compression_level,
        workers=workers,
        password=password or "",
        verify=verify,
        par2=par2,
    )

    manifest = run_compression(config)
    typer.echo(f"Compression complete. Manifest: {manifest}")


@compress_app.command("plan")
def compress_plan(
    cohort_root: Path,
    chunk: str = typer.Option("100GB", help="Max archive size"),
    strategy: str = typer.Option("ordered", help="Packing strategy: ordered or ffd"),
) -> None:
    """Estimate archives for originals without writing files."""

    cohort_root = cohort_root.resolve()
    originals = cohort_root / "derivatives" / "dcm-original"
    if not originals.exists() or not originals.is_dir():
        typer.echo("No dcm-original folder found; run anonymization setup first.")
        raise typer.Exit(code=1)

    strategy_value = strategy.lower()
    if strategy_value not in {"ordered", "ffd"}:
        typer.echo("Strategy must be 'ordered' or 'ffd'.")
        raise typer.Exit(code=1)
    config = CompressionConfig(
        root=originals,
        out_dir=cohort_root / "derivatives" / "archives",
        chunk=chunk,
        strategy=strategy_value,  # type: ignore[arg-type]
        compression=3,
        workers=1,
        password="placeholder",
        verify=False,
        par2=0,
    )

    plans = build_chunk_plan(config)
    typer.echo(f"Plan: {len(plans)} archive(s) using {chunk} chunks")
    for plan in plans:
        folders = ",".join(entry["pn"] for entry in plan.members[:6])
        if len(plan.members) > 6:
            folders += ",..."
        typer.echo(
            f"  - chunk {plan.chunk_id:04d}: {len(plan.members)} folder(s), {bytes_to_human(plan.total_bytes)} :: {folders}"
        )


@app.command()
def jobs_list() -> None:
    """List existing jobs."""

    jobs = job_service.list_jobs()
    table = Table(title="Jobs")
    table.add_column("ID", justify="right")
    table.add_column("Name")
    table.add_column("Stage")
    table.add_column("Status")
    table.add_column("Progress", justify="right")
    for job in jobs:
        table.add_row(str(job.id), job.name or "", job.stage, job.status.value, f"{job.progress}%")
    rprint(table)


@app.command()
def files_list(path: Optional[Path] = None) -> None:
    """List immediate subdirectories under the given path clamped to DATA_ROOT."""

    data_root = Path(os.getenv("DATA_ROOT", "/app/data")).resolve()
    root_str = str(data_root)

    candidate = (path or data_root).resolve()
    if not str(candidate).startswith(root_str):
        rprint("[red]Requested path is outside DATA_ROOT[/red]")
        raise typer.Exit(code=1)

    try:
        entries: List[str] = []
        with os.scandir(candidate) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    entries.append(entry.name)
        for name in sorted(entries):
            rprint(name)
    except FileNotFoundError:
        rprint("[yellow]Path not found[/yellow]")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
