"""Shared PostgreSQL backup helpers for application and metadata databases."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional

from sqlalchemy.engine import make_url

from db.config import get_backup_settings as get_app_backup_settings, get_settings as get_app_settings
from metadata_db.config import (
    get_backup_settings as get_metadata_backup_settings,
    get_settings as get_metadata_settings,
)


logger = logging.getLogger(__name__)


class BackupError(RuntimeError):
    """Raised when a backup or restore operation fails."""


class DatabaseKey(str, Enum):
    APPLICATION = "application"
    METADATA = "metadata"


@dataclass(frozen=True)
class DatabaseBackupConfig:
    """Runtime configuration for a database backup target."""

    key: DatabaseKey
    label: str
    url: str
    default_directory: Path
    verify_dumps: bool
    enabled: bool

    @property
    def allowed_root(self) -> Path:
        """Return the directory root within which backups are permitted."""

        return self.default_directory.resolve()


METADATA_SUFFIX = ".json"


def _legacy_directories(key: DatabaseKey) -> list[Path]:
    if key is DatabaseKey.APPLICATION:
        return [Path("resource/db/app_backups").resolve()]
    if key is DatabaseKey.METADATA:
        return [Path("resource/db/metadata_backups").resolve()]
    return []


def _prepare_backup_directory(key: DatabaseKey, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for legacy_root in _legacy_directories(key):
        try:
            if not legacy_root.exists():
                continue
            if legacy_root.resolve() == directory:
                continue
        except OSError as exc:  # pragma: no cover - best effort handling
            logger.warning("Skipping legacy backup directory %s due to access error: %s", legacy_root, exc)
            continue

        try:
            legacy_items = list(legacy_root.iterdir())
        except OSError as exc:  # pragma: no cover - best effort handling
            logger.warning("Unable to read legacy backup directory %s: %s", legacy_root, exc)
            continue

        for item in legacy_items:
            if not item.is_file():
                continue
            name = item.name
            if not (name.endswith(".dump") or name.endswith(".dump.json")):
                continue
            destination = directory / name
            if destination.exists():
                continue
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(item), str(destination))
                logger.info("Migrated legacy backup %s to %s", item, destination)
            except (OSError, shutil.Error):  # pragma: no cover - best effort
                logger.warning("Failed to migrate legacy backup %s", item, exc_info=True)
    return directory.resolve()


@lru_cache
def get_backup_config(key: DatabaseKey | str) -> DatabaseBackupConfig:
    database_key = DatabaseKey(key)

    if database_key is DatabaseKey.METADATA:
        db_settings = get_metadata_settings()
        backup_settings = get_metadata_backup_settings()
        directory = _prepare_backup_directory(database_key, backup_settings.directory.resolve())
        return DatabaseBackupConfig(
            key=database_key,
            label="Metadata",
            url=db_settings.url,
            default_directory=directory,
            verify_dumps=backup_settings.verify_dumps,
            enabled=backup_settings.enabled,
        )

    app_settings = get_app_settings()
    app_backup_settings = get_app_backup_settings()
    directory = _prepare_backup_directory(database_key, app_backup_settings.directory.resolve())
    return DatabaseBackupConfig(
        key=database_key,
        label="Application",
        url=app_settings.url,
        default_directory=directory,
        verify_dumps=app_backup_settings.verify_dumps,
        enabled=app_backup_settings.enabled,
    )


class PostgresBackupManager:
    """Manage pg_dump / pg_restore operations for a configured database."""

    def __init__(self, config: DatabaseBackupConfig) -> None:
        self.config = config
        self.config.default_directory.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _metadata_path(self, dump_path: Path) -> Path:
        return Path(f"{dump_path}{METADATA_SUFFIX}")

    def _write_metadata(
        self,
        dump_path: Path,
        *,
        directory: Optional[str | Path],
        filename: Optional[str],
        note: Optional[str] = None,
    ) -> None:
        try:
            stat = dump_path.stat()
        except FileNotFoundError:  # pragma: no cover - defensive, dump already missing
            return

        created_at = dt.datetime.fromtimestamp(stat.st_mtime, dt.timezone.utc).isoformat()
        metadata = {
            "database": self.config.key.value,
            "database_label": self.config.label,
            "filename": dump_path.name,
            "path": str(dump_path.resolve()),
            "size_bytes": stat.st_size,
            "created_at": created_at,
        }
        if directory:
            metadata["requested_directory"] = str(Path(directory).expanduser())
        if filename:
            metadata["requested_filename"] = filename
        if note:
            metadata["note"] = str(note)

        metadata_path = self._metadata_path(dump_path)
        try:
            metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True))
        except OSError:  # pragma: no cover - best effort persistence
            logger.warning("Failed to write metadata for backup %s", dump_path, exc_info=True)

    def _remove_metadata(self, dump_path: Path) -> None:
        metadata_path = self._metadata_path(dump_path)
        try:
            metadata_path.unlink()
        except FileNotFoundError:
            return
        except OSError:  # pragma: no cover - best effort cleanup
            logger.warning("Failed to remove metadata for backup %s", dump_path, exc_info=True)

    def _connection_args(self) -> tuple[list[str], dict[str, str]]:
        url = make_url(self.config.url)
        args = ["-h", url.host or "localhost"]
        if url.port:
            args.extend(["-p", str(url.port)])
        if url.username:
            args.extend(["-U", url.username])
        args.extend(["-d", url.database or "postgres"])
        env = os.environ.copy()
        if url.password:
            env["PGPASSWORD"] = url.password
        return args, env

    def _run(self, command: list[str], env: dict[str, str]) -> None:
        try:
            result = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:  # pragma: no cover - platform safeguard
            raise BackupError(f"Required command not found: {command[0]}") from exc
        if result.returncode != 0:
            message = result.stderr.strip() or result.stdout.strip() or "Backup command failed"
            raise BackupError(message)

    def _resolve_directory(self, directory: Optional[str | Path]) -> Path:
        if directory is None or str(directory).strip() == "":
            return self.config.default_directory

        candidate = Path(directory).expanduser().resolve()
        allowed_root = self.config.allowed_root
        try:
            candidate.relative_to(allowed_root)
        except ValueError as exc:
            raise BackupError(f"Directory must be inside {allowed_root}") from exc

        candidate.mkdir(parents=True, exist_ok=True)
        return candidate

    def _resolve_filename(self, filename: Optional[str], database_prefix: str) -> str:
        if not filename:
            timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            return f"{database_prefix}_{timestamp}.dump"

        sanitized = Path(filename).name
        if not sanitized.lower().endswith(".dump"):
            sanitized = f"{sanitized}.dump"
        return sanitized

    def _resolve_target_path(
        self,
        directory: Optional[str | Path],
        filename: Optional[str],
    ) -> Path:
        target_dir = self._resolve_directory(directory)
        name = self._resolve_filename(filename, self.config.key.value)
        return target_dir / name

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def create_backup(self, directory: Optional[str | Path] = None, *, note: Optional[str] = None) -> Path:
        if not self.config.enabled:
            raise BackupError("Backups are disabled")

        target = self._resolve_target_path(directory, None)
        if target.exists():
            raise BackupError(f"Backup already exists: {target}")

        args, env = self._connection_args()
        command = [
            "pg_dump",
            "-Fc",
            *args,
            "-f",
            str(target),
        ]
        self._run(command, env)

        if self.config.verify_dumps:
            self._run(["pg_restore", "-l", str(target)], env)

        self._write_metadata(target, directory=directory, filename=None, note=note)
        return target

    def restore(self, path: str | Path, *, post_restore_hook: callable = None) -> Path:
        """Restore database from backup.

        Uses a 3-phase restore for optimal speed while maintaining consistency:
        1. Pre-data (schema) - sequential to avoid race conditions
        2. Data - parallel for maximum throughput
        3. Post-data (indexes, constraints) - parallel for speed

        Args:
            path: Path to backup file (or None for latest)
            post_restore_hook: Optional callback to run after restore completes.
                             Useful for applying migrations to ensure schema compatibility.

        Returns:
            Path to the restored backup file
        """
        candidate = self.ensure_backup_path(path)
        args, env = self._connection_args()

        common_flags = ["--no-acl", "--no-owner"]

        # Phase 1: Restore schema (pre-data) - must be sequential to avoid race conditions
        # This drops and recreates tables, sequences, types
        pre_data_cmd = [
            "pg_restore",
            "--clean",
            "--if-exists",
            "--section=pre-data",
            *common_flags,
            *args,
            str(candidate),
        ]
        self._run(pre_data_cmd, env)

        # Phase 2: Restore data - parallel for speed (tables exist now, safe to parallelize)
        data_cmd = [
            "pg_restore",
            "--section=data",
            "-j", "4",  # Parallel data loading
            *common_flags,
            *args,
            str(candidate),
        ]
        self._run(data_cmd, env)

        # Phase 3: Restore indexes and constraints (post-data) - parallel for speed
        post_data_cmd = [
            "pg_restore",
            "--section=post-data",
            "-j", "4",  # Parallel index/constraint creation
            *common_flags,
            *args,
            str(candidate),
        ]
        self._run(post_data_cmd, env)

        # Run post-restore hook if provided (e.g., apply migrations)
        if post_restore_hook:
            post_restore_hook()

        return candidate

    def list_backups(self) -> Iterable[Path]:
        root = self.config.allowed_root
        if not root.exists():
            return []
        # Collect dumps only under allowed root
        return sorted(root.rglob("*.dump"))

    def latest_backup(self) -> Optional[Path]:
        dumps = list(self.list_backups())
        return dumps[-1] if dumps else None

    def ensure_backup_path(self, path: str | Path | None) -> Path:
        if path is None:
            latest = self.latest_backup()
            if not latest:
                raise BackupError("No backups available")
            return latest

        candidate = Path(path).expanduser().resolve()
        allowed_root = self.config.allowed_root
        try:
            candidate.relative_to(allowed_root)
        except ValueError as exc:
            raise BackupError(f"Path outside allowed backup directory: {allowed_root}") from exc
        if not candidate.exists():
            raise BackupError(f"Backup not found: {candidate}")
        return candidate

    def delete_backup(self, path: str | Path) -> Path:
        candidate = self.ensure_backup_path(path)
        try:
            candidate.unlink()
        except FileNotFoundError as exc:
            raise BackupError(f"Backup not found: {candidate}") from exc
        except OSError as exc:
            raise BackupError(f"Failed to delete backup {candidate}: {exc}") from exc

        self._remove_metadata(candidate)
        return candidate


def list_database_backups(database: DatabaseKey) -> list[Path]:
    manager = PostgresBackupManager(get_backup_config(database))
    return list(manager.list_backups())


# =============================================================================
# Python Cache Cleanup
# =============================================================================

@dataclass
class CleanupResult:
    """Result of a cleanup operation."""

    pycache_dirs_removed: int = 0
    pyc_files_removed: int = 0
    build_dirs_removed: int = 0
    egg_info_dirs_removed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total_removed(self) -> int:
        return (
            self.pycache_dirs_removed +
            self.pyc_files_removed +
            self.build_dirs_removed +
            self.egg_info_dirs_removed
        )

    def to_dict(self) -> dict:
        return {
            "pycache_dirs_removed": self.pycache_dirs_removed,
            "pyc_files_removed": self.pyc_files_removed,
            "build_dirs_removed": self.build_dirs_removed,
            "egg_info_dirs_removed": self.egg_info_dirs_removed,
            "total_removed": self.total_removed,
            "errors": self.errors,
        }


def clean_python_cache(
    root_dir: Optional[Path] = None,
    *,
    include_build: bool = True,
    include_egg_info: bool = True,
    dry_run: bool = False,
) -> CleanupResult:
    """
    Clean Python cache files and directories that can cause stale module imports.

    This removes:
    - __pycache__ directories (compiled bytecode)
    - *.pyc files (compiled Python files)
    - build/ directories (setuptools build artifacts)
    - *.egg-info directories (package metadata)

    Args:
        root_dir: Root directory to clean. Defaults to /app or current working directory.
        include_build: If True, also remove build/ directories.
        include_egg_info: If True, also remove *.egg-info directories.
        dry_run: If True, only report what would be removed without actually deleting.

    Returns:
        CleanupResult with counts of removed items and any errors.

    Example:
        # Clean all Python cache in the application
        result = clean_python_cache()
        print(f"Removed {result.total_removed} items")

        # Dry run to see what would be removed
        result = clean_python_cache(dry_run=True)
    """
    result = CleanupResult()

    # Determine root directory
    if root_dir is None:
        # Check common locations
        if Path("/app").exists():
            root_dir = Path("/app")
        else:
            root_dir = Path.cwd()

    root_dir = Path(root_dir).resolve()

    if not root_dir.exists():
        result.errors.append(f"Root directory does not exist: {root_dir}")
        return result

    logger.info("Cleaning Python cache in %s (dry_run=%s)", root_dir, dry_run)

    # 1. Remove __pycache__ directories
    for pycache_dir in root_dir.rglob("__pycache__"):
        if not pycache_dir.is_dir():
            continue
        try:
            if dry_run:
                logger.info("[DRY RUN] Would remove: %s", pycache_dir)
            else:
                shutil.rmtree(pycache_dir)
                logger.debug("Removed pycache: %s", pycache_dir)
            result.pycache_dirs_removed += 1
        except OSError as exc:
            result.errors.append(f"Failed to remove {pycache_dir}: {exc}")
            logger.warning("Failed to remove pycache %s: %s", pycache_dir, exc)

    # 2. Remove stray .pyc files (outside __pycache__)
    for pyc_file in root_dir.rglob("*.pyc"):
        if not pyc_file.is_file():
            continue
        # Skip if already handled by __pycache__ removal
        if "__pycache__" in pyc_file.parts:
            continue
        try:
            if dry_run:
                logger.info("[DRY RUN] Would remove: %s", pyc_file)
            else:
                pyc_file.unlink()
                logger.debug("Removed pyc file: %s", pyc_file)
            result.pyc_files_removed += 1
        except OSError as exc:
            result.errors.append(f"Failed to remove {pyc_file}: {exc}")
            logger.warning("Failed to remove pyc file %s: %s", pyc_file, exc)

    # 3. Remove build/ directories (setuptools artifacts)
    if include_build:
        for build_dir in root_dir.rglob("build"):
            if not build_dir.is_dir():
                continue
            # Only remove if it looks like a Python build directory
            # (contains lib/ or has .py files in parent)
            parent = build_dir.parent
            is_python_build = (
                (build_dir / "lib").exists() or
                any(parent.glob("*.py")) or
                any(parent.glob("setup.py")) or
                any(parent.glob("pyproject.toml"))
            )
            if not is_python_build:
                continue
            try:
                if dry_run:
                    logger.info("[DRY RUN] Would remove: %s", build_dir)
                else:
                    shutil.rmtree(build_dir)
                    logger.debug("Removed build dir: %s", build_dir)
                result.build_dirs_removed += 1
            except OSError as exc:
                result.errors.append(f"Failed to remove {build_dir}: {exc}")
                logger.warning("Failed to remove build dir %s: %s", build_dir, exc)

    # 4. Remove *.egg-info directories
    if include_egg_info:
        for egg_dir in root_dir.rglob("*.egg-info"):
            if not egg_dir.is_dir():
                continue
            try:
                if dry_run:
                    logger.info("[DRY RUN] Would remove: %s", egg_dir)
                else:
                    shutil.rmtree(egg_dir)
                    logger.debug("Removed egg-info: %s", egg_dir)
                result.egg_info_dirs_removed += 1
            except OSError as exc:
                result.errors.append(f"Failed to remove {egg_dir}: {exc}")
                logger.warning("Failed to remove egg-info %s: %s", egg_dir, exc)

    logger.info(
        "Cache cleanup complete: %d pycache dirs, %d pyc files, %d build dirs, %d egg-info dirs",
        result.pycache_dirs_removed,
        result.pyc_files_removed,
        result.build_dirs_removed,
        result.egg_info_dirs_removed,
    )

    return result


def clean_installed_packages(
    package_names: Optional[list[str]] = None,
    *,
    site_packages_dir: Optional[Path] = None,
    dry_run: bool = False,
) -> CleanupResult:
    """
    Clean installed packages from site-packages to force reimport from source.

    This is useful when you have both source code and installed packages,
    and Python is importing the stale installed version instead of source.

    Args:
        package_names: List of package names to remove. If None, uses default list.
        site_packages_dir: Path to site-packages. Auto-detected if None.
        dry_run: If True, only report what would be removed.

    Returns:
        CleanupResult with removal counts.

    Example:
        # Remove stale classification package from site-packages
        result = clean_installed_packages(["classification"])
    """
    result = CleanupResult()

    if package_names is None:
        # Default packages that might have stale installs
        package_names = ["classification", "sort", "db", "metadata_db", "backup"]

    # Find site-packages directory
    if site_packages_dir is None:
        import sys
        for path in sys.path:
            if "site-packages" in path:
                site_packages_dir = Path(path)
                break

    if site_packages_dir is None or not site_packages_dir.exists():
        result.errors.append("Could not find site-packages directory")
        return result

    logger.info("Cleaning installed packages from %s", site_packages_dir)

    for package_name in package_names:
        # Check for package directory
        package_dir = site_packages_dir / package_name
        if package_dir.exists() and package_dir.is_dir():
            try:
                if dry_run:
                    logger.info("[DRY RUN] Would remove: %s", package_dir)
                else:
                    shutil.rmtree(package_dir)
                    logger.info("Removed installed package: %s", package_dir)
                result.pycache_dirs_removed += 1  # Reusing counter for package dirs
            except OSError as exc:
                result.errors.append(f"Failed to remove {package_dir}: {exc}")

        # Check for egg-info
        for egg_dir in site_packages_dir.glob(f"{package_name}*.egg-info"):
            try:
                if dry_run:
                    logger.info("[DRY RUN] Would remove: %s", egg_dir)
                else:
                    shutil.rmtree(egg_dir)
                    logger.info("Removed egg-info: %s", egg_dir)
                result.egg_info_dirs_removed += 1
            except OSError as exc:
                result.errors.append(f"Failed to remove {egg_dir}: {exc}")

        # Check for dist-info
        for dist_dir in site_packages_dir.glob(f"{package_name}*.dist-info"):
            try:
                if dry_run:
                    logger.info("[DRY RUN] Would remove: %s", dist_dir)
                else:
                    shutil.rmtree(dist_dir)
                    logger.info("Removed dist-info: %s", dist_dir)
                result.egg_info_dirs_removed += 1
            except OSError as exc:
                result.errors.append(f"Failed to remove {dist_dir}: {exc}")

    return result


def full_clean(
    root_dir: Optional[Path] = None,
    *,
    clean_site_packages: bool = False,
    package_names: Optional[list[str]] = None,
    dry_run: bool = False,
) -> dict:
    """
    Perform a full cleanup of Python cache and optionally installed packages.

    This is the main entry point for --clean operations.

    Args:
        root_dir: Root directory to clean.
        clean_site_packages: If True, also clean installed packages from site-packages.
        package_names: Package names to remove from site-packages.
        dry_run: If True, only report what would be removed.

    Returns:
        Dict with cleanup results.

    Example:
        # Full clean including site-packages
        result = full_clean(clean_site_packages=True)
    """
    results = {
        "cache_cleanup": None,
        "site_packages_cleanup": None,
        "success": True,
        "errors": [],
    }

    # Clean Python cache
    cache_result = clean_python_cache(root_dir, dry_run=dry_run)
    results["cache_cleanup"] = cache_result.to_dict()
    results["errors"].extend(cache_result.errors)

    # Optionally clean site-packages
    if clean_site_packages:
        pkg_result = clean_installed_packages(package_names, dry_run=dry_run)
        results["site_packages_cleanup"] = pkg_result.to_dict()
        results["errors"].extend(pkg_result.errors)

    results["success"] = len(results["errors"]) == 0

    return results
