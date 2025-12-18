from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


@pytest.fixture
def backup_module(tmp_path, monkeypatch):
    monkeypatch.setenv("METADATA_DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:15432/testdb")
    monkeypatch.setenv("METADATA_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.setenv("METADATA_BACKUP_VERIFY", "false")
    monkeypatch.setenv("METADATA_BACKUP_ENABLED", "true")
    monkeypatch.setenv("METADATA_AUTO_RESTORE", "false")

    import metadata_db.config as config
    import metadata_db.backup as backup

    config.get_settings.cache_clear()
    config.get_backup_settings.cache_clear()

    importlib.reload(config)
    backup = importlib.reload(backup)

    return backup


def test_create_backup_creates_file(tmp_path, backup_module, monkeypatch):
    manager = backup_module.MetadataBackupManager()

    def fake_run(self, command, env):
        if command[0] == "pg_dump":
            target = Path(command[command.index("-f") + 1])
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"dummy")

    monkeypatch.setattr(backup_module.MetadataBackupManager, "_run", fake_run, raising=True)

    created = manager.create_backup()
    assert created.exists()
    assert created.read_bytes() == b"dummy"
    metadata = Path(f"{created}.json")
    assert metadata.exists()
    payload = json.loads(metadata.read_text())
    assert payload["filename"] == created.name
    assert payload["database"] == "metadata"


def test_auto_restore_skips_without_dump(backup_module):
    manager = backup_module.MetadataBackupManager()
    assert manager.auto_restore_if_empty(is_empty=True) is False


def test_metadata_backup_manager_migrates_legacy_backups(tmp_path, monkeypatch):
    from backup import manager as backup_manager
    import metadata_db.config as metadata_config

    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    dump = legacy_dir / "metadata_legacy.dump"
    dump.write_text("legacy")
    (legacy_dir / "metadata_legacy.dump.json").write_text(json.dumps({"created_at": "2024-01-01T00:00:00Z"}))

    target_dir = tmp_path / "new"
    monkeypatch.setenv("METADATA_DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:15432/testdb")
    monkeypatch.setenv("METADATA_BACKUP_DIR", str(target_dir))
    monkeypatch.setenv("METADATA_BACKUP_VERIFY", "false")
    monkeypatch.setenv("METADATA_BACKUP_ENABLED", "true")
    monkeypatch.setenv("METADATA_AUTO_RESTORE", "false")

    metadata_config.get_settings.cache_clear()
    metadata_config.get_backup_settings.cache_clear()
    backup_manager.get_backup_config.cache_clear()

    monkeypatch.setattr(backup_manager, "_legacy_directories", lambda key: [legacy_dir] if key is backup_manager.DatabaseKey.METADATA else [])

    config = backup_manager.get_backup_config(backup_manager.DatabaseKey.METADATA)
    backup_manager.PostgresBackupManager(config)

    migrated_dump = config.default_directory / dump.name
    assert migrated_dump.exists()
    assert not dump.exists()
    assert Path(f"{migrated_dump}.json").exists()

