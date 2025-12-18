from __future__ import annotations

from pathlib import Path

from compress.config import CompressionConfig
from compress.engine import build_chunk_plan, human_to_bytes


def _touch(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"0" * size)


def test_build_chunk_plan_scans_directories(tmp_path: Path) -> None:
    root = tmp_path / "root"
    _touch(root / "pn001" / "file.dcm", 1024)
    _touch(root / "pn002" / "file.dcm", 512)

    config = CompressionConfig(
        root=root,
        out_dir=tmp_path / "out",
        chunk="1MB",
        strategy="ordered",
        compression=3,
        workers=1,
        password="secret",
        verify=False,
        par2=0,
    )

    plans = build_chunk_plan(config)
    assert len(plans) == 1
    assert plans[0].total_bytes == 1536
    members = {item["pn"] for item in plans[0].members}
    assert members == {"pn001", "pn002"}


def test_build_chunk_plan_handles_multiple_chunks(tmp_path: Path) -> None:
    root = tmp_path / "root"
    _touch(root / "pn010" / "file.dcm", human_to_bytes("90KB"))
    _touch(root / "pn011" / "file.dcm", human_to_bytes("30KB"))

    config = CompressionConfig(
        root=root,
        out_dir=tmp_path / "out",
        chunk="100KB",
        strategy="ordered",
        compression=3,
        workers=1,
        password="secret",
        verify=False,
        par2=0,
    )

    plans = build_chunk_plan(config)
    assert len(plans) == 2
    assert {entry["pn"] for entry in plans[0].members} == {"pn010"}
    assert {entry["pn"] for entry in plans[1].members} == {"pn011"}


def test_human_to_bytes_handles_units() -> None:
    assert human_to_bytes("1GB") == 1_000_000_000
