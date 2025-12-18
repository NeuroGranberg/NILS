"""Unified compression engine mirroring the reference pack script."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

from .config import CompressionConfig

ProgressCallback = Optional[Callable[[int, int], None]]


# ---------------------------------------------------------------------------
# Size helpers
# ---------------------------------------------------------------------------


def human_to_bytes(value: str) -> int:
    cleaned = value.strip().lower().replace(" ", "")
    units = {
        "b": 1,
        "kb": 10**3,
        "mb": 10**6,
        "gb": 10**9,
        "tb": 10**12,
        "kib": 1024,
        "mib": 1024 ** 2,
        "gib": 1024 ** 3,
        "tib": 1024 ** 4,
    }
    if cleaned.isdigit():
        return int(cleaned)
    number = []
    unit = []
    for char in cleaned:
        if char.isdigit() or char == ".":
            number.append(char)
        else:
            unit.append(char)
    if not number:
        raise ValueError(f"Invalid size string '{value}'")
    unit_key = "".join(unit) or "b"
    if unit_key not in units:
        raise ValueError(f"Unknown size unit in '{value}'")
    return int(float("".join(number)) * units[unit_key])


def bytes_to_human(num_bytes: int) -> str:
    for suffix, threshold in (
        ("TiB", 1024 ** 4),
        ("GiB", 1024 ** 3),
        ("MiB", 1024 ** 2),
        ("KiB", 1024),
    ):
        if num_bytes >= threshold:
            return f"{num_bytes / threshold:.2f} {suffix}"
    return f"{num_bytes} B"


# ---------------------------------------------------------------------------
# Input discovery helpers
# ---------------------------------------------------------------------------


def _ensure_sevenz() -> str:
    for candidate in ("7zz", "7z"):
        location = shutil.which(candidate)
        if location:
            return location
    raise FileNotFoundError("Neither '7zz' nor '7z' is available on PATH. Install p7zip-full.")


def _have_par2() -> bool:
    return shutil.which("par2create") is not None


def _scan_top_level(root: Path) -> List[Dict[str, Any]]:
    discovered: List[Dict[str, Any]] = []
    for directory in sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name):
        total = 0
        files = 0
        for file_path in directory.rglob("*"):
            if not file_path.is_file():
                continue
            files += 1
            try:
                total += file_path.stat().st_size
            except OSError:
                continue
        discovered.append(
            {
                "pn": directory.name,
                "size": total,
                "num_files": files,
                "transfer_date": "",
            }
        )
    return discovered


# ---------------------------------------------------------------------------
# Packing strategies
# ---------------------------------------------------------------------------


def _pack_ordered(items: Sequence[Dict[str, Any]], limit: int) -> List[List[Dict[str, Any]]]:
    chunks: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    current_size = 0
    for entry in items:
        size = int(entry["size"])
        if current and current_size + size > limit:
            chunks.append(current)
            current = [entry]
            current_size = size
        else:
            current.append(entry)
            current_size += size
    if current:
        chunks.append(current)
    return chunks


def _pack_ffd(items: Sequence[Dict[str, Any]], limit: int) -> List[List[Dict[str, Any]]]:
    ordered = sorted(items, key=lambda x: int(x["size"]), reverse=True)
    bins: List[tuple[int, List[Dict[str, Any]]]] = []
    for entry in ordered:
        size = int(entry["size"])
        placed = False
        for index, (used, chunk) in enumerate(bins):
            if used + size <= limit:
                chunk.append(entry)
                bins[index] = (used + size, chunk)
                placed = True
                break
        if not placed:
            bins.append((size, [entry]))
    return [chunk for _, chunk in bins]


# ---------------------------------------------------------------------------
# 7-Zip execution helpers
# ---------------------------------------------------------------------------


def _build_archive_name(chunk_id: int, pns: Sequence[str]) -> str:
    if not pns:
        return f"dataset_chunk{chunk_id:04d}.7z"
    numeric = all(name.isdigit() for name in pns)
    start = min(pns) if numeric else pns[0]
    end = max(pns) if numeric else pns[-1]
    return f"dataset_chunk{chunk_id:04d}_pn{start}-{end}.7z"


def _build_7z_command(
    executable: str,
    archive: Path,
    relative_items: Sequence[str],
    password: str,
    compression: int,
) -> List[str]:
    cmd = [
        executable,
        "a",
        "-t7z",
        f"-mx={compression}",
        "-mmt=on",
        "-bb1",
        "-mhe=on",
        "-ms=off",
        f"-p{password}",
        str(archive),
    ]
    cmd.extend(relative_items)
    return cmd


@dataclass
class ChunkPlan:
    chunk_id: int
    members: List[Dict[str, Any]]
    total_bytes: int


@dataclass
class _ChunkResult:
    chunk_id: int
    archive: Path
    members: List[str]
    total_bytes: int
    elapsed_seconds: float
    checksum: str


def _sha256(path: Path, buffer_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(buffer_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _run_archive(
    root: Path,
    output_dir: Path,
    chunk_id: int,
    items: Sequence[Dict[str, Any]],
    executable: str,
    password: str,
    compression: int,
    verify: bool,
    par2: int,
) -> _ChunkResult:
    pn_names = [entry["pn"] for entry in items]
    archive_name = _build_archive_name(chunk_id, pn_names)
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / archive_name

    command = _build_7z_command(executable, archive_path, pn_names, password, compression)
    start = time.monotonic()
    proc = subprocess.run(command, cwd=str(root), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    elapsed = time.monotonic() - start
    if proc.returncode != 0:
        raise RuntimeError(f"7z failed for {archive_name}: {proc.stderr.strip()}")

    if verify:
        verify_cmd = [executable, "t", str(archive_path), f"-p{password}", "-bb1"]
        verify_proc = subprocess.run(verify_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if verify_proc.returncode != 0:
            raise RuntimeError(f"7z verify failed for {archive_name}: {verify_proc.stderr.strip()}")

    if par2 > 0 and _have_par2():
        subprocess.run(
            ["par2create", "-q", f"-r{par2}", str(archive_path)],
            cwd=str(output_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    checksum = _sha256(archive_path)
    total_bytes = sum(int(entry["size"]) for entry in items)
    return _ChunkResult(
        chunk_id=chunk_id,
        archive=archive_path,
        members=list(pn_names),
        total_bytes=total_bytes,
        elapsed_seconds=round(elapsed, 2),
        checksum=checksum,
    )


def _write_manifest(directory: Path, rows: Iterable[_ChunkResult]) -> Path:
    manifest = directory / "manifest_archives.csv"
    with manifest.open("w", newline="") as fh:
        import csv

        writer = csv.writer(fh)
        writer.writerow(["chunk_id", "archive", "num_entries", "entries", "total_bytes", "sha256", "elapsed_seconds"])
        for row in rows:
            writer.writerow(
                [
                    row.chunk_id,
                    row.archive.name,
                    len(row.members),
                    ";".join(row.members),
                    row.total_bytes,
                    row.checksum,
                    row.elapsed_seconds,
                ]
            )
    return manifest


# ---------------------------------------------------------------------------
# Planning & execution
# ---------------------------------------------------------------------------


def _load_top_level_entries(config: CompressionConfig) -> List[Dict[str, Any]]:
    return _scan_top_level(config.root)


def build_chunk_plan(config: CompressionConfig) -> List[ChunkPlan]:
    items = _load_top_level_entries(config)
    if not items:
        raise RuntimeError("No top-level folders found to compress.")

    limit = human_to_bytes(config.chunk)
    if config.strategy == "ffd":
        chunks = _pack_ffd(items, limit)
    else:
        chunks = _pack_ordered(items, limit)

    plans: List[ChunkPlan] = []
    for index, chunk in enumerate(chunks, start=1):
        total_bytes = sum(int(entry["size"]) for entry in chunk)
        plans.append(ChunkPlan(chunk_id=index, members=list(chunk), total_bytes=total_bytes))
    return plans


def run_compression(config: CompressionConfig, *, progress: ProgressCallback = None) -> Path:
    executable = _ensure_sevenz()
    plans = build_chunk_plan(config)
    total = len(plans)
    if progress:
        progress(0, total)

    password = config.password
    if not password:
        raise ValueError("Password is required for compression")

    results: List[_ChunkResult] = []
    failures: List[str] = []

    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = {
            executor.submit(
                _run_archive,
                config.root,
                config.out_dir,
                plan.chunk_id,
                plan.members,
                executable,
                password,
                config.compression,
                config.verify,
                config.par2,
            ): plan.chunk_id
            for plan in plans
        }
        for completed in as_completed(futures):
            index = futures[completed]
            try:
                result = completed.result()
                results.append(result)
            except Exception as exc:  # pragma: no cover - defensive
                failures.append(f"chunk {index}: {exc}")
            if progress:
                progress(len(results) + len(failures), total)

    if failures:
        raise RuntimeError("; ".join(failures))

    manifest = _write_manifest(config.out_dir, sorted(results, key=lambda r: r.chunk_id))
    return manifest


__all__ = ["run_compression", "build_chunk_plan", "bytes_to_human", "human_to_bytes"]
