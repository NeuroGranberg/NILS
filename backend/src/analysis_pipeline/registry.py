"""Analysis-pipeline registry service (frozen design §5).

A *registry* turns a git repository (Feature A, §9.1) into one or more
:class:`~analysis_pipeline.models.AnalysisPipeline` rows. The flow is:

1. **Fetch** the repo and **pin** its commit sha. A clone is done behind a thin
   :class:`RepoFetcher` seam so no real ``git``/network is required in tests
   (the default :class:`GitRepoFetcher` shells out to ``git``; tests inject a
   :class:`LocalFixtureRepoFetcher` that just resolves a local directory).
2. **Discover** descriptor files (``nils.job.yml`` at the repo root for the N=1
   case AND under ``pipelines/<slug>/nils.job.yml``).
3. **Parse + validate** each via the descriptor layer
   (:func:`analysis_pipeline.descriptor.parse_descriptor`).
4. **Pin** each descriptor's container digest.
5. **Persist** one :class:`AnalysisPipelineRepo` (identity ``(url, sha)``) plus
   one :class:`AnalysisPipeline` per descriptor (identity ``(repo, sha, slug)``).

Versioning is immutable-per-run (§17.10): ``refresh_repo`` NEVER mutates an
existing repo row. If the re-resolved sha differs it creates a brand-new repo
row at ``version = prior + 1`` with fresh pipelines; the old version and any
runs that pinned it are left untouched.

The git/scan/parse logic lives in **pure, DB-free helper functions**
(:func:`discover_descriptors`, :func:`build_pipeline_specs`,
:func:`next_repo_version`) so they are unit-testable without a database or the
network. Persistence (the :class:`AnalysisPipelineRegistry` methods) uses
``db.session.SessionLocal``.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Protocol

from sqlalchemy import select

from db.session import SessionLocal

from analysis_pipeline.config import get_scratch_root
from analysis_pipeline.descriptor import (
    Descriptor,
    DescriptorError,
    parse_descriptor,
)
from analysis_pipeline.models import (
    AnalysisPipeline,
    AnalysisPipelineRepo,
)

logger = logging.getLogger(__name__)

# Descriptor filename convention (Feature A, §9.1).
DESCRIPTOR_FILENAME = "nils.job.yml"

def _registry_cache_root() -> Path:
    """Local clone cache directory under the scratch root (``.../registry``).

    Reuses the single scratch-root resolver in :mod:`analysis_pipeline.config`
    (env ``PIPELINE_SCRATCH_ROOT``) rather than duplicating it.
    """
    return get_scratch_root() / "registry"


# ---------------------------------------------------------------------------
# Repo-fetch seam (so no real git/network is needed in tests)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchedRepo:
    """The result of fetching a repo: a local checkout pinned to a sha."""

    local_path: Path
    sha: str
    default_branch: Optional[str] = None


class RepoFetcher(Protocol):
    """Fetches a repo's content and resolves its pinned commit sha."""

    def fetch(self, url: str, ref: str | None = None) -> FetchedRepo:
        ...


class GitRepoFetcher:
    """Default fetcher: ``git clone`` into the local registry cache + pin sha.

    A shallow clone is done into ``<scratch>/registry/<safe-name>`` and the
    resolved commit sha is read via ``git rev-parse HEAD``. This is the only
    place that shells out to ``git``; it is bypassed in tests by injecting a
    :class:`LocalFixtureRepoFetcher`.
    """

    def __init__(self, cache_root: Path | None = None) -> None:
        self._cache_root = cache_root or _registry_cache_root()

    @staticmethod
    def _safe_dir_name(url: str) -> str:
        """Derive a filesystem-safe directory name from a repo URL."""
        tail = url.rstrip("/").rsplit("/", 1)[-1]
        if tail.endswith(".git"):
            tail = tail[: -len(".git")]
        safe = "".join(c if (c.isalnum() or c in "-_.") else "-" for c in tail)
        return safe or "repo"

    def fetch(self, url: str, ref: str | None = None) -> FetchedRepo:
        self._cache_root.mkdir(parents=True, exist_ok=True)
        # Unique destination so re-fetching a moving ref never collides with a
        # stale checkout (immutable-per-run: every fetch is a fresh snapshot).
        dest = Path(
            tempfile.mkdtemp(
                prefix=f"{self._safe_dir_name(url)}-",
                dir=str(self._cache_root),
            )
        )
        clone_cmd = ["git", "clone", "--depth", "1"]
        if ref:
            clone_cmd += ["--branch", ref]
        clone_cmd += [url, str(dest)]
        try:
            subprocess.run(
                clone_cmd,
                check=True,
                capture_output=True,
                text=True,
            )
            sha = subprocess.run(
                ["git", "-C", str(dest), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            branch_proc = subprocess.run(
                ["git", "-C", str(dest), "rev-parse", "--abbrev-ref", "HEAD"],
                check=False,
                capture_output=True,
                text=True,
            )
            branch = branch_proc.stdout.strip() or None
            if branch == "HEAD":  # detached
                branch = ref
        except (subprocess.CalledProcessError, OSError) as exc:
            stderr = getattr(exc, "stderr", "") or ""
            raise RegistryError(
                f"Failed to clone {url!r}: {stderr or exc}"
            ) from exc
        if not sha:
            raise RegistryError(f"Could not resolve commit sha for {url!r}.")
        return FetchedRepo(local_path=dest, sha=sha, default_branch=branch)


class LocalFixtureRepoFetcher:
    """Test/dev fetcher: resolves a repo to a local directory (NO network).

    ``url`` is treated as a local path to a repo checkout. The sha is read from
    git if the directory is a real repo, otherwise a deterministic synthetic sha
    is derived from the path so identity ``(url, sha, slug)`` stays stable across
    calls (this is what the scope boundary's "local fixture repo" relies on).
    """

    def __init__(self, sha: str | None = None) -> None:
        self._sha = sha

    def fetch(self, url: str, ref: str | None = None) -> FetchedRepo:
        root = Path(url)
        if not root.exists():
            raise RegistryError(f"Local fixture repo path does not exist: {url}")
        sha = self._sha or _read_git_sha(root) or _synthetic_sha(url)
        return FetchedRepo(local_path=root, sha=sha, default_branch=ref or "main")


def _read_git_sha(root: Path) -> str | None:
    """Best-effort ``git rev-parse HEAD`` for a local checkout (None if not a repo)."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    sha = proc.stdout.strip()
    return sha or None


def _synthetic_sha(value: str) -> str:
    """Deterministic 40-hex synthetic sha for non-git fixture directories."""
    import hashlib

    return hashlib.sha1(value.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Pure helpers (DB-free, network-free, unit-tested directly)
# ---------------------------------------------------------------------------


class RegistryError(RuntimeError):
    """Raised for registry-level failures (clone, no descriptors, hard delete)."""


@dataclass
class PipelineSpec:
    """A parsed-and-pinned descriptor ready to become an ``AnalysisPipeline`` row.

    DB-free intermediate produced by :func:`build_pipeline_specs`; the registry
    turns each into a persisted row. Carries the relative descriptor path for
    provenance/logging and any non-fatal descriptor warnings.
    """

    slug: str
    name: str
    description: Optional[str]
    work_unit: str
    analysis_level: str
    image_digest: Optional[str]
    schema_version: str
    descriptor: dict[str, Any]
    source_path: str
    warnings: list[str] = field(default_factory=list)


def discover_descriptors(repo_root: Path) -> list[Path]:
    """Find descriptor files in a repo checkout (PURE — filesystem scan only).

    Scans, in deterministic order:

    * ``<repo_root>/nils.job.yml`` — the N=1 single-pipeline repo case.
    * ``<repo_root>/pipelines/<slug>/nils.job.yml`` — multi-pipeline repos.

    Returns the matching paths sorted for stable ordering. Does not parse.
    """
    repo_root = Path(repo_root)
    found: list[Path] = []

    root_descriptor = repo_root / DESCRIPTOR_FILENAME
    if root_descriptor.is_file():
        found.append(root_descriptor)

    pipelines_dir = repo_root / "pipelines"
    if pipelines_dir.is_dir():
        for child in sorted(pipelines_dir.iterdir()):
            if not child.is_dir():
                continue
            candidate = child / DESCRIPTOR_FILENAME
            if candidate.is_file():
                found.append(candidate)

    return found


def build_pipeline_specs(repo_root: Path) -> list[PipelineSpec]:
    """Discover + parse + validate every descriptor under a repo checkout.

    PURE (DB-free, network-free): drives the unit tests for scan/parse/digest
    pinning. Raises :class:`RegistryError` if no descriptor is found. Descriptor
    parse/validation errors propagate as :class:`DescriptorError` /
    :class:`UnsupportedSchemaVersionError` from the descriptor layer.
    """
    repo_root = Path(repo_root)
    paths = discover_descriptors(repo_root)
    if not paths:
        raise RegistryError(
            f"No {DESCRIPTOR_FILENAME!r} descriptor found in repo at {repo_root}."
        )

    specs: list[PipelineSpec] = []
    seen_slugs: dict[str, str] = {}
    for path in paths:
        descriptor: Descriptor = parse_descriptor(path)
        spec = _spec_from_descriptor(descriptor, path, repo_root)
        if spec.slug in seen_slugs:
            raise RegistryError(
                f"Duplicate pipeline slug {spec.slug!r} in repo "
                f"(from {seen_slugs[spec.slug]} and {spec.source_path})."
            )
        seen_slugs[spec.slug] = spec.source_path
        specs.append(spec)
    return specs


def _spec_from_descriptor(
    descriptor: Descriptor, path: Path, repo_root: Path
) -> PipelineSpec:
    """Project a parsed :class:`Descriptor` into a :class:`PipelineSpec`."""
    try:
        rel = str(Path(path).resolve().relative_to(Path(repo_root).resolve()))
    except ValueError:
        rel = str(path)
    return PipelineSpec(
        slug=descriptor.slug,
        name=descriptor.name,
        description=descriptor.description,
        work_unit=descriptor.work_unit,
        analysis_level=descriptor.analysis_level,
        image_digest=descriptor.image_digest,  # container-hash pinning (§5)
        schema_version=descriptor.schema_version,
        descriptor=descriptor.model_dump(by_alias=True),
        source_path=rel,
        warnings=descriptor.warnings,
    )


def next_repo_version(prior_version: int | None) -> int:
    """Compute the version for a new repo row (§17.10 immutable-per-run).

    ``None`` (first registration) → 1; otherwise ``prior + 1``. Kept as a tiny
    pure function so the version-bump rule is unit-tested in isolation.
    """
    if prior_version is None:
        return 1
    return prior_version + 1


# ---------------------------------------------------------------------------
# Registry service (App-DB persistence)
# ---------------------------------------------------------------------------


class AnalysisPipelineRegistry:
    """Service that registers/refreshes/lists/removes repos + their pipelines.

    App-DB I/O uses ``db.session.SessionLocal``; repo content comes from an
    injected :class:`RepoFetcher` (default :class:`GitRepoFetcher`). All returned
    rows are expunged (detached) so callers can read them after the session
    closes.
    """

    def __init__(self, fetcher: RepoFetcher | None = None) -> None:
        self._fetcher = fetcher or GitRepoFetcher()

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _make_pipeline_row(
        repo_id: int, version: int, spec: PipelineSpec
    ) -> AnalysisPipeline:
        return AnalysisPipeline(
            repo_id=repo_id,
            slug=spec.slug,
            name=spec.name,
            description=spec.description,
            work_unit=spec.work_unit,
            analysis_level=spec.analysis_level,
            image_digest=spec.image_digest,
            schema_version=spec.schema_version,
            descriptor=spec.descriptor,
            version=version,
        )

    def _persist_repo_with_pipelines(
        self,
        *,
        url: str,
        fetched: FetchedRepo,
        specs: list[PipelineSpec],
        version: int,
    ) -> tuple[AnalysisPipelineRepo, list[AnalysisPipeline]]:
        """Create + commit a repo row + its pipelines; return detached rows."""
        with SessionLocal() as session:
            repo = AnalysisPipelineRepo(
                url=url,
                sha=fetched.sha,
                default_branch=fetched.default_branch,
                version=version,
            )
            session.add(repo)
            session.flush()  # assign repo.id

            pipelines = [
                self._make_pipeline_row(repo.id, version, spec) for spec in specs
            ]
            session.add_all(pipelines)
            session.commit()

            session.refresh(repo)
            for p in pipelines:
                session.refresh(p)
            session.expunge_all()
            return repo, pipelines

    # -- public API -------------------------------------------------------

    def register_repo(
        self, url: str, *, ref: str | None = None
    ) -> tuple[AnalysisPipelineRepo, list[AnalysisPipeline]]:
        """Register a repo by URL → repo row + one pipeline per descriptor.

        Fetch → pin sha → discover/parse/validate descriptors → create an
        :class:`AnalysisPipelineRepo` (version=1 if new) + one
        :class:`AnalysisPipeline` each. Identity is ``(url, sha)`` for the repo
        and ``(repo, slug)`` for each pipeline. If a repo row for this exact
        ``(url, sha)`` already exists, it is returned as-is (idempotent — no
        duplicate version).
        """
        fetched = self._fetcher.fetch(url, ref=ref)
        specs = build_pipeline_specs(fetched.local_path)

        existing = self._find_repo_by_url_sha(url, fetched.sha)
        if existing is not None:
            repo, pipelines = existing
            logger.info(
                "Repo %s@%s already registered (version=%s); returning existing.",
                url,
                fetched.sha[:8],
                repo.version,
            )
            return repo, pipelines

        repo, pipelines = self._persist_repo_with_pipelines(
            url=url, fetched=fetched, specs=specs, version=1
        )
        logger.info(
            "Registered repo %s@%s (version=1) with %d pipeline(s): %s",
            url,
            fetched.sha[:8],
            len(pipelines),
            ", ".join(p.slug for p in pipelines),
        )
        return repo, pipelines

    def refresh_repo(
        self, repo_id: int, *, ref: str | None = None
    ) -> tuple[AnalysisPipelineRepo, list[AnalysisPipeline]]:
        """Re-pin the latest sha for a repo → NEW immutable version (§17.10).

        Re-fetches the repo URL of ``repo_id``. If the resolved sha is unchanged
        the call is a no-op (returns the current row). If it changed, a brand-new
        :class:`AnalysisPipelineRepo` row is created at ``version = prior + 1``
        with fresh pipelines; the old version + any runs that pinned it are left
        untouched (NEVER mutated in place).
        """
        with SessionLocal() as session:
            current = session.get(AnalysisPipelineRepo, repo_id)
            if current is None:
                raise RegistryError(f"No analysis-pipeline repo with id={repo_id}.")
            url = current.url
            prior_version = current.version

        fetched = self._fetcher.fetch(url, ref=ref)

        # Unchanged sha → no-op: return the existing row (already-registered).
        existing = self._find_repo_by_url_sha(url, fetched.sha)
        if existing is not None:
            repo, pipelines = existing
            logger.info(
                "refresh_repo(%s): sha unchanged (%s); no new version.",
                repo_id,
                fetched.sha[:8],
            )
            return repo, pipelines

        # Changed sha → brand-new immutable version.
        specs = build_pipeline_specs(fetched.local_path)
        new_version = next_repo_version(prior_version)
        repo, pipelines = self._persist_repo_with_pipelines(
            url=url, fetched=fetched, specs=specs, version=new_version
        )
        logger.info(
            "refresh_repo(%s): sha changed → new repo version=%d (%s) with %d pipeline(s).",
            repo_id,
            new_version,
            fetched.sha[:8],
            len(pipelines),
        )
        return repo, pipelines

    def list_repos(self) -> list[AnalysisPipelineRepo]:
        """All registered repo versions (newest first)."""
        with SessionLocal() as session:
            repos = list(
                session.scalars(
                    select(AnalysisPipelineRepo).order_by(
                        AnalysisPipelineRepo.created_at.desc(),
                        AnalysisPipelineRepo.id.desc(),
                    )
                )
            )
            session.expunge_all()
            return repos

    def list_pipelines(
        self, *, repo_id: int | None = None
    ) -> list[AnalysisPipeline]:
        """All pipelines (optionally scoped to a single repo version)."""
        with SessionLocal() as session:
            stmt = select(AnalysisPipeline)
            if repo_id is not None:
                stmt = stmt.where(AnalysisPipeline.repo_id == repo_id)
            stmt = stmt.order_by(AnalysisPipeline.id)
            pipelines = list(session.scalars(stmt))
            session.expunge_all()
            return pipelines

    def get_pipeline(self, pipeline_id: int) -> AnalysisPipeline | None:
        """Fetch one pipeline by id (detached), or ``None``."""
        with SessionLocal() as session:
            pipeline = session.get(AnalysisPipeline, pipeline_id)
            if pipeline is not None:
                session.expunge(pipeline)
            return pipeline

    def get_pipeline_by_identity(
        self, *, repo_url: str, sha: str, slug: str
    ) -> AnalysisPipeline | None:
        """Resolve a pipeline by its ``(repo_url, sha, slug)`` identity."""
        with SessionLocal() as session:
            repo = session.scalars(
                select(AnalysisPipelineRepo).where(
                    AnalysisPipelineRepo.url == repo_url,
                    AnalysisPipelineRepo.sha == sha,
                )
            ).first()
            if repo is None:
                return None
            pipeline = session.scalars(
                select(AnalysisPipeline).where(
                    AnalysisPipeline.repo_id == repo.id,
                    AnalysisPipeline.slug == slug,
                )
            ).first()
            if pipeline is not None:
                session.expunge(pipeline)
            return pipeline

    def remove_repo(self, repo_id: int) -> None:
        """Unregister a repo version (§15.1 — past-run provenance survives).

        Deletes the repo row + its pipelines (cascade) IFF no run references any
        of those pipelines. If runs exist, the hard delete would violate the
        ``RESTRICT`` FK and destroy provenance, so it is refused with a
        :class:`RegistryError` (soft-unregister / forbid-delete-with-runs, §5).
        """
        with SessionLocal() as session:
            repo = session.get(AnalysisPipelineRepo, repo_id)
            if repo is None:
                raise RegistryError(f"No analysis-pipeline repo with id={repo_id}.")

            # Guard: refuse if any pipeline of this repo has runs (provenance).
            from analysis_pipeline.models import AnalysisPipelineRun

            run_count = session.scalar(
                select(AnalysisPipelineRun.id)
                .join(
                    AnalysisPipeline,
                    AnalysisPipelineRun.pipeline_id == AnalysisPipeline.id,
                )
                .where(AnalysisPipeline.repo_id == repo_id)
                .limit(1)
            )
            if run_count is not None:
                raise RegistryError(
                    f"Cannot remove repo id={repo_id}: it has pipelines with runs; "
                    "removal would destroy run provenance (§15.1)."
                )

            session.delete(repo)  # cascade deletes its pipelines
            session.commit()
        logger.info("Removed analysis-pipeline repo id=%s (no runs referenced it).", repo_id)

    # Backwards-friendly alias matching the frozen design's `remove` name.
    remove = remove_repo

    # -- internal lookups -------------------------------------------------

    def _find_repo_by_url_sha(
        self, url: str, sha: str
    ) -> tuple[AnalysisPipelineRepo, list[AnalysisPipeline]] | None:
        """Return an existing ``(repo, pipelines)`` for ``(url, sha)`` or ``None``."""
        with SessionLocal() as session:
            repo = session.scalars(
                select(AnalysisPipelineRepo).where(
                    AnalysisPipelineRepo.url == url,
                    AnalysisPipelineRepo.sha == sha,
                )
            ).first()
            if repo is None:
                return None
            pipelines = list(
                session.scalars(
                    select(AnalysisPipeline)
                    .where(AnalysisPipeline.repo_id == repo.id)
                    .order_by(AnalysisPipeline.id)
                )
            )
            session.expunge_all()
            return repo, pipelines


# Singleton (mirrors job_service / nils_pipeline_service singleton style).
analysis_pipeline_registry = AnalysisPipelineRegistry()


__all__ = [
    "DESCRIPTOR_FILENAME",
    "FetchedRepo",
    "RepoFetcher",
    "GitRepoFetcher",
    "LocalFixtureRepoFetcher",
    "RegistryError",
    "PipelineSpec",
    "discover_descriptors",
    "build_pipeline_specs",
    "next_repo_version",
    "AnalysisPipelineRegistry",
    "analysis_pipeline_registry",
]
