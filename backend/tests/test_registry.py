"""DB-free tests for the analysis-pipeline REGISTRY layer (frozen design §5).

There is NO running Postgres and NO network in this environment, so these tests
exercise only the **pure helpers** of ``analysis_pipeline.registry``:

* :func:`discover_descriptors` / :func:`build_pipeline_specs` pointed at a LOCAL
  fixture repo (``tests/fixtures/analysis_pipeline/fixture_repo`` — a root
  ``nils.job.yml`` for the N=1 case + a ``pipelines/<slug>/nils.job.yml`` for the
  multi-pipeline case): assert both descriptors are discovered + parsed, and that
  container digests are pinned from each descriptor.
* The version-immutability rule (:func:`next_repo_version`) on in-memory objects:
  refresh yields a NEW version, the old one is untouched (§17.10).
* :class:`LocalFixtureRepoFetcher` resolving a local path (NO network clone) and
  producing a deterministic, stable identity sha.

No DB session / engine / ``git clone`` is touched.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from analysis_pipeline.descriptor import (
    DescriptorError,
    UnsupportedSchemaVersionError,
)
from analysis_pipeline.registry import (
    DESCRIPTOR_FILENAME,
    FetchedRepo,
    LocalFixtureRepoFetcher,
    PipelineSpec,
    RegistryError,
    build_pipeline_specs,
    discover_descriptors,
    next_repo_version,
)


FIXTURE_REPO = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "analysis_pipeline"
    / "fixture_repo"
)


# ---------------------------------------------------------------------------
# discover_descriptors — scan root + pipelines/<slug>/
# ---------------------------------------------------------------------------


def test_fixture_repo_exists():
    assert FIXTURE_REPO.is_dir(), f"missing fixture repo at {FIXTURE_REPO}"
    assert (FIXTURE_REPO / DESCRIPTOR_FILENAME).is_file()
    assert (FIXTURE_REPO / "pipelines" / "mriqc" / DESCRIPTOR_FILENAME).is_file()


def test_discover_finds_root_and_nested_descriptors():
    paths = discover_descriptors(FIXTURE_REPO)
    rels = {str(p.relative_to(FIXTURE_REPO)) for p in paths}
    assert rels == {
        DESCRIPTOR_FILENAME,
        str(Path("pipelines") / "mriqc" / DESCRIPTOR_FILENAME),
    }
    # Root descriptor is discovered first (deterministic ordering).
    assert paths[0] == FIXTURE_REPO / DESCRIPTOR_FILENAME


def test_discover_empty_dir_returns_empty(tmp_path: Path):
    assert discover_descriptors(tmp_path) == []


def test_discover_ignores_non_descriptor_files(tmp_path: Path):
    (tmp_path / "README.md").write_text("not a descriptor", encoding="utf-8")
    pdir = tmp_path / "pipelines" / "foo"
    pdir.mkdir(parents=True)
    (pdir / "notes.txt").write_text("nope", encoding="utf-8")
    assert discover_descriptors(tmp_path) == []


# ---------------------------------------------------------------------------
# build_pipeline_specs — parse + validate + digest pinning
# ---------------------------------------------------------------------------


def test_build_specs_parses_both_pipelines():
    specs = build_pipeline_specs(FIXTURE_REPO)
    by_slug = {s.slug: s for s in specs}
    assert set(by_slug) == {"dcm2niix", "mriqc"}
    assert all(isinstance(s, PipelineSpec) for s in specs)


def test_build_specs_pins_container_digest_from_container_hash():
    # dcm2niix supplies an explicit container-hash → that is the pinned digest.
    specs = {s.slug: s for s in build_pipeline_specs(FIXTURE_REPO)}
    dcm = specs["dcm2niix"]
    assert dcm.image_digest == (
        "sha256:0123456789abcdef0123456789abcdef"
        "0123456789abcdef0123456789abcdef"
    )


def test_build_specs_pins_digest_from_image_ref_when_no_container_hash():
    # MRIQC has no container-hash; the digest is extracted from the @sha256 ref.
    specs = {s.slug: s for s in build_pipeline_specs(FIXTURE_REPO)}
    mriqc = specs["mriqc"]
    assert mriqc.image_digest == (
        "sha256:abcabcabcabcabcabcabcabcabcabcabc"
        "abcabcabcabcabcabcabcabcabcabca0"
    )


def test_build_specs_resolves_work_unit_and_level():
    specs = {s.slug: s for s in build_pipeline_specs(FIXTURE_REPO)}
    # dcm2niix: analysis-level=run + dicom formats → work_unit "stack".
    assert specs["dcm2niix"].analysis_level == "run"
    assert specs["dcm2niix"].work_unit == "stack"
    # mriqc: subject-level → subject work unit.
    assert specs["mriqc"].analysis_level == "subject"
    assert specs["mriqc"].work_unit == "subject"


def test_build_specs_carries_full_descriptor_and_schema_version():
    specs = {s.slug: s for s in build_pipeline_specs(FIXTURE_REPO)}
    dcm = specs["dcm2niix"]
    assert dcm.schema_version == "0.5"
    # Full descriptor snapshot is by-alias (hyphenated wire keys preserved).
    assert dcm.descriptor["name"] == "dcm2niix"
    assert dcm.descriptor["schema-version"] == "0.5"
    assert "x-nils" in dcm.descriptor
    # Source path is repo-relative for provenance.
    assert dcm.source_path == DESCRIPTOR_FILENAME


def test_build_specs_raises_when_no_descriptor(tmp_path: Path):
    with pytest.raises(RegistryError):
        build_pipeline_specs(tmp_path)


def test_build_specs_propagates_unsupported_schema_version(tmp_path: Path):
    (tmp_path / DESCRIPTOR_FILENAME).write_text(
        'name: bad\nschema-version: "9.9"\ncommand-line: "x"\n',
        encoding="utf-8",
    )
    with pytest.raises(UnsupportedSchemaVersionError):
        build_pipeline_specs(tmp_path)


def test_build_specs_propagates_missing_name(tmp_path: Path):
    (tmp_path / DESCRIPTOR_FILENAME).write_text(
        'schema-version: "0.5"\ncommand-line: "x"\n',
        encoding="utf-8",
    )
    with pytest.raises(DescriptorError):
        build_pipeline_specs(tmp_path)


def test_build_specs_rejects_duplicate_slug(tmp_path: Path):
    # Root and a nested pipeline with the same name → same slug → conflict.
    (tmp_path / DESCRIPTOR_FILENAME).write_text(
        'name: dup\nschema-version: "0.5"\ncommand-line: "x"\n',
        encoding="utf-8",
    )
    nested = tmp_path / "pipelines" / "dup"
    nested.mkdir(parents=True)
    (nested / DESCRIPTOR_FILENAME).write_text(
        'name: Dup\nschema-version: "0.5"\ncommand-line: "y"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegistryError):
        build_pipeline_specs(tmp_path)


# ---------------------------------------------------------------------------
# next_repo_version — immutable-per-run version bump (§17.10)
# ---------------------------------------------------------------------------


def test_next_repo_version_first_registration_is_one():
    assert next_repo_version(None) == 1


def test_next_repo_version_bumps_on_refresh():
    assert next_repo_version(1) == 2
    assert next_repo_version(7) == 8


def test_refresh_yields_new_version_old_untouched_in_memory():
    """Version-immutability on in-memory objects: a refresh produces a NEW
    version while the prior one is byte-for-byte untouched (§17.10)."""
    # Cohort declares relationship("NilsDatasetPipelineStep"); importing the
    # sibling module makes that class name resolvable so SQLAlchemy can configure
    # the app-DB mapper set before an AnalysisPipelineRepo instance is built.
    # DB-free: this only registers ORM classes (no engine/session touched).
    import nils_dataset_pipeline.models  # noqa: F401
    from analysis_pipeline.models import AnalysisPipelineRepo

    old = AnalysisPipelineRepo(
        id=1,
        url="https://example.test/repo.git",
        sha="a" * 40,
        default_branch="main",
        version=1,
    )
    # Re-fetch resolves a DIFFERENT sha → a brand-new row, never a mutation.
    new = AnalysisPipelineRepo(
        id=2,
        url=old.url,
        sha="b" * 40,
        default_branch="main",
        version=next_repo_version(old.version),
    )
    assert new.version == 2
    assert new.sha != old.sha
    # The old row is completely unchanged (immutable-per-run).
    assert old.version == 1
    assert old.sha == "a" * 40
    assert old.id == 1


# ---------------------------------------------------------------------------
# LocalFixtureRepoFetcher — resolve a local path, NO network
# ---------------------------------------------------------------------------


def test_local_fixture_fetcher_resolves_path_no_network():
    fetcher = LocalFixtureRepoFetcher()
    fetched = fetcher.fetch(str(FIXTURE_REPO))
    assert isinstance(fetched, FetchedRepo)
    assert fetched.local_path == FIXTURE_REPO
    assert fetched.sha  # non-empty


def test_local_fixture_fetcher_sha_is_deterministic_for_non_git_dir():
    # The fixture dir is not a git checkout → synthetic-but-stable identity sha.
    fetcher = LocalFixtureRepoFetcher()
    a = fetcher.fetch(str(FIXTURE_REPO))
    b = fetcher.fetch(str(FIXTURE_REPO))
    assert a.sha == b.sha
    assert len(a.sha) == 40  # 40-hex


def test_local_fixture_fetcher_explicit_sha_override():
    fetcher = LocalFixtureRepoFetcher(sha="deadbeef" * 5)
    fetched = fetcher.fetch(str(FIXTURE_REPO), ref="release")
    assert fetched.sha == "deadbeef" * 5
    assert fetched.default_branch == "release"


def test_local_fixture_fetcher_missing_path_raises():
    fetcher = LocalFixtureRepoFetcher()
    with pytest.raises(RegistryError):
        fetcher.fetch("/no/such/repo/path/exists")


def test_local_fixture_fetch_then_build_specs_end_to_end():
    """The DB-free half of register_repo: fetch (local) → build specs."""
    fetcher = LocalFixtureRepoFetcher(sha="c" * 40)
    fetched = fetcher.fetch(str(FIXTURE_REPO))
    specs = build_pipeline_specs(fetched.local_path)
    assert {s.slug for s in specs} == {"dcm2niix", "mriqc"}
    assert fetched.sha == "c" * 40


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
