"""DB-free route-layer tests for the analysis-pipeline API (frozen design §7).

These tests do NOT spin up the full FastAPI app against a live database. There
is no Postgres in this environment, and importing ``api.routes`` (the package
``__init__``) trips a PRE-EXISTING, unrelated FastAPI 204 collection error in a
sibling route module (``cohort_keywords``). So we load ONLY the
``analysis_pipelines`` route module by file path — bypassing the package
``__init__`` — and assert:

* the router exposes exactly the expected (method, path) routes (§7 table);
* the request-body models reject bad bodies (validation contract);
* the ``input_source`` enum guard maps unknown values to a clear error.

No route handler is invoked (that would need a DB / runner I/O), so nothing here
touches a session or the executor.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# Load the route module by file path WITHOUT executing api/routes/__init__.py
# (which has a pre-existing, unrelated 204 collection bug in a sibling module).
# ---------------------------------------------------------------------------

_SRC = Path(__file__).resolve().parents[1] / "src"


def _load_routes_module():
    if str(_SRC) not in sys.path:
        sys.path.insert(0, str(_SRC))
    # Provide a lightweight stand-in for the ``api.routes`` package so importing
    # the submodule by file does not run the real (buggy) package __init__.
    import api  # noqa: F401  # api/__init__ is light / safe

    if "api.routes" not in sys.modules:
        routes_pkg = types.ModuleType("api.routes")
        routes_pkg.__path__ = [str(_SRC / "api" / "routes")]
        sys.modules["api.routes"] = routes_pkg

    mod_name = "api.routes.analysis_pipelines"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    mod_path = _SRC / "api" / "routes" / "analysis_pipelines.py"
    spec = importlib.util.spec_from_file_location(mod_name, mod_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


routes = _load_routes_module()


# The (method, path) contract from frozen design §7.
EXPECTED_ROUTES = {
    ("POST", "/api/analysis-pipelines/repos"),
    ("GET", "/api/analysis-pipelines/repos"),
    ("POST", "/api/analysis-pipelines/repos/{repo_id}/refresh"),
    ("DELETE", "/api/analysis-pipelines/repos/{repo_id}"),
    ("GET", "/api/analysis-pipelines"),
    ("GET", "/api/analysis-pipelines/{pipeline_id}"),
    ("GET", "/api/analysis-pipelines/{pipeline_id}/runs"),
    ("POST", "/api/analysis-pipelines/{pipeline_id}/runs"),
    ("GET", "/api/analysis-pipelines/runs/{run_id}"),
    ("GET", "/api/analysis-pipelines/runs/{run_id}/state"),
    ("GET", "/api/analysis-pipelines/runs/{run_id}/logs"),
    ("GET", "/api/analysis-pipelines/runs/{run_id}/results"),
    ("POST", "/api/analysis-pipelines/runs/{run_id}/cancel"),
    ("POST", "/api/analysis-pipelines/runs/{run_id}/ingest"),
}


def _route_pairs() -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for r in routes.router.routes:
        methods = getattr(r, "methods", set()) or set()
        for method in methods:
            if method in {"HEAD", "OPTIONS"}:
                continue
            pairs.add((method, r.path))
    return pairs


# ---------------------------------------------------------------------------
# Router shape
# ---------------------------------------------------------------------------


def test_router_prefix_and_tags():
    assert routes.router.prefix == "/api/analysis-pipelines"
    # Tag is applied at include time via the router-level tags.
    assert "analysis-pipelines" in (routes.router.tags or [])


def test_router_exposes_exactly_the_expected_routes():
    pairs = _route_pairs()
    assert pairs == EXPECTED_ROUTES, (
        f"missing={EXPECTED_ROUTES - pairs} unexpected={pairs - EXPECTED_ROUTES}"
    )


def test_launch_and_ingest_routes_are_202():
    """Long-running launches/cancels/ingest return 202 (frozen design §7)."""
    by_path_method = {}
    for r in routes.router.routes:
        for method in getattr(r, "methods", set()) or set():
            by_path_method[(method, r.path)] = r

    for key in [
        ("POST", "/api/analysis-pipelines/{pipeline_id}/runs"),
        ("POST", "/api/analysis-pipelines/runs/{run_id}/cancel"),
        ("POST", "/api/analysis-pipelines/runs/{run_id}/ingest"),
    ]:
        assert by_path_method[key].status_code == 202

    # Register-repo creates a resource → 201.
    assert by_path_method[("POST", "/api/analysis-pipelines/repos")].status_code == 201
    # Remove is a 204 no-content delete.
    assert (
        by_path_method[("DELETE", "/api/analysis-pipelines/repos/{repo_id}")].status_code
        == 204
    )


# ---------------------------------------------------------------------------
# Request-body validation (the models reject bad bodies → 422 at the app layer)
# ---------------------------------------------------------------------------


def test_register_repo_body_requires_url():
    with pytest.raises(ValidationError):
        routes.RegisterRepoBody()  # url is required
    with pytest.raises(ValidationError):
        routes.RegisterRepoBody(url="")  # min_length=1
    body = routes.RegisterRepoBody(url="/some/repo")
    assert body.url == "/some/repo"
    assert body.ref is None


def test_create_run_body_requires_input_source():
    with pytest.raises(ValidationError):
        routes.CreateRunBody()  # input_source required
    body = routes.CreateRunBody(input_source="db_subset", stack_ids=[1, 2, 3])
    assert body.input_source == "db_subset"
    assert body.stack_ids == [1, 2, 3]
    # retain_input defaults to True (keep materialized input).
    assert body.retain_input is True


def test_create_run_body_rejects_non_int_stack_ids():
    with pytest.raises(ValidationError):
        routes.CreateRunBody(input_source="db_subset", stack_ids=["not-an-int"])


def test_refresh_repo_body_defaults_ref_none():
    body = routes.RefreshRepoBody()
    assert body.ref is None
    assert routes.RefreshRepoBody(ref="main").ref == "main"


def test_input_source_enum_guard_rejects_unknown_values():
    """The handler maps an unknown input_source to a clear 400 (enum coercion).

    We exercise the same coercion the handler uses (``InputSource(value)``)
    without invoking the handler (which would need a DB).
    """
    from analysis_pipeline.models import InputSource

    assert InputSource("db_subset") is InputSource.DB_SUBSET
    assert InputSource("external_path") is InputSource.EXTERNAL_PATH
    with pytest.raises(ValueError):
        InputSource("run_output")  # forward-compat seam, NOT a v1 value


# ---------------------------------------------------------------------------
# Wiring: the module exports `router` and is registered in api.routes.__all__
# ---------------------------------------------------------------------------


def test_module_exports_router():
    assert hasattr(routes, "router")
    assert routes.__all__ == ["router"]


def test_wired_into_routes_init_all():
    """The route __init__ exports + server registration reference the router.

    We read the source files (not import the buggy package) so this stays
    DB-free and avoids the pre-existing 204 collection error.
    """
    init_src = (_SRC / "api" / "routes" / "__init__.py").read_text(encoding="utf-8")
    assert (
        "from api.routes.analysis_pipelines import router as analysis_pipelines_router"
        in init_src
    )
    assert '"analysis_pipelines_router"' in init_src

    server_src = (_SRC / "api" / "server.py").read_text(encoding="utf-8")
    assert "analysis_pipelines_router" in server_src
    assert "app.include_router(analysis_pipelines_router)" in server_src
