"""CLI Contract: shared infrastructure for every NILS command.

This module is the single source of truth for the conventions documented in
`docs/cli-contract.md`. Every new CLI command MUST go through these helpers
rather than re-implementing flag parsing, config resolution, or output rendering.

The public surface is intentionally small:

* `CliContext` — runtime context populated by the universal Typer callback.
* `register_universal_callback` — wires the callback into a `typer.Typer` app.
* `resolve_config` — turns (config file + sets + runtime overrides + targets)
  into a validated Pydantic model.
* `cohort_target.resolve` — resolves `--cohort` / `--cohort-id` to a cohort.
* `ExitCode` — standard exit codes from the contract.

See `cli/output.py` for the matching output helpers.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

import typer
import yaml
from pydantic import BaseModel, ValidationError

ModelT = TypeVar("ModelT", bound=BaseModel)


# --------------------------------------------------------------------------- #
# Exit codes (contract §5)
# --------------------------------------------------------------------------- #


class ExitCode(IntEnum):
    """Standard CLI exit codes. Scripts depend on these — do not deviate."""

    SUCCESS = 0
    GENERIC_FAILURE = 1
    INVALID_ARGS = 2
    NOT_FOUND = 3
    CONFLICT = 4
    BACKEND_ERROR = 5
    INTERRUPTED = 130


def fail(message: str, code: ExitCode = ExitCode.GENERIC_FAILURE) -> None:
    """Exit with a one-line error message on stderr and the given code."""
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(code=int(code))


# --------------------------------------------------------------------------- #
# CliContext — populated by the universal Typer callback
# --------------------------------------------------------------------------- #


@dataclass
class CliContext:
    """Per-invocation context shared by every command body.

    Populated by `register_universal_callback`. Commands receive it via
    `typer.Context.obj`. Do not construct directly.
    """

    print_config: bool = False
    dry_run: bool = False
    json_output: bool = False
    quiet: bool = False
    verbose: int = 0
    yes: bool = False
    remote: Optional[str] = None
    profile: str = "default"

    extra: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------ #
    # Convenience predicates
    # ------------------------------------------------------------------ #

    @property
    def is_remote(self) -> bool:
        return bool(self.remote)

    @property
    def log_level(self) -> str:
        return {0: "WARNING", 1: "INFO"}.get(self.verbose, "DEBUG")


def get_context(typer_ctx: typer.Context) -> CliContext:
    """Retrieve the `CliContext` attached to a Typer context.

    Falls back to a default context when the universal callback hasn't been
    registered (e.g. in unit tests).
    """
    obj = getattr(typer_ctx, "obj", None)
    if isinstance(obj, CliContext):
        return obj
    return CliContext()


# --------------------------------------------------------------------------- #
# Universal callback (contract §3)
# --------------------------------------------------------------------------- #


def register_universal_callback(app: typer.Typer) -> None:
    """Attach the universal options to a Typer app's root callback.

    These flags are accepted by every command via `typer.Context`:

        --print-config, --dry-run, --json, -q/--quiet, -v/--verbose,
        -y/--yes, --remote URL, --profile NAME.

    Commands access them via `get_context(ctx)` rather than redeclaring them.
    """

    @app.callback()
    def _root(
        ctx: typer.Context,
        print_config: bool = typer.Option(
            False,
            "--print-config",
            help="Resolve config (file + sets + overrides), print as YAML, exit.",
        ),
        dry_run: bool = typer.Option(
            False, "--dry-run", help="Plan only; validate and show what would happen."
        ),
        json_output: bool = typer.Option(
            False, "--json", help="Emit machine-readable JSON instead of human tables."
        ),
        quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress informational output."),
        verbose: int = typer.Option(
            0, "--verbose", "-v", count=True, help="Increase log level (-v INFO, -vv DEBUG)."
        ),
        yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts."),
        remote: Optional[str] = typer.Option(
            None,
            "--remote",
            metavar="URL",
            help="Submit to a remote NILS instance instead of running locally.",
        ),
        profile: str = typer.Option(
            "default", "--profile", metavar="NAME", help="CLI profile name."
        ),
    ) -> None:
        ctx.obj = CliContext(
            print_config=print_config,
            dry_run=dry_run,
            json_output=json_output,
            quiet=quiet,
            verbose=verbose,
            yes=yes,
            remote=remote,
            profile=profile,
        )


# --------------------------------------------------------------------------- #
# Config resolution (contract §1.2, §4.1)
# --------------------------------------------------------------------------- #


def _load_config_file(path: Path) -> dict[str, Any]:
    """Load a config file by extension. YAML and JSON supported."""
    if not path.exists():
        fail(f"config file not found: {path}", ExitCode.NOT_FOUND)
    text = path.read_text()
    suffix = path.suffix.lower()
    try:
        if suffix in {".yaml", ".yml"}:
            data = yaml.safe_load(text)
        elif suffix == ".json":
            data = json.loads(text)
        else:
            # Try YAML first (a superset of JSON for most practical purposes).
            data = yaml.safe_load(text)
    except (yaml.YAMLError, json.JSONDecodeError) as exc:
        fail(f"failed to parse {path}: {exc}", ExitCode.INVALID_ARGS)
    if not isinstance(data, dict):
        fail(
            f"config file {path} must contain a mapping at the top level, got {type(data).__name__}",
            ExitCode.INVALID_ARGS,
        )
    return data


def _parse_value(raw: str) -> Any:
    """Best-effort parse of a `--set` RHS into a Python value.

    Tries JSON first (handles numbers, bools, null, arrays, objects), then
    falls back to the raw string. This mirrors how `helm --set` and
    `kubectl --set` behave.
    """
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _apply_set(target: dict[str, Any], dotted_key: str, value: Any) -> None:
    """Apply one `--set key.subkey=value` to a nested dict in place."""
    parts = dotted_key.split(".")
    cursor = target
    for part in parts[:-1]:
        existing = cursor.get(part)
        if not isinstance(existing, dict):
            existing = {}
            cursor[part] = existing
        cursor = existing
    cursor[parts[-1]] = value


def _parse_sets(sets: list[str]) -> dict[str, Any]:
    """Turn a list of `key=value` strings into a nested dict."""
    out: dict[str, Any] = {}
    for raw in sets:
        if "=" not in raw:
            fail(
                f"--set expects KEY=VALUE, got: {raw!r}", ExitCode.INVALID_ARGS
            )
        key, _, value = raw.partition("=")
        key = key.strip()
        if not key:
            fail(f"--set key must be non-empty: {raw!r}", ExitCode.INVALID_ARGS)
        _apply_set(out, key, _parse_value(value))
    return out


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge `overlay` into `base`. Lists and scalars overwrite."""
    out = dict(base)
    for key, value in overlay.items():
        if (
            key in out
            and isinstance(out[key], dict)
            and isinstance(value, dict)
        ):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def resolve_config(
    model: type[ModelT],
    *,
    config_file: Optional[Path] = None,
    sets: Optional[list[str]] = None,
    runtime_overrides: Optional[dict[str, Any]] = None,
    targets: Optional[dict[str, Any]] = None,
) -> ModelT:
    """Build a validated Pydantic model from layered inputs.

    Resolution order (weakest → strongest):

      1. Pydantic model defaults
      2. ``config_file`` contents
      3. ``sets`` (CLI ``--set k=v`` overrides)
      4. ``runtime_overrides`` (named runtime flags)
      5. ``targets`` (DB-entity references resolved by the command)

    ``runtime_overrides`` and ``targets`` are filtered: keys whose values are
    ``None`` are dropped so an unset flag doesn't clobber a config-file value.

    Validation failures exit with `ExitCode.INVALID_ARGS` and a clear message.
    """
    data: dict[str, Any] = {}

    if config_file is not None:
        data = _deep_merge(data, _load_config_file(config_file))

    if sets:
        data = _deep_merge(data, _parse_sets(sets))

    if runtime_overrides:
        clean = {k: v for k, v in runtime_overrides.items() if v is not None}
        data = _deep_merge(data, clean)

    if targets:
        clean = {k: v for k, v in targets.items() if v is not None}
        data = _deep_merge(data, clean)

    try:
        return model.model_validate(data)
    except ValidationError as exc:
        # Single-line summary on stderr, then a concise listing for debugging.
        typer.echo(f"error: invalid config for {model.__name__}", err=True)
        for err in exc.errors():
            loc = ".".join(str(p) for p in err["loc"]) or "<root>"
            typer.echo(f"  {loc}: {err['msg']}", err=True)
        raise typer.Exit(code=int(ExitCode.INVALID_ARGS))


# --------------------------------------------------------------------------- #
# Target resolvers (contract §3 — standard target flags)
# --------------------------------------------------------------------------- #


class CohortTarget:
    """Resolver for the standard ``--cohort`` / ``--cohort-id`` pair.

    Usage in a command::

        cohort: Optional[str] = cohort_target.NAME_OPT,
        cohort_id: Optional[int] = cohort_target.ID_OPT,
        ...
        cohort_obj = cohort_target.resolve(cohort, cohort_id)
    """

    NAME_OPT: Any = typer.Option(
        None, "--cohort", metavar="NAME", help="Cohort by name."
    )
    ID_OPT: Any = typer.Option(
        None, "--cohort-id", metavar="N", help="Cohort by numeric ID."
    )

    @staticmethod
    def resolve(name: Optional[str], cohort_id: Optional[int]) -> Any:
        """Look up a cohort by name or numeric ID. Exits cleanly on miss."""
        # Imported here to avoid pulling DB modules at import time of the
        # contract (helpful for unit tests of resolve_config etc.).
        from cohorts.service import cohort_service

        if cohort_id is None and not name:
            fail(
                "missing target: pass --cohort NAME or --cohort-id N",
                ExitCode.INVALID_ARGS,
            )
        if cohort_id is not None and name:
            fail(
                "ambiguous target: pass either --cohort or --cohort-id, not both",
                ExitCode.INVALID_ARGS,
            )

        if cohort_id is not None:
            cohort = cohort_service.get_cohort(cohort_id)
            if cohort is None:
                fail(f"cohort id {cohort_id} not found", ExitCode.NOT_FOUND)
            return cohort

        # Lookup by name. cohort_service may expose `get_by_name`; fall back
        # to scanning the list if not.
        lookup: Optional[Callable[[str], Any]] = getattr(
            cohort_service, "get_by_name", None
        )
        if lookup is not None:
            cohort = lookup(name)  # type: ignore[arg-type]
            if cohort is None:
                fail(f"cohort {name!r} not found", ExitCode.NOT_FOUND)
            return cohort

        matches = [c for c in cohort_service.list_cohorts() if c.name == name]
        if not matches:
            fail(f"cohort {name!r} not found", ExitCode.NOT_FOUND)
        if len(matches) > 1:
            fail(
                f"cohort name {name!r} is ambiguous ({len(matches)} matches); use --cohort-id",
                ExitCode.CONFLICT,
            )
        return matches[0]


cohort_target = CohortTarget()


# --------------------------------------------------------------------------- #
# Standard option factories (contract §3)
# --------------------------------------------------------------------------- #


class _ConfigOptions:
    CONFIG_OPT: Any = typer.Option(
        None,
        "--config",
        metavar="FILE",
        help="Load the full Pydantic config from a YAML or JSON file.",
    )
    SET_OPT: Any = typer.Option(
        None,
        "--set",
        metavar="KEY=VALUE",
        help="Override a config key (repeatable, dotted paths supported).",
    )


class _RuntimeOverrides:
    WORKERS_OPT: Any = typer.Option(
        None, "--workers", metavar="N", help="Concurrent workers (overrides config)."
    )
    OUTPUT_OPT: Any = typer.Option(
        None, "--output", metavar="PATH", help="Output destination (overrides config)."
    )


class _LongRunningOptions:
    SUBMIT_OPT: Any = typer.Option(
        False, "--submit", help="Create the job, print its ID, exit. Do not wait."
    )
    NO_JOB_OPT: Any = typer.Option(
        False, "--no-job", help="Run without creating a job record (testing only)."
    )
    JOB_NAME_OPT: Any = typer.Option(
        None, "--job-name", metavar="NAME", help="Human-readable job name."
    )


config_options = _ConfigOptions()
runtime_overrides = _RuntimeOverrides()
long_running_options = _LongRunningOptions()


# --------------------------------------------------------------------------- #
# Confirmation helper (contract §1.6)
# --------------------------------------------------------------------------- #


def confirm(message: str, ctx: CliContext) -> None:
    """Confirm a destructive action; respect ``--yes`` and non-TTY environments."""
    if ctx.yes:
        return
    if not sys.stdin.isatty():
        fail(
            f"refusing to run destructive action in non-interactive mode without --yes: {message}",
            ExitCode.CONFLICT,
        )
    if not typer.confirm(message, default=False):
        fail("aborted by user", ExitCode.INTERRUPTED)
