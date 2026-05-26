"""CLI Output: rendering helpers that respect ``--json`` and ``--quiet``.

Single rule: every command that prints structured data goes through this
module. Human output uses ``rich`` tables on stdout; progress and logs go to
stderr; ``--json`` produces stable, top-level-object JSON.

See `docs/cli-contract.md` §7 for the discipline these helpers enforce.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Mapping, Optional

import typer
import yaml
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from cli.contract import CliContext


# `stderr=True` keeps progress and logs off stdout so users can pipe results.
_stderr_console = Console(stderr=True, highlight=False)
_stdout_console = Console(highlight=False)


# --------------------------------------------------------------------------- #
# JSON-safe coercion
# --------------------------------------------------------------------------- #


def _to_jsonable(value: Any) -> Any:
    """Recursively coerce values to JSON-serializable equivalents."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in value]
    return value


# --------------------------------------------------------------------------- #
# YAML for --print-config
# --------------------------------------------------------------------------- #


def print_yaml(model: BaseModel) -> None:
    """Pretty-print a Pydantic model as YAML to stdout. Used by --print-config."""
    data = model.model_dump(mode="json")
    yaml.safe_dump(
        data,
        stream=sys.stdout,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )


# --------------------------------------------------------------------------- #
# Final result rendering
# --------------------------------------------------------------------------- #


def render_result(
    ctx: CliContext,
    result: Any,
    *,
    command: str,
    status: str = "completed",
    headline_rows: Optional[list[tuple[str, str]]] = None,
    title: Optional[str] = None,
) -> None:
    """Render a command's final result.

    - With ``--json``: emits a stable JSON object on stdout.
    - Otherwise: prints a rich table on stdout, plus optional headline rows.

    ``result`` may be a Pydantic model, a mapping, or any JSON-friendly value.
    ``headline_rows`` is an optional pre-formatted [(label, value), ...] for the
    human view; if omitted, the model is rendered field-by-field.
    """
    payload = {
        "version": _nils_version(),
        "command": command,
        "status": status,
        "result": _to_jsonable(result),
    }

    if ctx.json_output:
        json.dump(payload, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
        return

    if ctx.quiet:
        return

    table = Table(title=title or f"{command} — {status}", show_header=False)
    table.add_column("Field", style="bold")
    table.add_column("Value")

    rows = headline_rows or _model_to_rows(result)
    for label, value in rows:
        table.add_row(str(label), str(value))

    _stdout_console.print(table)


def render_plan(ctx: CliContext, plan: Any, *, command: str) -> None:
    """Render a `--dry-run` plan. Same shape as `render_result` with status=planned."""
    render_result(ctx, plan, command=command, status="planned", title=f"{command} — plan")


def render_job_submitted(ctx: CliContext, job_id: int, *, command: str) -> None:
    """Standard output when ``--submit`` returns a job ID.

    Always prints ``job_id=<N>`` on the first line of stderr (per contract §9)
    so scripts can grep for it, *and* either a table or JSON on stdout.
    """
    _stderr_console.print(f"job_id={job_id}")
    payload = {"job_id": job_id, "command": command, "status": "submitted"}
    if ctx.json_output:
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    if ctx.quiet:
        return
    _stdout_console.print(f"Submitted job [bold]{job_id}[/bold].")


# --------------------------------------------------------------------------- #
# Progress to stderr
# --------------------------------------------------------------------------- #


def info(message: str, ctx: CliContext) -> None:
    """Informational message on stderr. Suppressed by ``--quiet``."""
    if ctx.quiet:
        return
    _stderr_console.print(message)


def warn(message: str) -> None:
    """Warning on stderr. Always shown."""
    _stderr_console.print(f"[yellow]warning:[/yellow] {message}")


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #


def _model_to_rows(value: Any) -> list[tuple[str, str]]:
    """Best-effort field → string rendering for the default human view."""
    if isinstance(value, BaseModel):
        data = value.model_dump(mode="json")
    elif isinstance(value, Mapping):
        data = dict(value)
    else:
        return [("result", str(value))]

    rows: list[tuple[str, str]] = []
    for key, raw in data.items():
        if isinstance(raw, (dict, list)):
            rows.append((key, json.dumps(raw, default=str)))
        elif raw is None:
            rows.append((key, "-"))
        else:
            rows.append((key, str(raw)))
    return rows


def _nils_version() -> str:
    """Best-effort NILS version string for JSON output headers."""
    try:
        from importlib.metadata import version

        return version("nils-backend")
    except Exception:  # pragma: no cover - defensive
        return "unknown"
