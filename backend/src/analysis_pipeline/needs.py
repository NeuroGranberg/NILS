"""Runtime ``needs`` registry + forward-compatible ``apply_needs`` (§6.2).

A descriptor's ``x-nils.needs`` block declares runtime requirements (an FS
license, a TemplateFlow cache, GPU access, a scratch work dir). Each known need
maps to a handler that mutates a :class:`NeedsContext` — accumulating apptainer
``--bind`` args, injected env, extra exec flags (e.g. ``--nv``), and a work dir.

Forward-compatibility (§17.3.1) is the binding contract: an UNKNOWN need key
must WARN and continue, never hard-fail, so a newer descriptor declaring a need
this version doesn't understand still loads and runs. A falsey value (``need:
false``) is skipped.

Honesty about the environment (scope boundary): ``fs_license`` and
``templateflow`` are GATED SKELETONS — the assets aren't present in this slice,
so they record intent + emit a warning ("asset not available in this
environment") rather than pretending to mount a secret/cache. ``gpu`` (append
``--nv``) and ``work_dir`` (provision a real scratch dir) are REAL and cheap.
The real asset-provisioning handlers are a later drop-in keyed by the same name.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class NeedsContext(BaseModel):
    """Accumulator for runtime provisioning derived from ``x-nils.needs``.

    Handlers mutate this in place. The runner reads ``binds``/``env``/``flags``/
    ``work_dir`` to assemble the bridge ``create_run`` call; ``warnings`` is
    surfaced into the run's provenance + job log (never blocks a launch).
    """

    model_config = ConfigDict(extra="allow")

    binds: list[str] = Field(default_factory=list)  # apptainer --bind args
    env: dict[str, str] = Field(default_factory=dict)  # injected env
    flags: list[str] = Field(default_factory=list)  # extra exec flags (e.g. --nv)
    work_dir: str | None = None
    warnings: list[str] = Field(default_factory=list)


# A handler takes the context + the declared need value (truthy already checked
# by apply_needs) and mutates the context. It returns None.
NeedsHandler = Callable[["NeedsContext", Any], None]


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def inject_fs_license(ctx: NeedsContext, val: Any) -> None:
    """GATED SKELETON: FreeSurfer license mount (§16.2 — mriqc stays dry-run).

    No real secret store is wired in this slice, so we record the intent in env
    (so the seam is visible) and warn that the asset is unavailable. A real
    implementation binds ``$FS_LICENSE`` into the container.
    """
    ctx.env.setdefault("NILS_NEEDS_FS_LICENSE", "requested")
    ctx.warnings.append(
        "need 'fs_license': FreeSurfer license asset not available in this "
        "environment (handler not provisioned in v1)."
    )


def inject_templateflow(ctx: NeedsContext, val: Any) -> None:
    """GATED SKELETON: TemplateFlow cache mount.

    No real cache is present, so we record intent + warn. A real implementation
    binds the TemplateFlow home and sets ``TEMPLATEFLOW_HOME``.
    """
    ctx.env.setdefault("NILS_NEEDS_TEMPLATEFLOW", "requested")
    ctx.warnings.append(
        "need 'templateflow': TemplateFlow cache asset not available in this "
        "environment (handler not provisioned in v1)."
    )


def inject_gpu_flags(ctx: NeedsContext, val: Any) -> None:
    """REAL: request GPU passthrough by appending ``--nv`` to the exec flags."""
    if "--nv" not in ctx.flags:
        ctx.flags.append("--nv")


def provision_work_dir(ctx: NeedsContext, val: Any) -> None:
    """REAL: provision a scratch work dir.

    If ``val`` is a path-like string, it is used verbatim (the runner passes the
    run's ``run_work_dir`` here). Otherwise a private temp dir is created so the
    container has a writable scratch even outside a run context. The directory is
    created if it does not exist.
    """
    if isinstance(val, str) and val.strip() and val.strip() not in {"true", "True"}:
        target = Path(val.strip())
    else:
        # Defensive default: a private temp dir under the OS temp root. The
        # runner normally supplies the run's work dir as a string instead.
        import tempfile

        target = Path(tempfile.mkdtemp(prefix="nils-work-"))
    target.mkdir(parents=True, exist_ok=True)
    ctx.work_dir = str(target)
    ctx.binds.append(f"{target}:/work")


# String-keyed registry with a SAFE DEFAULT (unknown keys handled by apply_needs,
# never by a missing-key KeyError).
NEEDS_HANDLERS: dict[str, NeedsHandler] = {
    "fs_license": inject_fs_license,
    "templateflow": inject_templateflow,
    "gpu": inject_gpu_flags,
    "work_dir": provision_work_dir,
}


def apply_needs(needs: dict, ctx: NeedsContext | None = None) -> NeedsContext:
    """Apply a declared ``needs`` mapping to a :class:`NeedsContext`.

    Forward-compatible (§17.3.1):

    * Each key is looked up in :data:`NEEDS_HANDLERS`.
    * An UNKNOWN key → append a warning to ``ctx.warnings`` and CONTINUE
      (NEVER hard-fail) — a newer descriptor with a need we don't grok still runs.
    * A FALSEY value (``False``/``None``/``0``/``""``) → skip the handler.

    Mutates and returns ``ctx`` (a fresh one is created when omitted).
    """
    if ctx is None:
        ctx = NeedsContext()
    if not needs:
        return ctx
    if not isinstance(needs, dict):
        ctx.warnings.append(
            f"x-nils.needs must be a mapping, got {type(needs).__name__} (ignored)."
        )
        return ctx

    for key, value in needs.items():
        if not value:
            # Falsey → the need is not requested; skip silently.
            continue
        handler = NEEDS_HANDLERS.get(key)
        if handler is None:
            msg = (
                f"Unknown need {key!r} (carried, no handler in this version; "
                "forward-compatible skip)."
            )
            ctx.warnings.append(msg)
            logger.warning("apply_needs: %s", msg)
            continue
        try:
            handler(ctx, value)
        except Exception as exc:  # a handler must never break a launch
            ctx.warnings.append(f"need {key!r} handler failed: {exc}")
            logger.warning("apply_needs: handler %r failed: %s", key, exc)

    return ctx


__all__ = [
    "NeedsContext",
    "NeedsHandler",
    "NEEDS_HANDLERS",
    "apply_needs",
    "inject_fs_license",
    "inject_templateflow",
    "inject_gpu_flags",
    "provision_work_dir",
]
