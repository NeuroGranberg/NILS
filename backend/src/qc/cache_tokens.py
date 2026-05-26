"""CSV token helpers for series_classification_cache string columns.

Two columns in ``series_classification_cache`` are free-form, comma-separated
token lists that both the Axes QC service and the Main Acquisition QC service
need to mutate safely:

- ``classification_string`` - carries the human-readable classification
  expression (e.g. ``"Ax_T1w_MPRAGE"``) plus orthogonal tags such as
  ``main_acquisition``. Tokens are separated by ``,``.
- ``manual_review_reasons_csv`` - carries prefixed review tokens (e.g.
  ``"base:missing"``, ``"contrast:duplicate_prediction"``).

These helpers centralise the add / remove / normalise logic so callers don't
have to reimplement CSV parsing each time.
"""

from __future__ import annotations

from typing import Iterable


def _split(csv_value: str | None) -> list[str]:
    if not csv_value:
        return []
    return [t.strip() for t in csv_value.split(",") if t.strip()]


def _join(tokens: Iterable[str]) -> str | None:
    out = [t for t in tokens if t]
    return ",".join(out) if out else None


def add_token(csv_value: str | None, token: str) -> str | None:
    """Return a new CSV with ``token`` added (idempotent, preserves order)."""
    token = (token or "").strip()
    if not token:
        return csv_value or None
    tokens = _split(csv_value)
    if token not in tokens:
        tokens.append(token)
    return _join(tokens)


def remove_token(csv_value: str | None, token: str) -> str | None:
    """Return a new CSV with all occurrences of ``token`` removed."""
    token = (token or "").strip()
    if not token:
        return csv_value or None
    tokens = [t for t in _split(csv_value) if t != token]
    return _join(tokens)


def has_token(csv_value: str | None, token: str) -> bool:
    token = (token or "").strip()
    if not token:
        return False
    return token in _split(csv_value)


def remove_prefix(csv_value: str | None, prefix: str) -> str | None:
    """Remove all tokens starting with ``prefix`` (case-insensitive)."""
    prefix_l = (prefix or "").strip().lower()
    if not prefix_l:
        return csv_value or None
    tokens = [t for t in _split(csv_value) if not t.lower().startswith(prefix_l)]
    return _join(tokens)
