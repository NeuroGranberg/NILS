"""NILS backend HTTP API package (FastAPI app, routes, services).

This ``__init__.py`` is required: without it ``setuptools`` ``packages.find``
(``find_packages``, which needs a regular package marker) silently EXCLUDES the
whole ``api`` tree from the built/installed distribution. The app then imports
only by accident via ``PYTHONPATH=/app/src`` + namespace-package fallback — which
breaks under a strict editable install (e.g. ``uv run`` building a fresh venv),
surfacing as ``ModuleNotFoundError: No module named 'api'`` at the ``neuro-api``
entrypoint.
"""
