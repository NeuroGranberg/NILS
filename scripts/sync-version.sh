#!/usr/bin/env bash
# Propagate VERSION to all derived files.
# Edit VERSION first, then run this script. Single source of truth.
#
# Derived files updated here:
#   - frontend/package.json     (via node script)
#   - README.md                 (version badge URL)
#
# Backend (backend/pyproject.toml) reads VERSION dynamically via
# `dynamic = ["version"]` + setuptools file source — no edit needed.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f VERSION ]]; then
  echo "error: VERSION file not found at repo root" >&2
  exit 1
fi

VERSION="$(tr -d '[:space:]' < VERSION)"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: VERSION must be X.Y.Z (got: $VERSION)" >&2
  exit 1
fi

echo "syncing all version-bearing files to $VERSION"

node frontend/scripts/sync-version.mjs

# README badge — shields.io URL pattern: version-X.Y.Z-green.svg
if grep -qE 'version-[0-9]+\.[0-9]+\.[0-9]+-green\.svg' README.md; then
  sed -i.bak -E "s|version-[0-9]+\.[0-9]+\.[0-9]+-green\.svg|version-${VERSION}-green.svg|g" README.md
  rm -f README.md.bak
  echo "[sync-version] README.md badge -> $VERSION"
else
  echo "[sync-version] README.md: no badge pattern found, skipped"
fi

echo "done. Backend reads VERSION dynamically — no edit needed."
