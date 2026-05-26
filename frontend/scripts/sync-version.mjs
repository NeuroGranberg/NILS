#!/usr/bin/env node
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, '..', '..');
const versionPath = join(repoRoot, 'VERSION');
const pkgPath = join(here, '..', 'package.json');

const version = readFileSync(versionPath, 'utf8').trim();
const pkg = JSON.parse(readFileSync(pkgPath, 'utf8'));

if (pkg.version === version) {
  console.log(`[sync-version] package.json already at ${version}`);
  process.exit(0);
}

const previous = pkg.version;
pkg.version = version;
writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + '\n');
console.log(`[sync-version] package.json: ${previous} -> ${version}`);
