#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_VERSIONS="python3.11 python3.12 python3.13 python3.14"

echo "Building manylinux wheels for: $PYTHON_VERSIONS"
echo "Repo root: $REPO_ROOT"

docker run --rm \
    -v "$REPO_ROOT":/io \
    -w /io \
    ghcr.io/pyo3/maturin \
    build --release -m python/Cargo.toml -i $PYTHON_VERSIONS

echo ""
echo "Built wheels:"
ls -la "$REPO_ROOT/target/wheels/"*.whl
