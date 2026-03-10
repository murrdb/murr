#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:?Usage: ./release.sh <version>}"

# Update the single source of truth: workspace version in root Cargo.toml
sed -i "s/^version = \".*\"/version = \"$VERSION\"/" Cargo.toml

# Validate everything compiles (also updates Cargo.lock)
cargo check --all-targets

git add Cargo.toml Cargo.lock
git commit -m "release v$VERSION"
git tag "v$VERSION"

echo "Done. Run: git push && git push --tags"
