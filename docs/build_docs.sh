#!/usr/bin/env bash
# Build Sphinx HTML documentation.
# Usage: bash docs/build_docs.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR="${SCRIPT_DIR}/_build/html"

# Locate sphinx-build: prefer .venv (Windows Scripts/ or Linux bin/), fallback to PATH
if [ -f "${REPO_ROOT}/.venv/Scripts/sphinx-build" ]; then
    SPHINX="${REPO_ROOT}/.venv/Scripts/sphinx-build"
elif [ -f "${REPO_ROOT}/.venv/Scripts/sphinx-build.exe" ]; then
    SPHINX="${REPO_ROOT}/.venv/Scripts/sphinx-build.exe"
elif [ -f "${REPO_ROOT}/.venv/bin/sphinx-build" ]; then
    SPHINX="${REPO_ROOT}/.venv/bin/sphinx-build"
else
    SPHINX="sphinx-build"
fi

echo "Using sphinx-build: ${SPHINX}"
echo "Building docs: ${SCRIPT_DIR} -> ${OUT_DIR}"
"${SPHINX}" -M html "${SCRIPT_DIR}" "${OUT_DIR}" --keep-going

# Required by GitHub Pages to skip Jekyll processing
touch "${OUT_DIR}/.nojekyll"

echo "Done. Open: ${OUT_DIR}/index.html"
