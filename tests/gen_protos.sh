#!/usr/bin/env bash
# Regenerate test proto Python files. Run from the repo root:
#   bash tests/gen_protos.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
OUT_DIR="$SCRIPT_DIR/gen"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
    -I "$SCRIPT_DIR/protos" \
    --python_out="$OUT_DIR" \
    "$SCRIPT_DIR/protos/test_api/api.proto" \
    "$SCRIPT_DIR/protos/test_internal/internal.proto"

# Create __init__.py files so the generated packages are importable.
touch "$OUT_DIR/__init__.py"
touch "$OUT_DIR/test_api/__init__.py"
touch "$OUT_DIR/test_internal/__init__.py"

echo "Generated proto files in $OUT_DIR"
