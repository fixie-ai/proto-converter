# Dump the Python stack trace on crashes.
export PYTHONFAULTHANDLER := "1"

default: format check test

# Full set up (or update)
install:
    uv sync
    just build-protos

# Format code
format:
    uv run ruff format
    uv run ruff check --fix-only

# Run all checks (lint, type check, dependency audit)
check:
    uv run ruff format --check
    uv run ruff check
    uv run pyright
    uv run deptry src

# Run tests
test *ARGS=".":
    uv run pytest {{ ARGS }}

# Generate test proto files and install into venv
build-protos:
    bash tests/gen_protos.sh
