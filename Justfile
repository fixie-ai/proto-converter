default: format check test

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

# Full setup from scratch
setup:
    uv sync
    just build-protos
