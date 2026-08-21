# Agent Instructions

## Setup

```bash
uv sync --extra dev --extra all
```

## Verification

```bash
ruff check src/ tests/
pytest
```

## Conventions

- Early returns over nested conditionals
- Async/await for all I/O
- Type hints on all functions
- Docstrings on all public functions
- New pipeline stages get tests
- Match existing code style (check neighboring files)

## Git

- Conventional commits: `feat:`, `fix:`, `docs:`, `test:`
- Don't push to main without permission
- Stage only related files
