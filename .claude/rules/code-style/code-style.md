# Code Style and Conventions

## Core Principles

- **Vectorization first.** No `for` loops over data — use pandas/NumPy operations.
- **Flat is better.** No AbstractFactoryBuilder or deep nesting.
- **Explicit dependencies.** Pass in `__init__`, no DI containers.
- **Strict typing.** Type hints on all public functions. Pydantic V2 strict mode for schemas.
- **NumPy/SciPy style docstrings** on all public classes and functions.

## Tooling Config

- Ruff: line-length 100, target Python 3.11, rules: E, W, F, I, B, C4, UP, D (Google convention).
- Mypy: strict mode.
