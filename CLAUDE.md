# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Graph-Based Logistics Platform (`gbp`) — a universal graph-based logistics platform for network flow problems built on multi-commodity flow formulation. Domain-agnostic data model validated against bike-sharing (Citi Bike-style). Completed phases: **Foundation**, **Environment**. Next phase: **Rebalancer** (first real Task inside Environment).

The core concept is **Environment** — a step-by-step simulation/digital twin. The Strategic Optimizer (LP/MILP) is a separate, later consumer. Both use the same `ResolvedModelData`.

**Pipeline:** Raw Data → RawModelData (~46 DataFrames) → `build_model()` → ResolvedModelData (~52 DataFrames) → Consumer (Environment / Optimizer / Analytics).

## Commands

```bash
# Install (uses uv)
uv pip install -e ".[dev]"

# Lint
ruff check gbp/ tests/

# Type check
mypy gbp/

# Format
ruff format gbp/ tests/
```

## Code Style

- **Vectorization first.** No `for` loops over data — use pandas/NumPy operations.
- **Flat is better.** No AbstractFactoryBuilder or deep nesting.
- **Explicit dependencies.** Pass in `__init__`, no DI containers.
- **Strict typing.** Type hints on all public functions. Pydantic V2 strict mode for schemas.
- **Google-style docstrings** on all public classes and functions.
- **English only** in code, comments, and docstrings.
- Ruff config: line-length 100, target Python 3.11, rules: E, W, F, I, B, C4, UP, D (Google convention).
- Mypy: strict mode.
- **Notebook style:**
  - Single cell per notebook, divided by `# %% N. Section Name` markers. Start with a markdown map of all blocks (`# %% [markdown]`) so the reader sees the structure before the code.
  - No `display()`, no `print()`, no trailing bare expressions — cells end with assignments.
  - Full descriptive variable names — `n_stations` not `n_st`, `low`/`high` not `lo`/`hi`, `inventory` not `inv`.
  - Use intermediate variables to show data flow (`inv_after_demand`, `inv_after_returns`) instead of in-place mutation.
  - Use domain terms, not generic model terms — `returns` not `supply` for bike-sharing arrivals.

## AI Collaboration Rules

- Provide skeletons with detailed TODO comments, NOT full implementations of core algorithms (solver formulation, VRP).
- Refactoring, docstrings, boilerplate, and test scaffolds are OK to generate fully.
- Always validate changes against the data model invariants above.
- **Do NOT build or extend:** optimizer/solver, API, UI, Docker, database, cloud — these have their own future phases.
- **After completing a task**, check if `PROJECT_STATE.md` should be updated (e.g. marking items as done, adding new findings). Update it if relevant.
- **Verification notebook:** after completing a code task, create or update a notebook in `notebooks/verify/` that lets the user interactively test what changed. Keep cells minimal and focused — one cell per behavior. Name pattern: `NN_short_description.ipynb`. The user runs these by hand to build intuition. Notebooks must be in English (markdown cells, comments, print messages) — same rule as code.
- **Language:** code, comments, docstrings — English only. Communication with the user — Russian.
- **Plain-language explanations.** When explaining things to the user (in Russian), write in simple, easy-to-read language:
  - No abbreviations: write "то есть" instead of "т.е.", "потому что" instead of "т.к.", "например" instead of "напр.".
  - No unexplained jargon: on first mention, expand acronyms and technical terms ("GBFS" → "публичный API формата Citi Bike", "ground truth" → "точное правильное значение", "fixture" → "тестовые данные").
  - Prefer 2–3 short paragraphs over dense tables, ASCII diagrams, or symbol-heavy formatting unless the user explicitly asks for a comparison table.
  - Avoid English shorthand inside Russian text ("snapshot", "feed", "fallback") — translate or paraphrase.
  - Goal: the user should understand the answer on the first read, without decoding shortcuts.
- **Verify direction of derivation against code, not memory.** Any phrase like "X is derived from Y", "X comes from Y", "X is computed from Y" is a concrete claim about specific lines of code. Before writing it — re-open the function and check the actual generation order. The risk is highest in summary tables and minimalist diagrams, where 3-step dependency chains get silently compressed into 2-step ones and lose correctness. If you have not just looked at the code, say so explicitly ("по памяти, надо проверить") rather than asserting confidently.
- **Compactness must not cost correctness.** When the user asks for a minimalist or short answer, fewer rows/columns is fine — but each remaining cell must be exact. If precision cannot fit in the requested format, flag the conflict ("в таком формате точно не получится, вот более полная версия") instead of silently dropping fidelity.
- **Cite code for derivation claims.** When asserting that one entity is derived from another, include the `file:line` where the derivation happens. This forces verification before the claim is written and gives the user a place to check.