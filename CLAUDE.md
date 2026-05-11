# CLAUDE.md

Graph-Based Logistics Platform (`gbp`) — universal network flow simulation and optimization platform. Domain-agnostic data model, validated on bike-sharing (Citi Bike). Current phase: **Rebalancer**.

**Pipeline:** Raw Data → RawModelData → `build_model()` → ResolvedModelData → Consumer (Environment / Optimizer / Analytics).

## Commands

```bash
uv pip install -e ".[dev]"   # install
ruff check gbp/ tests/       # lint
ruff format gbp/ tests/      # format
mypy gbp/                    # typecheck
```

## Universal Rules

- **English** in code, comments, docstrings. **Russian** in communication with the user.
- **Plain Russian.** No abbreviations ("то есть", not "т.е."). Expand jargon on first use. Translate English terms instead of mixing them into Russian text. Short paragraphs over dense tables. The user should understand the answer on the first read.
- **Skeleton-first.** Provide skeletons with TODO comments for core algorithms (solvers, VRP). Full generation is OK for refactoring, docstrings, boilerplate, tests.
- **Do NOT build or extend:** optimizer/solver, API, UI, Docker, database, cloud — these have their own future phases.

## Detailed Guidelines (read when relevant)

- [Code style and conventions](.claude/code-style.md) — read before writing or reviewing code.
- [Notebook conventions](.claude/notebook-style.md) — read before creating or editing notebooks.
- [Epistemic rules](.claude/epistemic-rules.md) — read before writing documentation, storytelling guides, or making derivation claims.
- [Process and post-task](.claude/process.md) — read after completing a task.
