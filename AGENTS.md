# CLAUDE.md

Citi Bike Simulation Platform — vertical bike-sharing simulation built on the Citi Bike domain. Current phase: **codebase cleanup to canonical scenario minimum**.

**Pipeline:** Raw Citi Bike data → `DataLoaderGraph` → `RawModelData` → `build_model()` → `ResolvedModelData` → `Environment` → `SimulationLog`.

**Source of truth:** `notebooks/canonical_scenario.ipynb` — everything in the codebase must serve this scenario. If it doesn't, it should be removed.

## Commands

```bash
uv pip install -e ".[dev]"   # install
ruff check gbp/ tests/       # lint
ruff format gbp/ tests/      # format
mypy gbp/                    # typecheck
```

## Universal Rules

- **English** in code, comments, docstrings. **Russian** in communication with the user.
- **Plain Russian.** No abbreviations ("то есть", not "т.е."). Expand jargon on first use. Translate English terms instead of mixing them into Russian text. Short paragraphs over dense tables. First answer the specific question in one-two sentences, then add details only if asked. Do not explain things the user did not ask about. The user should understand the answer on the first read.
- **Vertical, not horizontal.** No "domain-agnostic" abstractions. If the canonical scenario doesn't use it, it doesn't belong in the codebase.
- **Skeleton-first.** Provide skeletons with TODO comments for core algorithms (solvers, VRP). Full generation is OK for refactoring, docstrings, boilerplate, tests.
- **Do NOT build or extend:** optimizer/solver, other domains, API, UI, Docker, database, cloud.
- **English in code, Russian in chat.** Code, comments, docstrings, documents — English only. Communication with the user — in Russian.

## Detailed Guidelines (read when relevant)

- [Code style and conventions](.claude/code-style.md) — read before writing or reviewing code.
- [Epistemic rules](.claude/epistemic-rules.md) — read before writing documentation, storytelling guides, or making derivation claims.
- [Process and post-task](.claude/process.md) — read after completing a task.
