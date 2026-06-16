# CLAUDE.md

Citi Bike Simulation Platform — vertical bike-sharing simulation built on the Citi Bike domain. Current phase: **codebase cleanup to canonical scenario minimum**.

**Source of truth:** `notebooks/test_pipeline.ipynb` — everything in the codebase must serve this scenario. If it doesn't, it should be removed.

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
- **Plain English in code.** Write comments and docstrings so an intermediate English reader gets them on the first read. Prefer short, common words over rare or figurative ones (avoid "dormant", "bite", "gating", "spine"). Keep field names, class names, and domain terms (OD matrix, stockout) unchanged. Short sentences over dense ones.
- **Canonical vocabulary.** `Notations.md` is the project's dictionary — one concept, one word. Before naming anything in code, docstrings, or chat, use the word in `Notations.md`. If a concept is missing, add it there first (anchored to the flow journal), then use it.
- **Vertical, not horizontal.** No "domain-agnostic" abstractions. If the canonical scenario doesn't use it, it doesn't belong in the codebase.
- **Skeleton-first.** Provide skeletons with TODO comments for core algorithms (solvers, VRP). Full generation is OK for refactoring, docstrings, boilerplate, tests.
- **Do NOT build or extend:** optimizer/solver, other domains, API, UI, Docker, database, cloud.
- **English in code, Russian in chat.** Code, comments, docstrings, documents — English only. Communication with the user — in Russian.
