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
- **Plain language in documents too — no jargon, no metaphors.** The same bar applies to every `.md` document, in **both** English and Russian. Never use figurative or metaphor words for a technical thing — say what it is. Examples to avoid: "scaffolding" / "леса" → "temporary helper columns dropped at the end" / "временные служебные колонки, которые удаляются в конце"; "densify" / "уплотнять" / "dense rank" → "number them in order: 0, 1, 2, …" / "нумеруем подряд: 0, 1, 2, …"; "projection" / "проекция" → "a value computed from the journal" / "величина, посчитанная из журнала". Define each concept in plain words the **first time** it appears, before using it. Keep code identifiers, file names, and function names (`step_id`, `redirect_round`, `finalize_flows`) exactly as in the code — never translate or rephrase them; translate only the surrounding prose. Short sentences, concrete words over abstract ones. A reader who does not know the codebase should understand on the first read.
- **Canonical vocabulary.** `Notations.md` is the project's dictionary — one concept, one word. Before naming anything in code, docstrings, or chat, use the word in `Notations.md`. If a concept is missing, add it there first (anchored to the flow journal), then use it.
- **Vertical, not horizontal.** No "domain-agnostic" abstractions. If the canonical scenario doesn't use it, it doesn't belong in the codebase.
- **Skeleton-first.** Provide skeletons with TODO comments for core algorithms (solvers, VRP). Full generation is OK for refactoring, docstrings, boilerplate, tests.
- **Do NOT build or extend:** optimizer/solver, other domains, API, UI, Docker, database, cloud.
- **Language.** Code, comments, docstrings — English only. Documents (`.md`) — English by default; add a Russian companion (`*_ru.md`) when the user asks, and keep the two in sync. Communication with the user — Russian. The plain-language, no-jargon rule above applies in every language.
