# CLAUDE.md

Citi Bike Simulation Platform — vertical bike-sharing simulation built on the Citi Bike domain. Current phase: **demand forecasting** — a model predicts future demand, and the simulator runs on that forecast next to the base replay on history. Plan: `docs/plans/ml_demand_forecast_plan.md`.

**Source of truth:** the canonical scenario is two runs. The base replay stays in `notebooks/test_pipeline.ipynb`, unchanged. The forecast run (Notations.md §11) gets a second notebook next to it, `notebooks/forecast_pipeline.ipynb`, created in phase 1 of the plan. Everything in the codebase must serve one of these two runs. If it doesn't, it should be removed.

## Commands

```bash
uv pip install -e ".[dev,ui,api,ml]" # install
ruff check gbp/ tests/ app/       # lint
ruff format gbp/ tests/ app/      # format
mypy gbp/                         # typecheck
streamlit run app/main.py         # UI
python app/runner.py --help       # run a scenario from the terminal
uvicorn api:app --app-dir app     # serve the run-artifact API (docs/explanation/api.md)
python -m gbp.ml.training --months 202502 202503  # download raw months, build the training table
python -m gbp.ml.backtest         # rolling-origin backtest of the model families, logged to MLflow
python -m gbp.ml.pipeline         # retraining pipeline: download → build-table → train → backtest → promote
python -m gbp.ml.forecast --champion --forecast-name <name>  # forecast with the registry champion
python app/evaluate.py --month 202601  # two-level evaluation: run the simulator on actual vs forecast demand
python -m gbp.ml.monitoring --month 202602  # score saved forecasts against the month's actuals, build the drift report
mlflow ui --backend-store-uri sqlite:///data/ml/mlflow/mlflow.db  # browse the experiments and the model registry
dvc status                        # data/raw and data/ml/training vs their .dvc files
```

## Universal Rules

- **English** in code, comments, docstrings. **Russian** in communication with the user.
- **Plain Russian.** No abbreviations ("то есть", not "т.е."). Expand jargon on first use. Translate English terms instead of mixing them into Russian text. Short paragraphs over dense tables. First answer the specific question in one-two sentences, then add details only if asked. Do not explain things the user did not ask about. The user should understand the answer on the first read.
- **Plain English in code.** Write comments and docstrings so an intermediate English reader gets them on the first read. Prefer short, common words over rare or figurative ones (avoid "dormant", "bite", "gating", "spine"). Keep field names, class names, and domain terms (OD matrix, stockout) unchanged. Short sentences over dense ones.
- **Plain language in documents too — no jargon, no metaphors.** The same bar applies to every `.md` document, in **both** English and Russian. Never use figurative or metaphor words for a technical thing — say what it is. Examples to avoid: "scaffolding" / "леса" → "temporary helper columns dropped at the end" / "временные служебные колонки, которые удаляются в конце"; "densify" / "уплотнять" / "dense rank" → "number them in order: 0, 1, 2, …" / "нумеруем подряд: 0, 1, 2, …"; "projection" / "проекция" → "a value computed from the journal" / "величина, посчитанная из журнала". Define each concept in plain words the **first time** it appears, before using it. Keep code identifiers, file names, and function names (`step_id`, `phase_round`, `finalize_flows`) exactly as in the code — never translate or rephrase them; translate only the surrounding prose. Short sentences, concrete words over abstract ones. A reader who does not know the codebase should understand on the first read.
- **Canonical vocabulary.** `Notations.md` is the project's dictionary — one concept, one word. Before naming anything in code, docstrings, or chat, use the word in `Notations.md`. If a concept is missing, add it there first (anchored to the flow journal), then use it.
## Answer Style (chat)

The pattern for every explanation: real code first, plain narration after, in execution order.

- Start with the answer: one-two sentences that answer the question. No document title, no "overview in one sentence" section, no roadmap of what you are about to say.
- If the walkthrough relies on a few key tables or variables, define them in one short block up front. Then walk the code top to bottom, in execution order.
- Headers carry the structure ("## Шаг 1: …"). Bold does not: at most 1–2 bold terms in the whole answer, never for emphasis inside a sentence.
- Code first, words after. Quote the real fragment with file and line numbers, then explain it below in 2–5 short sentences. Never paraphrase code in prose with bolded function names instead of showing it.
- One thought per sentence. At most one subordinate clause. If a sentence needs an em-dash insert plus parentheses, split it into two sentences.
- A subtlety is its own short paragraph opening with "Важно:" or "Обрати внимание:". Do not stack qualifiers inside an explanation paragraph.
- Branching or multiple outcomes: one small ASCII chain (`departed` → `redirected` → `arrived`), not a nested list.
- Clear beats complete. Skip edge cases the user did not ask about. No closing summary section, no "хочешь, я ещё…" endings.
- Do not grade the material: no "ключевая модель", "самое тонкое место", "важная тонкость".

Anti-example (paraphrase with bold — do not write like this):

> **`dock_up_to_capacity(due, free)`** → делит прибывающих на `(docked, overflow)`. Реализовано без цикла на Python: через `groupby(...).cumcount()` считается позиция велосипеда в очереди к своей станции, и если позиция меньше числа свободных слотов — велосипед влез.

Target (code, then narration — write like this):

> ```python
> rank = due.groupby("planned_target_id").cumcount()
> fits = rank < capacity_here
> ```
> Для каждой станции код нумерует прибывшие строки через `cumcount`: первая получает 0, вторая 1. Если номер строки меньше числа свободных доков, велосипед помещается. Остальные попадают в `overflow`.

## Codebase Rules

- **Vertical, not horizontal.** No "domain-agnostic" abstractions. If the canonical scenario doesn't use it, it doesn't belong in the codebase.
- **Minimalism.** Code must be hackable. No factories, heavy DI containers, or hidden magic.
- **Vectorization first.** All math via pandas/NumPy. No `for` loops over data in hot paths.
- **Strict typing.** Pydantic for all contracts. Type hints on all public functions.
- **UI.** The Streamlit app lives in `app/`. It is a reader of run artifacts (`data/runs/<run_name>/`, see Notations.md §12): it loads saved tables and draws them. It must not add abstractions to `gbp/` and must not compute anything the artifact builder (`app/artifacts.py`) can precompute. **All UI text (labels, captions, tooltips, page titles) is English only** — same as the code; Russian is for chat with the user, never for the app.
- **Language.** Code, comments, docstrings — English only. Documents (`.md`) — English by default; add a Russian companion (`*_ru.md`) when the user asks, and keep the two in sync. Communication with the user — Russian. The plain-language, no-jargon rule above applies in every language.
