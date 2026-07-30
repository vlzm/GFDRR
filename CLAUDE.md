# CLAUDE.md

A framework for problems on flow graphs — networks where commodities move between facilities; the first and so far only scenario is the Citi Bike bike-sharing system in New York City.

```bash
uv pip install -e ".[dev,ui,api,ml]" # install
ruff check gbp/ domains/ tests/ app/   # lint
ruff format gbp/ domains/ tests/ app/  # format
mypy gbp/ domains/                # typecheck
streamlit run app/main.py         # UI
python app/runner.py --help       # run a scenario from the terminal
uvicorn api:app --app-dir app     # serve the run-artifact API (docs/site/content/docs/reference/api.md)
python -m domains.citybike.ml.training --months 202502 202503  # download raw months, build the training table
python -m domains.citybike.ml.ops.backtest         # rolling-origin backtest of the model families, logged to MLflow
python -m domains.citybike.ml.ops.pipeline         # retraining pipeline: download → build-table → train → backtest → promote
python -m domains.citybike.ml.forecast --champion --forecast-name <name>  # forecast with the registry champion
python -m domains.citybike.ml.ops.evaluation --month 202601  # two-level evaluation: run the simulator on actual vs forecast demand
python -m domains.citybike.ml.ops.monitoring --month 202602  # score saved forecasts against the month's actuals, build the drift report
mlflow ui --backend-store-uri sqlite:///data/ml/mlflow/mlflow.db  # browse the experiments and the model registry
dvc status                        # data/raw and data/ml/training vs their .dvc files
```

## Universal Rules

- **English** in code, comments, docstrings. **Russian** in communication with the user.
- **Plain Russian.** No abbreviations ("то есть", not "т.е."). Expand jargon on first use. Translate English terms instead of mixing them into Russian text. Short paragraphs over dense tables. First answer the specific question in one-two sentences, then add details only if asked. Do not explain things the user did not ask about. The user should understand the answer on the first read. Exception: an explanation-style answer requested under the `write-explanation` skill may open with framing and give background the user did not literally ask for.
- **Plain English in code.** Write comments and docstrings so an intermediate English reader gets them on the first read. Prefer short, common words over rare or figurative ones (avoid "dormant", "bite", "gating", "spine"). Keep field names, class names, and domain terms (OD matrix, stockout) unchanged. Short sentences over dense ones.

## Codebase Rules

- **Layers.** `gbp/` is the domain-agnostic framework; `domains/<name>/` is one concrete scenario built on it. The dependency arrow is always `domains → gbp`; `gbp` must never import `domains`. Check with `grep -rn "domains\." gbp/` — the result must stay empty. `app/` is the one place allowed to import both layers.
- **Minimalism.** Code must be hackable. No factories, heavy DI containers, or hidden magic.
- **Deep modules.** A module's interface is everything a caller must know to use it — types, call order, invariants, error modes, required config — not just the signature. Aim for a lot of behaviour behind a small interface. Before adding a parameter, a helper, or a wrapper, apply the deletion test: if deleting it would only move the same complexity onto the callers, it is shallow — don't add it.
- **Vectorization first.** All math via pandas/NumPy. No `for` loops over data in hot paths.
- **Language.** Code, comments, docstrings — English only. Documents (`.md`) — English by default; add a Russian companion (`*_ru.md`) when the user asks, and keep the two in sync. Communication with the user — Russian. The plain-language, no-jargon rule above applies in every language.
