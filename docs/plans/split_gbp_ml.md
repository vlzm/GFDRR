# Plan: split `gbp/ml` into a small framework seam and `domains/citybike/ml`

Written 2026-07-30, right after `gbp/loaders` moved to `domains/citybike/loaders`.
Nothing here is implemented yet. Every file:line below was read at that commit —
re-check them before trusting a number.

## Goal

`gbp` must be able to run a scenario whose demand comes from a model instead of
history, without knowing anything about bikes, stations, or New York. Everything
about *how a model is built* — feature engineering, the model architectures, the
training loop — belongs to the domain.

The dependency arrow is `domains → gbp`, always. Today it points both ways: 13
places inside `gbp` import `domains.citybike` (`grep -rn "domains\." gbp/`), and
11 of them are in `gbp/ml/`.

## The one contract

From Notations.md, "Forecast demand table":

> A table in the shape of the historical demand table whose quantity comes from
> a model. The simulator reads it exactly as it reads historical demand — that
> is the whole integration contract.

So the framework needs exactly three things, and nothing else:

1. **Substitute a demand table.** `apply_forecast_demand` in
   `gbp/model/dataloader_graph.py:256` — already domain-agnostic. Done.
2. **Read a saved forecast.** The artifact format (`demand.parquet` +
   `meta.json`) and its reader. Today this lives in `gbp/ml/forecast.py` mixed
   with the builders.
3. **Call a fitted model.** The `DemandModel` interface, so a caller can hand in
   a boosting / scikit-learn / PyTorch model and get a demand table back.

Everything else in `gbp/ml/` — training tables, weather, holidays, station
status, backtests, MLflow, drift reports — is about *producing* a model. None of
it is needed to run the simulator on a forecast.

## What the survey found

3635 lines across 17 files. The split is not even: a few files are pure domain,
a few are pure infrastructure, and three files mix both inside themselves.

### Pure domain — move whole, no thought needed

| File | Lines | Why |
|---|---|---|
| `gbp/ml/station_status.py` | 191 | The citybik.es GBFS archive URL (`:15`), the file name `<YYYYMM>-citi-bike-nyc-stats.parquet` (`:36`), the feed schema `nuid/latitude/longitude/bikes/timestamp` (`:23`), the timezone `America/New_York` (`:82`), the trip-CSV columns `start_station_id/start_lat/start_lng` (`:124`), and "stockout means zero bikes" (`:87`). It imports nothing from `gbp` — it is a Citi Bike loader that ended up in the framework. |
| `gbp/ml/training.py` | 242 | Counts departures out of raw trip CSVs (`departure_counts:119` reads `start_station_id`, `rideable_type`), builds the monthly parquet partitions. |
| `gbp/ml/data.py` | 158 | NOAA Central Park weather station `USW00094728` (`:77`), the NCEI download, the month grid. |
| `gbp/ml/features.py` | 171 | US public holidays (`holidays.country_holidays("US", ...)`, `:64`) and the three NYC weather columns. The *machinery* (weekly lags, rolling means, facility means) is generic — see the open question below. |
| `gbp/ml/models/graph.py` | 330 | `station_graph_edges:31` globs `*-citibike-tripdata*.csv` through the Citi Bike downloader, and `fit:224` calls it when no edge table was passed. |

### Pure infrastructure — no domain knowledge at all

| File | Lines | Note |
|---|---|---|
| `gbp/ml/models/base.py` | 54 | `DemandModel(abc.ABC)` with `fit/predict/save/load/params`, plus `fractional_demand`. Vocabulary is already generic: `period_id, facility_id, commodity_category, quantity`. |
| `gbp/ml/models/__init__.py` | 54 | `create_model` / `load_model`. A hardcoded if-chain, duplicated twice — see the traps. |
| `gbp/ml/models/seasonal_naive.py` | 39 | Reads one feature column, `facility_hour_of_week_mean`. |
| `gbp/ml/models/sarimax.py` | 93 | statsmodels. Assumes a daily frequency and a weekly season (7) by default. |
| `gbp/ml/models/boosting.py` | 105 | LightGBM. Only coupling is `FEATURE_COLUMNS` from `features.py`. |
| `gbp/ml/metrics.py` | 87 | MAE, Poisson deviance, plus two metrics over the simulator panel — and the panel (`gbp/model/flows.py:588`) is itself agnostic. |
| `gbp/ml/registry.py` | 229 | The MLflow store and the `champion` alias. Its only domain import is `normalize_month` (`:16`), used on two lines. |

### Mixed inside one file — the real work

| File | Domain part | Infrastructure part |
|---|---|---|
| `gbp/ml/pipeline.py` (436) | `published_missing_months:72` (asks the Citi Bike S3 bucket), `refresh_dvc:91`, `step_download:107` (the tripdata bucket, the `-citibike-tripdata` name fragment, the status dumps), `step_build_table:130` | `step_train:153`, `step_backtest:187`, `family_score:217`, `promote_decision:227`, `step_promote:271`, `run_pipeline:308`. The seam is exactly at `run_pipeline:329-334`: the two domain steps run first, everything after is domain-free. |
| `gbp/ml/backtest.py` (222) | `month_forecast_input:82` (fetches NOAA weather), and the `load_training_table` / `load_actual_month` calls at `:141-144` | `BacktestSplit:31`, `backtest_splits:38`, `data_version:56`, `comparison_table:97`, the MLflow logging block `:159-173`. `paths`, `store`, `weather_df`, `log` are already injectable. |
| `gbp/ml/evaluation.py` (364) | everything from `:174` down — `build_graph_data(trips_path)` at `:276`, and the two bike-named output columns `initial_inventory_bikes` / `station_capacity_docks` (`:103`) | everything above `:174` — `EvalNames:50`, `ModelForecast:87`, `run_row:96`, `build_comparison:108`, already written against injected `RunFn` / `LoadMetaFn` / `LoadTableFn` callables (`:44-46`) |
| `gbp/ml/monitoring.py` (381) | `DRIFT_COLUMNS:37` (the NYC weather + history feature names) and the month-shaped assumptions | the metrics store `:60-92`, `metric_history:172` and its degraded rule, the Evidently mechanics `:310-323` |
| `gbp/ml/forecast.py` (478) | `forecast_input:118` (calls `build_features`), `naive_month_prediction:161`, `build_model_forecast:294`, `build_champion_forecast:337`, the whole `main():388` CLI | the artifact format: `ForecastMeta:45`, `forecast_periods_from_meta:72`, `counts_from_demand:77`, `round_forecast_demand:97`, `predict_horizon:141`, `save_forecast:177`, `load_forecast:193`, `list_forecasts:37`, `forecast_dir:32` |

### The one thread from the framework core into ML

`gbp/model/dataloader_graph.py:319`, inside `apply_saved_forecast`:

```python
from gbp.ml import forecast          # a deferred import, to dodge a cycle
```

This is the single place where the agnostic core reaches into ML. It only needs
`load_forecast` and `forecast_periods_from_meta` — both part of the artifact
format, both agnostic. After the split this import becomes honest and the
"circular import" comment above it can go.

## Target layout

```
gbp/
  ml/                    # the seam only, ~250 lines
    __init__.py
    model.py             # DemandModel, fractional_demand, DEMAND_KEYS, the registry
    artifact.py          # ForecastMeta, save/load/list_forecast, round_forecast_demand
    metrics.py           # forecast error + the two panel metrics (moved as is)
  model/
    dataloader_graph.py  # apply_forecast_demand / apply_saved_forecast (already here)

domains/citybike/
  loaders/               # done in the previous step
  ml/
    __init__.py
    data.py              # NOAA weather, month grid
    features.py          # calendar + weather + history features
    station_status.py    # the GBFS archive and stockout_share
    training.py          # trip CSVs -> monthly training partitions
    forecast.py          # the builders + the CLI
    models/              # seasonal_naive, sarimax, boosting, graph
    ops/                 # registry, backtest, pipeline, monitoring, evaluation
```

## Migration order

Do it in this order; each step ends with a green `ruff check`, `mypy gbp/ domains/`,
`pytest`. Never move two layers in one commit.

**Step 0 — packaging.** Create `domains/citybike/ml/` with `__init__.py`. Nothing
else. `pyproject.toml` already ships the whole `domains` package.

**Step 1 — extract the artifact format.** Split `gbp/ml/forecast.py` in two:
the format half (`ForecastMeta`, `forecast_dir`, `list_forecasts`,
`forecast_periods_from_meta`, `counts_from_demand`, `round_forecast_demand`,
`save_forecast`, `load_forecast`, `predict_horizon`) becomes
`gbp/ml/artifact.py`; the builders and the CLI move to
`domains/citybike/ml/forecast.py`. Then fix `gbp/model/dataloader_graph.py:319`
to import from `gbp.ml.artifact`.

Watch out: `predict_horizon:141` has a default that reaches for
`gbp.ml.training.load_history_counts`. Drop that default — make `history_df`
required, and let the domain builder supply it. This is the deletion test:
the default only moves the domain into the framework.

Also: `forecasts_root()` is `ml_dir() / "forecasts"`, and `ml_dir()` lives in the
domain-bound `data.py`. Move the `data/ml` path resolution into
`gbp/ml/artifact.py` (it is just `DATA_DIR`-aware path math) or pass the root in
from the caller.

**Step 2 — move the model families.** `gbp/ml/models/` → `domains/citybike/ml/models/`,
except `base.py`, which becomes `gbp/ml/model.py`. Turn the if-chain in
`models/__init__.py` into a `name -> class` dict the domain fills, so adding a
family never edits a framework file (see the traps).

**Step 3 — move the data preparation.** `data.py`, `features.py`,
`station_status.py`, `training.py` → `domains/citybike/ml/`. This is the biggest
diff but the least thinking: they have almost no incoming edges from the
framework.

**Step 4 — move the operations layer.** `registry.py`, `backtest.py`,
`pipeline.py`, `monitoring.py`, `evaluation.py` → `domains/citybike/ml/ops/`.
See the recommendation below on why they move whole rather than being split.

**Step 5 — the tails.** Command names in `CLAUDE.md:13-18`, the `.github/workflows/ci.yml`
paths, `docs/site/content/docs/architecture/ml-toolkit.md`, the how-to pages,
`app/views/model_monitoring.py:8`, `app/backend.py:63`. Then check
`grep -rn "domains\." gbp/` — only `gbp/consumers/run.py` should be left, and
that is the next task after this one.

**Tests to move with their subjects:** `test_ml_features.py` (311),
`test_ml_models.py` (419), `test_ml_training.py` (206), `test_ml_registry.py` (280),
`test_ml_monitoring.py` (281), `test_ml_evaluation.py` (228), `test_ml_data.py` (81),
`test_ml_smoke.py` (131). `test_forecast.py` (369) splits: the
`apply_forecast_demand` / `map_od_matrix_by_hour_of_week` half stays with the
framework, the builder half goes to the domain. `test_ml_metrics.py` (56) stays.

## Decisions to make, with recommendations

**1. Delete `gbp/ml` or shrink it?** — Shrink it. Deleting it would push the
`DemandModel` interface into `gbp/model/`, where it does not belong (that package
is the flow-journal vocabulary), or force every domain to invent its own. Three
small modules — the interface, the artifact format, the metrics — are the whole
framework surface, and each earns its place by being called from the framework
side.

**2. Where does the operations layer go — `gbp/ml/ops/` or the domain?** — The
domain, for now. This is the least obvious call in the plan, so here is the
reasoning: the "generic" ML-ops code is generic *by accident*. Every one of those
files is month-shaped — `normalize_month`, `month_bounds`, `<YYYYMM>.parquet`
partitions, `train_months` tags, "score the month's actuals". A calendar month is
not a framework concept; it is how Citi Bike publishes its files. A framework
interface designed from one example will encode that accident. Move the whole
operations layer into `domains/citybike/ml/ops/`, and extract it back into `gbp`
when a second domain exists to check the shape against. The cost of extracting
later is a rename; the cost of a wrong interface now is every future domain
working around it.

**3. Does `features.py` split?** — No, move it whole. The lag/rolling/facility-mean
machinery is tempting to keep as a generic feature builder, but it is welded to
`FEATURE_COLUMNS`, and `FEATURE_COLUMNS` names US holidays and three NYC weather
readings. Splitting it means inventing a feature-spec abstraction from one
example — the same trap as decision 2. Move it, and revisit when a second domain
needs features.

**4. Two model registries or one?** — One. `create_model(name)` in
`gbp/ml/model.py` should hold a dict the domain populates at import time
(`register_model(SeasonalNaiveModel)`), not the current if-chain. That way
`domains/citybike/ml/models/__init__.py` registers its four families, and adding
a fifth never touches `gbp`.

**5. `metrics.py` — framework or domain?** — Framework. It is fully agnostic
today, and `panel_departed_mae` / `lost_demand_busy_share` read the simulator
panel, which is a framework table (`gbp/model/flows.py:588`). Fix the duplicated
`DEMAND_KEYS` while moving it (`base.py:13` and `metrics.py:12` define the same
list).

## Traps

- **The import cycle.** `gbp/routing.py:12` does `from gbp.model import haversine_km`.
  That is why `gbp/model/__init__.py` must not re-export anything from
  `dataloader_graph`. Keep the same discipline in `gbp/ml/`: import submodules
  directly (`from gbp.ml.artifact import load_forecast`), not through the package.
- **`normalize_month` / `month_bounds` live in the Citi Bike downloader** and are
  imported by four otherwise-clean files (`backtest.py:16`, `evaluation.py:13`,
  `monitoring.py:14`, `registry.py:16`). If the operations layer moves to the
  domain (decision 2), this stops being a problem by itself. Do not create a
  `gbp/ml/calendar.py` just to hold them — that would be the framework absorbing
  a publishing cadence.
- **`torch` has module-level side effects** in `models/graph.py`: `torch.set_num_threads(1)`
  at `:25` and a `lightgbm` import at `:16` that exists purely to order OpenMP
  loading (with `# noqa: F401`). Keep both, keep them in that order, and keep the
  import lazy — `gbp/ml/models/__init__.py` currently imports each family only
  inside its branch, which is why `import gbp.ml` stays cheap.
- **`GraphSageModel.predict` returns a re-sorted grid**, not the caller's
  `feature_table` (`graph.py:281`). Do not "fix" this during the move.
- **`DRIFT_COLUMNS` (`monitoring.py:37`) is the single line** that makes an
  otherwise generic drift report bike-specific.
- **The MLflow store path** is `data/ml/mlflow/` with a sqlite backend
  (`registry.py:63-68`). Moving the code must not move the data — `data/ml/` stays
  where it is, and `data/raw.dvc` / `data/ml/training.dvc` keep their paths.
  `refresh_dvc` (`pipeline.py:96`) hardcodes both.
- **`app/` imports ML directly**: `app/views/model_monitoring.py:8` and the
  deferred import in `app/backend.py:63`. The app is a user of both layers, so
  pointing it at `domains.citybike.ml` is fine — but it must keep the deferred
  import in `backend.py`, or the Streamlit start-up pays for MLflow and torch.

## Definition of done

- `grep -rn "domains\." gbp/` returns only `gbp/consumers/run.py` (the next task).
- `gbp/ml/` is three modules and under ~300 lines.
- `python -m domains.citybike.ml.pipeline` runs the five steps end to end
  (`tests/test_ml_smoke.py` covers this on fixtures — keep it green).
- A forecast run still works: `python app/runner.py --demand-source forecast
  --forecast-name <name>`, and `gbp/model/dataloader_graph.py` imports the
  artifact reader without a deferred import.
