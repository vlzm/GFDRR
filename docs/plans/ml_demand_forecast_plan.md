# ML demand forecasting — implementation plan

This document is the plan for the next project phase: demand forecasting.
Today the simulator replays one month of historical demand. After this phase
it can also run on demand predicted by a model, including future periods that
have no history yet. The plan covers the full loop around the model: getting
more data, building a training table, training and comparing models, picking
the best one, running the simulator on its forecast, retraining when new data
arrives, and watching model quality over time.

Everything in this plan runs locally. Moving the system to Azure Databricks
is a later phase and is only sketched at the end.

## The integration contract

The seam for the model already exists in the code. The simulator does not
read raw trips. It reads a demand table checked by `HISTORICAL_DEMAND_SCHEMA`
(`gbp/loaders/dataloader_graph.py`):

| column | meaning |
|---|---|
| `period_id` | which period the demand happens in |
| `facility_id` | which station |
| `commodity_category` | which bike type |
| `quantity` | how many departures |

Destinations come from a second table, the OD matrix
(`HISTORICAL_OD_MATRIX_SCHEMA`, same file): for each
`(source, target, period, commodity)` it holds a probability and a mean
duration.

So the model's job is exact: produce a table in the demand schema for future
periods. This table is called the forecast demand table below. The OD matrix
for future periods is not predicted; it is reused from history (see the open
questions at the end).

The rest of the run chain already handles a non-historical demand:
`size_state_for_demand` sizes the initial inventory and capacities for any
demand table, and `run_sized_scenario` runs it. `app/runner.py` only needs a
new switch for where the demand comes from.

## Design decisions made up front

These are fixed before writing code, so every phase builds on the same
choices.

- **Target definition.** The target is the number of departures per
  `(period, facility, commodity)`. Observed departures are censored demand:
  when a station had no bikes, departures are zero even though people wanted
  to ride. Censored means the true value was cut off by a limit, so the
  recorded value is a lower bound, not the truth. Phase 3 states how this is
  handled; at minimum the assumption "observed departures ≈ demand" is
  written down explicitly.
- **One global model, not one model per station.** A separate classical model
  per station (thousands of SARIMAX fits) is expensive to train, store, and
  monitor, and usually loses to one gradient-boosting model trained on all
  stations at once with the station as a feature. Classical models stay as
  baselines on aggregated series.
- **Batch forecasts, not an online service.** The consumer is the simulator,
  which reads tables from disk. A forecast is a saved folder (a table plus
  metadata), not an HTTP endpoint. An endpoint can be added to `app/api.py`
  later without changing anything else.
- **Two-level evaluation.** Level 1: forecast accuracy against held-out
  months (backtest). Level 2: run the simulator on the forecast and on the
  actual demand for the same month and compare run totals (lost_demand,
  lost_dock_full, cost). A model is good when it leads to the same decisions,
  not only when its error is small.
- **One feature module for training and prediction.** The same functions
  build features for the training table and for the forecast input. Two
  separate implementations drift apart silently (train/serve skew: the model
  sees one definition of a feature in training and another in production).

## Folder and module layout

New code lives in `gbp/ml/`, new data under `data/ml/`:

```
gbp/ml/
    data.py        # download monthly files, harmonize schemas
    features.py    # one feature builder for training and prediction
    training.py    # training-table builder
    models/        # one file per model family, one shared interface
    forecast.py    # build + save a forecast demand table
    monitoring.py  # forecast-vs-actual metrics, drift reports

data/ml/
    training/      # training tables, partitioned by month
    forecasts/     # one folder per saved forecast (table + meta.json)
    monitoring/    # metric history, drift reports
    mlflow/        # local MLflow store
```

`data/raw/` keeps its current meaning (downloaded files, never edited) and
simply gains more months.

## Phase 0 — groundwork

Goal: the project rules and the dictionary know about the new phase before
any code is written.

1. Update `CLAUDE.md`: the current phase is no longer "cleanup to canonical
   minimum". State the new phase and what the canonical scenario becomes
   (a run on forecast demand next to the base replay).
2. Add the new words to `Notations.md` before they appear in code:
   - `forecast demand table` — a table in `HISTORICAL_DEMAND_SCHEMA` shape
     whose `quantity` comes from a model, not from history.
   - `forecast_` prefix in §10, next to `historical_` and `simulated_`.
   - `forecast run` in §11 — a sized run whose demand table is a forecast.
   - `training table`, `backtest`, `champion` (defined in the phases below).
3. Decide where the canonical scenario grows: a new section in
   `notebooks/test_pipeline.ipynb` or a second notebook next to it.

Done when: `CLAUDE.md` and `Notations.md` describe the new phase and its
vocabulary, and nothing in them contradicts this plan.

## Phase 1 — minimal end-to-end version

Goal: the smallest complete path from a forecast to a saved run artifact,
before any real ML. Every later phase improves a working system instead of
building parts that meet only at the end. Integration risks (fractional
quantities, sizing, the OD matrix for future periods) surface here, in the
first week.

1. Write the simplest forecast: seasonal naive. The forecast for a station
   at a given hour is the average of the same hour of the same weekday over
   the past weeks. No ML libraries needed.
2. Save it as a forecast artifact: `data/ml/forecasts/<forecast_name>/` with
   `demand.parquet` (the forecast demand table) and `meta.json` (model name
   and version, source months, created-at date, forecast horizon).
3. Extend the loaders with a forecast path: build `ResolvedModelData` whose
   demand table is the forecast and whose period grid covers the forecast
   horizon. Map the OD matrix onto the new periods by hour of week.
4. Decide the rounding rule once: forecasts are fractional, the engine moves
   whole bikes. Write the rule down in the forecast builder.
5. Size the state with `size_state_for_demand`, run with
   `run_sized_scenario` — the existing path, unchanged.
6. Add the switch to `app/runner.py`: `--demand-source history|forecast` and
   `--forecast-name`. Record both in the run's `meta.json`, so a forecast
   run names the forecast it used.
7. Add the same switch to the "Run scenario" page (English labels, as all UI
   text).

Done when: `python app/runner.py --run-name forecast_demo --demand-source
forecast --forecast-name <name>` saves a valid run artifact (empty
`violations` list), and the web interface shows the run.

## Phase 2 — data foundation

Goal: many months of trips on disk, versioned, and a training table built
from them.

1. Downloader in `gbp/ml/data.py`: take a list of months, download the
   monthly zip files from the public Citi Bike S3 bucket into `data/raw/`,
   skip months already present.
2. Schema harmonization: the published schema changed in February 2021
   (`ride_id` appeared, station id types changed). One function maps both
   old and new files to the single trips schema that `load_trips_raw_df`
   already checks. Document the mapping in the module docstring.
3. Data versioning with DVC, local remote: track `data/raw/` and
   `data/ml/training/`. Every training run can name the exact data version
   it used.
4. Training-table builder in `gbp/ml/training.py`: for each month, count
   departures per `(period, facility, commodity)` and write one parquet
   partition to `data/ml/training/`. Include zero rows: a station-hour with
   no departures is a real observation, not a missing one.
5. Start with 12 months of history; more can be added later by rerunning the
   downloader and the builder.

Done when: one command turns a list of months into partitioned training
parquet, rerunning it changes nothing (the steps are idempotent — running a
step twice gives the same result), and `dvc status` is clean.

## Phase 3 — features

Goal: one feature module, used both when building the training table and
when building the forecast input.

1. `gbp/ml/features.py`: functions that take the departure counts and a date
   range and return the feature columns. Both `training.py` and
   `forecast.py` call these functions — never their own copies.
2. Calendar features: hour of day, day of week, month, US public holidays.
3. Weather features: daily temperature and precipitation for the Central
   Park NOAA station, joined by date. Daily resolution is enough to start;
   hourly can come later.
4. History features: demand at the same hour one week ago, rolling mean over
   the past N weeks, station-level and hour-of-week averages.
5. Censored demand: write the assumption down first (observed departures ≈
   demand, biased low exactly where the system fails). Then, as a separate
   research step, look for archived station-status snapshots (bike counts
   per station over time). If they exist for the chosen months, mark
   station-hours where the station was empty and exclude or down-weight them
   in training. If they do not exist, the written assumption stands and the
   limitation is stated in the model report.
6. Unit tests on a small fixture: each feature function gets a tiny input
   table and an expected output table.

Done when: the training table and the forecast input for the same
station-day produce identical feature values, and a test proves it.

## Phase 4 — models, backtest, experiment tracking

Goal: several model families behind one interface, compared honestly.

1. One interface in `gbp/ml/models/`: `fit(training_table)` and
   `predict(feature_table) -> forecast demand table`. Every model, from
   naive to GNN, implements it. The forecast builder and the pipeline only
   know the interface.
2. Models, in order of effort:
   - seasonal naive (from phase 1, moved behind the interface) — the
     baseline every other model must beat;
   - SARIMAX on the city-level total series — the classical baseline;
   - LightGBM trained on all stations at once — the expected main model;
   - GraphSage on the station graph (edges from OD flows) — the research
     model; the question it answers is "does spatial structure add accuracy
     over boosting", and a negative answer is a valid result.
3. Validation: rolling-origin backtest. Train on months 1..k, forecast month
   k+1, move the split forward, average the scores. At least 3 splits. No
   random splits: rows from the future must never appear in training.
4. Metrics: MAE and Poisson deviance per `(period, facility, commodity)`
   row, reported overall and split by station traffic (busy stations vs
   quiet ones), so a model cannot hide bad performance on busy stations
   behind thousands of quiet ones.
5. Experiment tracking: a local MLflow store under `data/ml/mlflow/`. Every
   backtest run logs parameters, data version (from DVC), metrics per split,
   and the model file.

Done when: one MLflow experiment holds all model families over the same
backtest splits, and a table in the experiment shows each model against the
seasonal naive baseline.

## Phase 5 — evaluation through the simulator

Goal: measure what forecast errors do to decisions, not only to error
metrics. This is the second level of the two-level evaluation.

1. Pick a held-out month with full history.
2. Run the base replay on the actual demand — the reference run.
3. For each candidate model, build the forecast for that month and run a
   forecast run on it.
4. Compare the run totals from `meta.json` (demand, departed, lost_demand,
   lost_dock_full, cost) and the facility period panel between the reference
   run and each forecast run.
5. Write the result as a short report: forecast error (level 1) next to the
   difference in run totals (level 2), per model.

Done when: the report exists for at least two models and states which model
the platform should use and why.

## Phase 6 — model registry and the retraining pipeline

Goal: retraining is one command, and promotion is a rule, not a manual
choice.

1. Register models in the MLflow model registry. The alias `champion` marks
   the model version the platform currently uses. The forecast builder
   resolves the model by this alias — never by a file path.
2. A pipeline of idempotent steps, runnable as one command and as single
   steps: `download → build-table → train → backtest → promote`.
3. The promote rule: the new model version becomes champion only if it beats
   the current champion on the same backtest splits. Otherwise it stays in
   the registry as a challenger, with the comparison logged. (Challenger —
   a trained version that lost to the champion and waits for the next
   comparison.)
4. Trigger: a new monthly file is published (Citi Bike publishes with about
   a month's delay) → run the pipeline. Locally this is a manual command or
   a cron entry; the trigger logic stays in the pipeline, not in cron, so
   the cloud phase can swap the scheduler without touching the steps.
5. Every pipeline run appends one row to a log table: when, which data
   version, which model version, promoted or not, and why.

Done when: after dropping one new month of raw data, one command retrains,
compares, and either promotes or keeps the champion — with the decision and
the reason recorded.

## Phase 7 — monitoring

Goal: the system notices when the model gets worse, and shows it.

1. Forecasts are already logged (phase 1: every forecast artifact has
   `meta.json` with model version and date). This log is the base of all
   monitoring.
2. When a new actual month arrives, join it against every forecast that
   covered those periods and append rows to
   `data/ml/monitoring/metrics.parquet`: model version, month, MAE, Poisson
   deviance, bias.
3. Drift report: compare the new month's feature distributions against the
   training data of the current champion (drift — the new data stops looking
   like the data the model was trained on). Use Evidently; save the report
   file into `data/ml/monitoring/`.
4. A "Model monitoring" page in the Streamlit app (English labels): the
   metric history per model version, the months where a metric crossed its
   threshold, and links to the drift reports. The page only reads saved
   tables, like every other page.
5. Thresholds start simple: alert when the rolling MAE of the champion is
   worse than the seasonal naive baseline for that month.

Done when: the monitoring page shows the metric history, and a month with a
degraded metric is visibly marked.

## Phase 8 — tests and CI

Goal: the checks that already exist locally run on every push.

1. GitHub Actions workflow: `ruff check`, `ruff format --check`, `mypy
   gbp/`, `pytest`.
2. A training smoke test in `tests/`: on a tiny fixture, train the seasonal
   naive and the LightGBM models, produce a forecast demand table, and check
   it against `HISTORICAL_DEMAND_SCHEMA`.
3. A pipeline smoke test: the phase-6 steps run end to end on the fixture.

Done when: the workflow is green on a pull request and fails when a schema
or a feature test breaks.

## Deliberately not built

- Traffic features: no reliable public history of New York traffic; calendar
  and weather explain more for bike demand.
- One SARIMAX per station: thousands of small models to fit, store, and
  monitor — the global LightGBM replaces them.
- A feature store, Kubernetes, Docker, a database: outside the project
  rules and not needed at this scale (about ten users, batch forecasts).
- An online prediction service: the consumer is the simulator; batch is the
  fit. The API can serve saved forecasts later if needed.
- A workflow orchestrator (Airflow, Prefect): locally, idempotent CLI steps
  plus cron cover the need; the step structure is what transfers to the
  cloud phase, not the orchestrator.

## The cloud phase (later, out of scope here)

The order of moving to Azure Databricks: first the pretrained champion only
(the platform runs in the cloud, training stays local); then the retraining
pipeline as scheduled jobs; then Unity Catalog for data and model
versioning, replacing DVC and the local MLflow store. Retraining triggers
based on data drift belong to this phase, not the local one. This phase gets
its own plan when the local system works.

## Open questions

- The OD matrix for future periods: the plan maps historical shares by hour
  of week. Check on real data that shares are stable enough for this; if
  not, fall back to a coarser mapping (weekday/weekend × hour).
- The rounding rule for fractional forecast quantities: plain rounding
  changes the total demand; decide in phase 1 and write the rule down.
- Archived station-status data for censored-demand marking (phase 3): find
  out whether snapshots exist for the chosen months before promising the
  masking step.
- How much history the models need: the plan starts with 12 months; revisit
  after the first backtest.
