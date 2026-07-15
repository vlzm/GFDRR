# Demand forecasting

This document explains how `gbp/ml/` turns Citi Bike trip history into a
forecast that the simulator can run.

The connection to the simulator is small. The model produces a forecast
demand table — `period_id, facility_id, commodity_category, quantity` — the
same shape as historical demand, so the simulator loads it the same way
([decision record](../decisions/forecast-replaces-only-demand.md)). The full
process is:

```text
trip, weather, and station-status files
  -> monthly training table partitions
  -> fitted model
  -> fractional demand
  -> forecast demand table
  -> forecast artifact
  -> simulator run
```

The canonical terms are
[Notations.md §17](../../Notations.md#17-demand-forecasting-the-model-around-the-simulator).
Each module's exact behavior is in its docstring (start from the package
docstring, `gbp/ml/__init__.py`) — this page gives the map and the design.
How to run a forecast is [how-to/run-on-a-forecast.md](../how-to/run-on-a-forecast.md).

## Code And Data Map

| Part | Files | Purpose |
|---|---|---|
| Prepare data | `gbp/loaders/download.py`, `data.py`, `station_status.py`, `training.py`, `features.py` | Download source files and build the training table. The trip downloader lives in `gbp/loaders/` because the base replay uses the same CSVs. |
| Fit and use models | `models/`, `forecast.py` | Fit a model and save a forecast artifact. |
| Compare models | `metrics.py`, `backtest.py`, `app/evaluate.py` | Measure forecast error and simulator results. |
| Operate the model | `registry.py`, `pipeline.py`, `monitoring.py` | Choose the champion, retrain it, and check later results. |

| Path | Contents |
|---|---|
| `data/raw/` | Downloaded trip CSVs, weather CSVs, and station-status parquet files. |
| `data/ml/training/<YYYYMM>.parquet` | One monthly training table partition. |
| `data/ml/forecasts/<forecast_name>/` | One forecast artifact: `demand.parquet` and `meta.json`. |
| `data/ml/mlflow/` | MLflow experiment runs and the model registry. |
| `data/ml/evaluation/<month>/` | Results from comparing simulator runs. |
| `data/ml/monitoring/` | Metrics for completed months and drift reports. |

`data/raw/` and `data/ml/training/` are versioned with DVC: git stores the
DVC files that identify the exact content, and the git commit of those files
becomes the `data_version` of a training run.

## The Main Idea

The process passes four tables from step to step. The training table holds
observed departure counts and feature columns, one row per hour, station,
and bike type (zero rows kept). The forecast input holds the same feature
columns for the forecast horizon, with no observed `quantity`. Fractional
demand is the model output. The forecast demand table is fractional demand
rounded to whole bikes — the table the simulator reads.

Both the training table and the forecast input get their feature columns
from one module, `features.py` — calendar, weather, and history columns,
where the history window is the eight weeks before the rows being built and
never reaches the target rows. A missing value stays `NaN`, never zero.

The target is censored: observed departures are too low when a station had
no bikes to give. `stockout_share` — the part of an hour a station stood
empty, computed from the CityBikes status archive — marks that. It is not a
feature (future stockouts are unknown); LightGBM and GraphSage use it as a
training weight, `1 - stockout_share`.

Four model families implement one contract, `DemandModel`
(`fit / predict / save / load / params`), built through one factory,
`create_model`: `seasonal_naive` (the hour-of-week mean, the baseline),
`sarimax` (a daily city-wide series, divided back over stations),
`lightgbm` (one gradient-boosting model for all stations, Poisson
objective), and `graphsage` (stations as graph nodes, trips as weighted
edges). `predict` returns fractional demand on purpose; rounding to whole
bikes happens once, in the forecast builder (`round_forecast_demand`, which
preserves each `(period, commodity)` group total).

A saved forecast is an artifact — `demand.parquet` plus `meta.json`, written
last — under `data/ml/forecasts/<forecast_name>/`. A forecast run loads it
by name and runs the same chain as a replay
([data-model.md](data-model.md)).

## Operating The Model

| Command | What it does |
|---|---|
| `python -m gbp.ml.training --months ...` | Download missing months, build the monthly partitions, oldest first. |
| `python -m gbp.ml.backtest` | The rolling-origin backtest: train on earlier months, forecast the next, move forward; scores (MAE, Poisson deviance) and models logged to MLflow, with ratios against the seasonal naive baseline. |
| `python app/evaluate.py --month <YYYYMM>` | The two-level evaluation: level 1 compares demand tables, level 2 runs the simulator on the actual and on each forecast with the same replay state, so only demand differs. |
| `python -m gbp.ml.pipeline` | The retraining pipeline: `download -> build-table -> train -> backtest -> promote`; every decision appends one row to `data/ml/pipeline_log.csv`. |
| `python -m gbp.ml.monitoring --month <YYYYMM>` | After a month's actuals arrive: score the saved forecasts against them, mark degraded months, build the drift report. |

The registry (`MlflowStore`, under `data/ml/mlflow/`) keeps versions of one
registered model, `demand-model`; each version records `model_family`,
`train_months`, and `data_version`. The alias `champion` points at the
version forecasts use; `promote` moves the alias when a candidate's mean
backtest MAE is no worse than the champion's. Monitoring marks a month
degraded when the version's rolling MAE over its last three scored months is
worse than that month's seasonal naive MAE, and the drift report
(Evidently, Wasserstein distance) asks whether the month's feature values
still resemble the champion's training months.

## Why It Is Built This Way

### Fractional Until One Rounding Step

Rounding each model's output inside the model would remove demand
differently per family (many rows of 0.4 all become zero) and make families
incomparable. One rounding rule in the forecast builder keeps the group
totals and gives every family the same treatment.

### `stockout_share` Is A Weight, Not A Feature

A feature must be known for future rows, and future stockouts are unknown.
As a weight it fixes the censoring where it lives — in training: an hour the
station stood half empty counts half as much.

### The Backtest Keeps Time Order

Random splits would let a model peek at the future through its history
features. Rolling-origin splits move only forward, so the backtest measures
what a real monthly retrain would have seen.

### The Champion Is An Alias In A Registry

A hard-coded model path would make "which model do forecasts use" a code
question. The `champion` alias makes it an operations question: `promote`
moves the alias, `build_champion_forecast` resolves it, and losing
candidates stay registered as challengers next to their metrics.

### Two Evaluation Levels

Forecast error (level 1) does not say what the error changes inside the
simulator. Level 2 reruns the simulator with only the demand table swapped
([decision record](../decisions/sizing-run.md)), so the difference in run
totals is the forecast's doing and nothing else's.
